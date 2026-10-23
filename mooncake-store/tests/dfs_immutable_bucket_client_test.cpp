#include <gtest/gtest.h>
#include <glog/logging.h>
#include <ylt/struct_json/json_reader.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client_service.h"
#include "crc_checksum.h"
#include "environ.h"
#include "master_metric_manager.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "test_server_helpers.h"
#include "utils.h"

namespace mooncake::test {

namespace {

// CPU-backed device bytes let these tests count/fail only foreground D2H,
// independently of any memory-replica transfer on a worker thread.
class ChecksumStagingDevice : public device::AcceleratorDevice {
   public:
    void Reset(const void* data = nullptr, size_t size = 0) {
        start_ = reinterpret_cast<uintptr_t>(data);
        end_ = start_ + size;
        thread_ = std::this_thread::get_id();
        copies = 0;
        fail_copy = -1;
        throw_copy = false;
    }
    device::AcceleratorVendor Vendor() const override {
        return device::AcceleratorVendor::kSunrise;
    }
    bool Available(bool = false) const override { return true; }
    device::PointerInfo QueryPointer(const void* ptr) const override {
        const auto address = reinterpret_cast<uintptr_t>(ptr);
        return {address >= start_ && address < end_
                    ? device::MemoryKind::kDevice
                    : device::MemoryKind::kHost,
                0};
    }
    int32_t CurrentDeviceId() const override { return 0; }
    void SetContext(int32_t) const override {}
    bool Copy(void* dst, const void* src, size_t size,
              device::CopyDirection direction) const override {
        if (std::this_thread::get_id() == thread_ &&
            direction == device::CopyDirection::kDeviceToHost) {
            const int call = ++copies;
            if (fail_copy == 0 || call == fail_copy) {
                if (throw_copy)
                    throw std::runtime_error("injected D2H failure");
                return false;
            }
        }
        std::memcpy(dst, src, size);
        return true;
    }
    PinnedHostBuffer AllocatePinnedHost(size_t) const override { return {}; }

    mutable int copies = 0;
    int fail_copy = -1;
    bool throw_copy = false;

   private:
    uintptr_t start_ = 0, end_ = 0;
    std::thread::id thread_;
};

ChecksumStagingDevice staging_device;
device::AcceleratorDeviceRegistrar staging_device_registration(staging_device);

// Fails the Nth WriteAt so the asynchronous revoke path can be exercised.
class FailingBucketFsAdapter : public PosixFsAdapter {
   public:
    std::function<void()> after_read;
    int WriteCalls() const { return write_calls_.load(); }
    int FailedWrites() const { return failed_writes_.load(); }
    void FailWriteCall(int call) { fail_write_call_.store(call); }
    void FailAllWrites(bool fail) { fail_all_writes_.store(fail); }
    void FailAllOpens(bool fail) { fail_all_opens_.store(fail); }
    tl::expected<int, ErrorCode> OpenFileReadWriteExisting(
        const std::string& path) override {
        if (fail_all_opens_.load()) {
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        return PosixFsAdapter::OpenFileReadWriteExisting(path);
    }
    void BlockWrites() {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_ = true;
    }
    void ReleaseWrites() {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_ = false;
        cv_.notify_all();
    }
    void BlockReads() {
        std::lock_guard<std::mutex> lock(read_mutex_);
        reads_blocked_ = true;
        read_done_ = false;
    }
    void ReleaseReads() {
        std::lock_guard<std::mutex> lock(read_mutex_);
        reads_blocked_ = false;
        read_cv_.notify_all();
    }
    bool WaitUntilReadDone(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(read_mutex_);
        return read_cv_.wait_for(lock, timeout,
                                 [this] { return read_done_; });
    }

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        const int call = ++write_calls_;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !blocked_; });
        }
        if (fail_all_writes_.load() || call == fail_write_call_.load()) {
            ++failed_writes_;
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

    tl::expected<size_t, ErrorCode> DirectReadAt(int fd, iovec* iov,
                                                 int iovcnt,
                                                 int64_t offset) override {
        {
            std::unique_lock<std::mutex> lock(read_mutex_);
            read_cv_.wait(lock, [this] { return !reads_blocked_; });
        }
        auto result = PosixFsAdapter::DirectReadAt(fd, iov, iovcnt, offset);
        if (after_read) after_read();
        {
            std::lock_guard<std::mutex> lock(read_mutex_);
            read_done_ = true;
        }
        read_cv_.notify_all();
        return result;
    }

   private:
    std::atomic<int> write_calls_{0};
    std::atomic<int> failed_writes_{0};
    std::atomic<int> fail_write_call_{-1};
    std::atomic<bool> fail_all_writes_{false};
    std::atomic<bool> fail_all_opens_{false};
    std::mutex mutex_;
    std::condition_variable cv_;
    bool blocked_ = false;
    std::mutex read_mutex_;
    std::condition_variable read_cv_;
    bool reads_blocked_ = false;
    bool read_done_ = false;
};

std::string NextServiceEndpoint() {
    static std::atomic<int> next_port{41200};
    return "192.0.2.42:" + std::to_string(next_port.fetch_add(1));
}

std::optional<double> FindServiceMetricValue(
    const std::string& metric_name, const std::string& service_endpoint,
    const std::string& extra_label = "") {
    const std::string serialized =
        MasterMetricManager::instance().serialize_metrics();
    const std::string metric_prefix = metric_name + "{";
    const std::string endpoint_label =
        "service_endpoint=\"" + service_endpoint + "\"";
    size_t line_start = 0;
    while (line_start < serialized.size()) {
        const size_t line_end = serialized.find('\n', line_start);
        const auto line = serialized.substr(
            line_start, line_end == std::string::npos
                            ? std::string::npos
                            : line_end - line_start);
        if (line.compare(0, metric_prefix.size(), metric_prefix) == 0 &&
            line.find(endpoint_label) != std::string::npos &&
            (extra_label.empty() ||
             line.find(extra_label) != std::string::npos)) {
            const size_t value_start = line.find_last_of(' ');
            if (value_start == std::string::npos) return std::nullopt;
            try {
                return std::stod(line.substr(value_start + 1));
            } catch (const std::exception&) {
                return std::nullopt;
            }
        }
        if (line_end == std::string::npos) break;
        line_start = line_end + 1;
    }
    return std::nullopt;
}

std::optional<double> WaitForServiceMetricValue(
    const std::string& metric_name, const std::string& service_endpoint,
    const std::string& extra_label, double minimum_value,
    std::chrono::milliseconds timeout = std::chrono::seconds(8)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto value = FindServiceMetricValue(metric_name, service_endpoint,
                                           extra_label);
        if (value.has_value() && *value >= minimum_value) return value;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return FindServiceMetricValue(metric_name, service_endpoint, extra_label);
}

}  // namespace

class DfsBucketClientTest : public ::testing::Test {
   protected:
    virtual void ConfigureClientEnvironment() {}

    void SetUp() override {
        root_ = (std::filesystem::temp_directory_path() /
                 ("dfs_bucket_client_" + std::to_string(::getpid()) + "_" +
                  std::to_string(++next_root_)))
                    .string();
        std::filesystem::create_directories(root_);

        // Select BUCKET mode for the master's allocator.
        SetEnv("MOONCAKE_ENABLE_DFS", "1");
        SetEnv("MOONCAKE_DFS_FS_ADAPTER", "posix");
        SetEnv("MOONCAKE_DFS_ROOT_DIR", root_);
        SetEnv("MOONCAKE_DFS_ALLOCATOR_TYPE", "bucket");
        SetEnv("MOONCAKE_DFS_BUCKET_CAPACITY", "1048576");
        SetEnv("MOONCAKE_DFS_MAX_BUCKET_COUNT", "16");
        SetEnv("MOONCAKE_DFS_ALIGNMENT", "4096");
        SetEnv("MOONCAKE_DFS_EVICTION_ENABLED", "0");
        SetEnv("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
        SetEnv("MOONCAKE_DFS_SINGLE_TENANT", "true");
        // Existing tests below exercise the legacy enqueue-only contract.
        // Timeout behavior has a dedicated test with an explicitly bounded
        // client.
        SetEnv("MOONCAKE_DFS_BATCH_PUT_TIMEOUT_MS", "0");
        // Keep the shard fields valid; they are unused in bucket mode.
        SetEnv("MOONCAKE_DFS_SHARD_COUNT", "1");
        SetEnv("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
        ConfigureClientEnvironment();

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        writer_ = CreateClient("127.0.0.1:18201");
        provider_ = CreateClient("127.0.0.1:18202");
        ASSERT_NE(writer_, nullptr);
        ASSERT_NE(provider_, nullptr);

        segment_size_ = 16 * 1024 * 1024;
        segment_ = allocate_buffer_allocator_memory(segment_size_);
        ASSERT_NE(segment_, nullptr);
        ASSERT_TRUE(provider_->MountSegment(segment_, segment_size_, "tcp")
                        .has_value());

        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = root_;

        distributed_config_.fsdir = root_;
        distributed_config_.fs_adapter_type = "posix";
        distributed_config_.allocator_type = DfsAllocatorType::BUCKET;
        distributed_config_.bucket_capacity = 1024 * 1024;
        distributed_config_.max_bucket_count = 16;
        distributed_config_.alignment = 4096;
        distributed_config_.shard_count = 1;
        distributed_config_.shard_capacity = 1024 * 1024;

        auto adapter = std::make_unique<FailingBucketFsAdapter>();
        adapter_ = adapter.get();
        backend_ = std::make_shared<DistributedStorageBackend>(
            file_config, distributed_config_, std::move(adapter),
            writer_->GetDfsRootIdentity());
        ASSERT_TRUE(backend_->Init().has_value());
        writer_->SetDfsStorageBackend(backend_);
    }

    void TearDown() override {
        if (adapter_) {
            adapter_->ReleaseWrites();
            adapter_->ReleaseReads();
        }
        if (provider_ && segment_) {
            (void)provider_->UnmountSegment(segment_, segment_size_);
        }
        writer_.reset();
        staging_device.Reset();
        provider_.reset();
        backend_.reset();
        master_.Stop();
        if (segment_) {
            std::free(segment_);
            segment_ = nullptr;
        }
        RestoreEnv();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    std::shared_ptr<Client> CreateClient(const std::string& hostname) {
        auto client = Client::Create(hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_.master_address());
        return client ? *client : nullptr;
    }

    void PauseHolderPolling() {
        provider_->task_poll_running_.store(false);
        if (provider_->task_poll_thread_.joinable()) {
            provider_->task_poll_thread_.join();
        }
    }

    void ExecuteBackfill(const TaskAssignment& task) {
        provider_->ExecuteTask(Client::ClientTask{task});
    }

    void SubmitHolderTask(const TaskAssignment& task) {
        provider_->SubmitTask(task);
    }

    bool StartHolderUnmountTimer(
        void* buffer, size_t size,
        std::function<void(const UUID&)> cleanup) {
        if (provider_->transfer_engine_->registerLocalMemory(
                buffer, size, kWildcardLocation, true, true) != 0) {
            return false;
        }
        Segment segment;
        segment.id = generate_uuid();
        segment.base = reinterpret_cast<uintptr_t>(buffer);
        segment.size = size;
        {
            std::lock_guard<std::mutex> lock(provider_->mounted_segments_mutex_);
            // Model the state after the master has removed an expired segment.
            // Mounting another segment under the holder's existing name would
            // interfere with the source replicas of the blocked backfills.
            provider_->gracefully_unmounting_segments_.emplace(segment.id,
                                                              segment);
            provider_->graceful_unmount_cleanup_callbacks_.emplace(
                segment.id, std::move(cleanup));
        }
        provider_->StartGracefulUnmountTimer(segment.id, 1);
        return true;
    }

    std::shared_ptr<DistributedStorageBackend> HolderBackfillBackend() {
        auto backend = provider_->GetDfsBackfillBackend();
        EXPECT_TRUE(backend);
        return backend ? *backend : nullptr;
    }

    void ExpireHolderBackoff() {
        std::lock_guard<std::mutex> lock(provider_->dfs_backfill_backend_mutex_);
        provider_->dfs_backfill_retry_after_ = {};
    }

    std::vector<TaskAssignment> FetchHolderTasks(size_t count) {
        std::vector<TaskAssignment> tasks;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(8);
        while (tasks.size() < count &&
               std::chrono::steady_clock::now() < deadline) {
            auto fetched = provider_->FetchTasks(count - tasks.size());
            EXPECT_TRUE(fetched.has_value());
            if (!fetched) return tasks;
            tasks.insert(tasks.end(), fetched->begin(), fetched->end());
            if (tasks.size() < count) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
        return tasks;
    }

    void CheckHolderBackoffBounds() {
        for (int delay : {5, 10, 20, 30, 30}) {
            const auto task = generate_uuid();
            ASSERT_TRUE(provider_->BeginDfsBackfillAttempt(task));
            provider_->FinishDfsBackfillAttempt(task, ErrorCode::TRANSFER_FAIL, true);
            std::chrono::steady_clock::time_point deadline;
            {
                std::lock_guard<std::mutex> lock(provider_->dfs_backfill_backend_mutex_);
                deadline = provider_->dfs_backfill_retry_after_;
            }
            const auto remaining = deadline - std::chrono::steady_clock::now();
            EXPECT_GT(remaining, std::chrono::seconds(delay - 1));
            EXPECT_LE(remaining, std::chrono::seconds(delay));
            // Queued keys cannot extend the deadline or acquire another write.
            for (int i = 0; i < 64; ++i) {
                EXPECT_FALSE(provider_->BeginDfsBackfillAttempt(generate_uuid()));
            }
            ExpireHolderBackoff();
        }
        const auto probe = generate_uuid();
        ASSERT_TRUE(provider_->BeginDfsBackfillAttempt(probe));
        EXPECT_FALSE(provider_->BeginDfsBackfillAttempt(generate_uuid()));
        provider_->FinishDfsBackfillAttempt(probe, ErrorCode::OBJECT_NOT_FOUND, false);
        // A stale source is not a storage failure and must release the probe.
        const auto next = generate_uuid();
        ASSERT_TRUE(provider_->BeginDfsBackfillAttempt(next));
        provider_->FinishDfsBackfillAttempt(next, ErrorCode::OK, false);
        for (int i = 0; i < 64; ++i) {
            EXPECT_TRUE(provider_->BeginDfsBackfillAttempt(generate_uuid()));
        }
    }

    ReplicateConfig DfsConfig() const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        return config;
    }

    tl::expected<QueryResult, ErrorCode> PrepareDfsReadRetry(
        const QueryResult& original, const QueryResult& refreshed) {
        return writer_->PrepareDfsReadRetry(original, refreshed);
    }

    tl::expected<QueryResult, ErrorCode> QueryDfsOnly(const std::string& key) {
        auto query = writer_->Query(key);
        if (!query) return tl::make_unexpected(query.error());
        std::vector<Replica::Descriptor> replicas;
        for (const auto& replica : query->replicas) {
            if (replica.is_dfs_replica()) replicas.push_back(replica);
        }
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        return QueryResult(std::move(replicas), query->lease_timeout,
                           query->object_checksum);
    }

    // The DFS write is asynchronous in bucket mode, so wait for the replica to
    // become visible instead of assuming it already is.
    bool WaitForDfsReplica(const std::string& key,
                           std::chrono::milliseconds timeout =
                               std::chrono::seconds(5)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            auto query = QueryDfsOnly(key);
            if (query.has_value()) {
                for (const auto& replica : query->replicas) {
                    if (replica.status == ReplicaStatus::COMPLETE) return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    bool WaitForKeyGone(const std::string& key,
                        std::chrono::milliseconds timeout =
                            std::chrono::seconds(5)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            auto query = writer_->Query(key);
            if (!query.has_value() &&
                query.error() == ErrorCode::OBJECT_NOT_FOUND) {
                return true;
            }
            // The object may survive with only its MEMORY replica; that also
            // means the DFS replica was revoked.
            if (query.has_value()) {
                bool has_dfs = false;
                for (const auto& replica : query->replicas) {
                    if (replica.is_dfs_replica()) has_dfs = true;
                }
                if (!has_dfs) return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    void ExpectDfsValue(const std::string& key, const std::string& expected) {
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        std::vector<char> output(expected.size());
        const auto& descriptor = query->replicas[0].get_dfs_descriptor();
        auto results = backend_->BatchRead(
            {{key, descriptor, {{output.data(), output.size()}}}});
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value());
        EXPECT_EQ(std::memcmp(output.data(), expected.data(), expected.size()),
                  0);
    }

    static std::vector<std::vector<Slice>> MakeSlices(
        std::vector<std::string>& values) {
        std::vector<std::vector<Slice>> slices;
        slices.reserve(values.size());
        for (auto& value : values) {
            slices.push_back({Slice{value.data(), value.size()}});
        }
        return slices;
    }

    void SetEnv(const std::string& key, const std::string& value) {
        const char* old_value = ::getenv(key.c_str());
        saved_env_.push_back({key, old_value
                                       ? std::optional<std::string>(old_value)
                                       : std::nullopt});
        ::setenv(key.c_str(), value.c_str(), 1);
    }

    void RestoreEnv() {
        for (auto it = saved_env_.rbegin(); it != saved_env_.rend(); ++it) {
            if (it->second) {
                ::setenv(it->first.c_str(), it->second->c_str(), 1);
            } else {
                ::unsetenv(it->first.c_str());
            }
        }
        saved_env_.clear();
    }

    inline static std::atomic<int> next_root_{0};
    testing::InProcMaster master_;
    std::shared_ptr<Client> writer_;
    std::shared_ptr<Client> provider_;
    std::shared_ptr<DistributedStorageBackend> backend_;
    DistributedStorageConfig distributed_config_;
    FailingBucketFsAdapter* adapter_ = nullptr;
    void* segment_ = nullptr;
    size_t segment_size_ = 0;
    std::string root_;
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_env_;
};

TEST_F(DfsBucketClientTest, PutThenGetRoundTrip) {
    const std::string key = "bucket_put_get";
    std::string value(4096, 'A');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()).has_value());
    ASSERT_TRUE(WaitForDfsReplica(key));
    ExpectDfsValue(key, value);

    // Read it back through the client's own Get path.
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    std::string out(value.size(), '\0');
    std::vector<Slice> read_slices{{out.data(), out.size()}};
    ASSERT_TRUE(writer_->Get(key, *query, read_slices).has_value());
    EXPECT_EQ(out, value);
}

class DfsBucketEpochClientTest : public DfsBucketClientTest {
   protected:
    bool RestartMasterAndReconnect() {
        const int port = master_.rpc_port();
        master_.Stop();
        if (!master_.Start(
                InProcMasterConfigBuilder().set_rpc_port(port).build()))
            return false;
        for (const auto& client : {writer_, provider_}) {
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            bool connected = false;
            do {
                auto probe = client->Query("never_written_after_restart");
                if (!probe && probe.error() == ErrorCode::OBJECT_NOT_FOUND) {
                    connected = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            } while (std::chrono::steady_clock::now() < deadline);
            if (!connected) return false;
        }
        return true;
    }

    void SetUp() override {
        // Make bucket-id reuse deterministic instead of depending on which
        // preallocation worker completes first.
        SetEnv("MOONCAKE_DFS_READY_BUCKET_COUNT", "0");
        DfsBucketClientTest::SetUp();
    }
};

TEST_F(DfsBucketEpochClientTest,
       MasterRestartReusesBucketWithoutWaitingForHeartbeat) {
    const std::string key = "restart_reused_bucket";
    std::string old_value(4096, 'O');
    std::string new_value(4096, 'N');
    std::vector<Slice> old_slices{{old_value.data(), old_value.size()}};
    ASSERT_TRUE(writer_->Put(key, old_slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto old_query = QueryDfsOnly(key);
    ASSERT_TRUE(old_query);
    const auto old_desc = old_query->replicas[0].get_dfs_descriptor();
    ASSERT_NE(old_desc.MasterEpoch(), UUID{});
    ExpectDfsValue(key, old_value);  // Populate normal and direct FD caches.

    ASSERT_TRUE(provider_->UnmountSegment(segment_, segment_size_));
    const int rpc_port = master_.rpc_port();
    master_.Stop();
    for (const auto& file : std::filesystem::directory_iterator(root_)) {
        const auto name = file.path().filename().string();
        if (name.rfind("bucket_", 0) == 0 &&
            (file.path().extension() == ".data" ||
             file.path().extension() == ".meta")) {
            ASSERT_TRUE(std::filesystem::remove(file.path()));
        }
    }
    ASSERT_TRUE(master_.Start(
        InProcMasterConfigBuilder().set_rpc_port(rpc_port).build()));
    // Retain both clients and backend. Only replace the master's connections;
    // the first pooled request can fail with EOF after the old server exits.
    for (const auto& client : {writer_, provider_}) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        bool connected = false;
        do {
            auto probe = client->Query("never_written_after_restart");
            if (!probe && probe.error() == ErrorCode::OBJECT_NOT_FOUND) {
                connected = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        } while (std::chrono::steady_clock::now() < deadline);
        ASSERT_TRUE(connected);
    }
    ASSERT_TRUE(provider_->MountSegment(segment_, segment_size_, "tcp"));
    std::vector<std::vector<Slice>> new_slices{
        {{new_value.data(), new_value.size()}}};
    auto puts = writer_->BatchPut({key}, new_slices, DfsConfig());
    ASSERT_EQ(puts.size(), 1u);
    ASSERT_TRUE(puts[0]);
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto new_query = QueryDfsOnly(key);
    ASSERT_TRUE(new_query);
    const auto new_desc = new_query->replicas[0].get_dfs_descriptor();
    EXPECT_EQ(new_desc.shard_idx, old_desc.shard_idx);
    EXPECT_EQ(new_desc.file_path, old_desc.file_path);
    EXPECT_NE(new_desc.MasterEpoch(), old_desc.MasterEpoch());
    ExpectDfsValue(key, new_value);
    // Independently verify the linked file, not the writer's cached inode.
    std::ifstream disk(new_desc.file_path, std::ios::binary);
    disk.seekg(static_cast<std::streamoff>(new_desc.offset));
    std::string on_disk(new_value.size(), '\0');
    disk.read(on_disk.data(), on_disk.size());
    ASSERT_TRUE(disk.good());
    EXPECT_EQ(on_disk, new_value);

    std::string output(old_value.size(), '\0');
    auto stale = backend_->BatchRead(
        {{key, old_desc, {{output.data(), output.size()}}}});
    ASSERT_FALSE(stale[0]);
    EXPECT_EQ(stale[0].error(), ErrorCode::DFS_STALE_HANDLE);
    ExpectDfsValue(key, new_value);
}

TEST_F(DfsBucketEpochClientTest,
       HealthyRestartRefreshesOnlyUnchangedReadViews) {
    const std::string key = "healthy_restart_read";
    std::string value(4096, 'R');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto old = QueryDfsOnly(key);
    ASSERT_TRUE(old);
    ExpectDfsValue(key, value);
    // Force the source bucket to be sealed and persisted for normal recovery.
    std::string rollover(1024 * 1024, 'P');
    std::vector<Slice> rollover_slices{{rollover.data(), rollover.size()}};
    ASSERT_TRUE(writer_->Put("healthy_rollover", rollover_slices, DfsConfig()));
    ASSERT_TRUE(RestartMasterAndReconnect());
    auto fresh = QueryDfsOnly(key);
    ASSERT_TRUE(fresh);
    ASSERT_NE(fresh->replicas[0].get_dfs_descriptor().MasterEpoch(),
              old->replicas[0].get_dfs_descriptor().MasterEpoch());
    // Bucket metadata does not persist object checksums. Do not silently drop
    // the old read view's checksum when refreshing after recovery.
    ASSERT_FALSE(fresh->object_checksum);
    const bool checksum_changed = old->object_checksum.has_value();
    // Observe the new epoch before consuming a descriptor queried pre-restart.
    ExpectDfsValue(key, value);
    std::string output(value.size(), '\0');
    std::vector<Slice> output_slices{{output.data(), output.size()}};
    const auto expect_read = [&](const tl::expected<void, ErrorCode>& result) {
        if (checksum_changed) {
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::DFS_STALE_HANDLE);
            EXPECT_EQ(output, std::string(value.size(), '\0'));
        } else {
            ASSERT_TRUE(result);
            EXPECT_EQ(output, value);
        }
    };
    expect_read(writer_->Get(key, *old, output_slices));
    std::vector<QueryResult> queries;
    queries.push_back(std::move(*old));
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {key, output_slices}};
    std::fill(output.begin(), output.end(), '\0');
    auto read = writer_->BatchGet({key}, queries, batch);
    expect_read(read.at(0));
    // Rebuilding the caller's view explicitly permits the recovered object.
    ASSERT_TRUE(writer_->Get(key, *fresh, output_slices));
    EXPECT_EQ(output, value);
    ASSERT_TRUE(writer_->Remove(key, true));
    auto missing = writer_->Get(key, queries[0], output_slices);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DfsBucketClientTest, DfsReadRetryPreservesReplicaFilter) {
    const auto lease =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    Replica::Descriptor dfs{};
    dfs.status = ReplicaStatus::COMPLETE;
    dfs.descriptor_variant =
        DistributedFSDescriptor{"bucket.data", 0, 4096, 4096, 0};
    dfs.get_dfs_descriptor().master_epoch = UUID{1, 1};
    const QueryResult original({dfs}, lease, 123);
    dfs.get_dfs_descriptor().master_epoch = UUID{1, 2};
    Replica::Descriptor local_disk{};
    local_disk.status = ReplicaStatus::COMPLETE;
    local_disk.descriptor_variant =
        LocalDiskDescriptor{UUID{2, 1}, 4096, "ssd"};
    const auto new_lease = lease + std::chrono::seconds(1);
    const QueryResult refreshed({local_disk, dfs}, new_lease, 123);

    auto retry = PrepareDfsReadRetry(original, refreshed);
    ASSERT_TRUE(retry);
    ASSERT_EQ(retry->replicas.size(), 1u);
    ASSERT_TRUE(retry->replicas[0].is_dfs_replica());
    EXPECT_EQ(retry->replicas[0].get_dfs_descriptor().MasterEpoch(),
              (UUID{1, 2}));
    EXPECT_EQ(retry->lease_timeout, new_lease);
    EXPECT_EQ(retry->object_checksum, original.object_checksum);

    const QueryResult local_only({local_disk}, new_lease, 123);
    auto rejected = PrepareDfsReadRetry(original, local_only);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), ErrorCode::DFS_STALE_HANDLE);
}

TEST_F(DfsBucketClientTest, DfsReadRetryRejectsChangedObjectView) {
    const auto lease =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    Replica::Descriptor dfs{};
    dfs.status = ReplicaStatus::COMPLETE;
    dfs.descriptor_variant =
        DistributedFSDescriptor{"bucket.data", 0, 4096, 4096, 0};
    const QueryResult original({dfs}, lease, 123);
    for (const uint64_t size : {2048, 8192}) {
        auto changed = dfs;
        changed.get_dfs_descriptor().object_size = size;
        const QueryResult refreshed({changed}, lease, 123);
        auto retry = PrepareDfsReadRetry(original, refreshed);
        ASSERT_FALSE(retry);
        EXPECT_EQ(retry.error(), ErrorCode::DFS_STALE_HANDLE);
    }
    for (const auto checksum :
         {std::optional<uint64_t>{456}, std::optional<uint64_t>{}}) {
        const QueryResult refreshed({dfs}, lease, checksum);
        auto retry = PrepareDfsReadRetry(original, refreshed);
        ASSERT_FALSE(retry);
        EXPECT_EQ(retry.error(), ErrorCode::DFS_STALE_HANDLE);
    }
    const QueryResult without_checksum({dfs}, lease);
    auto retry = PrepareDfsReadRetry(without_checksum, original);
    ASSERT_FALSE(retry);
    EXPECT_EQ(retry.error(), ErrorCode::DFS_STALE_HANDLE);
}

TEST_F(DfsBucketClientTest, StaleReadsRejectChangedObjectViewBeforeRetry) {
    const std::string key = "stale_changed_view";
    std::string value(4096, 'V');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    const auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query);
    for (const bool change_size : {true, false}) {
        SCOPED_TRACE(change_size);
        auto replicas = query->replicas;
        auto& old = replicas[0].get_dfs_descriptor();
        old.master_epoch = UUID{};
        auto checksum = query->object_checksum;
        if (change_size) {
            old.object_size *= 2;
        } else {
            checksum = checksum.value_or(0) + 1;
        }
        std::vector<QueryResult> queries;
        queries.emplace_back(std::move(replicas), query->lease_timeout,
                             checksum);
        std::string output(value.size() * 2, '?');
        const auto untouched = output;
        std::vector<Slice> output_slices{{output.data(), output.size()}};
        auto single = writer_->Get(key, queries[0], output_slices);
        ASSERT_FALSE(single);
        EXPECT_EQ(single.error(), ErrorCode::DFS_STALE_HANDLE);
        EXPECT_EQ(output, untouched);
        std::unordered_map<std::string, std::vector<Slice>> batch{
            {key, output_slices}};
        auto results = writer_->BatchGet({key}, queries, batch);
        ASSERT_EQ(results.size(), 1u);
        ASSERT_FALSE(results[0]);
        EXPECT_EQ(results[0].error(), ErrorCode::DFS_STALE_HANDLE);
        EXPECT_EQ(output, untouched);
    }
}

TEST_F(DfsBucketClientTest, StaleDfsReadRetryHonorsRuntimeFilter) {
    const std::string key = "stale_read_runtime_filter";
    std::string value(4096, 'F');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query);
    auto replicas = query->replicas;
    replicas[0].get_dfs_descriptor().master_epoch = UUID{};
    std::vector<QueryResult> queries;
    queries.emplace_back(std::move(replicas), query->lease_timeout,
                         query->object_checksum);

    ASSERT_TRUE(master_.service()->SetDfsFilterMode(1));
    std::string output(value.size(), '?');
    const auto untouched = output;
    std::vector<Slice> output_slices{{output.data(), output.size()}};
    auto single = writer_->Get(key, queries[0], output_slices);
    ASSERT_FALSE(single);
    EXPECT_EQ(single.error(), ErrorCode::DFS_STALE_HANDLE);
    EXPECT_EQ(output, untouched);
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {key, output_slices}};
    auto results = writer_->BatchGet({key}, queries, batch);
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::DFS_STALE_HANDLE);
    EXPECT_EQ(output, untouched);

    // Filtering preserves the existing DFS metadata; switching back permits
    // the same bounded retry, with no stale fallback to a different tier.
    ASSERT_TRUE(master_.service()->SetDfsFilterMode(0));
    ASSERT_TRUE(writer_->Get(key, queries[0], output_slices));
    EXPECT_EQ(output, value);
}

TEST_F(DfsBucketClientTest, BatchLeaseExpiryExemptsOnlyBucketDfs) {
    const std::vector<std::string> keys{"lease_memory", "lease_dfs"};
    std::vector<std::string> values{std::string(4096, 'M'),
                                    std::string(4096, 'D')};
    auto writes = MakeSlices(values);
    auto put = writer_->BatchPut(keys, writes, DfsConfig());
    ASSERT_EQ(put.size(), keys.size());
    for (const auto& result : put) ASSERT_TRUE(result);
    for (const auto& key : keys) ASSERT_TRUE(WaitForDfsReplica(key));

    std::vector<QueryResult> queries;
    const auto expired =
        std::chrono::steady_clock::now() - std::chrono::seconds(1);
    for (size_t i = 0; i < keys.size(); ++i) {
        auto query = writer_->Query(keys[i]);
        ASSERT_TRUE(query);
        std::vector<Replica::Descriptor> selected;
        for (const auto& replica : query->replicas) {
            if ((i == 0 && replica.is_memory_replica()) ||
                (i == 1 && replica.is_dfs_replica())) {
                selected.push_back(replica);
            }
        }
        ASSERT_EQ(selected.size(), 1u);
        queries.emplace_back(std::move(selected), expired,
                             query->object_checksum);
    }
    std::vector<std::string> outputs(2, std::string(4096, '\0'));
    std::unordered_map<std::string, std::vector<Slice>> batch;
    for (size_t i = 0; i < keys.size(); ++i) {
        batch[keys[i]] = {{outputs[i].data(), outputs[i].size()}};
    }
    auto results = writer_->BatchGet(keys, queries, batch);
    ASSERT_EQ(results.size(), keys.size());
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::BATCH_GET_LEASE_EXPIRED);
    EXPECT_TRUE(results[1]);
    EXPECT_EQ(outputs, values);
}

TEST_F(DfsBucketClientTest, BatchReadCrossesLeaseDeadlineAfterValidation) {
    const std::string key = "bucket_crosses_lease";
    std::string value(4096, 'L');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query);
    const QueryResult expiring(
        std::vector(query->replicas),
        std::chrono::steady_clock::now() + std::chrono::milliseconds(50),
        query->object_checksum);
    adapter_->after_read = [deadline = expiring.lease_timeout] {
        std::this_thread::sleep_until(deadline + std::chrono::milliseconds(1));
    };
    std::string output(value.size(), '\0');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {key, {{output.data(), output.size()}}}};
    auto results = writer_->BatchGet({key}, {expiring}, batch);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(expiring.IsLeaseExpired());
    ASSERT_TRUE(results[0]);
    EXPECT_EQ(output, value);

    // Expiry exemption must not mask checksum or short-read failures.
    if (query->object_checksum) {
        const QueryResult corrupt(std::vector(query->replicas),
                                  expiring.lease_timeout,
                                  *query->object_checksum ^ uint64_t{1});
        results = writer_->BatchGet({key}, {corrupt}, batch);
        ASSERT_FALSE(results[0]);
        EXPECT_EQ(results[0].error(), ErrorCode::CHECKSUM_MISMATCH);
    }
    const auto& desc = query->replicas[0].get_dfs_descriptor();
    std::filesystem::resize_file(desc.file_path, 0);
    results = writer_->BatchGet({key}, {expiring}, batch);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::FILE_READ_FAIL);
}

TEST_F(DfsBucketEpochClientTest,
       BackfillReopensReusedBucketAfterMasterRestart) {
    const std::string key = "backfill_restart_epoch";
    std::string old_value(4096, 'O');
    std::vector<Slice> old_slices{{old_value.data(), old_value.size()}};
    ASSERT_TRUE(writer_->Put(key, old_slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto old_query = QueryDfsOnly(key);
    ASSERT_TRUE(old_query);
    const auto old_desc = old_query->replicas[0].get_dfs_descriptor();
    auto holder_backend = HolderBackfillBackend();
    ASSERT_NE(holder_backend, nullptr);
    // The production executor initializes this lazily created backend.
    ASSERT_TRUE(holder_backend->Init());
    std::string output(old_value.size(), '\0');
    ASSERT_TRUE(
        holder_backend
            ->BatchRead({{key, old_desc, {{output.data(), output.size()}}}})
            .at(0));
    EXPECT_EQ(output, old_value);
    ASSERT_TRUE(provider_->UnmountSegment(segment_, segment_size_));
    master_.Stop();
    for (const auto& file : std::filesystem::directory_iterator(root_)) {
        const auto name = file.path().filename().string();
        if (name.rfind("bucket_", 0) == 0 &&
            (file.path().extension() == ".data" ||
             file.path().extension() == ".meta")) {
            ASSERT_TRUE(std::filesystem::remove(file.path()));
        }
    }
    ASSERT_TRUE(RestartMasterAndReconnect());
    ASSERT_TRUE(provider_->MountSegment(segment_, segment_size_, "tcp"));
    adapter_->FailAllWrites(
        true);  // The independent holder backend is healthy.
    std::string value(4096, 'B');
    std::vector<std::vector<Slice>> slices{{{value.data(), value.size()}}};
    ASSERT_TRUE(writer_->BatchPut({key}, slices, DfsConfig()).at(0));
    ASSERT_TRUE(WaitForDfsReplica(key, std::chrono::seconds(12)));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query);
    const auto desc = query->replicas[0].get_dfs_descriptor();
    EXPECT_EQ(desc.shard_idx, old_desc.shard_idx);
    EXPECT_NE(desc.MasterEpoch(), old_desc.MasterEpoch());
    EXPECT_EQ(HolderBackfillBackend(), holder_backend);
    std::ifstream disk(desc.file_path, std::ios::binary);
    disk.seekg(static_cast<std::streamoff>(desc.offset));
    disk.read(output.data(), output.size());
    ASSERT_TRUE(disk.good());
    EXPECT_EQ(output, value);
}

TEST_F(DfsBucketClientTest, AllFinalizationRpcsRejectStaleMasterEpoch) {
    const std::string key = "stale_completion_epoch";
    std::string value(4096, 'C');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query);
    const UUID stale_epoch{0, 1};
    ASSERT_NE(query->replicas[0].get_dfs_descriptor().MasterEpoch(),
              stale_epoch);
    MasterClient rpc(writer_->getClientId(), nullptr);
    ASSERT_EQ(rpc.Connect(master_.master_address()), ErrorCode::OK);
    const ObjectMeta meta{key, std::nullopt};
    auto expect_stale = [](const tl::expected<void, ErrorCode>& result) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_WRITE);
    };
    expect_stale(rpc.PutEnd(meta, ReplicaType::DFS, stale_epoch));
    expect_stale(rpc.PutRevoke(key, ReplicaType::DFS, stale_epoch));
    expect_stale(rpc.UpsertEnd(meta, ReplicaType::DFS, stale_epoch));
    expect_stale(rpc.UpsertRevoke(key, ReplicaType::DFS, stale_epoch));
    expect_stale(rpc.BatchPutEnd({meta}, ReplicaType::DFS, stale_epoch).at(0));
    expect_stale(
        rpc.BatchPutRevoke({key}, ReplicaType::DFS, stale_epoch).at(0));
    expect_stale(rpc.BatchUpsertEnd({meta}, stale_epoch).at(0));
    expect_stale(rpc.BatchUpsertRevoke({key}, stale_epoch).at(0));
    expect_stale(
        writer_->BatchPutEnd({meta}, ReplicaType::DFS, {stale_epoch}).at(0));
    expect_stale(
        writer_->BatchPutRevoke({key}, ReplicaType::DFS, {stale_epoch}).at(0));
    ExpectDfsValue(key, value);
}

TEST_F(DfsBucketClientTest, RegexQueryCarriesCurrentDfsEpoch) {
    const std::string key = "epoch_regex_query";
    std::string value(4096, 'R');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query);
    const auto epoch = query->replicas[0].get_dfs_descriptor().MasterEpoch();
    auto matches = writer_->QueryByRegex("^epoch_regex_query$");
    ASSERT_TRUE(matches);
    ASSERT_EQ(matches->size(), 1u);
    bool found_dfs = false;
    for (const auto& replica : matches->at(key)) {
        if (!replica.is_dfs_replica()) continue;
        found_dfs = true;
        const auto& desc = replica.get_dfs_descriptor();
        EXPECT_EQ(desc.MasterEpoch(), epoch);
        std::string output(value.size(), '\0');
        auto read = backend_->BatchRead(
            {{key, desc, {{output.data(), output.size()}}}});
        ASSERT_TRUE(read[0]);
        EXPECT_EQ(output, value);
    }
    EXPECT_TRUE(found_dfs);
}

TEST_F(DfsBucketClientTest,
       EpochProviderOutlivesClientAndFollowsAddressChange) {
    auto snapshot = writer_->GetDfsBucketSnapshot(UUID{}, 0);
    ASSERT_TRUE(snapshot);
    UUID expected_epoch = snapshot->epoch;
    std::function<tl::expected<UUID, ErrorCode>()> provider;
    {
        MasterClient rpc(writer_->getClientId(), nullptr);
        ASSERT_EQ(rpc.Connect(master_.master_address()), ErrorCode::OK);
        provider = rpc.MakeDfsMasterEpochProvider();
        auto initial = provider();
        ASSERT_TRUE(initial);
        EXPECT_EQ(*initial, expected_epoch);
        // Pool recreation was removed: change the address through the shared
        // registry and verify the existing provider follows the same holder.
        const int old_port = master_.rpc_port();
        master_.Stop();
        int new_port;
        do {
            new_port = getFreeTcpPort();
        } while (new_port == old_port);
        ASSERT_TRUE(master_.Start(
            InProcMasterConfigBuilder().set_rpc_port(new_port).build()));
        ASSERT_EQ(rpc.Connect(master_.master_address()), ErrorCode::OK);
        auto current =
            rpc.GetDfsBucketSnapshot(writer_->getClientId(), UUID{}, 0);
        ASSERT_TRUE(current);
        ASSERT_NE(current->epoch, expected_epoch);
        expected_epoch = current->epoch;
        auto epoch = provider();
        ASSERT_TRUE(epoch);
        EXPECT_EQ(*epoch, expected_epoch);
    }
    auto epoch = provider();
    ASSERT_TRUE(epoch);
    EXPECT_EQ(*epoch, expected_epoch);
}

TEST_F(DfsBucketClientTest, EmptyStartupIdentityRecoversFromMasterWithoutRestart) {
    ASSERT_TRUE(provider_->UnmountSegment(segment_, segment_size_).has_value());
    writer_.reset();
    provider_.reset();
    backend_.reset();
    adapter_ = nullptr;
    const int rpc_port = master_.rpc_port();
    const auto master_config =
        InProcMasterConfigBuilder().set_rpc_port(rpc_port).build();
    master_.Stop();
    SetEnv("MOONCAKE_ENABLE_DFS", "0");
    ASSERT_TRUE(master_.Start(master_config));
    writer_ = CreateClient("127.0.0.1:18201");
    ASSERT_NE(writer_, nullptr);
    ASSERT_TRUE(writer_->GetDfsRootIdentity().empty());

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = root_;
    // Keep this client and backend alive across the master's DFS recovery.
    auto recovering = std::make_shared<DistributedStorageBackend>(
        file_config, distributed_config_, std::make_unique<PosixFsAdapter>());
    ASSERT_FALSE(recovering->Init().has_value());
    recovering->SetRootIdentityProvider(
        [weak_client = std::weak_ptr<Client>(writer_)]()
            -> tl::expected<std::string, ErrorCode> {
            auto client = weak_client.lock();
            if (!client) {
                return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
            }
            return client->FetchDfsRootIdentity();
        });
    writer_->SetDfsStorageBackend(recovering);
    ASSERT_FALSE(recovering->ProbeWrite().has_value());
    master_.Stop();
    SetEnv("MOONCAKE_ENABLE_DFS", "1");
    ASSERT_TRUE(master_.Start(master_config));
    // A pooled connection can see EOF on the first probe after restart.
    const auto recovery_deadline = std::chrono::steady_clock::now() +
                                   std::chrono::seconds(5);
    while (!recovering->ProbeWrite() &&
           std::chrono::steady_clock::now() < recovery_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(recovering->ProbeWrite().has_value());
    ASSERT_TRUE(recovering->ProbeRead().has_value());

    provider_ = CreateClient("127.0.0.1:18202");
    ASSERT_NE(provider_, nullptr);
    ASSERT_TRUE(provider_->MountSegment(segment_, segment_size_, "tcp")
                    .has_value());

    const std::string key = "recovered_startup_identity";
    std::string value(4096, 'r');
    std::vector<Slice> slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, slices, DfsConfig()));
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    std::string output(value.size(), '\0');
    auto read = recovering->BatchRead(
        {{key, query->replicas[0].get_dfs_descriptor(),
          {{output.data(), output.size()}}}});
    ASSERT_EQ(read.size(), 1u);
    ASSERT_TRUE(read[0].has_value());
    EXPECT_EQ(output, value);
}

class DfsBucketClientChunkLimitTest : public DfsBucketClientTest {
   protected:
    void ConfigureClientEnvironment() override {
        SetEnv("MC_STORE_DFS_WRITE_CHUNK_SIZE_BYTES", "16384");
        SetEnv("MC_STORE_DFS_WRITE_ARENA_COUNT", "1");
    }
};

TEST_F(DfsBucketClientChunkLimitTest,
       ChunkLimitFailureKeepsSuccessfulMemoryReplica) {
    const std::string key = "chunk_limit_memory_fallback";
    std::string value(32 * 1024, 'X');
    std::vector<std::vector<Slice>> slices{{{value.data(), value.size()}}};

    auto results = writer_->BatchPut({key}, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());
    auto query = writer_->Query(key);
    ASSERT_TRUE(query.has_value());
    ASSERT_EQ(query->replicas.size(), 1u);
    EXPECT_TRUE(query->replicas[0].is_memory_replica());
}

class DfsBucketClientSlotTimeoutTest : public DfsBucketClientTest {
   protected:
    void ConfigureClientEnvironment() override {
        SetEnv("MC_STORE_DFS_WRITE_CHUNK_SIZE_BYTES", "4096");
        SetEnv("MC_STORE_DFS_WRITE_ARENA_COUNT", "1");
        SetEnv("MC_STORE_DFS_WRITE_ARENA_WAIT_MS", "1");
    }
};

TEST_F(DfsBucketClientSlotTimeoutTest,
       SlotTimeoutKeepsMemoryReplicasForUnsubmittedChunks) {
    adapter_->BlockWrites();
    std::vector<std::string> keys{"slot_timeout_0", "slot_timeout_1"};
    std::vector<std::string> values(2, std::string(3072, 'T'));
    auto slices = MakeSlices(values);

    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    ASSERT_TRUE(results[0].has_value());
    ASSERT_TRUE(results[1].has_value());

    for (const auto& key : keys) {
        auto query = writer_->Query(key);
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 1u);
        EXPECT_TRUE(query->replicas[0].is_memory_replica());
    }

    adapter_->ReleaseWrites();
    ASSERT_TRUE(WaitForDfsReplica(keys[0]));
    auto query = writer_->Query(keys[1]);
    ASSERT_TRUE(query.has_value());
    ASSERT_EQ(query->replicas.size(), 1u);
    EXPECT_TRUE(query->replicas[0].is_memory_replica());
}

class DfsBucketClientMultiChunkTest : public DfsBucketClientTest {
   protected:
    void ConfigureClientEnvironment() override {
        SetEnv("MC_STORE_DFS_WRITE_CHUNK_SIZE_BYTES", "4096");
        SetEnv("MC_STORE_DFS_WRITE_ARENA_COUNT", "1");
    }
};

TEST_F(DfsBucketClientMultiChunkTest, BatchPutProcessesMultipleChunks) {
    std::vector<std::string> keys{"multi_chunk_0", "multi_chunk_1",
                                  "multi_chunk_2"};
    std::vector<std::string> values(3, std::string(3072, 'M'));
    auto slices = MakeSlices(values);

    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_TRUE(results[i].has_value())
            << "key " << keys[i] << ": " << toString(results[i].error());
        ASSERT_TRUE(WaitForDfsReplica(keys[i]));
        ExpectDfsValue(keys[i], values[i]);
    }
}

TEST_F(DfsBucketClientTest, BatchPutWritesEveryKeyAndKeepsBatchContiguous) {
    std::vector<std::string> keys{"bucket_batch_0", "bucket_batch_1",
                                  "bucket_batch_2", "bucket_batch_3"};
    std::vector<std::string> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        values.push_back(std::string(2048, static_cast<char>('a' + i)));
    }
    auto slices = MakeSlices(values);

    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_TRUE(results[i].has_value())
            << "key " << keys[i] << ": " << toString(results[i].error());
    }

    std::vector<DistributedFSDescriptor> descriptors;
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(WaitForDfsReplica(keys[i])) << "key " << keys[i];
        ExpectDfsValue(keys[i], values[i]);
        auto query = QueryDfsOnly(keys[i]);
        ASSERT_TRUE(query.has_value());
        descriptors.push_back(query->replicas[0].get_dfs_descriptor());
    }

    // The whole batch must be reserved as one contiguous run inside a single
    // bucket, in request order.
    for (size_t i = 1; i < descriptors.size(); ++i) {
        EXPECT_EQ(descriptors[i].shard_idx, descriptors[0].shard_idx)
            << "entry " << i << " landed in a different bucket";
    }
    for (size_t i = 1; i < descriptors.size(); ++i) {
        const uint64_t previous_entry_start =
            descriptors[i - 1].offset - 8 - keys[i - 1].size();
        const uint64_t previous_end =
            previous_entry_start + descriptors[i - 1].aligned_size;
        const uint64_t entry_start = descriptors[i].offset - 8 - keys[i].size();
        EXPECT_EQ(entry_start, previous_end)
            << "entry " << i << " is not contiguous with its predecessor";
    }
}

TEST_F(DfsBucketClientTest, ChecksumReusesDfsD2hBeforeMemoryPublication) {
    const std::vector<std::string> keys{"checksum_staged_a",
                                        "checksum_staged_b"};
    const std::vector<std::string> expected{"123456789", "abcdef"};
    std::string source = "!" + expected[0] + expected[1];
    staging_device.Reset(source.data(), source.size());
    std::vector<std::vector<Slice>> slices{
        {{source.data() + 1, 3}, {source.data() + 4, 6}},
        {{source.data() + 10, expected[1].size()}}};
    adapter_->BlockWrites();
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), 2u);
    ASSERT_TRUE(results[0].has_value());
    ASSERT_TRUE(results[1].has_value());
    EXPECT_EQ(staging_device.copies, 3);  // One normal D2H per nonempty slice.
    for (size_t i = 0; i < keys.size(); ++i) {
        auto query = writer_->Query(keys[i]);
        ASSERT_TRUE(query.has_value());
        EXPECT_EQ(query->object_checksum,
                  Environ::Get().GetStoreChecksumEnabled()
                      ? std::optional<uint64_t>(ComputeCrcChecksum(
                            expected[i].data(), expected[i].size()))
                      : std::nullopt);
        for (const auto& replica : query->replicas) {
            EXPECT_TRUE(replica.is_memory_replica());
        }
    }
    source.assign(source.size(), 'X');
    adapter_->ReleaseWrites();
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(WaitForDfsReplica(keys[i]));
        auto query = writer_->Query(keys[i]);
        ASSERT_TRUE(query.has_value());
        EXPECT_EQ(query->object_checksum,
                  Environ::Get().GetStoreChecksumEnabled()
                      ? std::optional<uint64_t>(ComputeCrcChecksum(
                            expected[i].data(), expected[i].size()))
                      : std::nullopt);
        ExpectDfsValue(keys[i], expected[i]);
    }
}

TEST_F(DfsBucketClientTest,
       DfsStagingFailureKeepsCompletedMemoryReplica) {
    for (const bool throw_copy : {false, true}) {
        std::vector<std::string> keys{"checksum_stage_fail"};
        std::string value(4099, 'F');
        staging_device.Reset(value.data(), value.size());
        staging_device.fail_copy = 1;
        staging_device.throw_copy = throw_copy;
        std::vector<std::vector<Slice>> slices{{{value.data(), value.size()}}};
        auto results = writer_->BatchPut(keys, slices, DfsConfig());
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value());
        EXPECT_EQ(staging_device.copies,
                  Environ::Get().GetStoreChecksumEnabled() ? 2 : 1);
        auto query = writer_->Query(keys[0]);
        ASSERT_TRUE(query.has_value());
        ASSERT_TRUE(std::any_of(query->replicas.begin(), query->replicas.end(),
                                [](const auto& replica) {
                                    return replica.is_memory_replica();
                                }));
        EXPECT_TRUE(std::none_of(query->replicas.begin(), query->replicas.end(),
                                 [](const auto& replica) {
                                     return replica.is_dfs_replica();
                                 }));
        ASSERT_TRUE(writer_->Remove(keys[0], true).has_value());
        staging_device.Reset(value.data(), value.size());
        results = writer_->BatchPut(keys, slices, DfsConfig());
        ASSERT_TRUE(results[0].has_value());
        ASSERT_TRUE(WaitForDfsReplica(keys[0]));
        ASSERT_TRUE(writer_->Remove(keys[0], true).has_value());
    }
}

TEST_F(DfsBucketClientTest, ChecksumFallbackFailureDoesNotPublishAnyReplica) {
    if (!Environ::Get().GetStoreChecksumEnabled()) GTEST_SKIP();
    const std::vector<std::string> keys{"checksum_fallback_fail"};
    std::string value(4099, 'F');
    staging_device.Reset(value.data(), value.size());
    staging_device.fail_copy = 0;
    std::vector<std::vector<Slice>> slices{{{value.data(), value.size()}}};
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(staging_device.copies, 2);
    auto query = writer_->Query(keys[0]);
    ASSERT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DfsBucketClientTest, MissingBackendKeepsMemoryAndOriginalD2hPath) {
    writer_->SetDfsStorageBackend(nullptr);
    const std::vector<std::string> keys{"checksum_no_backend"};
    std::string value(4099, 'F');
    staging_device.Reset(value.data(), value.size());
    std::vector<std::vector<Slice>> slices{{{value.data(), value.size()}}};
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());
    EXPECT_EQ(staging_device.copies,
              Environ::Get().GetStoreChecksumEnabled() ? 1 : 0);
    auto query = writer_->Query(keys[0]);
    ASSERT_TRUE(query.has_value());
    ASSERT_TRUE(std::any_of(query->replicas.begin(), query->replicas.end(),
                            [](const auto& replica) {
                                return replica.is_memory_replica();
                            }));
    EXPECT_TRUE(std::none_of(query->replicas.begin(), query->replicas.end(),
                             [](const auto& replica) {
                                 return replica.is_dfs_replica();
                             }));
}

TEST_F(DfsBucketClientTest, ChecksumRejectsNullSliceBeforeTransfers) {
    if (!Environ::Get().GetStoreChecksumEnabled()) GTEST_SKIP();
    const std::vector<std::string> keys{"checksum_null_slice"};
    std::vector<std::vector<Slice>> slices{{{nullptr, 4099}}};
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(adapter_->WriteCalls(), 0);
}

TEST_F(DfsBucketClientTest, ChecksumSurvivesEitherEndOrderAndRetries) {
    for (const bool dfs_first : {false, true}) {
        const std::string key =
            dfs_first ? "checksum_dfs_first" : "checksum_mem_first";
        auto allocations =
            writer_->StartBatchPutForSizes({key}, {4096}, DfsConfig());
        ASSERT_EQ(allocations.size(), 1u);
        ASSERT_TRUE(allocations[0].has_value());
        const ObjectMeta meta{key, uint64_t{0}};
        for (const auto type :
             {dfs_first ? ReplicaType::DFS : ReplicaType::MEMORY,
              dfs_first ? ReplicaType::MEMORY : ReplicaType::DFS}) {
            for (int attempt = 0; attempt < 2; ++attempt) {
                auto results = writer_->BatchPutEnd({meta}, type);
                ASSERT_EQ(results.size(), 1u);
                ASSERT_TRUE(results[0].has_value());
                auto query = writer_->Query(key);
                ASSERT_TRUE(query.has_value());
                EXPECT_EQ(query->object_checksum, std::optional<uint64_t>(0));
            }
        }
    }
}

TEST_F(DfsBucketClientTest, AsyncWriteSucceedsAfterCallerBufferIsOverwritten) {
    const std::string key = "bucket_buffer_lifetime";
    const std::string expected(8192, 'L');

    {
        // A heap buffer that goes away right after BatchPut returns. The async
        // task must have copied it, not captured a pointer to it.
        auto buffer = std::make_unique<char[]>(expected.size());
        std::memcpy(buffer.get(), expected.data(), expected.size());
        std::vector<std::vector<Slice>> slices{
            {Slice{buffer.get(), expected.size()}}};
        std::vector<std::string> keys{key};
        auto results = writer_->BatchPut(keys, slices, DfsConfig());
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value());
        // Scribble over the buffer before the background write can run, then
        // free it.
        std::memset(buffer.get(), 'X', expected.size());
    }

    ASSERT_TRUE(WaitForDfsReplica(key));
    ExpectDfsValue(key, expected);
}

TEST_F(DfsBucketClientTest,
       BatchPutDeadlineKeepsMemoryAndLateDfsWriteIsRevoked) {
    writer_.reset();
    SetEnv("MOONCAKE_DFS_BATCH_PUT_TIMEOUT_MS", "50");
    writer_ = CreateClient("127.0.0.1:18203");
    ASSERT_NE(writer_, nullptr);
    writer_->SetDfsStorageBackend(backend_);

    adapter_->BlockWrites();
    const std::string key = "bucket_write_timeout";
    std::vector<std::string> values{std::string(4096, 'T')};
    auto slices = MakeSlices(values);
    const auto started = std::chrono::steady_clock::now();
    auto results = writer_->BatchPut({key}, slices, DfsConfig());
    const auto elapsed = std::chrono::steady_clock::now() - started;
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());
    EXPECT_LT(elapsed, std::chrono::seconds(2));
    EXPECT_TRUE(writer_->DfsWriteAllowed());

    // <=1 GiB uses 4x the configured baseline for BatchPut. Keep the backend
    // blocked beyond that publication deadline before allowing pwrite to end.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    adapter_->ReleaseWrites();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        auto query = writer_->Query(key);
        if (query && std::none_of(query->replicas.begin(), query->replicas.end(),
                                  [](const auto& replica) {
                                      return replica.is_dfs_replica();
                                  })) {
            ASSERT_TRUE(std::any_of(query->replicas.begin(), query->replicas.end(),
                                    [](const auto& replica) {
                                        return replica.is_memory_replica();
                                    }));
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    FAIL() << "late DFS write was not revoked after timeout";
}

TEST_F(DfsBucketClientTest, TimelyBoundedBatchPutPublishesAsynchronously) {
    writer_.reset();
    SetEnv("MOONCAKE_DFS_BATCH_PUT_TIMEOUT_MS", "1000");
    writer_ = CreateClient("127.0.0.1:18204");
    ASSERT_NE(writer_, nullptr);
    writer_->SetDfsStorageBackend(backend_);

    const std::string key = "bounded_write_is_published";
    std::vector<std::string> values{std::string(4096, 'P')};
    auto slices = MakeSlices(values);
    auto results = writer_->BatchPut({key}, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());

    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    ASSERT_EQ(query->replicas.size(), 1u);
    EXPECT_EQ(query->replicas[0].status, ReplicaStatus::COMPLETE);
}

TEST_F(DfsBucketClientTest, FailedBoundedAsyncWriteKeepsMemoryReplica) {
    writer_.reset();
    SetEnv("MOONCAKE_DFS_BATCH_PUT_TIMEOUT_MS", "10000");
    writer_ = CreateClient("127.0.0.1:18205");
    ASSERT_NE(writer_, nullptr);
    writer_->SetDfsStorageBackend(backend_);

    const std::string key = "bucket_write_fail";
    std::string value(4096, 'F');

    adapter_->FailAllWrites(true);
    std::vector<std::string> keys{key};
    std::vector<std::string> values{value};
    auto slices = MakeSlices(values);
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    // BatchPut does not wait for the DFS write, so it reports success for the
    // MEMORY replica.
    ASSERT_TRUE(results[0].has_value());

    // The background task must revoke the DFS replica rather than leaving it
    // PROCESSING forever.
    ASSERT_TRUE(WaitForKeyGone(key));
    adapter_->FailAllWrites(false);

    auto query = writer_->Query(key);
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->object_checksum,
              Environ::Get().GetStoreChecksumEnabled()
                  ? std::optional<uint64_t>(
                        ComputeCrcChecksum(value.data(), value.size()))
                  : std::nullopt);
    for (const auto& replica : query->replicas) {
        EXPECT_FALSE(replica.is_dfs_replica())
            << "a failed DFS write must not leave a DFS replica behind";
    }
}

TEST_F(DfsBucketClientTest, FailedAsyncWriteIsBackfilledFromSurvivingMemory) {
    provider_->SetDfsStorageBackend(backend_);
    const std::string key = "backfill_after_io_failure";
    std::string value(4096, 'B');
    std::vector<std::string> values{value};
    auto slices = MakeSlices(values);
    adapter_->FailAllWrites(true);
    auto put = writer_->BatchPut({key}, slices, DfsConfig());
    ASSERT_EQ(put.size(), 1u);
    ASSERT_TRUE(put.front().has_value());
    ASSERT_TRUE(WaitForKeyGone(key));
    // Also fail a background attempt. Recovery must retry that task without
    // losing the sole good memory copy or retaining its failed allocation.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(8);
    while (adapter_->FailedWrites() < 2 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GE(adapter_->FailedWrites(), 2);
    ASSERT_TRUE(WaitForKeyGone(key));
    adapter_->FailAllWrites(false);
    ASSERT_TRUE(WaitForDfsReplica(key, std::chrono::seconds(12)));
    ExpectDfsValue(key, value);
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    std::string out(value.size(), '\0');
    std::vector<Slice> output{{out.data(), out.size()}};
    ASSERT_TRUE(writer_->Get(key, *query, output));
    EXPECT_EQ(out, value);
}

TEST_F(DfsBucketClientTest, BackfillWaitsForHolderWriteHealthRecovery) {
    provider_->SetDfsAccessPolicy(true, false);
    const std::string key = "backfill_write_health";
    const std::string value(4096, 'H');
    std::vector<std::string> values{value};
    auto slices = MakeSlices(values);
    adapter_->FailAllWrites(true);
    auto put = writer_->BatchPut({key}, slices, DfsConfig());
    ASSERT_EQ(put.size(), 1u);
    ASSERT_TRUE(put.front());
    ASSERT_TRUE(WaitForKeyGone(key));
    // The native memory holder can write this path, but its health policy
    // forbids DFS writes. It must neither allocate nor publish a DFS replica.
    EXPECT_FALSE(WaitForDfsReplica(key, std::chrono::seconds(6)));
    provider_->SetDfsAccessPolicy(true, true);
    ASSERT_TRUE(WaitForDfsReplica(key, std::chrono::seconds(12)));
    ExpectDfsValue(key, value);
}

TEST_F(DfsBucketClientTest, HolderBackfillBackoffBoundsProbesAndResetsOnSuccess) {
    PauseHolderPolling();
    CheckHolderBackoffBounds();
}

TEST_F(DfsBucketClientTest, BlockedBackfillsDoNotStarveMaintenanceTasks) {
    PauseHolderPolling();
    provider_->SetDfsStorageBackend(backend_);

    const std::vector<std::string> keys{
        "isolated_backfill_0", "isolated_backfill_1",
        "isolated_backfill_2", "isolated_backfill_3"};
    std::vector<std::string> values(keys.size(), std::string(4096, 'R'));
    auto slices = MakeSlices(values);
    adapter_->FailAllWrites(true);
    auto put = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(put.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(put[i]);
        ASSERT_TRUE(WaitForKeyGone(keys[i]));
    }
    auto backfills = FetchHolderTasks(keys.size());
    ASSERT_EQ(backfills.size(), keys.size());

    // Separate keys avoid COPY/MOVE contending with a backfill's source pin.
    ReplicateConfig memory_config;
    memory_config.replica_num = 1;
    memory_config.preferred_segment = "127.0.0.1:18202";
    std::string memory_value(4096, 'M');
    std::vector<Slice> memory_slices{{memory_value.data(), memory_value.size()}};
    ASSERT_TRUE(writer_->Put("isolated_copy", memory_slices, memory_config));
    ASSERT_TRUE(writer_->Put("isolated_move", memory_slices, memory_config));

    auto target_segment = std::shared_ptr<void>(
        allocate_buffer_allocator_memory(segment_size_),
        [client = writer_, size = segment_size_](void* ptr) {
            if (ptr) {
                (void)client->UnmountSegment(ptr, size);
                std::free(ptr);
            }
        });
    ASSERT_NE(target_segment, nullptr);
    ASSERT_TRUE(
        writer_->MountSegment(target_segment.get(), segment_size_, "tcp"));

    auto grace_segment = std::shared_ptr<void>(
        allocate_buffer_allocator_memory(segment_size_), std::free);
    ASSERT_NE(grace_segment, nullptr);
    auto cleanup = std::make_shared<std::promise<void>>();
    auto cleaned = cleanup->get_future();

    adapter_->FailAllWrites(false);
    adapter_->BlockWrites();
    const int writes = adapter_->WriteCalls();
    // Four assignments would occupy every shared worker before the fix.
    for (const auto& task : backfills) {
        ASSERT_EQ(task.type, TaskType::DFS_BACKFILL);
        SubmitHolderTask(task);
    }
    const auto write_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (adapter_->WriteCalls() == writes &&
           std::chrono::steady_clock::now() < write_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_GT(adapter_->WriteCalls(), writes);

    auto copy = writer_->CreateCopyTask("isolated_copy", {"127.0.0.1:18201"});
    auto move = writer_->CreateMoveTask("isolated_move", "127.0.0.1:18202",
                                        "127.0.0.1:18201");
    ASSERT_TRUE(copy);
    ASSERT_TRUE(move);
    auto maintenance = FetchHolderTasks(2);
    ASSERT_EQ(maintenance.size(), 2u);
    for (const auto& task : maintenance) SubmitHolderTask(task);
    ASSERT_TRUE(StartHolderUnmountTimer(
        grace_segment.get(), segment_size_,
        [cleanup, grace_segment](const UUID&) { cleanup->set_value(); }));

    auto wait_for_success = [&](const UUID& id) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            auto task = writer_->QueryTask(id);
            if (!task || task->status == TaskStatus::FAILED) return false;
            if (task->status == TaskStatus::SUCCESS) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    };
    EXPECT_TRUE(wait_for_success(*copy));
    EXPECT_TRUE(wait_for_success(*move));
    // The production graceful-unmount timer includes a ten-second delay.
    EXPECT_EQ(cleaned.wait_for(std::chrono::seconds(15)),
              std::future_status::ready);
    for (const auto& task : backfills) {
        auto status = writer_->QueryTask(task.id);
        ASSERT_TRUE(status);
        EXPECT_EQ(status->status, TaskStatus::PROCESSING);
    }

    adapter_->ReleaseWrites();
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(wait_for_success(backfills[i].id));
        ASSERT_TRUE(WaitForDfsReplica(keys[i]));
        ExpectDfsValue(keys[i], values[i]);
    }
}

TEST_F(DfsBucketClientTest, HolderBackoffCoversDistinctKeysAndRecoversTogether) {
    provider_->SetDfsStorageBackend(backend_);
    PauseHolderPolling();
    std::vector<std::string> keys;
    std::vector<std::string> values(64, std::string(4096, 'R'));
    for (size_t i = 0; i < values.size(); ++i) {
        keys.push_back("holder_backoff_" + std::to_string(i));
    }
    auto slices = MakeSlices(values);
    adapter_->FailAllWrites(true);
    auto put = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(put.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(put[i]);
        ASSERT_TRUE(WaitForKeyGone(keys[i]));
    }
    auto tasks = FetchHolderTasks(keys.size());
    ASSERT_EQ(tasks.size(), keys.size());
    auto& metrics = MasterMetricManager::instance();
    const auto starts = metrics.get_copy_start_requests();
    const int failures = adapter_->FailedWrites();
    for (const auto& task : tasks) ExecuteBackfill(task);
    // Distinct keys must not bypass the holder's pause and allocate DFS holes.
    EXPECT_EQ(metrics.get_copy_start_requests() - starts, 1);
    EXPECT_EQ(adapter_->FailedWrites() - failures, 1);

    tasks = FetchHolderTasks(keys.size());
    ASSERT_EQ(tasks.size(), keys.size());
    adapter_->FailAllWrites(false);
    ExpireHolderBackoff();
    adapter_->BlockWrites();
    const int writes = adapter_->WriteCalls();
    const auto recovery_starts = metrics.get_copy_start_requests();
    std::thread probe([&] { ExecuteBackfill(tasks[0]); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (adapter_->WriteCalls() == writes && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(adapter_->WriteCalls(), writes + 1);
    ExecuteBackfill(tasks[1]);
    EXPECT_EQ(metrics.get_copy_start_requests() - recovery_starts, 1);
    adapter_->ReleaseWrites();
    probe.join();
    DfsBackfillPayload probe_payload;
    struct_json::from_json(probe_payload, tasks[0].payload);
    ASSERT_TRUE(WaitForDfsReplica(probe_payload.key));
    const auto recovered_at = std::chrono::steady_clock::now();
    for (size_t i = 2; i < tasks.size(); ++i) ExecuteBackfill(tasks[i]);
    // The one task held behind the probe follows only the normal 5s task retry.
    auto remaining = FetchHolderTasks(1);
    ASSERT_EQ(remaining.size(), 1u);
    ExecuteBackfill(remaining.front());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(WaitForDfsReplica(keys[i], std::chrono::seconds(1)));
        ExpectDfsValue(keys[i], values[i]);
    }
    EXPECT_LT(std::chrono::steady_clock::now() - recovered_at, std::chrono::seconds(10));
}

TEST_F(DfsBucketClientTest, FailedWriteIsBackfilledByMemoryOnlyHolder) {
    // Only writer_ has a DFS backend. The memory holder must create its own
    // backfill backend without requiring SetDfsStorageBackend from the caller.
    const std::vector<std::string> keys{"backfill_memory_only_holder_0",
                                      "backfill_memory_only_holder_1"};
    std::vector<std::string> values{std::string(4096, 'M'),
                                   std::string(4096, 'N')};
    auto slices = MakeSlices(values);
    adapter_->FailAllWrites(true);
    auto put = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(put.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(put[i].has_value());
        ASSERT_TRUE(WaitForDfsReplica(keys[i], std::chrono::seconds(12)));
        ExpectDfsValue(keys[i], values[i]);
    }
}

TEST_F(DfsBucketClientTest,
       ServiceMetricsSynchronousDualWriteReportsPhysicalPartialOutcome) {
    const auto endpoint = NextServiceEndpoint();
    ASSERT_EQ(writer_->SetServiceEndpoint(endpoint), 0);
    adapter_->FailAllWrites(true);

    const std::string key = "service_metric_sync_partial";
    std::string value(4096, 'M');
    std::vector<Slice> slices{{value.data(), value.size()}};
    // The public Put result may follow the existing finalize/revoke contract;
    // the metric records the physical memory and DFS transfers independently.
    (void)writer_->Put(key, slices, DfsConfig());
    writer_.reset();  // Flush the cumulative client snapshot to the master.

    auto memory_only = FindServiceMetricValue(
        "mooncake_master_service_dual_write_total", endpoint,
        "outcome=\"memory_only\"");
    ASSERT_TRUE(memory_only.has_value());
    EXPECT_DOUBLE_EQ(*memory_only, 1.0);
}

TEST_F(DfsBucketClientTest,
       ServiceMetricsAsyncWriteMovesFromPendingToPhysicalOutcome) {
    const auto endpoint = NextServiceEndpoint();
    ASSERT_EQ(writer_->SetServiceEndpoint(endpoint), 0);
    adapter_->FailAllWrites(true);
    adapter_->BlockWrites();

    const std::string key = "service_metric_async_pending";
    std::vector<std::string> values{std::string(4096, 'A')};
    auto slices = MakeSlices(values);
    auto results = writer_->BatchPut({key}, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results.front().has_value());
    const auto write_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (adapter_->WriteCalls() == 0 &&
           std::chrono::steady_clock::now() < write_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_GT(adapter_->WriteCalls(), 0);

    auto pending = WaitForServiceMetricValue(
        "mooncake_master_service_dual_write_pending", endpoint, "", 1.0);
    ASSERT_TRUE(pending.has_value());
    EXPECT_DOUBLE_EQ(*pending, 1.0);

    adapter_->ReleaseWrites();
    ASSERT_TRUE(WaitForKeyGone(key));
    writer_.reset();  // Drains the async DFS task, then flushes its result.

    auto settled_pending = FindServiceMetricValue(
        "mooncake_master_service_dual_write_pending", endpoint);
    auto memory_only = FindServiceMetricValue(
        "mooncake_master_service_dual_write_total", endpoint,
        "outcome=\"memory_only\"");
    ASSERT_TRUE(settled_pending.has_value());
    ASSERT_TRUE(memory_only.has_value());
    EXPECT_DOUBLE_EQ(*settled_pending, 0.0);
    EXPECT_DOUBLE_EQ(*memory_only, 1.0);
}

TEST_F(DfsBucketClientTest,
       ServiceMetricsCountBatchReadFailuresOncePerKeyAndReplicaTier) {
    const auto endpoint = NextServiceEndpoint();
    ASSERT_EQ(writer_->SetServiceEndpoint(endpoint), 0);

    const std::string memory_key = "service_metric_read_memory_failure";
    const std::string dfs_key = "service_metric_read_dfs_failure";
    std::vector<std::string> values{std::string(4096, 'm'),
                                    std::string(4096, 'd')};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut({memory_key, dfs_key}, write_slices,
                                         DfsConfig());
    ASSERT_EQ(put_results.size(), 2u);
    for (const auto& result : put_results) ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(WaitForDfsReplica(memory_key));
    ASSERT_TRUE(WaitForDfsReplica(dfs_key));

    auto memory_query_all = writer_->Query(memory_key);
    ASSERT_TRUE(memory_query_all.has_value());
    std::vector<Replica::Descriptor> memory_replicas;
    for (auto replica : memory_query_all->replicas) {
        if (!replica.is_memory_replica()) continue;
        // Force this selected memory-tier read to fail during submit/connect.
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_ =
            "127.0.0.1:1";
        memory_replicas.emplace_back(std::move(replica));
    }
    ASSERT_EQ(memory_replicas.size(), 1u);
    QueryResult memory_query(std::move(memory_replicas),
                             memory_query_all->lease_timeout,
                             memory_query_all->object_checksum);

    auto dfs_query = QueryDfsOnly(dfs_key);
    ASSERT_TRUE(dfs_query.has_value());
    const auto& dfs_descriptor =
        dfs_query->replicas.front().get_dfs_descriptor();
    const auto bucket_path =
        std::filesystem::path(root_) /
        ("bucket_" +
         ImmutableBucketAllocator::FormatBucketId(dfs_descriptor.shard_idx) +
         ".data");
    ASSERT_TRUE(std::filesystem::remove(bucket_path));

    std::vector<QueryResult> queries;
    queries.emplace_back(std::move(memory_query));
    queries.emplace_back(std::move(*dfs_query));
    std::unordered_map<std::string, std::vector<Slice>> output_slices;
    std::string memory_output(4096, '\0');
    std::string dfs_output(4096, '\0');
    output_slices[memory_key] = {{memory_output.data(), memory_output.size()}};
    output_slices[dfs_key] = {{dfs_output.data(), dfs_output.size()}};

    auto get_results = writer_->BatchGet({memory_key, dfs_key}, queries,
                                         output_slices);
    ASSERT_EQ(get_results.size(), 2u);
    EXPECT_FALSE(get_results[0].has_value());
    EXPECT_FALSE(get_results[1].has_value());
    writer_.reset();  // Flush both per-key counters.

    auto memory_failures = FindServiceMetricValue(
        "mooncake_master_service_read_failures_total", endpoint,
        "replica_type=\"memory\"");
    auto dfs_failures = FindServiceMetricValue(
        "mooncake_master_service_read_failures_total", endpoint,
        "replica_type=\"dfs\"");
    ASSERT_TRUE(memory_failures.has_value());
    ASSERT_TRUE(dfs_failures.has_value());
    EXPECT_DOUBLE_EQ(*memory_failures, 1.0);
    EXPECT_DOUBLE_EQ(*dfs_failures, 1.0);
}

TEST_F(DfsBucketClientTest, OpenFailuresAreRateLimitedAndRevokeEveryDfsReplica) {
    struct Capture : google::LogSink {
        Capture() { google::AddLogSink(this); }
        ~Capture() override { google::RemoveLogSink(this); }
        std::atomic<int> opens{0}, writes{0};
        void send(google::LogSeverity, const char*, const char*, int,
                  const struct ::tm*, const char* message, size_t length) override {
            const std::string text(message, length);
            if (text.find("Failed to open DFS bucket file") != std::string::npos) {
                ++opens;
                EXPECT_NE(text.find("suppressed="), std::string::npos);
            }
            if (text.find("Async DFS write failed for key") != std::string::npos) {
                ++writes;
                EXPECT_NE(text.find("suppressed="), std::string::npos);
            }
        }
    } capture;
    adapter_->FailAllOpens(true);
    std::vector<std::string> keys;
    std::vector<std::string> values(32, std::string(4096, 'F'));
    for (int i = 0; i < 32; ++i) {
        keys.push_back("open_failure_" + std::to_string(i));
    }
    auto slices = MakeSlices(values);
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(results[i].has_value());
        ASSERT_TRUE(WaitForKeyGone(keys[i]));
        auto query = writer_->Query(keys[i]);
        ASSERT_TRUE(query.has_value());
        for (const auto& replica : query->replicas) {
            EXPECT_TRUE(replica.is_memory_replica());
        }
    }
    // Earlier tests can consume these process-wide log quotas. The limiter
    // unit tests verify first-log and suppressed-count behavior independently.
    EXPECT_LE(capture.opens.load(), 1);
    EXPECT_LE(capture.writes.load(), 1);
}

TEST_F(DfsBucketClientTest, BatchIoFailureRevokesAllKeysInMergedRun) {
    std::vector<std::string> keys{"bucket_mixed_ok", "bucket_mixed_fail"};
    std::vector<std::string> values{std::string(4096, 'G'),
                                    std::string(4096, 'H')};
    auto slices = MakeSlices(values);

    // A merged contiguous run is one filesystem operation. If that operation
    // fails, none of its entries is known durable, so every DFS replica in the
    // run must be revoked.
    adapter_->FailWriteCall(adapter_->WriteCalls() + 1);
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    ASSERT_TRUE(results[0].has_value());
    ASSERT_TRUE(results[1].has_value());

    ASSERT_TRUE(WaitForKeyGone(keys[0]));
    ASSERT_TRUE(WaitForKeyGone(keys[1]));
}

TEST_F(DfsBucketClientTest, MissingBackendKeepsCompletedMemoryReplica) {
    auto client_without_backend = CreateClient("127.0.0.1:18203");
    ASSERT_NE(client_without_backend, nullptr);
    std::string value(4096, 'I');
    std::vector<Slice> slices{{value.data(), value.size()}};

    auto result =
        client_without_backend->Put("bucket_no_backend", slices, DfsConfig());
    ASSERT_TRUE(result.has_value());
    auto query = client_without_backend->Query("bucket_no_backend");
    ASSERT_TRUE(query.has_value());
    ASSERT_TRUE(std::any_of(query->replicas.begin(), query->replicas.end(),
                            [](const auto& replica) {
                                return replica.is_memory_replica();
                            }));
    EXPECT_TRUE(std::none_of(query->replicas.begin(), query->replicas.end(),
                             [](const auto& replica) {
                                 return replica.is_dfs_replica();
                             }));
}

TEST_F(DfsBucketClientTest, RemoveFreesBucketSpaceAndKeyBecomesReusable) {
    const std::string key = "bucket_remove";
    std::string first(4096, 'R');
    std::vector<Slice> first_slices{{first.data(), first.size()}};
    ASSERT_TRUE(writer_->Put(key, first_slices, DfsConfig()).has_value());
    ASSERT_TRUE(WaitForDfsReplica(key));

    // force=true: WaitForDfsReplica polls Query, and every Query grants a fresh
    // read lease, so an unforced Remove would fail with OBJECT_HAS_LEASE.
    ASSERT_TRUE(writer_->Remove(key, /*force=*/true).has_value());
    auto removed = writer_->Query(key);
    ASSERT_FALSE(removed.has_value());

    // The key must be reusable, at a different offset (buckets are append-only).
    std::string second(4096, 'S');
    std::vector<Slice> second_slices{{second.data(), second.size()}};
    ASSERT_TRUE(writer_->Put(key, second_slices, DfsConfig()).has_value());
    ASSERT_TRUE(WaitForDfsReplica(key));
    ExpectDfsValue(key, second);
}

TEST_F(DfsBucketClientTest, UpsertKeepsSynchronousDfsSemantics) {
    const std::string key = "bucket_upsert";
    std::string initial(4096, 'U');
    std::vector<Slice> initial_slices{{initial.data(), initial.size()}};
    ASSERT_TRUE(writer_->Put(key, initial_slices, DfsConfig()).has_value());
    ASSERT_TRUE(WaitForDfsReplica(key));
    ExpectDfsValue(key, initial);
    auto initial_query = QueryDfsOnly(key);
    ASSERT_TRUE(initial_query.has_value());
    const auto initial_descriptor =
        initial_query->replicas[0].get_dfs_descriptor();

    // Upsert stays synchronous, so the new value is readable as soon as it
    // returns, but it must use a newly appended range.
    std::string updated(4096, 'V');
    std::vector<Slice> updated_slices{{updated.data(), updated.size()}};
    ASSERT_TRUE(writer_->Upsert(key, updated_slices, DfsConfig()).has_value());
    ExpectDfsValue(key, updated);
    auto updated_query = QueryDfsOnly(key);
    ASSERT_TRUE(updated_query.has_value());
    const auto& updated_descriptor =
        updated_query->replicas[0].get_dfs_descriptor();
    EXPECT_TRUE(updated_descriptor.file_path != initial_descriptor.file_path ||
                updated_descriptor.offset != initial_descriptor.offset);
}

TEST_F(DfsBucketClientTest, BatchUpsertKeepsSynchronousDfsSemantics) {
    std::vector<std::string> keys{"bucket_bupsert_0", "bucket_bupsert_1"};
    std::vector<std::string> initial{std::string(4096, 'C'),
                                     std::string(4096, 'D')};
    auto initial_slices = MakeSlices(initial);
    auto put_results = writer_->BatchPut(keys, initial_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    for (const auto& result : put_results) ASSERT_TRUE(result.has_value());
    for (const auto& key : keys) ASSERT_TRUE(WaitForDfsReplica(key));

    std::vector<std::string> updated{std::string(4096, 'E'),
                                     std::string(4096, 'F')};
    auto updated_slices = MakeSlices(updated);
    auto upsert_results =
        writer_->BatchUpsert(keys, updated_slices, DfsConfig());
    ASSERT_EQ(upsert_results.size(), keys.size());
    for (const auto& result : upsert_results) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }
    // BatchUpsert finishes the DFS write before returning.
    for (size_t i = 0; i < keys.size(); ++i) {
        ExpectDfsValue(keys[i], updated[i]);
    }
}

TEST_F(DfsBucketClientTest, BatchGetInvalidatesMissingBucket) {
    std::vector<std::string> keys{"missing_bucket_0", "missing_bucket_1",
                                  "missing_bucket_2", "missing_bucket_3"};
    std::vector<std::string> values{std::string(4096, 'P'),
                                    std::string(4096, 'Q'),
                                    std::string(4096, 'R'),
                                    std::string(4096, 'S')};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut(keys, write_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    for (const auto& result : put_results) ASSERT_TRUE(result.has_value());

    std::vector<QueryResult> queries;
    for (const auto& key : keys) {
        ASSERT_TRUE(WaitForDfsReplica(key));
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        queries.push_back(*query);
    }

    const auto& descriptor = queries.front().replicas.front()
                                 .get_dfs_descriptor();
    for (const auto& query : queries) {
        EXPECT_EQ(query.replicas.front().get_dfs_descriptor().shard_idx,
                  descriptor.shard_idx);
    }
    const auto bucket_path =
        std::filesystem::path(root_) /
        ("bucket_" +
         ImmutableBucketAllocator::FormatBucketId(descriptor.shard_idx) +
         ".data");
    ASSERT_TRUE(std::filesystem::remove(bucket_path));

    std::vector<std::string> output(1, std::string(4096, '\0'));
    std::unordered_map<std::string, std::vector<Slice>> read_slices;
    read_slices[keys.front()] = {{output.front().data(), output.front().size()}};
    auto get_results = writer_->BatchGet(
        {keys.front()}, {queries.front()}, read_slices);
    ASSERT_EQ(get_results.size(), 1);
    ASSERT_FALSE(get_results.front().has_value());
    EXPECT_EQ(get_results.front().error(), ErrorCode::FILE_NOT_FOUND);
    for (const auto& key : keys) {
        EXPECT_TRUE(WaitForKeyGone(key));
    }
}

TEST_F(DfsBucketClientTest, BatchGetReadsBucketDescriptors) {
    std::vector<std::string> keys{"bucket_get_0", "bucket_get_1"};
    std::vector<std::string> values{std::string(4096, 'N'),
                                    std::string(4096, 'O')};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut(keys, write_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    for (const auto& result : put_results) ASSERT_TRUE(result.has_value());
    for (const auto& key : keys) ASSERT_TRUE(WaitForDfsReplica(key));

    std::vector<QueryResult> queries;
    for (const auto& key : keys) {
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        queries.push_back(*query);
    }

    std::vector<std::string> output{std::string(4096, '\0'),
                                    std::string(4096, '\0')};
    std::unordered_map<std::string, std::vector<Slice>> read_slices;
    for (size_t i = 0; i < keys.size(); ++i) {
        read_slices[keys[i]] = {{output[i].data(), output[i].size()}};
    }
    auto get_results = writer_->BatchGet(keys, queries, read_slices);
    ASSERT_EQ(get_results.size(), keys.size());
    for (size_t i = 0; i < get_results.size(); ++i) {
        ASSERT_TRUE(get_results[i].has_value()) << "key " << keys[i];
        EXPECT_EQ(output[i], values[i]) << "key " << keys[i];
    }
}

TEST_F(DfsBucketClientTest, ReadTimeoutDoesNotChangeClientAccessPolicy) {
    const std::string key = "bucket_read_timeout_policy";
    const std::string value(4096, 'Q');
    std::vector<std::string> values{value};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut({key}, write_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), 1u);
    ASSERT_TRUE(put_results[0].has_value());
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());

    auto timeout_config = distributed_config_;
    timeout_config.batch_get_timeout_ms = 25;
    auto timeout_adapter = std::make_unique<FailingBucketFsAdapter>();
    adapter_ = timeout_adapter.get();
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = root_;
    backend_ = std::make_shared<DistributedStorageBackend>(
        file_config, timeout_config, std::move(timeout_adapter),
        writer_->GetDfsRootIdentity());
    ASSERT_TRUE(backend_->Init().has_value());
    writer_->SetDfsStorageBackend(backend_);

    auto output = std::make_shared<std::string>(value.size(), '\0');
    std::unordered_map<std::string, std::vector<Slice>> read_slices;
    read_slices[key] = {{output->data(), output->size()}};
    std::unordered_map<std::string, std::shared_ptr<void>> buffer_owners;
    buffer_owners.emplace(key, output);
    adapter_->BlockReads();
    const QueryResult expired(std::vector(query->replicas),
                              std::chrono::steady_clock::now(),
                              query->object_checksum);
    auto timed_out =
        writer_->BatchGet({key}, {expired}, read_slices, false, &buffer_owners);
    ASSERT_EQ(timed_out.size(), 1u);
    ASSERT_FALSE(timed_out[0].has_value());
    EXPECT_EQ(timed_out[0].error(), ErrorCode::DFS_BATCH_GET_TIMEOUT);
    EXPECT_TRUE(writer_->DfsReadAllowed());

    adapter_->ReleaseReads();
    ASSERT_TRUE(adapter_->WaitUntilReadDone(std::chrono::seconds(2)));
    ASSERT_TRUE(backend_->ProbeRead().has_value());
    std::fill(output->begin(), output->end(), '\0');
    auto recovered = writer_->BatchGet({key}, {*query}, read_slices, false,
                                       &buffer_owners);
    ASSERT_EQ(recovered.size(), 1u);
    ASSERT_TRUE(recovered[0].has_value());
    EXPECT_EQ(*output, value);
}

TEST_F(DfsBucketClientTest, DuplicateKeyTimeoutKeepsEveryBufferAlive) {
    const std::string key = "bucket_duplicate_key_timeout";
    const std::string value(4096, 'R');
    std::vector<std::string> values{value};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut({key}, write_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), 1u);
    ASSERT_TRUE(put_results[0].has_value());
    ASSERT_TRUE(WaitForDfsReplica(key));
    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());

    auto timeout_config = distributed_config_;
    timeout_config.batch_get_timeout_ms = 25;
    auto timeout_adapter = std::make_unique<FailingBucketFsAdapter>();
    adapter_ = timeout_adapter.get();
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = root_;
    backend_ = std::make_shared<DistributedStorageBackend>(
        file_config, timeout_config, std::move(timeout_adapter),
        writer_->GetDfsRootIdentity());
    ASSERT_TRUE(backend_->Init().has_value());
    writer_->SetDfsStorageBackend(backend_);

    auto first = std::make_shared<std::string>(value.size(), '\0');
    auto second = std::make_shared<std::string>(value.size(), '\0');
    std::weak_ptr<std::string> first_weak = first;
    std::weak_ptr<std::string> second_weak = second;
    std::vector<std::vector<Slice>> read_slices{
        {{first->data(), first->size()}},
        {{second->data(), second->size()}}};
    std::vector<std::shared_ptr<void>> buffer_owners{first, second};

    adapter_->BlockReads();
    auto timed_out = writer_->BatchGet({key, key}, {*query, *query},
                                       read_slices, false, &buffer_owners);
    first.reset();
    second.reset();
    buffer_owners.clear();

    ASSERT_EQ(timed_out.size(), 2u);
    ASSERT_FALSE(timed_out[0].has_value());
    ASSERT_FALSE(timed_out[1].has_value());
    EXPECT_EQ(timed_out[0].error(), ErrorCode::DFS_BATCH_GET_TIMEOUT);
    EXPECT_EQ(timed_out[1].error(), ErrorCode::DFS_BATCH_GET_TIMEOUT);
    EXPECT_FALSE(first_weak.expired());
    EXPECT_FALSE(second_weak.expired());

    adapter_->ReleaseReads();
    ASSERT_TRUE(adapter_->WaitUntilReadDone(std::chrono::seconds(2)));
    const auto owners_released_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((!first_weak.expired() || !second_weak.expired()) &&
           std::chrono::steady_clock::now() < owners_released_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(first_weak.expired());
    EXPECT_TRUE(second_weak.expired());
}

TEST_F(DfsBucketClientTest, ClientDestructionDrainsInFlightAsyncWrites) {
    // A client torn down immediately after BatchPut must not leave a task
    // touching its RPC client or backend. The drain in ~Client makes this
    // deterministic; ASan/TSan runs would catch a regression here.
    const std::string key = "bucket_drain";
    std::string value(65536, 'W');
    auto local_client = CreateClient("127.0.0.1:18204");
    ASSERT_NE(local_client, nullptr);
    local_client->SetDfsStorageBackend(backend_);

    std::vector<std::string> keys{key};
    std::vector<std::vector<Slice>> slices{
        {Slice{value.data(), value.size()}}};
    auto results = local_client->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());

    // Destroy while the DFS write may still be queued.
    local_client.reset();

    // The write either completed or was revoked, but the data must never be
    // partially visible: if the replica is COMPLETE, its bytes must be correct.
    auto query = QueryDfsOnly(key);
    if (query.has_value()) {
        for (const auto& replica : query->replicas) {
            if (replica.status != ReplicaStatus::COMPLETE) continue;
            std::vector<char> out(value.size());
            auto read = backend_->BatchRead(
                {{key, replica.get_dfs_descriptor(),
                  {{out.data(), out.size()}}}});
            ASSERT_EQ(read.size(), 1u);
            ASSERT_TRUE(read[0].has_value());
            EXPECT_EQ(std::memcmp(out.data(), value.data(), value.size()), 0);
        }
    }
}

TEST_F(DfsBucketClientTest, ZeroSizeAndEmptyKeyArePutRejected) {
    std::string value(16, 'Z');
    std::vector<Slice> empty_slices;
    auto zero_size = writer_->Put("bucket_zero", empty_slices, DfsConfig());
    EXPECT_FALSE(zero_size.has_value());

    std::vector<Slice> slices{{value.data(), value.size()}};
    auto empty_key = writer_->Put("", slices, DfsConfig());
    EXPECT_FALSE(empty_key.has_value());
}

TEST_F(DfsBucketClientTest, ObjectLargerThanBucketCapacityIsRejected) {
    // bucket_capacity is 1 MiB; a 2 MiB object can never be placed
    // contiguously, so PutStart must fail instead of writing out of bounds.
    std::string value(2 * 1024 * 1024, 'B');
    std::vector<Slice> slices{{value.data(), value.size()}};
    auto result = writer_->Put("bucket_too_large", slices, DfsConfig());
    ASSERT_FALSE(result.has_value());
    auto query = writer_->Query("bucket_too_large");
    EXPECT_FALSE(query.has_value());

    staging_device.Reset(value.data(), value.size());
    const std::vector<std::string> keys{"bucket_batch_too_large"};
    std::vector<std::vector<Slice>> batch_slices{slices};
    auto results = writer_->BatchPut(keys, batch_slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(staging_device.copies, 0);
    auto batch_query = writer_->Query(keys[0]);
    EXPECT_FALSE(batch_query.has_value());
}

TEST_F(DfsBucketClientTest, ConcurrentBatchPutsFromMultipleThreads) {
    constexpr int kThreads = 4;
    constexpr int kKeysPerThread = 4;
    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> all_keys(kThreads);
    std::vector<std::vector<std::string>> all_values(kThreads);
    std::atomic<int> failures{0};

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, t, &all_keys, &all_values, &failures]() {
            auto& keys = all_keys[t];
            auto& values = all_values[t];
            for (int i = 0; i < kKeysPerThread; ++i) {
                keys.push_back("conc_t" + std::to_string(t) + "_k" +
                               std::to_string(i));
                values.push_back(std::string(
                    1024, static_cast<char>('a' + (t * kKeysPerThread + i) %
                                                     26)));
            }
            auto slices = MakeSlices(values);
            auto results = writer_->BatchPut(keys, slices, DfsConfig());
            if (results.size() != keys.size()) {
                ++failures;
                return;
            }
            for (const auto& result : results) {
                if (!result.has_value()) ++failures;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(failures.load(), 0);

    for (int t = 0; t < kThreads; ++t) {
        for (size_t i = 0; i < all_keys[t].size(); ++i) {
            ASSERT_TRUE(WaitForDfsReplica(all_keys[t][i]))
                << "key " << all_keys[t][i];
            ExpectDfsValue(all_keys[t][i], all_values[t][i]);
        }
    }
}

}  // namespace mooncake::test
