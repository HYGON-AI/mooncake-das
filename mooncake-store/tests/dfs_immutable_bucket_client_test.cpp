#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
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
    int WriteCalls() const { return write_calls_.load(); }
    void FailWriteCall(int call) { fail_write_call_.store(call); }
    void FailAllWrites(bool fail) { fail_all_writes_.store(fail); }
    void BlockWrites() {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_ = true;
    }
    void ReleaseWrites() {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_ = false;
        cv_.notify_all();
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
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

   private:
    std::atomic<int> write_calls_{0};
    std::atomic<int> fail_write_call_{-1};
    std::atomic<bool> fail_all_writes_{false};
    std::mutex mutex_;
    std::condition_variable cv_;
    bool blocked_ = false;
};

}  // namespace

class DfsBucketClientTest : public ::testing::Test {
   protected:
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
        // Keep the shard fields valid; they are unused in bucket mode.
        SetEnv("MOONCAKE_DFS_SHARD_COUNT", "1");
        SetEnv("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");

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
        if (adapter_) adapter_->ReleaseWrites();
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

    ReplicateConfig DfsConfig() const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        return config;
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
        return QueryResult(std::move(replicas), query->lease_timeout);
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
       ChecksumStagingFailureFallsBackAndReclaimsAllocations) {
    for (const bool throw_copy : {false, true}) {
        std::vector<std::string> keys{"checksum_stage_fail"};
        std::string value(4099, 'F');
        staging_device.Reset(value.data(), value.size());
        staging_device.fail_copy = 1;
        staging_device.throw_copy = throw_copy;
        std::vector<std::vector<Slice>> slices{{{value.data(), value.size()}}};
        auto results = writer_->BatchPut(keys, slices, DfsConfig());
        ASSERT_EQ(results.size(), 1u);
        ASSERT_FALSE(results[0].has_value());  // BUCKET requires both replicas.
        EXPECT_EQ(staging_device.copies,
                  Environ::Get().GetStoreChecksumEnabled() ? 2 : 1);
        auto query = writer_->Query(keys[0]);
        ASSERT_FALSE(query.has_value());
        EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);
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

TEST_F(DfsBucketClientTest, ChecksumMissingBackendKeepsOriginalD2hPath) {
    writer_->SetDfsStorageBackend(nullptr);
    const std::vector<std::string> keys{"checksum_no_backend"};
    std::string value(4099, 'F');
    staging_device.Reset(value.data(), value.size());
    std::vector<std::vector<Slice>> slices{{{value.data(), value.size()}}};
    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(staging_device.copies,
              Environ::Get().GetStoreChecksumEnabled() ? 1 : 0);
    auto query = writer_->Query(keys[0]);
    ASSERT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);
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

TEST_F(DfsBucketClientTest, FailedAsyncWriteRevokesOnlyTheDfsReplica) {
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

TEST_F(DfsBucketClientTest, MissingBackendFailsPutWithoutStrandingMetadata) {
    auto client_without_backend = CreateClient("127.0.0.1:18203");
    ASSERT_NE(client_without_backend, nullptr);
    std::string value(4096, 'I');
    std::vector<Slice> slices{{value.data(), value.size()}};

    auto result =
        client_without_backend->Put("bucket_no_backend", slices, DfsConfig());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    auto query = client_without_backend->Query("bucket_no_backend");
    ASSERT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);
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
