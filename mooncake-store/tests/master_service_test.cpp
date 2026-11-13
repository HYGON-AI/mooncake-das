#include <cerrno>
#include <iterator>
#include "utils.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "storage/distributed/dfs_allocator_interface.h"
#include "storage/distributed/dfs_root_identity.h"
#include "storage/distributed/dfs_read_probe.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "master_service.h"
#include "common/shrink_buckets.h"
#include "config/replica_selection_config.h"
#include "master_service/master_service_test_peer.h"
#include "rpc_service.h"
#include "storage/distributed/kvcs/kvcs_capacity_provider.h"
#include "storage/distributed/kvcs/kvcs_driver.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unordered_set>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <condition_variable>
#include <mutex>

#include "tenant/quota_policy_store.h"
#include "random.h"
#include "types.h"
#include "master_service_test_fixture.h"

namespace mooncake::test {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        Capture();
        ::unsetenv(name_.c_str());
    }

    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        Capture();
        ::setenv(name_.c_str(), value, 1);
    }

    ~ScopedEnvVar() {
        if (previous_value_.has_value()) {
            ::setenv(name_.c_str(), previous_value_->c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }

   private:
    void Capture() {
        const char* value = ::getenv(name_.c_str());
        if (value != nullptr) {
            previous_value_ = value;
        }
    }

    std::string name_;
    std::optional<std::string> previous_value_;
};

class ScopedDfsFallbackLogCapture : public google::LogSink {
   public:
    ScopedDfsFallbackLogCapture() { google::AddLogSink(this); }
    ~ScopedDfsFallbackLogCapture() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity severity, const char* full_filename,
              const char* base_filename, int line, const struct ::tm* tm_time,
              const char* message, size_t message_len) override {
        (void)severity;
        (void)full_filename;
        (void)base_filename;
        (void)line;
        (void)tm_time;
        const std::string text(message, message_len);
        std::lock_guard<std::mutex> lock(mutex_);
        if (text.find("Failed to allocate DFS replica for key=") !=
                std::string::npos ||
            text.find("DFS batch allocate active bucket failed") !=
                std::string::npos) {
            errors_.push_back(text);
        }
        if (text.find("action=dfs_replica_skipped_memory_fallback") ==
            std::string::npos) {
            return;
        }
        messages_.push_back(text);
    }

    std::vector<std::string> messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return messages_;
    }

    std::vector<std::string> errors() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return errors_;
    }

   private:
    mutable std::mutex mutex_;
    std::vector<std::string> messages_;
    std::vector<std::string> errors_;
};

namespace {

class BlockingDfsAllocator : public DfsAllocatorInterface {
   public:
    explicit BlockingDfsAllocator(bool blocked) : blocked_(blocked) {}

    DfsAllocatorType Type() const override { return DfsAllocatorType::SHARD; }
    tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig&) override {
        return {};
    }
    bool IsInitialized() const override { return true; }
    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string&, uint64_t) override {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    std::vector<BatchAllocateResult> BatchAllocate(
        const std::vector<BatchAllocateRequest>&) override {
        return {};
    }
    void Free(const std::string&, const DistributedFSDescriptor&) override {}
    void UpdateAccess(const std::string&,
                      const DistributedFSDescriptor&) override {}
    bool IsEvictionEnabled() const override {
        std::unique_lock<std::mutex> lock(mutex_);
        ++checks;
        cv_.wait(lock, [this] { return !blocked_; });
        return false;
    }
    std::chrono::seconds GetEvictionCheckInterval() const override {
        return std::chrono::seconds(1);
    }
    uint64_t GetTotalCapacity() const override { return 0; }
    uint64_t GetUsedBytes() const override { return 0; }
    uint64_t GetFileCount() const override { return 0; }

    void Unblock() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            blocked_ = false;
        }
        cv_.notify_all();
    }

    mutable std::atomic<size_t> checks{0};

   private:
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    bool blocked_;
};

// Applies the environment needed to run the master with the BUCKET allocator.
class ScopedBucketDfsEnv {
   public:
    ScopedBucketDfsEnv(const std::string& root, const char* bucket_capacity,
                       const char* max_bucket_count,
                       const char* eviction_enabled = "0",
                       const char* high_watermark = "0.9",
                       const char* low_watermark = "0.7")
        : enable_dfs_("MOONCAKE_ENABLE_DFS", "1"),
          fs_adapter_("MOONCAKE_DFS_FS_ADAPTER", "posix"),
          root_dir_("MOONCAKE_DFS_ROOT_DIR", root.c_str()),
          allocator_type_("MOONCAKE_DFS_ALLOCATOR_TYPE", "bucket"),
          bucket_capacity_("MOONCAKE_DFS_BUCKET_CAPACITY", bucket_capacity),
          max_bucket_count_("MOONCAKE_DFS_MAX_BUCKET_COUNT", max_bucket_count),
          alignment_("MOONCAKE_DFS_ALIGNMENT", "4096"),
          eviction_("MOONCAKE_DFS_EVICTION_ENABLED", eviction_enabled),
          high_watermark_("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK",
                          high_watermark),
          low_watermark_("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", low_watermark),
          deferred_free_("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0"),
          single_tenant_("MOONCAKE_DFS_SINGLE_TENANT", "true"),
          shard_count_("MOONCAKE_DFS_SHARD_COUNT", "1"),
          shard_capacity_("MOONCAKE_DFS_SHARD_CAPACITY", "1048576") {}

   private:
    ScopedEnvVar enable_dfs_;
    ScopedEnvVar fs_adapter_;
    ScopedEnvVar root_dir_;
    ScopedEnvVar allocator_type_;
    ScopedEnvVar bucket_capacity_;
    ScopedEnvVar max_bucket_count_;
    ScopedEnvVar alignment_;
    ScopedEnvVar eviction_;
    ScopedEnvVar high_watermark_;
    ScopedEnvVar low_watermark_;
    ScopedEnvVar deferred_free_;
    ScopedEnvVar single_tenant_;
    ScopedEnvVar shard_count_;
    ScopedEnvVar shard_capacity_;
};

std::string MakeDfsTestRoot(const std::string& suffix) {
    static std::atomic<int> counter{0};
    auto path = std::filesystem::temp_directory_path() /
                ("master_dfs_bucket_" + std::to_string(::getpid()) + "_" +
                 suffix + "_" + std::to_string(++counter));
    std::filesystem::create_directories(path);
    return path.string();
}

const Replica::Descriptor* FindDfsDescriptor(
    const std::vector<Replica::Descriptor>& replicas) {
    for (const auto& replica : replicas) {
        if (replica.is_dfs_replica()) return &replica;
    }
    return nullptr;
}

std::optional<int64_t> ReadPrometheusCounter(const std::string& serialized,
                                             const std::string& name) {
    const std::string line_prefix = name + " ";
    size_t line_begin = 0;
    while (line_begin < serialized.size()) {
        const size_t line_end = serialized.find('\n', line_begin);
        if (serialized.compare(line_begin, line_prefix.size(), line_prefix) ==
            0) {
            const size_t value_begin = line_begin + line_prefix.size();
            const size_t value_end = line_end == std::string::npos
                                         ? serialized.size()
                                         : line_end;
            return std::stoll(
                serialized.substr(value_begin, value_end - value_begin));
        }
        if (line_end == std::string::npos) break;
        line_begin = line_end + 1;
    }
    return std::nullopt;
}

const std::vector<std::string>& DfsBucketEvictionMetricNames() {
    static const std::vector<std::string> names = {
        "master_dfs_bucket_eviction_attempts_total",
        "master_dfs_bucket_evictions_total",
        "master_dfs_bucket_low_utilization_eviction_attempts_total",
        "master_dfs_bucket_low_utilization_evictions_total",
        "master_dfs_bucket_evicted_replicas_total",
        "master_dfs_bucket_evicted_bytes_total",
        "master_attempted_evictions_total",
        "master_successful_evictions_total",
        "master_evicted_key_count",
        "master_evicted_size_bytes",
        "master_attempted_evictions_mem",
        "master_successful_evictions_mem",
        "master_evicted_key_count_mem",
        "master_evicted_size_bytes_mem",
        "master_attempted_evictions_nof",
        "master_successful_evictions_nof",
        "master_evicted_key_count_nof",
        "master_evicted_size_bytes_nof",
    };
    return names;
}

using DfsBucketEvictionMetrics = std::map<std::string, int64_t>;

DfsBucketEvictionMetrics CaptureDfsBucketEvictionMetrics() {
    const auto serialized = MasterMetricManager::instance().serialize_metrics();
    DfsBucketEvictionMetrics metrics;
    const auto& names = DfsBucketEvictionMetricNames();
    for (size_t i = 0; i < 6; ++i) {
        const auto& name = names[i];
        const auto value = ReadPrometheusCounter(serialized, name);
        EXPECT_TRUE(value.has_value()) << "missing metric " << name;
        metrics.emplace(name, value.value_or(-1));
    }

    // Read legacy counters through getters because not all of their series are
    // exported. Keep checking their values without changing their export.
    auto& manager = MasterMetricManager::instance();
    metrics.emplace(names[6], manager.get_eviction_attempts());
    metrics.emplace(names[7], manager.get_eviction_success());
    metrics.emplace(names[8], manager.get_evicted_key_count());
    metrics.emplace(names[9], manager.get_evicted_size());
    metrics.emplace(names[10], manager.get_mem_eviction_attempts());
    metrics.emplace(names[11], manager.get_mem_eviction_success());
    metrics.emplace(names[12], manager.get_mem_evicted_key_count());
    metrics.emplace(names[13], manager.get_mem_evicted_size());
    metrics.emplace(names[14], manager.get_nof_eviction_attempts());
    metrics.emplace(names[15], manager.get_nof_eviction_success());
    metrics.emplace(names[16], manager.get_nof_evicted_key_count());
    metrics.emplace(names[17], manager.get_nof_evicted_size());
    return metrics;
}

std::optional<int64_t> ReadDfsReplicaGauge(const std::string& serialized,
                                           const std::string& status) {
    return ReadPrometheusCounter(
        serialized, "master_dfs_replicas{status=\"" + status + "\"}");
}

}  // namespace

struct KvcsWatermarkDriverState {
    std::vector<ObjectKey> deleted_keys;
    bool fail_delete{false};
};

class KvcsWatermarkDriver final : public KvcsDriver {
   public:
    explicit KvcsWatermarkDriver(
        std::shared_ptr<KvcsWatermarkDriverState> state)
        : state_(std::move(state)) {}

    tl::expected<void, ErrorCode> Init() override { return {}; }
    uint64_t MaxValueSize() const override { return 4 * 1024 * 1024; }
    tl::expected<std::vector<KvcsMountpointStat>, ErrorCode>
    GetMountpointStats() override {
        return std::vector<KvcsMountpointStat>{{.index = 1,
                                                .id = "target-mp",
                                                .total_bytes = 100,
                                                .used_bytes = 95}};
    }
    KvcsIoResults BatchPut(std::span<const KvcsIoRequest> requests) override {
        return Errors(requests.size(), ErrorCode::NOT_SUPPORTED);
    }
    KvcsIoResults BatchGet(std::span<const KvcsIoRequest> requests) override {
        return Errors(requests.size(), ErrorCode::NOT_SUPPORTED);
    }
    KvcsIoResults BatchDelete(std::span<const ObjectKey> keys) override {
        KvcsIoResults results;
        results.reserve(keys.size());
        for (const auto& key : keys) {
            state_->deleted_keys.push_back(key);
            if (state_->fail_delete) {
                results.emplace_back(tl::make_unexpected(ErrorCode::RPC_FAIL));
            } else {
                results.emplace_back();
            }
        }
        return results;
    }

   private:
    static KvcsIoResults Errors(size_t count, ErrorCode error) {
        return KvcsIoResults(count, tl::make_unexpected(error));
    }

    std::shared_ptr<KvcsWatermarkDriverState> state_;
};

TEST_F(MasterServiceTest,
       KvcsStorageModeSkipsFilesystemAllocatorRecoveryRestriction) {
    ScopedEnvVar clear_enable_dfs("MOONCAKE_ENABLE_DFS");
    ScopedEnvVar clear_dfs_enabled("MOONCAKE_DFS_ENABLED");
    ScopedEnvVar kvcs_adapter("MOONCAKE_DFS_FS_ADAPTER", "kvcs-lowlevel");
    MasterServiceConfig config;
    config.enable_ha = true;
    config.enable_oplog = true;
    config.ha_backend_type = "etcd";
    MasterService service(config);
    EXPECT_TRUE(MasterServiceTestPeer::EnableDfs(service));
    EXPECT_EQ(MasterServiceTestPeer::DfsAllocator(service), nullptr);
    EXPECT_TRUE(MasterServiceTestPeer::UsesKvcs(service));
}

TEST_F(MasterServiceTest,
       KvcsCapacityWatermarkEvictsDfsReplicaAndProviderData) {
    ScopedEnvVar clear_enable_dfs("MOONCAKE_ENABLE_DFS");
    ScopedEnvVar clear_dfs_enabled("MOONCAKE_DFS_ENABLED");
    ScopedEnvVar clear_adapter("MOONCAKE_DFS_FS_ADAPTER");

    MasterServiceConfig config;
    config.default_kv_lease_ttl = 1;
    config.default_kv_soft_pin_ttl = 1;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    MasterServiceTestPeer peer(service);
    peer.EvictionRunning(service).store(false, std::memory_order_relaxed);
    if (peer.EvictionThread(service).joinable()) {
        peer.EvictionThread(service).join();
    }

    auto driver_state = std::make_shared<KvcsWatermarkDriverState>();
    peer.ConfigureKvcsCapacityForTesting(
        std::make_unique<KvcsCapacityProvider>(
            1, "target-mp",
            std::make_unique<KvcsWatermarkDriver>(driver_state)),
        true, 0.9, 0.7);

    ReplicateConfig replicate;
    replicate.replica_num = 1;
    replicate.dfs_replica_num = 1;
    constexpr std::string_view key = "kvcs-watermark";
    ASSERT_TRUE(service.PutStart(context.client_id, std::string(key),
                                 TenantId::Default(), 64, replicate));
    ASSERT_TRUE(service.PutEnd(context.client_id, std::string(key),
                               TenantId::Default(), ReplicaType::ALL));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    driver_state->fail_delete = true;
    peer.RefreshKvcsDfsUsageForTesting();
    auto unchanged =
        service.GetReplicaList(std::string(key), TenantId::Default());
    ASSERT_TRUE(unchanged);
    ASSERT_EQ(unchanged->replicas.size(), 2u);
    ASSERT_EQ(driver_state->deleted_keys.size(), 1u);

    driver_state->fail_delete = false;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    peer.RefreshKvcsDfsUsageForTesting();

    auto remaining =
        service.GetReplicaList(std::string(key), TenantId::Default());
    ASSERT_TRUE(remaining);
    ASSERT_EQ(remaining->replicas.size(), 1u);
    EXPECT_TRUE(remaining->replicas.front().is_memory_replica());
    ASSERT_EQ(driver_state->deleted_keys.size(), 2u);
    EXPECT_EQ(driver_state->deleted_keys.back(), EncodeKvcsKey(key));

    const auto usage = service.GetStorageUsageSnapshot().dfs;
    EXPECT_TRUE(usage.enabled);
    EXPECT_EQ(usage.used_bytes, 95u);
    EXPECT_EQ(usage.capacity_bytes, 100u);
}

TEST_F(MasterServiceTest,
       FilesystemStorageModeRetainsAllocatorRecoveryRestriction) {
    ScopedEnvVar clear_enable_dfs("MOONCAKE_ENABLE_DFS");
    ScopedEnvVar clear_dfs_enabled("MOONCAKE_DFS_ENABLED");

    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_recovery_restriction_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    MasterServiceConfig config;
    config.enable_ha = true;
    config.enable_oplog = true;
    config.ha_backend_type = "etcd";
    EXPECT_THROW({ MasterService service(config); }, std::invalid_argument);
    std::filesystem::remove_all(dfs_root);
}

TEST_F(MasterServiceTest, KvcsStandbyRestoreAcceptsObjectStorageDfsReplica) {
    ScopedEnvVar clear_enable_dfs("MOONCAKE_ENABLE_DFS");
    ScopedEnvVar clear_dfs_enabled("MOONCAKE_DFS_ENABLED");
    ScopedEnvVar kvcs_adapter("MOONCAKE_DFS_FS_ADAPTER", "kvcs-lowlevel");

    MasterServiceConfig config;
    config.enable_ha = true;
    config.enable_oplog = true;
    config.ha_backend_type = "etcd";
    MasterService service(config);

    constexpr uint64_t kObjectSize = 128;
    DistributedFSDescriptor dfs_descriptor;
    dfs_descriptor.object_size = kObjectSize;
    dfs_descriptor.aligned_size = kObjectSize;
    dfs_descriptor.shard_idx = -1;
    Replica replica(std::move(dfs_descriptor), ReplicaStatus::COMPLETE);

    StandbyObjectMetadata metadata;
    metadata.client_id = generate_uuid();
    metadata.size = kObjectSize;
    metadata.replicas.push_back(replica.get_descriptor());

    const std::string key = "restored-kvcs-dfs-object";
    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {{std::string(TenantId::kDefaultValue), key, metadata}},
                        /*initial_oplog_sequence_id=*/0, {})
                    .has_value());

    auto response = service.GetReplicaListForAdmin(key, TenantId::Default());
    ASSERT_TRUE(response);
    ASSERT_EQ(response->replicas.size(), 1u);
    ASSERT_TRUE(response->replicas[0].is_dfs_replica());
    const auto& restored = response->replicas[0].get_dfs_descriptor();
    EXPECT_EQ(restored.object_size, kObjectSize);
}

TEST_F(MasterServiceTest, KvcsMissEvictsOnlyTheObservedDfsReplica) {
    ScopedEnvVar clear_enable_dfs("MOONCAKE_ENABLE_DFS");
    ScopedEnvVar clear_dfs_enabled("MOONCAKE_DFS_ENABLED");
    ScopedEnvVar kvcs_adapter("MOONCAKE_DFS_FS_ADAPTER", "kvcs-lowlevel");

    MasterService service;
    const UUID client_id = generate_uuid();
    const std::string key = "missing-kvcs-value";
    DistributedFSDescriptor descriptor;
    descriptor.object_size = 128;
    descriptor.aligned_size = 128;
    descriptor.shard_idx = -1;
    Replica first(descriptor, ReplicaStatus::COMPLETE);
    Replica second(descriptor, ReplicaStatus::COMPLETE);
    const ReplicaID first_id = first.id();
    const ReplicaID second_id = second.id();
    ASSERT_NE(first_id, second_id);

    StandbyObjectMetadata metadata;
    metadata.client_id = client_id;
    metadata.size = descriptor.object_size;
    metadata.replicas = {first.get_descriptor(), second.get_descriptor()};
    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {{std::string(TenantId::kDefaultValue), key, metadata}},
                        /*initial_oplog_sequence_id=*/0, {})
                    .has_value());

    auto unconditional = service.EvictDiskReplica(
        client_id, key, TenantId::Default(), ReplicaType::DFS);
    ASSERT_FALSE(unconditional.has_value());
    EXPECT_EQ(unconditional.error(), ErrorCode::INVALID_PARAMS);

    ReplicaID missing_id = 0;
    while (missing_id == first_id || missing_id == second_id) ++missing_id;
    auto stale = service.EvictDiskReplicaIfCurrent(
        client_id, key, TenantId::Default(), ReplicaType::DFS, missing_id);
    ASSERT_TRUE(stale.has_value());
    EXPECT_FALSE(*stale);
    ASSERT_TRUE(service.GetReplicaList(key, TenantId::Default()).has_value());

    auto current = service.EvictDiskReplicaIfCurrent(
        client_id, key, TenantId::Default(), ReplicaType::DFS, first_id);
    ASSERT_TRUE(current.has_value());
    EXPECT_TRUE(*current);
    auto remaining = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(remaining.has_value());
    ASSERT_EQ(remaining->replicas.size(), 1u);
    EXPECT_EQ(remaining->replicas.front().id, second_id);

    current = service.EvictDiskReplicaIfCurrent(
        client_id, key, TenantId::Default(), ReplicaType::DFS, second_id);
    ASSERT_TRUE(current.has_value());
    EXPECT_TRUE(*current);
    auto missing = service.GetReplicaList(key, TenantId::Default());
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(TenantScopedStorageKeyTest, RoundTripsAndParsesLegacyKeys) {
    const auto scoped =
        TenantId("tenant:with:colon").MakeScopedKey("path/key:with:colon");
    EXPECT_NE(scoped.find('\0'), std::string::npos);

    auto [tenant_id, key] = TenantId::ParseScopedKey(scoped);
    EXPECT_EQ(tenant_id.value(), "tenant:with:colon");
    EXPECT_EQ(key, "path/key:with:colon");

    auto [default_tenant, default_key] = TenantId::ParseScopedKey("raw_key");
    EXPECT_EQ(default_tenant.value(), TenantId::kDefaultValue);
    EXPECT_EQ(default_key, "raw_key");

    std::string legacy = "legacy_tenant";
    legacy.push_back('\0');
    legacy.append("legacy_key");
    auto [legacy_tenant, legacy_key] = TenantId::ParseScopedKey(legacy);
    EXPECT_EQ(legacy_tenant.value(), "legacy_tenant");
    EXPECT_EQ(legacy_key, "legacy_key");
}

std::string GenerateKeyForSegment(const UUID& client_id,
                                  const std::unique_ptr<MasterService>& service,
                                  const std::string& segment_name) {
    static std::atomic<uint64_t> counter(0);

    while (true) {
        std::string key = "key_" + std::to_string(counter.fetch_add(1));
        std::vector<Replica::Descriptor> replica_list;

        // Check if the key already exists.
        auto exist_result = service->ExistKey(key, TenantId::Default());
        if (exist_result.has_value() && exist_result.value()) {
            continue;  // Retry if the key already exists
        }

        // Attempt to put the key.
        auto put_result = service->PutStart(client_id, key, TenantId::Default(),
                                            {1024}, {.replica_num = 1});
        if (put_result.has_value()) {
            replica_list = std::move(put_result.value());
        }
        ErrorCode code =
            put_result.has_value() ? ErrorCode::OK : put_result.error();

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;  // Retry if the key already exists
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }
        auto put_end_result = service->PutEnd(
            client_id, key, TenantId::Default(), ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_ == segment_name) {
            return key;
        }
        // Clean up failed attempt
        auto remove_result = service->Remove(key, TenantId::Default());
        if (!remove_result.has_value()) {
            // Ignore cleanup failure
        }
    }
}

TEST_F(MasterServiceTest, HcaHintFallsBackForLargeOffsetObject) {
    MasterService service(MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build());
    const auto owner = generate_uuid();
    constexpr size_t mib = 1024 * 1024;
    auto matching = MakeSegment("receiver");
    matching.size = 16 * mib;
    matching.locality = SegmentLocality{7, {"mlx5_0"}};
    auto original = matching;
    original.id = generate_uuid();
    original.base += matching.size;
    original.size = 32 * mib;
    original.locality.reset();
    ASSERT_TRUE(service.MountSegment(matching, owner));
    ASSERT_TRUE(service.MountSegment(original, owner));
    ReplicateConfig request;
    request.preferred_hca_names = std::vector<std::string>{"mlx5_0"};
    const auto result = service.PutStart(
        owner, "large-hca-object", TenantId::Default(), 24 * mib, request);
    ASSERT_TRUE(result) << toString(result.error());
    const auto& buffer = (*result)[0].get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(buffer.size_, 24 * mib);
    EXPECT_GE(buffer.buffer_address_, original.base);
    EXPECT_LT(buffer.buffer_address_, original.base + original.size);
}

TEST_F(MasterServiceTest, MixedGpuBatchUsesEachKeysHcaHint) {
    if (!StoreHcaAffinityEnabled()) GTEST_SKIP();
    MasterService service(MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build());
    const auto owner = generate_uuid();
    auto first = MakeSegment("receiver");
    first.protocol = "rdma";
    first.locality = SegmentLocality{7, {"mlx5_0"}};
    auto second = first;
    second.id = generate_uuid();
    second.base += first.size;
    second.locality = SegmentLocality{0, {"mlx5_1"}};
    ASSERT_TRUE(service.MountSegment(first, owner));
    ASSERT_TRUE(service.MountSegment(second, owner));
    ReplicateConfig config;
    config.per_key_preferred_hca_names =
        std::vector<std::vector<std::string>>{{"mlx5_0"}, {"mlx5_1"}};
    auto results = service.BatchUpsertStart(
        owner, {"gpu0", "gpu1"}, TenantId::Default(), {1024, 1024}, config);
    ASSERT_EQ(results.size(), 2);
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_TRUE(results[i]);
        const auto& memory = (*results[i])[0].get_memory_descriptor();
        ASSERT_TRUE(memory.locality);
        EXPECT_EQ(memory.locality->numa_node, i == 0 ? 7 : 0);
    }
    config.per_key_preferred_hca_names->pop_back();
    results =
        service.BatchUpsertStart(owner, {"invalid0", "invalid1"},
                                 TenantId::Default(), {1024, 1024}, config);
    for (const auto& result : results) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_F(MasterServiceTest, PutAndBatchUpsertHonorReceiverHcaLocality) {
    if (!StoreHcaAffinityEnabled()) GTEST_SKIP();
    auto config = MasterServiceConfig::builder()
                      .set_memory_allocator(BufferAllocatorType::OFFSET)
                      .build();
    MasterService service(config);
    const auto owner = generate_uuid();
    auto other = MakeSegment("receiver");
    other.protocol = "rdma";
    other.locality = SegmentLocality{0, {"mlx5_1"}};
    auto matching = other;
    matching.id = generate_uuid();
    matching.base += matching.size;
    matching.locality = SegmentLocality{7, {"mlx5_0"}};
    ASSERT_TRUE(service.MountSegment(other, owner));
    ASSERT_TRUE(service.MountSegment(matching, owner));
    ReplicateConfig request;
    request.preferred_hca_names = std::vector<std::string>{"mlx5_0"};
    auto result = service.PutStart(owner, "hca-put", TenantId::Default(),
                                   {1024}, request);
    ASSERT_TRUE(result);
    const auto address =
        (*result)[0].get_memory_descriptor().buffer_descriptor.buffer_address_;
    EXPECT_GE(address, matching.base);
    EXPECT_LT(address, matching.base + matching.size);
    auto conflicting = matching;
    conflicting.locality = SegmentLocality{0, {"mlx5_1"}};
    auto remounted = service.ReMountSegment({conflicting}, owner);
    ASSERT_FALSE(remounted);
    EXPECT_EQ(remounted.error(), ErrorCode::INVALID_PARAMS);
    auto batch =
        service.BatchUpsertStart(owner, {"hca-upsert-a", "hca-upsert-b"},
                                 TenantId::Default(), {1024, 1024}, request);
    ASSERT_EQ(batch.size(), 2);
    for (const auto& entry : batch) {
        ASSERT_TRUE(entry);
        const auto target = (*entry)[0]
                                .get_memory_descriptor()
                                .buffer_descriptor.buffer_address_;
        EXPECT_GE(target, matching.base);
        EXPECT_LT(target, matching.base + matching.size);
    }
}

TEST_F(MasterServiceTest, DisabledAffinityIgnoresExplicitHints) {
    if (StoreHcaAffinityEnabled()) GTEST_SKIP();
    MasterService service(
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_allocation_strategy_type(AllocationStrategyType::RANDOM)
            .build());
    const auto owner = generate_uuid();
    auto first = MakeSegment("receiver");
    first.locality = SegmentLocality{0, {"mlx5_0"}};
    auto second = first;
    second.id = generate_uuid();
    second.base += first.size;
    second.locality = SegmentLocality{1, {"mlx5_1"}};
    ASSERT_TRUE(service.MountSegment(first, owner));
    ASSERT_TRUE(service.MountSegment(second, owner));
    const auto engine = threadLocalRandomEngine();
    auto baseline =
        service.PutStart(owner, "without-hint", TenantId::Default(), 1024, {});
    ASSERT_TRUE(baseline);
    const auto locality = baseline->front().get_memory_descriptor().locality;
    ASSERT_TRUE(locality);
    ReplicateConfig request;
    request.preferred_hca_names = std::vector<std::string>{
        locality->numa_node == 0 ? "mlx5_1" : "mlx5_0"};
    threadLocalRandomEngine() = engine;
    auto hinted = service.PutStart(owner, "with-hint", TenantId::Default(),
                                   1024, request);
    ASSERT_TRUE(hinted);
    EXPECT_EQ(hinted->front().get_memory_descriptor().locality, locality);
}

TEST_F(MasterServiceTest, DuplicateMountEnrichesOnlyUnknownLocality) {
    MasterService service(MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build());
    const auto owner = generate_uuid();
    auto legacy = MakeSegment("legacy-locality");
    ASSERT_TRUE(service.MountSegment(legacy, owner));
    auto known = legacy;
    known.locality = SegmentLocality{7, {"mlx5_0"}};
    ASSERT_TRUE(service.MountSegment(known, owner));
    ASSERT_TRUE(service.MountSegment(legacy, owner));
    auto replicas = service.PutStart(owner, "enriched-locality",
                                     TenantId::Default(), 1024, {});
    ASSERT_TRUE(replicas);
    EXPECT_EQ(replicas->front().get_memory_descriptor().locality,
              known.locality);
}

TEST_F(MasterServiceTest, MountUnmountSegmentWithCachelibAllocator) {
    // Create a MasterService instance for testing.
    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::CACHELIB)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Base is not aligned
    segment.base = original_base + 1;
    segment.size = original_size;
    auto mount_result3 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result3.error());

    // Size is not aligned
    segment.base = original_base;
    segment.size = original_size + 1;
    auto mount_result4 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result4.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result4.error());

    // Test normal mount operation.
    segment.base = original_base;
    segment.size = original_size;
    auto mount_result5 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result5.has_value());

    // Test mounting the same segment again (idempotent request should succeed).
    auto mount_result6 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result6.has_value());

    // Test unmounting the segment.
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result1.has_value());

    // Test unmounting the same segment again (idempotent request should
    // succeed).
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result2.has_value());

    // Test unmounting a non-existent segment (idempotent request should
    // succeed).
    UUID non_existent_id = generate_uuid();
    auto unmount_result3 = service_->UnmountSegment(non_existent_id, client_id);
    EXPECT_TRUE(unmount_result3.has_value());

    // Test remounting after unmount.
    auto mount_result7 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result7.has_value());
    auto unmount_result4 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result4.has_value());
}

TEST_F(MasterServiceTest, MountUnmountSegmentWithOffsetAllocator) {
    // Create a MasterService instance for testing.
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Test normal mount operation.
    segment.base = original_base;
    segment.size = original_size;
    auto mount_result5 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result5.has_value());

    // Test mounting the same segment again (idempotent request should succeed).
    auto mount_result6 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result6.has_value());

    // Test unmounting the segment.
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result1.has_value());

    // Test unmounting the same segment again (idempotent request should
    // succeed).
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result2.has_value());

    // Test unmounting a non-existent segment (idempotent request should
    // succeed).
    UUID non_existent_id = generate_uuid();
    auto unmount_result3 = service_->UnmountSegment(non_existent_id, client_id);
    EXPECT_TRUE(unmount_result3.has_value());

    // Test remounting after unmount.
    auto mount_result7 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result7.has_value());
    auto unmount_result4 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result4.has_value());
}

TEST_F(MasterServiceTest, StandaloneSoftPinLifecycle) {
    auto config = MasterServiceConfig::builder()
                      .set_max_kv_soft_pin_ttl(60000)
                      .set_default_kv_soft_pin_ttl(1000)
                      .build();
    MasterService service(config);
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const auto client = generate_uuid();
    ASSERT_TRUE(service.PutStart(client, "pin-key", TenantId::Default(), 1024,
                                 ReplicateConfig{}));
    ASSERT_TRUE(service.PutEnd(client, {"pin-key", std::nullopt},
                               TenantId::Default(), ReplicaType::MEMORY));
    const auto baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();
    auto results = service.BatchUpdateSoftPin({"pin-key", "missing", ""},
                                              TenantId::Default(), 30000);
    ASSERT_EQ(results.size(), 3);
    ASSERT_TRUE(results[0]);
    EXPECT_EQ(results[1].error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(results[2].error(), ErrorCode::INVALID_PARAMS);
    auto first = GetSoftPinDeadline(service, "pin-key");
    ASSERT_TRUE(first);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);
    ASSERT_TRUE(
        service.BatchUpdateSoftPin({"pin-key"}, TenantId::Default(), 60000)[0]);
    auto renewed = GetSoftPinDeadline(service, "pin-key");
    ASSERT_GT(*renewed, *first);
    CleanupExpiredSoftPinsAt(service, *first);
    EXPECT_EQ(GetSoftPinDeadline(service, "pin-key"), renewed);
    ASSERT_TRUE(
        service.BatchUpdateSoftPin({"pin-key"}, TenantId::Default(), 10000)[0]);
    auto shortened = GetSoftPinDeadline(service, "pin-key");
    ASSERT_LT(*shortened, *renewed);
    CleanupExpiredSoftPinsAt(service, *shortened);
    EXPECT_FALSE(GetSoftPinDeadline(service, "pin-key"));
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    ASSERT_TRUE(
        service.BatchUpdateSoftPin({"pin-key"}, TenantId::Default(), 60000)[0]);
    ASSERT_TRUE(
        service.BatchUpdateSoftPin({"pin-key"}, TenantId::Default(), 0)[0]);
    ASSERT_TRUE(
        service.BatchUpdateSoftPin({"pin-key"}, TenantId::Default(), 0)[0]);
    EXPECT_FALSE(GetSoftPinDeadline(service, "pin-key"));
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    EXPECT_TRUE(service.ExistKey("pin-key", TenantId::Default()).value());
}

TEST_F(MasterServiceTest, StandaloneSoftPinRejectsPendingWriteAndInvalidTtl) {
    MasterService service(MasterServiceConfig::builder().build());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const auto client = generate_uuid();
    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = 30000;
    ASSERT_TRUE(service.PutStart(client, "pending-pin", TenantId::Default(),
                                 1024, config));
    auto pending =
        service.BatchUpdateSoftPin({"pending-pin"}, TenantId::Default(), 0);
    EXPECT_EQ(pending[0].error(), ErrorCode::REPLICA_IS_NOT_READY);
    ASSERT_TRUE(service.PutEnd(client, {"pending-pin", std::nullopt},
                               TenantId::Default(), ReplicaType::MEMORY));
    auto deadline = GetSoftPinDeadline(service, "pending-pin");
    ASSERT_TRUE(deadline);
    auto invalid =
        service.BatchUpdateSoftPin({"pending-pin"}, TenantId::Default(),
                                   std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(invalid[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(GetSoftPinDeadline(service, "pending-pin"), deadline);
    EXPECT_TRUE(service.BatchUpdateSoftPin({}, TenantId::Default(), 0).empty());
}

TEST_F(MasterServiceTest,
       StandaloneSoftPinIsTenantScopedAndPreservesOtherMetadata) {
    MasterService service(MakeStrictTenantConfig({"pin-a", "pin-b"}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const auto client = generate_uuid();
    ReplicateConfig config;
    config.with_hard_pin = true;
    config.session_ids = std::vector<std::string>{"writing-session"};
    for (const auto& tenant : {TenantId("pin-a"), TenantId("pin-b")}) {
        ASSERT_TRUE(
            service.PutStart(client, "shared-key", tenant, 1024, config));
        ASSERT_TRUE(service.PutEnd(client, {"shared-key", std::nullopt}, tenant,
                                   ReplicaType::MEMORY));
    }
    auto read_deadline = [&]() {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service,
            MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                "shared-key", TenantId("pin-a")));
        EXPECT_TRUE(accessor.Get().IsHardPinned());
        EXPECT_EQ(accessor.Get().session_id,
                  std::optional<std::string>("writing-session"));
        return accessor.Get().EvictionDeadline();
    };
    const auto lease = read_deadline();
    ASSERT_TRUE(service.BatchUpdateSoftPin({"shared-key"}, TenantId("pin-a"),
                                           30000)[0]);
    EXPECT_FALSE(GetSoftPinDeadline(service, "shared-key", "pin-b"));
    ASSERT_TRUE(service.BatchUpdateSoftPin({"shared-key"}, TenantId("pin-b"),
                                           30000)[0]);
    const auto other_deadline =
        GetSoftPinDeadline(service, "shared-key", "pin-b");
    ASSERT_TRUE(
        service.BatchUpdateSoftPin({"shared-key"}, TenantId("pin-a"), 0)[0]);
    EXPECT_FALSE(GetSoftPinDeadline(service, "shared-key", "pin-a"));
    EXPECT_EQ(GetSoftPinDeadline(service, "shared-key", "pin-b"),
              other_deadline);
    EXPECT_EQ(read_deadline(), lease);
}

TEST_F(MasterServiceTest, SoftPinZeroTtlSkipsSoftPinRegistration) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(50)
                              .set_max_kv_soft_pin_ttl(100)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = 0;
    ASSERT_TRUE(
        service
            ->PutStart(client_id, "zero_ttl", TenantId::Default(), 1024, config)
            .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "zero_ttl", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "zero_ttl").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest, SoftPinMasterConfigRejectsDefaultAboveMaximum) {
    auto invalid_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(101)
                              .set_max_kv_soft_pin_ttl(100)
                              .build();
    EXPECT_THROW(MasterService service(invalid_config), std::invalid_argument);
}

TEST_F(MasterServiceTest, SoftPinDeadlineCalculationSaturatesAtMaximum) {
    using Clock = std::chrono::system_clock;

    const auto normal_now = Clock::time_point(std::chrono::seconds(10));
    EXPECT_EQ(ComputeSoftPinDeadlineForTest(normal_now,
                                            std::chrono::milliseconds(25)),
              normal_now + std::chrono::milliseconds(25));
    EXPECT_EQ(ComputeSoftPinDeadlineForTest(normal_now,
                                            std::chrono::milliseconds::max()),
              Clock::time_point::max());

    const auto near_max =
        Clock::time_point::max() - std::chrono::milliseconds(5);
    EXPECT_EQ(
        ComputeSoftPinDeadlineForTest(near_max, std::chrono::milliseconds(10)),
        Clock::time_point::max());
}

#ifdef USE_NOF
TEST_F(MasterServiceTest, PutEndAllCompletesMemoryAndNoFReplicas) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    NoFSegment nof_segment = MakeNoFSegment();
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_all", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(
        client_id, "test_key_all", TenantId::Default(), ReplicaType::ALL);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_all", TenantId::Default());
    ASSERT_TRUE(get_replica_result.has_value());

    bool has_complete_memory = false;
    bool has_complete_nof = false;
    for (const auto& replica : get_replica_result->replicas) {
        if (replica.is_memory_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_complete_memory = true;
        }
        if (replica.is_nof_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_complete_nof = true;
        }
    }
    EXPECT_TRUE(has_complete_memory);
    EXPECT_TRUE(has_complete_nof);
}

TEST_F(MasterServiceTest, PutEndMemoryDoesNotCompleteNoFReplica) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    NoFSegment nof_segment =
        MakeNoFSegment("test_nof_segment_2", "test_nof_segment_endpoint_2");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_split", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(
        client_id, "test_key_split", TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_split", TenantId::Default());
    ASSERT_TRUE(get_replica_result.has_value());
    ASSERT_EQ(get_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(get_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(get_replica_result->replicas[0].status, ReplicaStatus::COMPLETE);

    auto put_revoke_result = service_->PutRevoke(
        client_id, "test_key_split", TenantId::Default(), ReplicaType::NOF_SSD);
    ASSERT_TRUE(put_revoke_result.has_value());

    auto final_replica_result =
        service_->GetReplicaList("test_key_split", TenantId::Default());
    ASSERT_TRUE(final_replica_result.has_value());
    ASSERT_EQ(final_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(final_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(final_replica_result->replicas[0].status,
              ReplicaStatus::COMPLETE);
}

TEST_F(MasterServiceTest, PartialRevokePreservesPendingSoftPin) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service);
    NoFSegment nof_segment =
        MakeNoFSegment("soft_pin_nof", "soft_pin_nof_endpoint");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountNoFSegment(nof_segment, client_id).has_value());

    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();
    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "partial_revoke_soft_pin",
                               TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutRevoke(client_id, "partial_revoke_soft_pin",
                                TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(service
                    ->PutEnd(client_id, "partial_revoke_soft_pin",
                             TenantId::Default(), ReplicaType::NOF_SSD)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);
    EXPECT_TRUE(
        GetSoftPinDeadline(*service, "partial_revoke_soft_pin").has_value());
}

TEST_F(MasterServiceTest, LaterReplicaEndDoesNotRefreshSoftPin) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service);
    NoFSegment nof_segment =
        MakeNoFSegment("soft_pin_later_end", "soft_pin_later_end_endpoint");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "later_end_soft_pin",
                               TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "later_end_soft_pin",
                             TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    const auto first_deadline =
        GetSoftPinDeadline(*service, "later_end_soft_pin");
    ASSERT_TRUE(first_deadline.has_value());

    ASSERT_TRUE(service
                    ->PutEnd(client_id, "later_end_soft_pin",
                             TenantId::Default(), ReplicaType::NOF_SSD)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "later_end_soft_pin"),
              first_deadline);
}

TEST_F(MasterServiceTest, PutStartOnePlusOneAllowsSingleAllocatedReplica) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_one_plus_one", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(put_start_result->size(), 1u);
    EXPECT_TRUE(put_start_result->front().is_memory_replica());
}
#endif

TEST_F(MasterServiceTest, DfsPutEndAllAndUpsertTopologyAreAtomic) {
    const auto dfs_root = (std::filesystem::temp_directory_path() /
                           ("master_dfs_sync_" + std::to_string(::getpid())))
                              .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto start = service.PutStart(context.client_id, "dfs_atomic",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        ASSERT_EQ(start->size(), 2);
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_atomic",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        auto query = service.GetReplicaList("dfs_atomic", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 2);
        for (const auto& replica : query->replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }

        ReplicateConfig mismatched_config;
        auto leased_upsert = service.UpsertStart(
            context.client_id, "dfs_atomic", TenantId::Default(), 4096, config);
        ASSERT_FALSE(leased_upsert.has_value());
        EXPECT_EQ(leased_upsert.error(), ErrorCode::OBJECT_HAS_LEASE);

        mismatched_config.replica_num = 1;
        auto upsert =
            service.UpsertStart(context.client_id, "dfs_atomic",
                                TenantId::Default(), 4096, mismatched_config);
        ASSERT_FALSE(upsert.has_value());
        EXPECT_EQ(upsert.error(), ErrorCode::INVALID_PARAMS);

        query = service.GetReplicaList("dfs_atomic", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 2);
        for (const auto& replica : query->replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }

        auto revoke_start = service.PutStart(context.client_id, "dfs_revoke",
                                             TenantId::Default(), 4096, config);
        ASSERT_TRUE(revoke_start.has_value());
        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "dfs_revoke",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        auto revoked =
            service.GetReplicaList("dfs_revoke", TenantId::Default());
        ASSERT_FALSE(revoked.has_value());
        EXPECT_EQ(revoked.error(), ErrorCode::OBJECT_NOT_FOUND);
    }

    {
        MasterService service(MakeStrictTenantConfig({"default"}));
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig dfs_config;
        dfs_config.replica_num = 1;
        dfs_config.dfs_replica_num = 1;

        auto failed = service.PutStart(context.client_id, "dfs_quota_failure",
                                       TenantId::Default(),
                                       kStrictTenantQuotaBytes, dfs_config);
        ASSERT_TRUE(failed.has_value()) << toString(failed.error());
        ASSERT_EQ(failed->size(), 1u);
        EXPECT_TRUE(failed->front().is_memory_replica());
        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "dfs_quota_failure",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        ReplicateConfig memory_config;
        memory_config.replica_num = 1;
        auto retry = service.PutStart(
            context.client_id, "quota_after_dfs_failure", TenantId::Default(),
            kStrictTenantQuotaBytes, memory_config);
        ASSERT_TRUE(retry.has_value()) << toString(retry.error());
        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "quota_after_dfs_failure",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());
    }
    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

// A one-object shard allocator: any DFS extent leaked by a lifecycle path makes
// the next DFS-only allocation fail.
class OneSlotDfsShardEnv {
   public:
    explicit OneSlotDfsShardEnv(const std::string& root)
        : enable_dfs_("MOONCAKE_ENABLE_DFS", "1"),
          fs_adapter_("MOONCAKE_DFS_FS_ADAPTER", "posix"),
          root_dir_("MOONCAKE_DFS_ROOT_DIR", root.c_str()),
          shard_count_("MOONCAKE_DFS_SHARD_COUNT", "1"),
          shard_capacity_("MOONCAKE_DFS_SHARD_CAPACITY", "32768"),
          alignment_("MOONCAKE_DFS_ALIGNMENT", "4096"),
          eviction_("MOONCAKE_DFS_EVICTION_ENABLED", "0"),
          deferred_free_("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0"),
          single_tenant_("MOONCAKE_DFS_SINGLE_TENANT", "true") {}

    static constexpr uint64_t kObjectSize = 20480;

   private:
    ScopedEnvVar enable_dfs_, fs_adapter_, root_dir_, shard_count_,
        shard_capacity_, alignment_, eviction_, deferred_free_, single_tenant_;
};

ReplicateConfig DfsOnlyConfig() {
    ReplicateConfig config;
    config.replica_num = 0;
    config.dfs_replica_num = 1;
    return config;
}

TEST_F(MasterServiceTest, DfsOnlyLifecyclePathsReleaseTheExtent) {
    const auto dfs_root = (std::filesystem::temp_directory_path() /
                           ("master_dfs_only_" + std::to_string(::getpid())))
                              .string();
    std::filesystem::create_directories(dfs_root);
    OneSlotDfsShardEnv env(dfs_root);
    constexpr uint64_t kSize = OneSlotDfsShardEnv::kObjectSize;

    MasterServiceConfig master_config;
    master_config.put_start_discard_timeout_sec = 1;
    master_config.put_start_release_timeout_sec = 2;
    MasterService service(master_config);
    const UUID writer = generate_uuid();
    const UUID other = generate_uuid();

    auto start = service.PutStart(writer, "a", TenantId::Default(), kSize,
                                  DfsOnlyConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    ASSERT_EQ(start->size(), 1u);
    EXPECT_TRUE(start->front().is_dfs_replica());
    auto not_ready = service.GetReplicaList("a", TenantId::Default());
    ASSERT_FALSE(not_ready.has_value());
    EXPECT_EQ(not_ready.error(), ErrorCode::REPLICA_IS_NOT_READY);
    auto full = service.PutStart(writer, "b", TenantId::Default(), kSize,
                                 DfsOnlyConfig());
    ASSERT_FALSE(full.has_value());
    EXPECT_EQ(full.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Revoke releases the extent.
    ASSERT_TRUE(
        service.PutRevoke(writer, "a", TenantId::Default(), ReplicaType::ALL));
    auto revoked = service.GetReplicaList("a", TenantId::Default());
    ASSERT_FALSE(revoked.has_value());
    EXPECT_EQ(revoked.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Upsert preemption releases the in-flight writer's extent.
    ASSERT_TRUE(service.PutStart(writer, "b", TenantId::Default(), kSize,
                                 DfsOnlyConfig()));
    ASSERT_TRUE(service.UpsertStart(other, "b", TenantId::Default(), kSize,
                                    DfsOnlyConfig()));
    auto stale_end =
        service.PutEnd(writer, "b", TenantId::Default(), ReplicaType::ALL);
    ASSERT_FALSE(stale_end.has_value());
    EXPECT_EQ(stale_end.error(), ErrorCode::ILLEGAL_CLIENT);
    ASSERT_TRUE(
        service.UpsertEnd(other, "b", TenantId::Default(), ReplicaType::ALL));
    auto complete = service.GetReplicaList("b", TenantId::Default());
    ASSERT_TRUE(complete.has_value());
    ASSERT_EQ(complete->replicas.size(), 1u);
    EXPECT_EQ(complete->replicas[0].status, ReplicaStatus::COMPLETE);

    // Remove honours the read lease, then releases the extent.
    auto leased = service.Remove("b", TenantId::Default());
    ASSERT_FALSE(leased.has_value());
    EXPECT_EQ(leased.error(), ErrorCode::OBJECT_HAS_LEASE);
    ASSERT_TRUE(service.Remove("b", TenantId::Default(), /*force=*/true));

    // An abandoned write is reaped after the release timeout.
    ASSERT_TRUE(service.PutStart(writer, "c", TenantId::Default(), kSize,
                                 DfsOnlyConfig()));
    std::this_thread::sleep_for(std::chrono::seconds(4));
    auto reaped = service.GetReplicaList("c", TenantId::Default());
    ASSERT_FALSE(reaped.has_value());
    EXPECT_EQ(reaped.error(), ErrorCode::OBJECT_NOT_FOUND);
    auto reuse = service.PutStart(other, "d", TenantId::Default(), kSize,
                                  DfsOnlyConfig());
    ASSERT_TRUE(reuse.has_value()) << toString(reuse.error());

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, DfsOnlyObjectsIgnoreMemoryEvictionAndSegmentLoss) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_only_eviction_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    OneSlotDfsShardEnv env(dfs_root);

    MasterServiceConfig master_config;
    master_config.default_kv_lease_ttl = 1;
    MasterService service(master_config);
    const auto context = PrepareSimpleSegment(service);
    const UUID writer = generate_uuid();

    ASSERT_TRUE(service.PutStart(writer, "dfs_only", TenantId::Default(),
                                 OneSlotDfsShardEnv::kObjectSize,
                                 DfsOnlyConfig()));
    ASSERT_TRUE(service.PutEnd(writer, "dfs_only", TenantId::Default(),
                               ReplicaType::ALL));
    ReplicateConfig memory_config;
    memory_config.replica_num = 1;
    for (int i = 0; i < 8; ++i) {
        const auto key = "memory_" + std::to_string(i);
        ASSERT_TRUE(service.PutStart(writer, key, TenantId::Default(), 1 << 20,
                                     memory_config));
        ASSERT_TRUE(service.PutEnd(writer, key, TenantId::Default(),
                                   ReplicaType::MEMORY));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);

    auto query = service.GetReplicaList("dfs_only", TenantId::Default());
    ASSERT_TRUE(query.has_value()) << toString(query.error());
    ASSERT_EQ(query->replicas.size(), 1u);
    EXPECT_TRUE(query->replicas[0].is_dfs_replica());
    ASSERT_TRUE(service.UnmountSegment(context.segment_id, context.client_id));
    EXPECT_TRUE(
        service.GetReplicaList("dfs_only", TenantId::Default()).has_value());

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, DfsBucketMemoryAllocationFailurePreservesBuckets) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_bucket_memory_failure_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator("MOONCAKE_DFS_ALLOCATOR", "bucket");
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "8192");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "4");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "1.0");
    ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.9");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service, "small_memory",
                                                  kDefaultSegmentBase, 8192);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        for (const char* key : {"memory_full_a", "memory_full_b"}) {
            auto start = service.PutStart(context.client_id, key,
                                          TenantId::Default(), 4096, config);
            ASSERT_TRUE(start.has_value()) << key << ": " << start.error();
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }

        ASSERT_TRUE(
            service.UnmountSegment(context.segment_id, context.client_id)
                .has_value());

        auto failed = service.PutStart(context.client_id, "memory_full_c",
                                       TenantId::Default(), 4096, config);
        ASSERT_FALSE(failed.has_value());
        EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);

        for (const char* key : {"memory_full_a", "memory_full_b"}) {
            auto query = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(query.has_value()) << key;
            ASSERT_EQ(query->replicas.size(), 1u);
            EXPECT_TRUE(query->replicas.front().is_dfs_replica());
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, DfsBucketUpsertRestoresEvictedReplica) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_bucket_upsert_restore_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator("MOONCAKE_DFS_ALLOCATOR", "bucket");
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "8192");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "2");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.7");
    ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.5");
    ScopedEnvVar eviction_interval("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
                                   "60");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        std::optional<uint64_t> old_memory_address;
        std::optional<int> old_bucket_id;
        for (const std::string key :
             {"upsert_restore", "bucket_0_peer", "bucket_1_peer"}) {
            auto start = service.PutStart(context.client_id, key,
                                          TenantId::Default(), 4096, config);
            ASSERT_TRUE(start.has_value()) << key << ": " << start.error();
            if (key == "upsert_restore") {
                for (const auto& descriptor : *start) {
                    if (descriptor.is_memory_replica()) {
                        old_memory_address =
                            descriptor.get_memory_descriptor()
                                .buffer_descriptor.buffer_address_;
                    } else if (descriptor.is_dfs_replica()) {
                        old_bucket_id =
                            descriptor.get_dfs_descriptor().shard_idx;
                    }
                }
            }
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }
        ASSERT_TRUE(old_memory_address.has_value());
        ASSERT_TRUE(old_bucket_id.has_value());

        MasterServiceTestPeer(service).RunDfsEvictionForTesting();
        auto after_eviction =
            service.GetReplicaList("upsert_restore", TenantId::Default());
        ASSERT_TRUE(after_eviction.has_value());
        ASSERT_EQ(after_eviction->replicas.size(), 1u);
        EXPECT_TRUE(after_eviction->replicas.front().is_memory_replica());

        auto mismatched_config = config;
        mismatched_config.replica_num = 2;
        auto mismatched =
            service.UpsertStart(context.client_id, "upsert_restore",
                                TenantId::Default(), 4096, mismatched_config);
        ASSERT_FALSE(mismatched.has_value());
        EXPECT_EQ(mismatched.error(), ErrorCode::INVALID_PARAMS);

        auto restored = service.UpsertStart(context.client_id, "upsert_restore",
                                            TenantId::Default(), 4096, config);
        ASSERT_TRUE(restored.has_value()) << restored.error();
        ASSERT_EQ(restored->size(), 2u);
        bool restored_memory = false;
        bool restored_dfs = false;
        for (const auto& descriptor : *restored) {
            if (descriptor.is_memory_replica()) {
                restored_memory = true;
                EXPECT_NE(descriptor.get_memory_descriptor()
                              .buffer_descriptor.buffer_address_,
                          *old_memory_address);
            } else if (descriptor.is_dfs_replica()) {
                restored_dfs = true;
                EXPECT_NE(descriptor.get_dfs_descriptor().shard_idx,
                          *old_bucket_id);
            }
        }
        EXPECT_TRUE(restored_memory);
        EXPECT_TRUE(restored_dfs);
        ASSERT_TRUE(service
                        .UpsertEnd(context.client_id, "upsert_restore",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        auto complete =
            service.GetReplicaList("upsert_restore", TenantId::Default());
        ASSERT_TRUE(complete.has_value());
        EXPECT_EQ(complete->replicas.size(), 2u);
        EXPECT_EQ(
            std::count_if(complete->replicas.begin(), complete->replicas.end(),
                          [](const Replica::Descriptor& descriptor) {
                              return descriptor.is_dfs_replica();
                          }),
            1);
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

// A bucket goes as a whole: one member under a live lease keeps every
// member's replica in it.
TEST_F(MasterServiceTest, DfsBucketEvictionKeepsBucketWithLeasedMember) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_bucket_leased_member_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator("MOONCAKE_DFS_ALLOCATOR", "bucket");
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "8192");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "2");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.7");
    ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.5");
    ScopedEnvVar eviction_interval("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
                                   "60");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        // The first two keys fill the first bucket, the third starts another.
        for (const std::string key :
             {"leased_member", "bucket_0_peer", "bucket_1_peer"}) {
            auto start = service.PutStart(context.client_id, key,
                                          TenantId::Default(), 4096, config);
            ASSERT_TRUE(start.has_value()) << key << ": " << start.error();
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }
        // A read leases the member.
        ASSERT_TRUE(service.GetReplicaList("leased_member", TenantId::Default())
                        .has_value());

        MasterServiceTestPeer(service).RunDfsEvictionForTesting();

        for (const std::string key : {"leased_member", "bucket_0_peer"}) {
            auto replicas = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(replicas.has_value()) << key;
            EXPECT_EQ(std::count_if(replicas->replicas.begin(),
                                    replicas->replicas.end(),
                                    [](const Replica::Descriptor& descriptor) {
                                        return descriptor.is_dfs_replica();
                                    }),
                      1)
                << key;
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest,
       DfsBucketConcurrentPutAndUpsertShareSingleRecoveryBucket) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_bucket_single_flight_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator("MOONCAKE_DFS_ALLOCATOR", "bucket");
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "32768");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "4");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "1.0");
    ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.9");
    ScopedEnvVar eviction_interval("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
                                   "60");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        constexpr size_t kObjectSize = 4096;
        constexpr size_t kConcurrentWrites = 8;
        constexpr size_t kOldKeyCount = 32;
        std::vector<std::string> old_keys;
        std::vector<std::string> new_keys;
        for (size_t i = 0; i < kOldKeyCount; ++i) {
            old_keys.push_back("single_flight_old_" + std::to_string(i));
        }
        for (size_t i = 0; i < kConcurrentWrites; ++i) {
            new_keys.push_back("single_flight_new_" + std::to_string(i));
        }

        for (const auto& key : old_keys) {
            auto start =
                service.PutStart(context.client_id, key, TenantId::Default(),
                                 kObjectSize, config);
            ASSERT_TRUE(start.has_value()) << key << ": " << start.error();
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }

        // Holding every old key blocks the eviction's validation of the
        // bucket they share.
        std::vector<route::WriteGuard> old_key_holds;
        for (const auto& key : old_keys) {
            auto hold = MasterServiceTestPeer::WriteObject(
                service, MasterServiceTestPeer::ObjectIdentity{
                             TenantId::Default(), key});
            ASSERT_TRUE(hold.has_value()) << key;
            old_key_holds.push_back(std::move(*hold));
        }
        std::barrier start_barrier(kConcurrentWrites + 1);
        std::vector<int> errors(kConcurrentWrites,
                                static_cast<int>(ErrorCode::INTERNAL_ERROR));
        std::vector<int> bucket_ids(kConcurrentWrites, -1);
        std::vector<std::thread> writers;
        writers.reserve(kConcurrentWrites);
        for (size_t i = 0; i < kConcurrentWrites; ++i) {
            writers.emplace_back([&, i] {
                start_barrier.arrive_and_wait();
                auto result =
                    i % 2 == 0
                        ? service.PutStart(context.client_id, new_keys[i],
                                           TenantId::Default(), kObjectSize,
                                           config)
                        : service.UpsertStart(context.client_id, new_keys[i],
                                              TenantId::Default(), kObjectSize,
                                              config);
                if (!result) {
                    errors[i] = static_cast<int>(result.error());
                    return;
                }
                errors[i] = static_cast<int>(ErrorCode::OK);
                for (const auto& descriptor : *result) {
                    if (descriptor.is_dfs_replica()) {
                        bucket_ids[i] =
                            descriptor.get_dfs_descriptor().shard_idx;
                    }
                }
            });
        }

        start_barrier.arrive_and_wait();
        // Keep eviction validation blocked long enough for all writers to
        // encounter the full allocator. Without single-flight recovery they
        // freeze different LRU buckets while waiting on these keys.
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        old_key_holds.clear();
        for (auto& writer : writers) writer.join();

        for (size_t i = 0; i < kConcurrentWrites; ++i) {
            EXPECT_EQ(errors[i], static_cast<int>(ErrorCode::OK))
                << new_keys[i];
            EXPECT_GE(bucket_ids[i], 0) << new_keys[i];
            EXPECT_EQ(bucket_ids[i], bucket_ids.front()) << new_keys[i];
        }

        size_t old_dfs_replicas = 0;
        for (const auto& key : old_keys) {
            auto query = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(query.has_value()) << key;
            old_dfs_replicas +=
                std::count_if(query->replicas.begin(), query->replicas.end(),
                              [](const Replica::Descriptor& descriptor) {
                                  return descriptor.is_dfs_replica();
                              });
        }
        EXPECT_EQ(old_dfs_replicas, kOldKeyCount - kConcurrentWrites);
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, LeasedUpsertAllocationFailurePreservesObject) {
    MasterServiceConfig service_config;
    service_config.memory_allocator = BufferAllocatorType::OFFSET;
    service_config.default_kv_lease_ttl = 10 * 1000;
    MasterService service(service_config);

    constexpr size_t kSegmentSize = 1024 * 1024;
    const auto context = PrepareSimpleSegment(
        service, "leased_upsert_segment", kDefaultSegmentBase, kSegmentSize);
    ReplicateConfig config;
    config.replica_num = 1;

    const std::string key = "leased_upsert_allocation_failure";
    auto initial = service.PutStart(context.client_id, key, TenantId::Default(),
                                    kSegmentSize, config);
    ASSERT_TRUE(initial.has_value()) << toString(initial.error());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());

    // Retain a read lease so UpsertStart must allocate a replacement instead
    // of reusing the old buffer.
    auto snapshot = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(snapshot.has_value());

    auto failed = service.UpsertStart(
        context.client_id, key, TenantId::Default(), kSegmentSize, config);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    auto still_readable = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(still_readable.has_value());
    ASSERT_EQ(still_readable->replicas.size(), snapshot->replicas.size());
    EXPECT_EQ(still_readable->replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              snapshot->replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_);
}

// DFS replicas live in their own variant branch, so is_disk_replica() does not
// match them. KV subscribers still expect one logical tier per storage class,
// which is what these assertions pin down.
TEST_F(MasterServiceTest, DfsReplicaNormalizesToDiskMediumForKvEvents) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_kv_media_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto start = service.PutStart(context.client_id, "dfs_kv_media",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        ASSERT_EQ(start->size(), 2);
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_kv_media",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        // Memory + DFS collapse to exactly the two logical tiers, sorted the
        // same way the publisher sorts them.
        auto media = KvMediaForKey(service, "dfs_kv_media");
        ASSERT_TRUE(media.has_value());
        EXPECT_EQ(std::vector<std::string>({"cpu", "disk"}), *media);

        auto removal_media = KvRemovalMediaForKey(service, "dfs_kv_media");
        ASSERT_TRUE(removal_media.has_value());
        EXPECT_EQ(std::vector<std::string>({"cpu", "disk"}), *removal_media);
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

// A removal path can run while a replica is still PROCESSING. That medium was
// never announced as available, but the removal snapshot must still name it:
// otherwise a tier that a subscriber may have seen is never retracted. The
// contrast with KvMediaForKey, which counts only completed replicas, is the
// whole point of having two helpers.
TEST_F(MasterServiceTest, ProcessingReplicaMediumAppearsInRemovalSnapshot) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    // PutStart without PutEnd leaves the replica PROCESSING.
    auto start = service.PutStart(context.client_id, "processing_kv_media",
                                  TenantId::Default(), 4096, config);
    ASSERT_TRUE(start.has_value());

    auto media = KvMediaForKey(service, "processing_kv_media");
    ASSERT_TRUE(media.has_value());
    EXPECT_TRUE(media->empty()) << "a PROCESSING replica is not yet available "
                                   "and must not be announced";

    auto removal_media = KvRemovalMediaForKey(service, "processing_kv_media");
    ASSERT_TRUE(removal_media.has_value());
    EXPECT_EQ(std::vector<std::string>({"cpu"}), *removal_media);
}

// A RemoveAll whose oplog slot reservation fails must leave the object in
// place. That is the state in which announcing `cleared` would be a lie, so the
// skip has to be recorded rather than treated as a successful wipe.
TEST_F(MasterServiceTest, RemoveAllKeepsObjectWhenOpLogReservationFails) {
    // enable_oplog_ needs HA + oplog + the etcd backend, but the ordered oplog
    // writer is only installed by a separate init that needs a live etcd. So
    // this config reaches ReserveBatchOpLogSlot with a null writer, which is
    // exactly the reservation-failure branch.
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_enable_oplog(true)
                      .set_ha_backend_type("etcd")
                      .build();
    auto service = std::make_unique<MasterService>(config);
    const auto context = PrepareSimpleSegment(*service);

    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;
    ASSERT_TRUE(service
                    ->PutStart(context.client_id, "reservation_fail_key",
                               TenantId::Default(), 4096, replicate_config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(context.client_id, "reservation_fail_key",
                             TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    // force=true clears the lease gate, so the reservation is the only thing
    // left that can stop the removal.
    EXPECT_EQ(0, service->RemoveAll(TenantId::Default(), true));

    auto exists =
        service->ExistKey("reservation_fail_key", TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value())
        << "the object survived, so the tenant was never emptied";
}

// RemoveAll holds no lock across its walk, so a commit can land in a tenant the
// walk already passed. The walk's own bookkeeping cannot see it, and publishing
// `cleared` would order it after that commit's `stored` — telling subscribers
// to drop an object that is live. Pause the walk right after the tenant the new
// key belongs to, commit there, and the clear must be withheld.
TEST_F(MasterServiceTest, ConcurrentCommitDuringScanSuppressesClear) {
    MasterService service;
    MasterServiceTestPeer(service).SetKvTenantEpochTrackingForTesting(true);
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service
                    .PutStart(context.client_id, "victim_key",
                              TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "victim_key",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    bool committed = false;
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(
        [&](const TenantId& tenant_id) {
            // Commit exactly once, immediately after the walk finishes the
            // tenant the new key belongs to, so the walk can never observe it.
            if (!tenant_id.IsDefault() || committed) {
                return;
            }
            committed = true;
            ASSERT_TRUE(service
                            .PutStart(context.client_id, "racer_key",
                                      TenantId::Default(), 1024, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, "racer_key",
                                    TenantId::Default(), ReplicaType::ALL)
                            .has_value());
        });

    service.RemoveAll(true);
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(nullptr);

    ASSERT_TRUE(committed) << "the hook never fired, so nothing was raced";
    auto exists = service.ExistKey("racer_key", TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value()) << "the raced commit must still be live";

    EXPECT_EQ(0u,
              MasterServiceTestPeer(service).GetKvClearedPublishedForTesting())
        << "a clear here would retract racer_key, which was just announced";
    EXPECT_EQ(
        1u, MasterServiceTestPeer(service).GetKvClearedSuppressedForTesting());
}

// The mirror image: with no concurrent commit the epoch is unchanged, so the
// clear must still go out. Without this the fix could pass by never publishing.
TEST_F(MasterServiceTest, UncontendedScanStillPublishesClear) {
    MasterService service;
    MasterServiceTestPeer(service).SetKvTenantEpochTrackingForTesting(true);
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service
                    .PutStart(context.client_id, "lonely_key",
                              TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "lonely_key",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    service.RemoveAll(true);

    EXPECT_EQ(1u,
              MasterServiceTestPeer(service).GetKvClearedPublishedForTesting());
    EXPECT_EQ(
        0u, MasterServiceTestPeer(service).GetKvClearedSuppressedForTesting());
}

// The tenant-scoped overload reads the epoch before its scan instead of at
// first sight of an object, so it needs its own coverage of the same ordering
// rule.
TEST_F(MasterServiceTest, TenantScopedRemoveAllSuppressesClearOnRace) {
    MasterService service;
    MasterServiceTestPeer(service).SetKvTenantEpochTrackingForTesting(true);
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service
                    .PutStart(context.client_id, "scoped_victim",
                              TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "scoped_victim",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    bool committed = false;
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(
        [&](const TenantId& tenant_id) {
            if (!tenant_id.IsDefault() || committed) {
                return;
            }
            committed = true;
            ASSERT_TRUE(service
                            .PutStart(context.client_id, "scoped_racer",
                                      TenantId::Default(), 1024, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, "scoped_racer",
                                    TenantId::Default(), ReplicaType::ALL)
                            .has_value());
        });

    service.RemoveAll(TenantId::Default(), true);
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(nullptr);

    ASSERT_TRUE(committed) << "the hook never fired, so nothing was raced";
    EXPECT_EQ(0u,
              MasterServiceTestPeer(service).GetKvClearedPublishedForTesting());
    EXPECT_EQ(
        1u, MasterServiceTestPeer(service).GetKvClearedSuppressedForTesting());
}

namespace {

StandbyObjectEntry MakeStandbyEntry(const std::string& key, uint64_t obj_size,
                                    uintptr_t addr,
                                    const std::string& endpoint) {
    StandbyObjectEntry entry;
    entry.tenant_id = TenantId::Default().value();
    entry.key = key;
    entry.metadata.client_id = generate_uuid();
    entry.metadata.size = obj_size;
    AllocatedBuffer::Descriptor buf;
    buf.size_ = obj_size;
    buf.buffer_address_ = addr;
    buf.transport_endpoint_ = endpoint;
    Replica::Descriptor desc;
    desc.id = addr;
    desc.descriptor_variant = MemoryDescriptor{buf};
    desc.status = ReplicaStatus::COMPLETE;
    entry.metadata.replicas.push_back(desc);
    return entry;
}

StandbySegmentInfo MakeStandbySegment(const std::string& name,
                                      const std::string& endpoint,
                                      uint64_t capacity) {
    StandbySegmentInfo seg;
    seg.segment_name = name;
    seg.transport_endpoint = endpoint;
    seg.capacity = capacity;
    seg.is_memory_segment = true;
    return seg;
}

}  // namespace

TEST_F(MasterServiceTest, StandbyRemountFindsSameEndpointRangeByAddress) {
    MasterService service(MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build());
    auto first = MakeSegment("shared-range");
    auto second = first;
    second.id = generate_uuid();
    second.base += first.size;
    first.locality = SegmentLocality{0, {"mlx5_0"}};
    second.locality = SegmentLocality{1, {"mlx5_1"}};
    auto a = MakeStandbyEntry("range-a", 1024, first.base, first.te_endpoint);
    auto b = MakeStandbyEntry("range-b", 1024, second.base, second.te_endpoint);
    ASSERT_TRUE(service.RestoreFromStandbySnapshot(
        {a, b}, 1,
        {MakeStandbySegment(first.name, first.te_endpoint, first.size)}));
    ASSERT_TRUE(service.ReMountSegment({first, second}, generate_uuid()));
    for (const auto& [key, address] :
         {std::pair{"range-a", first.base}, {"range-b", second.base}}) {
        auto result = service.GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(result);
        EXPECT_EQ(result->replicas.front()
                      .get_memory_descriptor()
                      .buffer_descriptor.buffer_address_,
                  address);
    }
}

TEST_F(MasterServiceTest, StandbyRestoreSkipsInvalidObjectAndRestoresTheRest) {
    // #3760: one bad descriptor must not cost the whole index.
    MasterService service(MakeStrictTenantConfig({"default"}));
    // Mount the endpoint live: replicas restored onto an unmounted endpoint
    // stay unreadable until the segment comes back, which is not what this
    // test checks.
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service, "ep1");
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto good = MakeStandbyEntry("good_key", 128, 0x1000, "ep1");
    auto bad = MakeStandbyEntry("bad_key", 128, 0x2000, "ep1");
    bad.metadata.size = 256;  // descriptor size no longer matches metadata

    auto result = service.RestoreFromStandbySnapshot(
        {good, bad},
        /*initial_oplog_sequence_id=*/0, {seg});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(
        service.ExistKey("good_key", TenantId::Default()).value_or(false));
    EXPECT_FALSE(
        service.ExistKey("bad_key", TenantId::Default()).value_or(true));
}

TEST_F(MasterServiceTest, StandbyRestoreDiscardsAmbiguousOverlap) {
    // #3760: eviction frees and reallocates a buffer; a standby that missed
    // the removal replays both replicas at the same address. No replay order
    // reaches the promotion context, so neither side can prove it is newer:
    // both are discarded, and with nothing restored the restore fails closed.
    MasterService service(MakeStrictTenantConfig({"default"}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service, "ep1");
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto stale = MakeStandbyEntry("stale_key", 128, 0x1000, "ep1");
    auto fresh = MakeStandbyEntry("fresh_key", 128, 0x1040, "ep1");

    auto result = service.RestoreFromStandbySnapshot(
        {stale, fresh},
        /*initial_oplog_sequence_id=*/0, {seg});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(
        service.ExistKey("stale_key", TenantId::Default()).value_or(true));
    EXPECT_FALSE(
        service.ExistKey("fresh_key", TenantId::Default()).value_or(false));
}

TEST_F(MasterServiceTest, StandbySnapshotRestorePreservesTenantScopedKeys) {
    const TenantId tenant_a("tenant_restore_a");
    const TenantId tenant_b("tenant_restore_b");
    MasterService service(
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()}));
    const std::string key = "shared_restore_key";

    Replica replica(generate_uuid(), 128, "local://standby",
                    ReplicaStatus::COMPLETE);
    StandbyObjectMetadata metadata;
    metadata.client_id = generate_uuid();
    metadata.size = 128;
    metadata.replicas.push_back(replica.get_descriptor());

    ASSERT_TRUE(
        service
            .RestoreFromStandbySnapshot({{tenant_a.value(), key, metadata}},
                                        /*initial_oplog_sequence_id=*/0, {})
            .has_value());

    EXPECT_TRUE(service.ExistKey(key, tenant_a).value_or(false));
    EXPECT_FALSE(service.ExistKey(key, tenant_b).value_or(true));
    EXPECT_FALSE(service.ExistKey(key, TenantId::Default()).value_or(true));
}

TEST_F(MasterServiceTest, GetAllKeysListsOnlyRequestedTenant) {
    const TenantId tenant_a("tenant_get_all_keys_a");
    auto service_ = std::make_unique<MasterService>(MakeStrictTenantConfig(
        {std::string(TenantId::kDefaultValue), tenant_a.value()}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string shared_key = "shared_listing_key";
    const std::string default_only_key = "default_listing_key";
    const std::string tenant_only_key = "tenant_listing_key";

    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(
        service_
            ->PutStart(client_id, shared_key, TenantId::Default(), 1024, config)
            .has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, shared_key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(service_
                    ->PutStart(client_id, default_only_key, TenantId::Default(),
                               1024, config)
                    .has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, default_only_key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, shared_key, tenant_a, 1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(client_id, shared_key, tenant_a, ReplicaType::MEMORY)
            .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, tenant_only_key, tenant_a, 1024, config)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(client_id, tenant_only_key, tenant_a, ReplicaType::MEMORY)
            .has_value());

    auto default_keys = service_->GetAllKeys(TenantId::Default());
    ASSERT_TRUE(default_keys.has_value());
    EXPECT_NE(std::find(default_keys->begin(), default_keys->end(), shared_key),
              default_keys->end());
    EXPECT_NE(
        std::find(default_keys->begin(), default_keys->end(), default_only_key),
        default_keys->end());
    EXPECT_EQ(
        std::find(default_keys->begin(), default_keys->end(), tenant_only_key),
        default_keys->end());

    auto tenant_keys = service_->GetAllKeys(tenant_a);
    ASSERT_TRUE(tenant_keys.has_value());
    EXPECT_NE(std::find(tenant_keys->begin(), tenant_keys->end(), shared_key),
              tenant_keys->end());
    EXPECT_NE(
        std::find(tenant_keys->begin(), tenant_keys->end(), tenant_only_key),
        tenant_keys->end());
    EXPECT_EQ(
        std::find(tenant_keys->begin(), tenant_keys->end(), default_only_key),
        tenant_keys->end());
}

TEST_F(MasterServiceTest, TenantScopedPutsAndRemovesUpdateGlobalKeyCount) {
    const std::string key = "shared_user_key";
    const TenantId tenant_a("tenant_key_count_a");
    const TenantId tenant_b("tenant_key_count_b");
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    EXPECT_EQ(service_->GetKeyCount(), 0u);
    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_a, 1024, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_a, ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_b, 2048, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_b, ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(service_->GetKeyCount(), 2u);

    ASSERT_TRUE(service_->Remove(key, tenant_a, /*force=*/true).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());
    EXPECT_EQ(service_->GetKeyCount(), 1u);

    ASSERT_TRUE(service_->Remove(key, tenant_b, /*force=*/true).has_value());
    EXPECT_EQ(service_->GetKeyCount(), 0u);
}

TEST_F(MasterServiceTest, MasterConfigParsesLocalFirstStrategy) {
    MasterConfig config{};
    config.allocation_strategy = "local_first";

    WrappedMasterServiceConfig wrapped_config(config, 0);
    MasterServiceConfig service_config(wrapped_config);
    EXPECT_EQ(service_config.allocation_strategy_type,
              AllocationStrategyType::LOCAL_FIRST);
}

TEST_F(MasterServiceTest, ProtectCopyMoveSourceFromEviction) {
    const uint64_t kv_lease_ttl = 100;
    const uint64_t client_live_ttl = 600;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_client_live_ttl_sec(client_live_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment, each
    // 16 MB
    constexpr size_t kBaseAddr = 0x100000000;
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;  // 16 MB
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1", kBaseAddr, kSegmentSize);
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2", kBaseAddr, kSegmentSize);

    const UUID client_id = context1.client_id;

    const std::string copy_key = "copy_key";
    const std::string move_key = "move_key";
    uint64_t slice_length = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Put two objects for move and copy tests.
    auto put_start_result = service_->PutStart(
        client_id, copy_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(
        client_id, copy_key, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    put_start_result = service_->PutStart(
        client_id, move_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    put_end_result = service_->PutEnd(client_id, move_key, TenantId::Default(),
                                      ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Start copy and move operations.
    auto copy_start_result = service_->CopyStart(
        client_id, copy_key, TenantId::Default(), "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    auto move_start_result = service_->MoveStart(
        client_id, move_key, TenantId::Default(), "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Put more objects to trigger eviction. Do not prefer any segments.
    config.preferred_segment = "";
    for (size_t i = 0; i < 128 * (kSegmentSize * 2 / slice_length); ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Wait all objects lease expiring and then remove them.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    auto remove_all_result = service_->RemoveAll();
    ASSERT_TRUE(remove_all_result > 0);

    // Try end copy and move operations, should success.
    auto copy_end_result =
        service_->CopyEnd(client_id, copy_key, TenantId::Default());
    EXPECT_TRUE(copy_end_result.has_value());

    auto move_end_result =
        service_->MoveEnd(client_id, move_key, TenantId::Default());
    EXPECT_TRUE(move_end_result.has_value());
}

TEST_F(MasterServiceTest, DiscardTimeoutCopyMove) {
    const uint64_t kv_lease_ttl = 100;
    const uint64_t client_live_ttl = 600;
    const uint64_t put_discard_timeout = 1;
    const uint64_t put_release_timeout = 2;
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(kv_lease_ttl)
            .set_client_live_ttl_sec(client_live_ttl)
            .set_put_start_discard_timeout_sec(put_discard_timeout)
            .set_put_start_release_timeout_sec(put_release_timeout)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment, each
    // 16 MB
    constexpr size_t kBaseAddr = 0x100000000;
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;  // 16 MB
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1", kBaseAddr, kSegmentSize);
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2", kBaseAddr, kSegmentSize);

    const UUID client_id = context1.client_id;

    const std::string copy_key = "copy_key";
    const std::string move_key = "move_key";
    uint64_t slice_length = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Put two objects for move and copy tests.
    auto put_start_result = service_->PutStart(
        client_id, copy_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(
        client_id, copy_key, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    put_start_result = service_->PutStart(
        client_id, move_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    put_end_result = service_->PutEnd(client_id, move_key, TenantId::Default(),
                                      ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Start copy and move operations.
    auto copy_start_result = service_->CopyStart(
        client_id, copy_key, TenantId::Default(), "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    auto move_start_result = service_->MoveStart(
        client_id, move_key, TenantId::Default(), "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Wait for the operations timeout.
    std::this_thread::sleep_for(std::chrono::seconds(put_release_timeout));

    // Put more objects to trigger eviction. Do not prefer any segments.
    config.preferred_segment = "";
    for (size_t i = 0; i < 128 * (kSegmentSize * 2 / slice_length); ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Try end copy and move operations, should fail because the objects are
    // evicted.
    auto copy_end_result =
        service_->CopyEnd(client_id, copy_key, TenantId::Default());
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(copy_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);

    auto move_end_result =
        service_->MoveEnd(client_id, move_key, TenantId::Default());
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(move_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServiceTest, CleanupStaleHandlesTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a segment for testing
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;  // 16MB
    auto segment = MakeSegment("test_segment", buffer, size);
    UUID client_id = generate_uuid();

    // Mount the segment
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create an object that will be stored in the segment
    std::string key = "segment_object";
    uint64_t slice_length = 1024 * 1024;  // One 1MB slice
    ReplicateConfig config;
    config.replica_num = 1;  // One replica

    // Create the object
    auto put_start_result = service_->PutStart(
        client_id, key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, TenantId::Default(),
                                           ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Verify object exists
    auto get_result = service_->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(get_result.has_value());
    auto retrieved_replicas = get_result.value().replicas;
    ASSERT_EQ(1, retrieved_replicas.size());

    // Unmount the segment
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    ASSERT_TRUE(unmount_result1.has_value());

    // Try to get the object - it should be automatically removed since the
    // replica is invalid
    auto get_result2 = service_->GetReplicaList(key, TenantId::Default());
    EXPECT_FALSE(get_result2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result2.error());

    // Mount the segment again
    mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create another object
    std::string key2 = "another_segment_object";
    auto put_start_result2 = service_->PutStart(
        client_id, key2, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 = service_->PutEnd(
        client_id, key2, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result2.has_value());

    // Verify we can get it
    auto get_result3 = service_->GetReplicaList(key2, TenantId::Default());
    ASSERT_TRUE(get_result3.has_value());

    // Unmount the segment
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    ASSERT_TRUE(unmount_result2.has_value());

    // Try to remove the object that should already be cleaned up
    auto remove_result = service_->Remove(key2, TenantId::Default());
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, remove_result.error());
}

TEST_F(MasterServiceTest, UnmountSegmentHidesReplicasBeforeAsyncCleanup) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount two segments for testing
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    auto segment1 = MakeSegment("segment1", buffer1, size);
    auto segment2 = MakeSegment("segment2", buffer2, size);
    UUID client_id = generate_uuid();
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Create two objects in the two segments
    std::string key1 =
        GenerateKeyForSegment(client_id, service_, segment1.name);
    std::string key2 =
        GenerateKeyForSegment(client_id, service_, segment2.name);
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    PauseReplicaCleanup(*service_);

    // Unmount segment1. The allocator becomes unavailable synchronously while
    // physical metadata cleanup runs on the background worker.
    auto unmount_result1 = service_->UnmountSegment(segment1.id, client_id);
    ASSERT_TRUE(unmount_result1.has_value());

    // Query paths must not expose the unavailable replica while its physical
    // metadata is still waiting for background cleanup.
    ASSERT_EQ(2u, service_->GetKeyCount());
    auto get_result1 = service_->GetReplicaList(key1, TenantId::Default());
    ASSERT_FALSE(get_result1.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result1.error());

    auto exists1 = service_->ExistKey(key1, TenantId::Default());
    ASSERT_TRUE(exists1.has_value());
    EXPECT_FALSE(*exists1);
    auto exists2 = service_->ExistKey(key2, TenantId::Default());
    ASSERT_TRUE(exists2.has_value());
    EXPECT_TRUE(*exists2);

    auto batch_exists =
        service_->BatchExistKey({key1, key2}, TenantId::Default());
    ASSERT_EQ(2u, batch_exists.size());
    ASSERT_TRUE(batch_exists[0].has_value());
    EXPECT_FALSE(*batch_exists[0]);
    ASSERT_TRUE(batch_exists[1].has_value());
    EXPECT_TRUE(*batch_exists[1]);

    auto all_keys = service_->GetAllKeys(TenantId::Default());
    ASSERT_TRUE(all_keys.has_value());
    EXPECT_EQ(all_keys->end(),
              std::find(all_keys->begin(), all_keys->end(), key1));
    EXPECT_NE(all_keys->end(),
              std::find(all_keys->begin(), all_keys->end(), key2));

    // Verify objects in segment2 is still there
    auto get_result2 = service_->GetReplicaList(key2, TenantId::Default());
    ASSERT_TRUE(get_result2.has_value());

    // The worker eventually removes the old physical metadata, after which
    // the same key can be inserted again.
    ResumeReplicaCleanup(*service_);
    for (size_t i = 0; i < 100 && service_->GetKeyCount() != 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(1u, service_->GetKeyCount());

    // Verify put key1 will put into segment2 rather than segment1
    auto put_start_result = service_->PutStart(
        client_id, key1, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    auto put_end_result = service_->PutEnd(client_id, key1, TenantId::Default(),
                                           ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());
    auto get_result3 = service_->GetReplicaList(key1, TenantId::Default());
    ASSERT_TRUE(get_result3.has_value());
    auto retrieved = get_result3.value();
    ASSERT_EQ(replica_list[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              segment2.name);
    EXPECT_EQ(2u, service_->GetKeyCount());
}

// A mass client expiry marks handles stale all over the metadata table, so
// the sweep cleans each object under its own write lock instead of holding a
// stripe exclusively for the whole walk. Dropping and retaking the lock
// mid-stripe must not cost coverage: every key of the departed segment is
// erased, and every key of a live segment survives. The keys are forced onto
// one stripe of the route so the sweep returns to it many times.
TEST_F(MasterServiceTest, ClearInvalidHandlesSweepsStripeAcrossLockBatches) {
    auto service = std::make_unique<MasterService>();
    PauseReplicaCleanup(*service);

    constexpr size_t kSegmentSize = 1024 * 1024 * 128;
    const std::string stale_segment_name = "batch_sweep_stale_segment";
    const std::string live_segment_name = "batch_sweep_live_segment";
    const auto stale_segment = PrepareSimpleSegment(
        *service, stale_segment_name, 0x300000000, kSegmentSize);
    const auto live_segment = PrepareSimpleSegment(*service, live_segment_name,
                                                   0x400000000, kSegmentSize);

    // Enough keys per segment that draining the stripe takes several batches.
    constexpr size_t kKeysPerSegment = 100;
    const size_t target_stripe = RouteStripeIndex("batch_sweep_seed");

    std::vector<std::string> stale_keys;
    std::vector<std::string> live_keys;
    for (size_t i = 0;
         stale_keys.size() + live_keys.size() < 2 * kKeysPerSegment; ++i) {
        ASSERT_LT(i, 1000000u)
            << "could not gather enough keys on stripe " << target_stripe;
        const std::string key = "batch_sweep_key_" + std::to_string(i);
        if (RouteStripeIndex(key) != target_stripe) {
            continue;
        }

        const bool on_stale_segment = stale_keys.size() < kKeysPerSegment;
        const UUID& client_id =
            on_stale_segment ? stale_segment.client_id : live_segment.client_id;
        const std::string& segment_name =
            on_stale_segment ? stale_segment_name : live_segment_name;
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name};

        auto put_start = service->PutStart(client_id, key, TenantId::Default(),
                                           1024, config);
        ASSERT_TRUE(put_start.has_value()) << "key=" << key;
        ASSERT_EQ(1u, put_start->size());
        ASSERT_EQ(segment_name, (*put_start)[0]
                                    .get_memory_descriptor()
                                    .buffer_descriptor.transport_endpoint_);
        ASSERT_TRUE(service
                        ->PutEnd(client_id, key, TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value());
        (on_stale_segment ? stale_keys : live_keys).push_back(key);
    }

    ASSERT_TRUE(
        service
            ->UnmountSegment(stale_segment.segment_id, stale_segment.client_id)
            .has_value());
    ClearInvalidHandlesForTest(*service);

    // GetKeyCount counts physical metadata, so it distinguishes "swept" from
    // "merely hidden from the read paths by the unmount".
    EXPECT_EQ(live_keys.size(), service->GetKeyCount());
    for (const auto& key : stale_keys) {
        auto get_result = service->GetReplicaList(key, TenantId::Default());
        ASSERT_FALSE(get_result.has_value()) << "key=" << key;
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error())
            << "key=" << key;
    }
    for (const auto& key : live_keys) {
        auto get_result = service->GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(get_result.has_value()) << "key=" << key << " was swept";
        ASSERT_EQ(1u, get_result->replicas.size()) << "key=" << key;
        EXPECT_TRUE(get_result->replicas[0].is_memory_replica())
            << "key=" << key;
    }
}

// The sweep walks one tenant at a time, so a departed segment's replicas must
// be cleared from every tenant, and one tenant's sweep must leave another's
// object under the same user key alone.
TEST_F(MasterServiceTest, ClearInvalidHandlesSweepsEveryTenant) {
    const TenantId tenant_a("sweep_tenant_a");
    const TenantId tenant_b("sweep_tenant_b");
    auto service = std::make_unique<MasterService>(
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()}));
    PauseReplicaCleanup(*service);

    const std::string stale_segment_name = "tenant_sweep_stale_segment";
    const std::string live_segment_name = "tenant_sweep_live_segment";
    const auto stale_segment =
        PrepareSimpleSegment(*service, stale_segment_name, 0x300000000);
    const auto live_segment =
        PrepareSimpleSegment(*service, live_segment_name, 0x400000000);

    ReplicateConfig stale_config;
    stale_config.replica_num = 1;
    stale_config.preferred_segments = {stale_segment_name};
    ReplicateConfig live_config;
    live_config.replica_num = 1;
    live_config.preferred_segments = {live_segment_name};
    for (const auto& tenant_id : {tenant_a, tenant_b}) {
        PutCompletedObject(*service, stale_segment.client_id, "stale_key",
                           tenant_id, stale_config);
        PutCompletedObject(*service, live_segment.client_id, "live_key",
                           tenant_id, live_config);
    }
    // tenant_b also holds the user key tenant_a loses, on the live segment.
    PutCompletedObject(*service, live_segment.client_id, "mixed_key", tenant_b,
                       live_config);
    PutCompletedObject(*service, stale_segment.client_id, "mixed_key", tenant_a,
                       stale_config);

    ASSERT_TRUE(
        service
            ->UnmountSegment(stale_segment.segment_id, stale_segment.client_id)
            .has_value());
    ClearInvalidHandlesForTest(*service);

    EXPECT_EQ(3u, service->GetKeyCount());
    for (const auto& tenant_id : {tenant_a, tenant_b}) {
        auto stale = service->GetReplicaList("stale_key", tenant_id);
        ASSERT_FALSE(stale.has_value()) << "tenant=" << tenant_id;
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, stale.error())
            << "tenant=" << tenant_id;
        EXPECT_TRUE(service->GetReplicaList("live_key", tenant_id).has_value())
            << "tenant=" << tenant_id;
    }
    EXPECT_FALSE(service->GetReplicaList("mixed_key", tenant_a).has_value());
    EXPECT_TRUE(service->GetReplicaList("mixed_key", tenant_b).has_value());
}

TEST_F(MasterServiceTest, UnmountSegmentKeepsSynchronousCleanupInHaMode) {
    auto config = MasterServiceConfig::builder().set_enable_ha(true).build();
    auto service = std::make_unique<MasterService>(config);

    auto segment = MakeSegment("ha_sync_segment");
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());
    const auto key = GenerateKeyForSegment(client_id, service, segment.name);
    auto exists = service->ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());

    ASSERT_TRUE(service->UnmountSegment(segment.id, client_id).has_value());
    EXPECT_EQ(0u, service->GetKeyCount());
}

TEST_F(MasterServiceTest, CopyInProgressDoesNotKeepUnmountedSourceVisible) {
    auto service = std::make_unique<MasterService>();
    const auto source =
        PrepareSimpleSegment(*service, "copy_source", kDefaultSegmentBase);
    PrepareSimpleSegment(*service, "copy_target",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    const UUID client_id = source.client_id;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "copy_source";
    PutCompletedObject(*service, client_id, "copy_key", config);
    ASSERT_TRUE(service
                    ->CopyStart(client_id, "copy_key", TenantId::Default(),
                                "copy_source", {"copy_target"})
                    .has_value());

    PauseReplicaCleanup(*service);
    ASSERT_TRUE(service->UnmountSegment(source.segment_id, source.client_id)
                    .has_value());

    ExpectKeyHiddenFromReadApis(*service, "copy_key");

    ASSERT_TRUE(service->CopyRevoke(client_id, "copy_key", TenantId::Default())
                    .has_value());
    ResumeReplicaCleanup(*service);
}

TEST_F(MasterServiceTest, MoveInProgressDoesNotKeepUnmountedSourceVisible) {
    auto service = std::make_unique<MasterService>();
    const auto source =
        PrepareSimpleSegment(*service, "move_source", kDefaultSegmentBase);
    PrepareSimpleSegment(*service, "move_target",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    const UUID client_id = source.client_id;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "move_source";
    PutCompletedObject(*service, client_id, "move_key", config);
    ASSERT_TRUE(service
                    ->MoveStart(client_id, "move_key", TenantId::Default(),
                                "move_source", "move_target")
                    .has_value());

    PauseReplicaCleanup(*service);
    ASSERT_TRUE(service->UnmountSegment(source.segment_id, source.client_id)
                    .has_value());

    ExpectKeyHiddenFromReadApis(*service, "move_key");

    ASSERT_TRUE(service->MoveRevoke(client_id, "move_key", TenantId::Default())
                    .has_value());
    ResumeReplicaCleanup(*service);
}

TEST_F(MasterServiceTest, MoveEndReleasesSourceRefcountWhenTargetGone) {
    // The client-visible flow (MoveEnd fails with REPLICA_IS_GONE, a
    // subsequent UpsertStart succeeds, MoveRevoke reports no task) is covered
    // by the MoveEndWithVanishedTargetReleasesSource scenario. This test keeps
    // the private invariant: the source refcount must be released before the
    // move task is erased.
    auto service = std::make_unique<MasterService>();
    const auto source =
        PrepareSimpleSegment(*service, "refcnt_source", kDefaultSegmentBase);
    const auto target = PrepareSimpleSegment(
        *service, "refcnt_target", kDefaultSegmentBase + kDefaultSegmentSize);

    const UUID client_id = source.client_id;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "refcnt_source";
    PutCompletedObject(*service, client_id, "refcnt_key", config);

    ASSERT_TRUE(service
                    ->MoveStart(client_id, "refcnt_key", TenantId::Default(),
                                "refcnt_source", "refcnt_target")
                    .has_value());
    ASSERT_TRUE(service->UnmountSegment(target.segment_id, target.client_id)
                    .has_value());

    auto move_end =
        service->MoveEnd(client_id, "refcnt_key", TenantId::Default());
    ASSERT_FALSE(move_end.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, move_end.error());

    const auto source_refcnt =
        GetReplicaRefcntBySegmentName(*service, "refcnt_key", "refcnt_source");
    ASSERT_TRUE(source_refcnt.has_value());
    EXPECT_EQ(0, source_refcnt.value());
}

TEST_F(MasterServiceTest, PutStartPartialAllocationIsObservable) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount two segments only
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t segment_size = 1024 * 1024 * 64;  // 64MB

    auto segment1 = MakeSegment("segment1", buffer1, segment_size);
    auto segment2 = MakeSegment("segment2", buffer2, segment_size);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(segment1, client_id).has_value());
    ASSERT_TRUE(service_->MountSegment(segment2, client_id).has_value());

    auto& metrics = MasterMetricManager::instance();
    const int64_t partial_before = metrics.get_put_start_partial_allocations();

    // Request more replicas than available segments: best-effort keeps the
    // put successful but the degradation must be recorded.
    ReplicateConfig config;
    config.replica_num = 3;
    auto put_start_result = service_->PutStart(
        client_id, "partial_alloc_key", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(2u, put_start_result->size());
    ASSERT_EQ(metrics.get_put_start_partial_allocations(), partial_before + 1);

    // A fully satisfied allocation must not be counted as partial.
    ReplicateConfig full_config;
    full_config.replica_num = 2;
    auto full_result = service_->PutStart(
        client_id, "full_alloc_key", TenantId::Default(), 1024, full_config);
    ASSERT_TRUE(full_result.has_value());
    ASSERT_EQ(2u, full_result->size());
    ASSERT_EQ(metrics.get_put_start_partial_allocations(), partial_before + 1);
}

TEST_F(MasterServiceTest, UnmountSegmentPerformance) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 256;  // 256MB
    std::string segment_name = "perf_test_segment";
    auto segment = MakeSegment(segment_name, kBufferAddress, kSegmentSize);
    UUID client_id = generate_uuid();

    // Mount a segment for testing
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create 10000 keys for testing
    constexpr int kNumKeys = 1000;
    std::vector<std::string> keys;
    keys.reserve(kNumKeys);

    auto start = std::chrono::steady_clock::now();

    // Create `kNumKeys` keys
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key =
            GenerateKeyForSegment(client_id, service_, segment_name);
        keys.push_back(key);
    }

    auto create_end = std::chrono::steady_clock::now();

    // Execute unmount operation and record operation time
    auto unmount_start = std::chrono::steady_clock::now();
    auto unmount_result = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result.has_value());
    auto unmount_end = std::chrono::steady_clock::now();

    auto unmount_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(unmount_end -
                                                              unmount_start);

    // Unmount operation should be very fast, so we set 1s limit
    EXPECT_LE(unmount_duration.count(), 1000)
        << "Unmount operation took " << unmount_duration.count()
        << "ms which exceeds 1 second limit";

    // Verify all keys are gone
    for (const auto& key : keys) {
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }

    // Output performance report
    auto total_create_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(create_end -
                                                              start);
    std::cout << "\nPerformance Metrics:\n"
              << "Keys created: " << kNumKeys << "\n"
              << "Creation time: " << total_create_duration.count() << "ms\n"
              << "Unmount time: " << unmount_duration.count() << "ms\n";
}

TEST_F(MasterServiceTest, ShrinkBucketsIfSparseThresholds) {
    // Small containers stay untouched regardless of sparsity: their bucket
    // memory is negligible and rehash churn is not worth it.
    std::unordered_map<std::string, int> small;
    small.emplace("small_key", 0);
    const size_t small_buckets = small.bucket_count();
    ASSERT_LE(small_buckets, kShrinkMinBucketCount);
    ShrinkBucketsIfSparse(small);
    EXPECT_EQ(small.bucket_count(), small_buckets);

    // Grow a map well past the bucket floor, then erase most entries: the
    // bucket array keeps its high-water size until explicitly shrunk.
    std::unordered_map<std::string, int> map;
    for (size_t i = 0; i < 4 * kShrinkMinBucketCount; ++i) {
        map.emplace("key" + std::to_string(i), 0);
    }
    const size_t high_water = map.bucket_count();
    ASSERT_GT(high_water, kShrinkMinBucketCount);

    // At exactly a quarter full there is nothing to shrink yet.
    while (map.size() > high_water / 4) {
        map.erase(map.begin());
    }
    ShrinkBucketsIfSparse(map);
    EXPECT_EQ(map.bucket_count(), high_water);

    // One more erase crosses the threshold and triggers the shrink.
    map.erase(map.begin());
    ShrinkBucketsIfSparse(map);
    EXPECT_LT(map.bucket_count(), high_water);
    EXPECT_GE(map.bucket_count(), map.size());
}

TEST_F(MasterServiceTest, BatchEvictShrinksSparseMetadataMaps) {
    // Zero lease TTL so every committed object is immediately evictable.
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();
    constexpr size_t buffer = 0x300000000;
    constexpr size_t object_size = 1024;
    constexpr size_t object_count = 2 * kShrinkMinBucketCount;
    // Size the segment with ample headroom so the background eviction
    // thread never fires; only the explicit call below evicts.
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service_, "test_segment", buffer, object_size * object_count * 16);

    // Pick keys that all hash to one stripe of the route so its map grows
    // past the shrink floor; random keys would spread these objects thinly
    // across all 1024 stripes.
    const size_t target_stripe = RouteStripeIndex("shrink_key_0");
    std::vector<std::string> keys;
    for (size_t i = 0; keys.size() < object_count; ++i) {
        std::string key = "shrink_key_" + std::to_string(i);
        if (RouteStripeIndex(key) != target_stripe) continue;
        keys.push_back(std::move(key));
    }

    ReplicateConfig config;
    config.replica_num = 1;
    for (const auto& key : keys) {
        // Hard-pin the first object: it is excluded from eviction, so the
        // tenant (and its metadata map) deterministically survives the
        // full eviction below and the shrunk bucket count stays
        // observable.
        config.with_hard_pin = (&key == &keys.front());
        ASSERT_TRUE(service_
                        ->PutStart(client_id, key, TenantId::Default(),
                                   object_size, config)
                        .has_value());
        ASSERT_TRUE(service_
                        ->PutEnd(client_id, key, TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value());
    }

    const size_t buckets_before =
        RouteStripeBucketCount(*service_, target_stripe);
    ASSERT_GT(buckets_before, kShrinkMinBucketCount);

    MasterServiceTestPeer(*service_).RunBatchEvictForTesting(1.0, 1.0);

    const size_t buckets_after =
        RouteStripeBucketCount(*service_, target_stripe);
    ASSERT_GT(buckets_after, 0u);
    // Without the route shrinking a stripe as its slots go, the bucket array
    // would still sit at its high-water mark and this assertion would fail.
    EXPECT_LT(buckets_after, buckets_before / 2);
}

TEST_F(MasterServiceTest, BatchEvictCountsARoundOnlyWhenItHadSomethingToDo) {
    // Zero lease TTL so the committed object is immediately evictable.
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service = std::make_unique<MasterService>(service_config);
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(
        service->PutStart(client_id, "key", TenantId::Default(), 1024, config)
            .has_value());
    ASSERT_TRUE(
        service
            ->PutEnd(client_id, "key", TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    auto& metrics = MasterMetricManager::instance();
    const int64_t successes = metrics.get_mem_eviction_success();
    const int64_t attempts = metrics.get_mem_eviction_attempts();

    MasterServiceTestPeer(*service).RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_EQ(metrics.get_mem_eviction_success(), successes + 1);
    EXPECT_EQ(metrics.get_mem_eviction_attempts(), attempts + 1);

    // Nothing left filed and nothing discarded: neither a success nor a
    // failure.
    MasterServiceTestPeer(*service).RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_EQ(metrics.get_mem_eviction_attempts(), attempts + 1);
}

TEST_F(MasterServiceTest, ClearStaleHandlesShrinksSparseMetadataMaps) {
    // Regression for the lease-expire / client-offboarding delete path.
    // ClearInvalidHandles -> ClearStaleHandles can erase tens of millions of
    // keys from a shared tenant; erase() never returns bucket memory, so a
    // tenant that loses most (but not all) of its keys would keep its
    // high-water bucket array forever and RSS would never drop. The route
    // shrinks a stripe as its slots go, whichever path erases them.
    auto service = std::make_unique<MasterService>();
    PauseReplicaCleanup(*service);

    constexpr size_t kSegmentSize = 1024 * 1024 * 128;
    const std::string stale_segment_name = "clear_shrink_stale_segment";
    const std::string live_segment_name = "clear_shrink_live_segment";
    const auto stale_segment = PrepareSimpleSegment(
        *service, stale_segment_name, 0x300000000, kSegmentSize);
    const auto live_segment = PrepareSimpleSegment(*service, live_segment_name,
                                                   0x400000000, kSegmentSize);

    // Gather keys on one stripe of the route so its map grows past the shrink
    // floor; spreading them across all 1024 stripes would leave each map tiny.
    const size_t target_stripe = RouteStripeIndex("clear_shrink_key_0");
    // A few live keys keep the shared tenant alive after the sweep (partial
    // drain, not full erase); the rest are swept and must trigger a shrink.
    constexpr size_t kLiveKeys = 128;
    constexpr size_t kTotalKeys = 2 * kShrinkMinBucketCount;

    std::vector<std::string> stale_keys;
    std::vector<std::string> live_keys;
    for (size_t i = 0; stale_keys.size() + live_keys.size() < kTotalKeys; ++i) {
        ASSERT_LT(i, 5000000u)
            << "could not gather enough keys on stripe " << target_stripe;
        const std::string key = "clear_shrink_key_" + std::to_string(i);
        if (RouteStripeIndex(key) != target_stripe) {
            continue;
        }

        const bool on_live = live_keys.size() < kLiveKeys;
        const UUID& client_id =
            on_live ? live_segment.client_id : stale_segment.client_id;
        const std::string& segment_name =
            on_live ? live_segment_name : stale_segment_name;
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name};

        ASSERT_TRUE(
            service->PutStart(client_id, key, TenantId::Default(), 1024, config)
                .has_value())
            << "key=" << key;
        ASSERT_TRUE(service
                        ->PutEnd(client_id, key, TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value())
            << "key=" << key;
        (on_live ? live_keys : stale_keys).push_back(key);
    }
    ASSERT_GT(stale_keys.size(), live_keys.size());

    const size_t buckets_before =
        RouteStripeBucketCount(*service, target_stripe);
    ASSERT_GT(buckets_before, kShrinkMinBucketCount);

    // Unmount the stale segment, then sweep inline. The tenant survives
    // because the live segment still holds keys, so the metadata map is only
    // partially drained — exactly the case that leaks bucket memory without
    // the shrink.
    ASSERT_TRUE(
        service
            ->UnmountSegment(stale_segment.segment_id, stale_segment.client_id)
            .has_value());
    ClearInvalidHandlesForTest(*service);

    // GetKeyCount counts physical metadata, so it distinguishes "swept" from
    // "merely hidden by the unmount".
    EXPECT_EQ(live_keys.size(), service->GetKeyCount());

    const size_t buckets_after =
        RouteStripeBucketCount(*service, target_stripe);
    ASSERT_GT(buckets_after, 0u);
    // Without the route shrinking a stripe as its slots go, the bucket array
    // would stay at its high-water mark and this assertion would fail.
    EXPECT_LT(buckets_after, buckets_before / 2);
    // The shrunk map must still be large enough to hold every live key.
    EXPECT_GE(buckets_after, live_keys.size());

    for (const auto& key : live_keys) {
        auto get_result = service->GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(get_result.has_value()) << "key=" << key << " was swept";
        ASSERT_EQ(1u, get_result->replicas.size()) << "key=" << key;
    }
    for (const auto& key : stale_keys) {
        auto get_result = service->GetReplicaList(key, TenantId::Default());
        ASSERT_FALSE(get_result.has_value()) << "key=" << key;
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error())
            << "key=" << key;
    }
}

TEST_F(MasterServiceTest, RemoveSoftPinObject) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;

    // Verify soft pin does not block remove
    ASSERT_TRUE(service_
                    ->PutStart(client_id, key, TenantId::Default(),
                               slice_length, config)
                    .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 1u);
    EXPECT_TRUE(service_->Remove(key, TenantId::Default()).has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 0u);

    // Verify soft pin does not block RemoveAll
    ASSERT_TRUE(service_
                    ->PutStart(client_id, key, TenantId::Default(),
                               slice_length, config)
                    .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 1u);
    EXPECT_EQ(1, service_->RemoveAll());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 0u);
}

TEST_F(MasterServiceTest, SoftPinActionsCommitOnFirstReadableUpsert) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(10000)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    enable.soft_pin_ttl_ms = 5000;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "action_key", TenantId::Default(),
                               1024, enable)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    const auto before_first_completion = std::chrono::system_clock::now();
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "action_key", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    const auto initial_deadline = GetSoftPinDeadline(*service, "action_key");
    ASSERT_TRUE(initial_deadline.has_value());
    EXPECT_GT(*initial_deadline,
              before_first_completion + std::chrono::seconds(4));
    EXPECT_LT(*initial_deadline,
              before_first_completion + std::chrono::seconds(9));
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    ReplicateConfig preserve;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "action_key", TenantId::Default(),
                                  1024, preserve)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "action_key"), initial_deadline);
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "action_key", TenantId::Default(),
                                ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "action_key"), initial_deadline);

    ReplicateConfig disable;
    disable.soft_pin_action = SoftPinAction::DISABLE;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "action_key", TenantId::Default(),
                                  2048, disable)
                    .has_value());
    EXPECT_TRUE(GetSoftPinDeadline(*service, "action_key").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "action_key", TenantId::Default(),
                                ReplicaType::MEMORY)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "action_key").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ReplicateConfig enable_again;
    enable_again.soft_pin_action = SoftPinAction::ENABLE;
    enable_again.soft_pin_ttl_ms = 3000;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "action_key", TenantId::Default(),
                                  2048, enable_again)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "action_key").has_value());
    const auto before_enable_again = std::chrono::system_clock::now();
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "action_key", TenantId::Default(),
                                ReplicaType::MEMORY)
                    .has_value());
    const auto enabled_again_deadline =
        GetSoftPinDeadline(*service, "action_key");
    ASSERT_TRUE(enabled_again_deadline.has_value());
    EXPECT_GT(*enabled_again_deadline,
              before_enable_again + std::chrono::seconds(2));
    EXPECT_LT(*enabled_again_deadline,
              before_enable_again + std::chrono::seconds(5));
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);
}

TEST_F(MasterServiceTest, SoftPinDeadlineIndexExpiresOnlyDueEntries) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    PutCompletedObject(*service, client_id, "deadline_key", config);

    ReplicateConfig grouped_config = config;
    grouped_config.group_ids =
        std::vector<std::string>{"grouped_deadline_key_group"};
    PutCompletedObject(*service, client_id, "grouped_deadline_key",
                       grouped_config);

    const auto first_deadline =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    const auto second_deadline = first_deadline + std::chrono::seconds(1);
    SetSoftPinDeadlineForTest(*service, "deadline_key", first_deadline);
    SetSoftPinDeadlineForTest(*service, "grouped_deadline_key",
                              second_deadline);

    EXPECT_EQ(SoftPinRegistrationCount(*service), 2u);
    CleanupExpiredSoftPinsAt(*service, first_deadline);
    EXPECT_FALSE(GetSoftPinDeadline(*service, "deadline_key").has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "grouped_deadline_key"),
              second_deadline);
    EXPECT_EQ(SoftPinRegistrationCount(*service), 1u);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    CleanupExpiredSoftPinsAt(*service, second_deadline);
    EXPECT_FALSE(
        GetSoftPinDeadline(*service, "grouped_deadline_key").has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service), 0u);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest, SoftPinTtlUpdateInvalidatesOldHeapEntry) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(5000)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    PutCompletedObject(*service, client_id, "ttl_update_key", enable);
    const auto first_deadline = GetSoftPinDeadline(*service, "ttl_update_key");
    ASSERT_TRUE(first_deadline.has_value());

    enable.soft_pin_ttl_ms = 20000;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "ttl_update_key",
                                  TenantId::Default(), 1024, enable)
                    .has_value());
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "ttl_update_key",
                                TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    const auto updated_deadline =
        GetSoftPinDeadline(*service, "ttl_update_key");
    ASSERT_TRUE(updated_deadline.has_value());
    EXPECT_GT(*updated_deadline, *first_deadline);
    EXPECT_EQ(SoftPinRegistrationCount(*service), 1u);
    EXPECT_GE(SoftPinDeadlineHeapSize(*service), 2u);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    CleanupExpiredSoftPinsAt(*service, *first_deadline);
    EXPECT_EQ(GetSoftPinDeadline(*service, "ttl_update_key"), updated_deadline);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    CleanupExpiredSoftPinsAt(*service, *updated_deadline);
    EXPECT_FALSE(GetSoftPinDeadline(*service, "ttl_update_key").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest,
       SizeChangingUpsertIndexesInheritedDeadlineBeforeCompletion) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    PutCompletedObject(*service, client_id, "resize_pending", enable);
    const auto inherited_deadline =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    SetSoftPinDeadlineForTest(*service, "resize_pending", inherited_deadline);

    ReplicateConfig preserve;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "resize_pending",
                                  TenantId::Default(), 2048, preserve)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "resize_pending"),
              inherited_deadline);
    EXPECT_EQ(SoftPinRegistrationCount(*service), 1u);

    CleanupExpiredSoftPinsAt(*service, inherited_deadline);
    EXPECT_FALSE(GetSoftPinDeadline(*service, "resize_pending").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "resize_pending",
                                TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "resize_pending").has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service), 0u);
}

TEST_F(MasterServiceTest, SoftPinDeadlineHeapCompactsRepeatedUpdates) {
    MasterService service;
    const auto base = std::chrono::system_clock::now();
    constexpr size_t kUpdates = 5000;
    for (size_t i = 0; i < kUpdates; ++i) {
        UpsertSoftPinDeadlineIndexForTest(
            service, "compaction_key", base + std::chrono::milliseconds(i + 1));
    }

    EXPECT_EQ(SoftPinRegistrationCount(service), 1u);
    EXPECT_LE(SoftPinDeadlineHeapSize(service), 4096u);
    EXPECT_EQ(PopExpiredSoftPinDeadlinesForTest(
                  service, base + std::chrono::milliseconds(kUpdates - 1)),
              0u);
    EXPECT_EQ(SoftPinRegistrationCount(service), 1u);
    EXPECT_EQ(PopExpiredSoftPinDeadlinesForTest(
                  service, base + std::chrono::milliseconds(kUpdates)),
              1u);
}

TEST_F(MasterServiceTest,
       ExpiredSoftPinIsNotCarriedAcrossUpsertMetadataReplacement) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_a = generate_uuid();
    const UUID client_b = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    enable.soft_pin_ttl_ms = 10000;
    ReplicateConfig preserve;

    ASSERT_TRUE(
        service
            ->PutStart(client_a, "preempted", TenantId::Default(), 1024, enable)
            .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_a, "preempted", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    SetSoftPinDeadlineForTest(
        *service, "preempted",
        std::chrono::system_clock::now() - std::chrono::seconds(1));
    ASSERT_TRUE(service
                    ->UpsertStart(client_a, "preempted", TenantId::Default(),
                                  1024, preserve)
                    .has_value());
    ASSERT_TRUE(service
                    ->UpsertStart(client_b, "preempted", TenantId::Default(),
                                  1024, preserve)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "preempted").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(
        service
            ->PutStart(client_a, "resized", TenantId::Default(), 1024, enable)
            .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_a, "resized", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    SetSoftPinDeadlineForTest(
        *service, "resized",
        std::chrono::system_clock::now() - std::chrono::seconds(1));
    ASSERT_TRUE(service
                    ->UpsertStart(client_a, "resized", TenantId::Default(),
                                  2048, preserve)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "resized").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest, RepeatedPutEndDoesNotRefreshSoftPin) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = 5000;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "repeat_end", TenantId::Default(),
                               1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "repeat_end", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    const auto first_deadline = GetSoftPinDeadline(*service, "repeat_end");
    ASSERT_TRUE(first_deadline.has_value());

    ASSERT_TRUE(service
                    ->PutEnd(client_id, "repeat_end", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "repeat_end"), first_deadline);
}

TEST_F(MasterServiceTest, SoftPinExpiresAndGetDoesNotReactivate) {
    const uint64_t kv_lease_ttl = 200;
    const uint64_t kv_soft_pin_ttl = 20;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();
    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    ASSERT_TRUE(service_
                    ->PutStart(client_id, "pin_key", TenantId::Default(),
                               value_size, config)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, "pin_key", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    const auto deadline = GetSoftPinDeadline(*service_, "pin_key");
    ASSERT_TRUE(deadline.has_value());
    CleanupExpiredSoftPinsAt(*service_, *deadline);
    ASSERT_TRUE(
        service_->GetReplicaList("pin_key", TenantId::Default()).has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(service_->ExistKey("pin_key", TenantId::Default()).value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, ExistKeyLeasesPinSegmentButProbeKeyDoesNot) {
    // Set a long lease TTL so leases granted by ExistKey will not expire
    // during the test.
    const uint64_t kv_lease_ttl = 2000;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    constexpr size_t kObjectSize = 2 * 1024 * 1024;

    // ExistKey grants a read lease on every hit, so a scan-heavy client can
    // pin the entire segment: eviction cannot reclaim the probed objects and
    // new allocations fail.
    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context =
            PrepareSimpleSegment(*service_, "exist_lease_segment",
                                 kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        ReplicateConfig config;
        config.replica_num = 1;
        for (const auto& key : {"exist_key_a", "exist_key_b"}) {
            PutCompletedObject(*service_, client_id, key, config, kObjectSize);
            auto exists = service_->ExistKey(key, TenantId::Default());
            ASSERT_TRUE(exists.has_value());
            ASSERT_TRUE(exists.value());
        }

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        auto trigger_result = service_->PutStart(
            client_id, "trigger_exist_eviction", TenantId::Default(),
            kObjectSize, trigger_config);
        ASSERT_FALSE(trigger_result.has_value());
        EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());
    }

    // ProbeKey shares the lookup path but grants no lease, so the probed
    // objects stay evictable and the allocation eventually succeeds by
    // evicting them.
    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "probe_segment", kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        ReplicateConfig config;
        config.replica_num = 1;
        for (const auto& key : {"probe_key_a", "probe_key_b"}) {
            PutCompletedObject(*service_, client_id, key, config, kObjectSize);
            auto probed = service_->ProbeKey(key, TenantId::Default());
            ASSERT_TRUE(probed.has_value());
            ASSERT_TRUE(probed.value());
        }

        // A missing key reports false.
        auto missing =
            service_->ProbeKey("probe_missing_key", TenantId::Default());
        ASSERT_TRUE(missing.has_value());
        EXPECT_FALSE(missing.value());

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        bool allocated = false;
        for (int i = 0; i < 40 && !allocated; ++i) {
            auto trigger_result = service_->PutStart(
                client_id, "trigger_probe_eviction_" + std::to_string(i),
                TenantId::Default(), kObjectSize, trigger_config);
            allocated = trigger_result.has_value();
            if (!allocated) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        EXPECT_TRUE(allocated);
    }
}

TEST_F(MasterServiceTest, BatchProbeKeyReportsPointInTimeExistence) {
    std::unique_ptr<MasterService> service_(new MasterService());

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    auto segment = MakeSegment("probe_batch_segment", buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    const std::string existing_key = "probe_batch_existing_key";
    ReplicateConfig config;
    config.replica_num = 1;
    PutCompletedObject(*service_, client_id, existing_key, config);

    const std::string missing_key = "probe_batch_missing_key";
    auto results = service_->BatchProbeKey(
        {}, {existing_key, missing_key, existing_key}, TenantId::Default());
    ASSERT_EQ(3u, results.size());
    ASSERT_TRUE(results[0].has_value());
    EXPECT_TRUE(*results[0]);
    ASSERT_TRUE(results[1].has_value());
    EXPECT_FALSE(*results[1]);
    ASSERT_TRUE(results[2].has_value());
    EXPECT_TRUE(*results[2]);
}

TEST_F(MasterServiceTest, WrappedBatchExistKeyUsesTenantAwareBatchPath) {
    const TenantId tenant_id("wrapped_batch_exist_tenant");
    auto service_config = MakeStrictWrappedConfig(
        {std::string(TenantId::kDefaultValue), tenant_id.value()});
    WrappedMasterService service_(service_config);

    Segment segment = MakeSegment("wrapped_batch_exist_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_.MountSegment(segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string tenant_key_a = "wrapped_batch_tenant_a";
    const std::string tenant_key_b = "wrapped_batch_tenant_b";
    const std::string default_only_key = "wrapped_batch_default_only";
    const std::string missing_key = "wrapped_batch_missing";

    std::vector<std::string> tenant_keys = {tenant_key_a, tenant_key_b};
    std::vector<uint64_t> tenant_sizes = {1024, 2048};
    auto tenant_put_start = service_.BatchPutStart(
        client_id, tenant_keys, tenant_sizes, config, tenant_id.value());
    ASSERT_EQ(tenant_put_start.size(), tenant_keys.size());
    for (const auto& result : tenant_put_start) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }
    auto tenant_put_end =
        service_.BatchPutEnd(client_id, MakeObjectMetas(tenant_keys),
                             ReplicaType::MEMORY, tenant_id.value());
    ASSERT_EQ(tenant_put_end.size(), tenant_keys.size());
    for (const auto& result : tenant_put_end) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    auto default_put_start =
        service_.PutStart(client_id, default_only_key, 1024, config);
    ASSERT_TRUE(default_put_start.has_value());
    ASSERT_TRUE(service_
                    .PutEnd(client_id,
                            ObjectMeta{default_only_key, std::nullopt},
                            ReplicaType::MEMORY)
                    .has_value());

    auto& metrics = MasterMetricManager::instance();
    const auto base_requests = metrics.get_batch_exist_key_requests();
    const auto base_items = metrics.get_batch_exist_key_items();
    const auto base_failures = metrics.get_batch_exist_key_failures();
    const auto base_partial = metrics.get_batch_exist_key_partial_successes();
    const auto base_failed_items = metrics.get_batch_exist_key_failed_items();

    std::vector<std::string> lookup_keys = {tenant_key_a, default_only_key,
                                            missing_key, tenant_key_b};
    auto resp = service_.BatchExistKey(lookup_keys, tenant_id.value());
    ASSERT_EQ(resp.size(), lookup_keys.size());
    EXPECT_TRUE(resp[0].value());
    EXPECT_FALSE(resp[1].value());
    EXPECT_FALSE(resp[2].value());
    EXPECT_TRUE(resp[3].value());

    EXPECT_EQ(base_requests + 1, metrics.get_batch_exist_key_requests());
    EXPECT_EQ(base_items + lookup_keys.size(),
              metrics.get_batch_exist_key_items());
    EXPECT_EQ(base_failures, metrics.get_batch_exist_key_failures());
    EXPECT_EQ(base_partial, metrics.get_batch_exist_key_partial_successes());
    EXPECT_EQ(base_failed_items, metrics.get_batch_exist_key_failed_items());
}

TEST_F(MasterServiceTest, WrappedWriteBoundaryRejectsInvalidTenantIds) {
    WrappedMasterService service(
        MakeStrictWrappedConfig({"registered-tenant"}));
    ReplicateConfig config;
    config.replica_num = 1;
    const UUID client_id = generate_uuid();

    auto empty =
        service.PutStart(client_id, "empty-tenant-key", 1024, config, "");
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), ErrorCode::TENANT_NOT_REGISTERED);

    const std::string control_tenant("tenant\0bad", 10);
    auto invalid = service.PutStart(client_id, "invalid-tenant-key", 1024,
                                    config, control_tenant);
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), ErrorCode::TENANT_NOT_REGISTERED);
}

TEST_F(MasterServiceTest, WrappedRequestBoundaryRejectsInvalidTenantIds) {
    WrappedMasterService service(
        MakeStrictWrappedConfig({std::string(TenantId::kDefaultValue)}));

    auto invalid = service.GetReplicaList("missing-key", "_invalid-tenant");
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);

    const std::string control_tenant("tenant\0bad", 10);
    auto batch =
        service.BatchGetReplicaList({"key-a", "key-b"}, control_tenant);
    ASSERT_EQ(batch.size(), 2u);
    for (const auto& result : batch) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    std::vector<OffloadTaskItem> tasks = {
        {.tenant_id = "_invalid-tenant", .key = "key", .size = 1}};
    std::vector<StorageObjectMetadata> metadatas = {
        {.bucket_id = 0,
         .offset = 0,
         .key_size = 3,
         .data_size = 1,
         .transport_endpoint = "segment"}};
    auto offload =
        service.NotifyOffloadSuccess(generate_uuid(), tasks, metadatas);
    ASSERT_FALSE(offload.has_value());
    EXPECT_EQ(offload.error(), ErrorCode::INVALID_PARAMS);

    // RemoveAll has a legacy scalar return type and cannot carry ErrorCode.
    EXPECT_EQ(service.RemoveAll(false, "_invalid-tenant"), 0);
}

TEST_F(MasterServiceTest, WrappedRequestBoundaryPreservesTenantNormalization) {
    WrappedMasterService multi_tenant_service(
        MakeStrictWrappedConfig({std::string(TenantId::kDefaultValue)}));
    auto empty = multi_tenant_service.ExistKey("missing-key", "");
    ASSERT_TRUE(empty.has_value());
    EXPECT_FALSE(empty.value());

    WrappedMasterServiceConfig single_tenant_config;
    single_tenant_config.default_kv_lease_ttl = 100;
    single_tenant_config.enable_metric_reporting = false;
    single_tenant_config.enable_multi_tenants = false;
    WrappedMasterService single_tenant_service(single_tenant_config);
    auto invalid =
        single_tenant_service.ExistKey("missing-key", "_invalid-tenant");
    ASSERT_TRUE(invalid.has_value());
    EXPECT_FALSE(invalid.value());
}

TEST_F(MasterServiceTest, PutStartExpiringTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    MasterServiceConfig master_config;
    master_config.put_start_discard_timeout_sec = 3;
    master_config.put_start_release_timeout_sec = 5;
    std::unique_ptr<MasterService> service_(new MasterService(master_config));

    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB

    // Mount 3 segments.
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client_id used to put objects.
    auto client_id = generate_uuid();
    std::string key_1 = "test_key_1", key_2 = "test_key_2";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = kReplicaCnt;

    // Put key_1, should success.
    auto put_start_result = service_->PutStart(
        client_id, key_1, TenantId::Default(), slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Put key_1 again, should fail because the key exists.
    put_start_result = service_->PutStart(client_id, key_1, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Wait for a while until the put-start expired.
    for (size_t i = 0; i <= master_config.put_start_discard_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_1 again, should success because the old one has expired and will
    // be discarded by this put.
    put_start_result = service_->PutStart(client_id, key_1, TenantId::Default(),
                                          slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_1.
    auto put_end_result = service_->PutEnd(
        client_id, key_1, TenantId::Default(), ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Protect key_1 from eviction.
    auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
    EXPECT_TRUE(get_result.has_value());

    // Put key_2, should fail because the key_1 occupied 12MB (6MB processing,
    // 6MB discarded but not yet released) on each segment.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Wait for a while until the discarded replicas are released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec -
                                master_config.put_start_discard_timeout_sec;
         i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should success because the discarded replica has been
    // released.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Wait for a while until key_2 can be discarded and released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should fail because eviction has not been triggered. And
    // this PutStart should trigger the eviction. Only BatchEvict moves the
    // eviction attempt counter, so take the baseline before the trigger: the
    // eviction thread polls every 10 ms, and sampling after the failing
    // PutStart could race a completed BatchEvict and wait for a second one
    // that never comes.
    const int64_t eviction_attempts_before =
        MasterMetricManager::instance().get_mem_eviction_attempts();
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // The failed PutStart above sets need_mem_eviction_, and the eviction
    // thread answers with an asynchronous BatchEvict. Polling PutStart for up
    // to put_start_release_timeout_sec cannot tell that path apart from the
    // periodic DiscardExpiredProcessingReplicas fallback, which releases the
    // same replicas on the same 5 s scale and would pass the test without
    // exercising the immediate eviction, and the periodic path never touches
    // the attempt counter.
    WaitUntil([&] {
        return MasterMetricManager::instance().get_mem_eviction_attempts() >
               eviction_attempts_before;
    });
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    ASSERT_TRUE(put_start_result.has_value())
        << toString(put_start_result.error());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_2.
    put_end_result = service_->PutEnd(client_id, key_2, TenantId::Default(),
                                      ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

TEST_F(MasterServiceTest, TenantTasksCarryTenantInPayload) {
    const TenantId tenant_id("tenant_for_async_task");
    auto service = std::make_unique<MasterService>(
        MakeStrictTenantConfig({tenant_id.value()}));
    const auto ctx0 = PrepareSimpleSegment(*service, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key = "tenant_task_key";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(service
                    ->PutStart(put_client_id, key, tenant_id,
                               /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(
        service->PutEnd(put_client_id, key, tenant_id, ReplicaType::MEMORY)
            .has_value());

    auto copy_task_id = service->CreateCopyTask(key, tenant_id, {"segment_1"});
    ASSERT_TRUE(copy_task_id.has_value());
    auto move_task_id =
        service->CreateMoveTask(key, tenant_id, "segment_0", "segment_1");
    ASSERT_TRUE(move_task_id.has_value());

    auto fetched = service->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched.has_value());
    ASSERT_EQ(fetched->size(), 2u);

    bool saw_copy = false;
    bool saw_move = false;
    for (const auto& assignment : *fetched) {
        if (assignment.id == copy_task_id.value()) {
            ReplicaCopyPayload payload;
            struct_json::from_json(payload, assignment.payload);
            EXPECT_EQ(payload.tenant_id, tenant_id.value());
            EXPECT_EQ(payload.key, key);
            saw_copy = true;
        } else if (assignment.id == move_task_id.value()) {
            ReplicaMovePayload payload;
            struct_json::from_json(payload, assignment.payload);
            EXPECT_EQ(payload.tenant_id, tenant_id.value());
            EXPECT_EQ(payload.key, key);
            saw_move = true;
        }
    }
    EXPECT_TRUE(saw_copy);
    EXPECT_TRUE(saw_move);
}

TEST_F(MasterServiceTest, LegacyTaskPayloadDefaultsTenant) {
    ReplicaCopyPayload copy_payload;
    struct_json::from_json(
        copy_payload,
        R"({"key":"legacy_copy_key","source":"segment_0","targets":["segment_1"]})");
    EXPECT_EQ(copy_payload.tenant_id, TenantId::kDefaultValue);
    EXPECT_EQ(copy_payload.key, "legacy_copy_key");
    EXPECT_EQ(copy_payload.source, "segment_0");
    ASSERT_EQ(copy_payload.targets.size(), 1u);
    EXPECT_EQ(copy_payload.targets[0], "segment_1");

    ReplicaMovePayload move_payload;
    struct_json::from_json(
        move_payload,
        R"({"key":"legacy_move_key","source":"segment_0","target":"segment_1"})");
    EXPECT_EQ(move_payload.tenant_id, TenantId::kDefaultValue);
    EXPECT_EQ(move_payload.key, "legacy_move_key");
    EXPECT_EQ(move_payload.source, "segment_0");
    EXPECT_EQ(move_payload.target, "segment_1");
}

TEST_F(MasterServiceTest,
       CreateDrainJobMarksSegmentDrainingAndSkipsAllocation) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::DRAINING);

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    auto put_result =
        service_->PutStart(ctx0.client_id, "drain_skip_allocation_key",
                           TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_result.has_value());
    ASSERT_EQ(put_result->size(), 1u);
    EXPECT_EQ(put_result->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_1");
    ASSERT_TRUE(service_
                    ->PutEnd(ctx0.client_id, "drain_skip_allocation_key",
                             TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
}

TEST_F(MasterServiceTest, DrainJobSchedulesMoveTaskAndConvergesToDrained) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key =
        PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    WaitUntil(
        [&] { return ExecutePendingMoveTasks(*service_, ctx0.client_id); });

    WaitUntil([&] {
        auto query = service_->QueryDrainJob(job_id.value());
        return query.has_value() && query->status == JobStatus::SUCCEEDED;
    });

    auto query = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->status, JobStatus::SUCCEEDED);
    EXPECT_EQ(query->active_units, 0u);
    EXPECT_GE(query->succeeded_units, 1u);

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::DRAINED);

    auto replicas = service_->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : replicas->replicas) {
        segment_names.insert(replica.get_memory_descriptor()
                                 .buffer_descriptor.transport_endpoint_);
    }
    EXPECT_TRUE(segment_names.contains("segment_1"));
    EXPECT_FALSE(segment_names.contains("segment_0"));
}

// Drain plans and completes one tenant at a time: the segment counts as
// drained only once every tenant's object under the same user key has left
// it.
TEST_F(MasterServiceTest, DrainJobMovesObjectsOfEveryTenant) {
    const TenantId tenant_a("drain_tenant_a");
    const TenantId tenant_b("drain_tenant_b");
    auto service_config =
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()});
    service_config.default_kv_lease_ttl = 0;
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"segment_0"};
    const std::string key = "drain_shared_key";
    for (const auto& tenant_id : {tenant_a, tenant_b}) {
        PutCompletedObject(*service_, generate_uuid(), key, tenant_id, config);
    }

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 2;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    WaitUntil(
        [&] {
            (void)ExecutePendingMoveTasks(*service_, ctx0.client_id);
            auto query = service_->QueryDrainJob(job_id.value());
            return query.has_value() && query->status == JobStatus::SUCCEEDED;
        },
        std::chrono::seconds(10));

    auto query = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->succeeded_units, 2u);

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::DRAINED);

    for (const auto& tenant_id : {tenant_a, tenant_b}) {
        auto replicas = service_->GetReplicaList(key, tenant_id);
        ASSERT_TRUE(replicas.has_value()) << "tenant=" << tenant_id;
        ASSERT_EQ(1u, replicas->replicas.size()) << "tenant=" << tenant_id;
        EXPECT_EQ("segment_1", replicas->replicas[0]
                                   .get_memory_descriptor()
                                   .buffer_descriptor.transport_endpoint_)
            << "tenant=" << tenant_id;
    }
}

TEST_F(MasterServiceTest, CancelDrainJobRestoresSegmentStatus) {
    auto service_ = std::make_unique<MasterService>();

    [[maybe_unused]] const auto ctx0 = PrepareSimpleSegment(
        *service_, "segment_0", 0x300000000, kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    auto draining_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(draining_status.has_value());
    EXPECT_EQ(draining_status.value(), SegmentStatus::DRAINING);

    auto cancel_result = service_->CancelDrainJob(job_id.value());
    ASSERT_TRUE(cancel_result.has_value());

    auto job = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->status, JobStatus::CANCELED);

    auto restored_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(restored_status.has_value());
    EXPECT_EQ(restored_status.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, CancelDrainJobRejectsActiveMoveTasks) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    WaitUntil([&] {
        auto fetched = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
        return fetched.has_value() && !fetched->empty();
    });

    auto cancel_result = service_->CancelDrainJob(job_id.value());
    ASSERT_FALSE(cancel_result.has_value());
    EXPECT_EQ(cancel_result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(MasterServiceTest, DrainJobFailsAfterRetryBudgetExhausted) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    for (int attempt = 0; attempt < 3; ++attempt) {
        WaitUntil(
            [&] { return FailPendingMoveTasks(*service_, ctx0.client_id); });
    }

    WaitUntil([&] {
        auto query = service_->QueryDrainJob(job_id.value());
        return query.has_value() && query->status == JobStatus::FAILED;
    });

    auto query = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->status, JobStatus::FAILED);
    EXPECT_EQ(query->active_units, 0u);
    EXPECT_GE(query->failed_units, 3u);

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, SetSegmentStatusStopsAndRestoresAllocation) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    auto replica_endpoints = [&](const std::string& key) {
        std::unordered_set<std::string> endpoints;
        auto replicas = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_TRUE(replicas.has_value());
        if (replicas.has_value()) {
            for (const auto& replica : replicas->replicas) {
                endpoints.insert(replica.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_);
            }
        }
        return endpoints;
    };
    auto put_preferring_segment_0 = [&] {
        return replica_endpoints(
            PutObjectOnSegment(*service_, ctx0.client_id, "segment_0"));
    };

    const std::string existing_key =
        PutObjectOnSegment(*service_, ctx0.client_id, "segment_0");

    ASSERT_TRUE(service_->SetSegmentStatus("segment_0", SegmentStatus::DRAINING)
                    .has_value());
    ASSERT_TRUE(service_->SetSegmentStatus("segment_0", SegmentStatus::DRAINING)
                    .has_value());
    auto status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::DRAINING);

    EXPECT_EQ(put_preferring_segment_0(),
              std::unordered_set<std::string>{"segment_1"});
    EXPECT_EQ(replica_endpoints(existing_key),
              std::unordered_set<std::string>{"segment_0"});

    ASSERT_TRUE(
        service_->SetSegmentStatus("segment_0", SegmentStatus::OK).has_value());
    status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::OK);

    EXPECT_EQ(put_preferring_segment_0(),
              std::unordered_set<std::string>{"segment_0"});
}

TEST_F(MasterServiceTest, SetSegmentStatusRejectsOtherTargetStatuses) {
    auto service_ = std::make_unique<MasterService>();
    [[maybe_unused]] const auto ctx0 = PrepareSimpleSegment(
        *service_, "segment_0", 0x300000000, kDefaultSegmentSize);

    for (auto target :
         {SegmentStatus::UNDEFINED, SegmentStatus::DRAINED,
          SegmentStatus::GRACEFULLY_UNMOUNTING, SegmentStatus::UNMOUNTING}) {
        auto result = service_->SetSegmentStatus("segment_0", target);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    auto status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, SetSegmentStatusRejectsUnknownSegment) {
    auto service_ = std::make_unique<MasterService>();

    auto result =
        service_->SetSegmentStatus("no_such_segment", SegmentStatus::DRAINING);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(MasterServiceTest, SetSegmentStatusRejectsSegmentUnderDrainJob) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    // The object keeps the job unfinished because no one runs its move task.
    PutObjectOnSegment(*service_, ctx0.client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;
    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    for (auto target : {SegmentStatus::OK, SegmentStatus::DRAINING}) {
        auto result = service_->SetSegmentStatus("segment_0", target);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    auto status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::DRAINING);
}

// ===================== Client Offboarding Tests =====================

TEST_F(MasterServiceTest, ClientOffboardingRetryPolicy) {
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(1), std::chrono::seconds(1));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(2), std::chrono::seconds(2));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(3), std::chrono::seconds(4));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(4), std::chrono::seconds(8));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(5), std::chrono::seconds(16));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(6), std::chrono::seconds(30));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(100),
              std::chrono::seconds(30));
    EXPECT_FALSE(ClientOffboardingShouldAlertForTest(9));
    EXPECT_TRUE(ClientOffboardingShouldAlertForTest(10));
    EXPECT_TRUE(ClientOffboardingShouldAlertForTest(11));
}

TEST_F(MasterServiceTest, ReMountDoesNotRecoverSuspectedClient) {
    MasterService service;
    auto segment = MakeSegment("suspected_remount_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    const auto liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(liveness);
    ASSERT_EQ(
        liveness->Evaluate(ClientLivenessRecord::Clock::now(),
                           std::chrono::seconds::zero(), std::chrono::hours(1)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    MasterMetricManager::instance().client_liveness_became_suspected();

    ASSERT_TRUE(service.ReMountSegment({segment}, client_id).has_value());
    EXPECT_EQ(liveness->state(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(service.Ping(client_id)->client_status, ClientStatus::OK);
    EXPECT_EQ(liveness->state(), ClientLivenessState::ACTIVE);
}

TEST_F(MasterServiceTest,
       InPlaceUpsertRejectsSuspectedTargetWithoutChangingMetadata) {
    MasterService service;
    auto segment = MakeSegment("suspected_upsert_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    const auto key = PutObjectOnSegment(service, client_id, segment.name);
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = segment.name;

    const auto liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(liveness);
    ASSERT_EQ(
        liveness->Evaluate(ClientLivenessRecord::Clock::now(),
                           std::chrono::seconds::zero(), std::chrono::hours(1)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    MasterMetricManager::instance().client_liveness_became_suspected();

    auto upsert =
        service.UpsertStart(client_id, key, TenantId::Default(), 1024, config);
    ASSERT_FALSE(upsert.has_value());
    EXPECT_EQ(upsert.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    ASSERT_TRUE(service.Ping(client_id).has_value());
    auto get = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(get.has_value()) << toString(get.error());
    ASSERT_EQ(get->replicas.size(), 1u);
    EXPECT_EQ(get->replicas.front().status, ReplicaStatus::COMPLETE);
}

TEST_F(MasterServiceTest,
       ClientOffboardingProcessesRealSegmentAndMetadataResiduals) {
    MasterService service;
    auto segment = MakeSegment("offboarding_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    const std::string key =
        PutObjectOnSegment(service, client_id, segment.name);
    const auto liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(liveness);

    ClientOffboardingJob job;
    job.client_id = client_id;
    job.liveness = liveness;
    job.pending_prepare_segments.push_back(
        {.segment_id = segment.id,
         .segment_name = segment.name,
         .transport_endpoint = segment.te_endpoint});

    ASSERT_TRUE(ProcessClientOffboardingForTest(service, job));
    EXPECT_TRUE(job.pending_prepare_segments.empty());
    EXPECT_TRUE(job.prepared_segments.empty());
    EXPECT_TRUE(job.metadata_cleanup_accepted);
    EXPECT_TRUE(job.local_ssd_unregistered);
    EXPECT_FALSE(FindClientLivenessForTest(service, client_id));
    EXPECT_FALSE(service.QuerySegmentStatusById(segment.id).has_value());

    auto exists = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(*exists);
}

TEST_F(MasterServiceTest,
       ClientOffboardingKeepsPreparedResidualWithoutRepreparingIt) {
    MasterService service;
    const UUID client_id = generate_uuid();
    auto prepared_segment = MakeSegment("offboarding_prepared_segment");
    auto blocked_segment =
        MakeSegment("offboarding_blocked_segment", /*base=*/0x400000000);
    ASSERT_TRUE(service.MountSegment(prepared_segment, client_id).has_value());
    ASSERT_TRUE(service.MountSegment(blocked_segment, client_id).has_value());
    size_t blocked_metrics_dec_capacity = 0;
    ASSERT_EQ(ErrorCode::OK,
              PrepareUnmountSegmentForTest(service, blocked_segment.id,
                                           blocked_metrics_dec_capacity));

    ClientOffboardingJob job;
    job.client_id = client_id;
    job.liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(job.liveness);
    job.pending_prepare_segments = {
        {.segment_id = prepared_segment.id,
         .segment_name = prepared_segment.name,
         .transport_endpoint = prepared_segment.te_endpoint},
        {.segment_id = blocked_segment.id,
         .segment_name = blocked_segment.name,
         .transport_endpoint = blocked_segment.te_endpoint}};

    ASSERT_FALSE(ProcessClientOffboardingForTest(service, job));
    ASSERT_EQ(job.prepared_segments.size(), 1u);
    ASSERT_EQ(job.pending_prepare_segments.size(), 1u);
    EXPECT_EQ(job.prepared_segments.front().segment_id, prepared_segment.id);
    EXPECT_EQ(job.pending_prepare_segments.front().segment_id,
              blocked_segment.id);
    const auto retained_capacity =
        job.prepared_segments.front().metrics_dec_capacity;

    ASSERT_FALSE(ProcessClientOffboardingForTest(service, job));
    ASSERT_EQ(job.prepared_segments.size(), 1u);
    ASSERT_EQ(job.pending_prepare_segments.size(), 1u);
    EXPECT_EQ(job.prepared_segments.front().segment_id, prepared_segment.id);
    EXPECT_EQ(job.prepared_segments.front().metrics_dec_capacity,
              retained_capacity);

    ASSERT_EQ(ErrorCode::OK, CommitUnmountSegmentForTest(
                                 service, blocked_segment.id, client_id,
                                 blocked_metrics_dec_capacity));
    ASSERT_TRUE(ProcessClientOffboardingForTest(service, job));
    EXPECT_TRUE(job.prepared_segments.empty());
    EXPECT_TRUE(job.pending_prepare_segments.empty());
    EXPECT_FALSE(FindClientLivenessForTest(service, client_id));
}

// Localhost-only HTTP server that records DELETE request lines. A test picks
// which requests (by 1-based arrival number) the server holds before it
// answers, and releases them one by one, so the cleanup worker can be stopped
// while a chosen DELETE is in flight.
class RecordingDeleteServer {
   public:
    RecordingDeleteServer() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(listen_fd_, 0) << "socket() failed";
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        EXPECT_EQ(0, ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr),
                            sizeof(addr)))
            << "bind() failed";
        EXPECT_EQ(0, ::listen(listen_fd_, 16)) << "listen() failed";
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { Serve(); });
    }

    ~RecordingDeleteServer() {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            stopping_ = true;
        }
        cv_.notify_all();
        thread_.join();
        ::close(listen_fd_);
    }

    std::string Url() const {
        return "http://127.0.0.1:" + std::to_string(port_) + "/metadata";
    }

    // Hold the answer to the request_number-th DELETE until it is released.
    // Call before that request can arrive.
    void HoldRequest(size_t request_number) {
        std::lock_guard<std::mutex> lk(mutex_);
        held_.insert(request_number);
    }

    void ReleaseRequest(size_t request_number) {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            released_.insert(request_number);
        }
        cv_.notify_all();
    }

    size_t DeleteCount() {
        std::lock_guard<std::mutex> lk(mutex_);
        return requests_.size();
    }

    // Blocks until at least count DELETE requests have arrived.
    bool WaitForDeleteCount(size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lk(mutex_);
        return cv_.wait_for(lk, timeout,
                            [&] { return requests_.size() >= count; });
    }

    std::vector<std::string> Requests() {
        std::lock_guard<std::mutex> lk(mutex_);
        return requests_;
    }

   private:
    void Serve() {
        while (!stopping_) {
            pollfd pfd{listen_fd_, POLLIN, 0};
            if (::poll(&pfd, 1, 20) <= 0) {
                continue;
            }
            const int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) {
                continue;
            }
            Handle(fd);
            ::close(fd);
        }
    }

    void Handle(int fd) {
        std::string request;
        char buf[1024];
        while (request.find("\r\n\r\n") == std::string::npos) {
            pollfd pfd{fd, POLLIN, 0};
            if (::poll(&pfd, 1, 2000) <= 0) {
                return;
            }
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) {
                return;
            }
            request.append(buf, static_cast<size_t>(n));
        }
        {
            std::unique_lock<std::mutex> lk(mutex_);
            requests_.push_back(request.substr(0, request.find("\r\n")));
            const size_t request_number = requests_.size();
            cv_.notify_all();
            if (held_.count(request_number) != 0) {
                cv_.wait(lk, [&] {
                    return stopping_ || released_.count(request_number) != 0;
                });
            }
        }
        static constexpr char kOk[] =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        (void)::send(fd, kOk, sizeof(kOk) - 1, MSG_NOSIGNAL);
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::string> requests_;
    std::set<size_t> held_;
    std::set<size_t> released_;
};

// Drives one deterministic sequence against a held request:
//  1. The server holds DELETE #1 (segment_a, ram key). segment_b and
//     segment_c queue behind it as one batch.
//  2. Releasing #1 lets the worker send segment_a's rpc key (#2), then take
//     [segment_b, segment_c].
//  3. The server holds DELETE #3 (segment_b, ram key); segment_d is queued and
//     stop starts on another thread.
//  4. While #3 is held, stop has not returned (it joins the worker) and the
//     queue is empty.
//  5. Releasing #3 ends the worker without segment_b's rpc key and without
//     segment_c; stop returns.
//  6. A cleanup request after stop is dropped.
TEST_F(MasterServiceTest, StopHttpMetadataCleanupWaitsForInFlightDelete) {
    RecordingDeleteServer server;
    server.HoldRequest(1);
    server.HoldRequest(3);

    MasterService service;
    service.setHttpMetadataRemoteUrl(server.Url());
    MasterServiceTestPeer peer(service);

    std::thread stop_thread;
    // Never leave a held request or the stop thread behind on an early
    // assertion failure.
    auto cleanup = [&] {
        server.ReleaseRequest(1);
        server.ReleaseRequest(3);
        if (stop_thread.joinable()) {
            stop_thread.join();
        }
    };

    // 1. The worker takes [segment_a] and blocks inside DELETE #1.
    peer.CleanupHttpMetadataForTesting("segment_a");
    if (!server.WaitForDeleteCount(1, std::chrono::seconds(5))) {
        cleanup();
        FAIL() << "the cleanup worker never sent its first DELETE";
    }
    peer.CleanupHttpMetadataForTesting("segment_b");
    peer.CleanupHttpMetadataForTesting("segment_c");
    EXPECT_EQ(2u, peer.HttpMetadataCleanupQueueSizeForTesting());

    // 2. Release #1: the worker sends segment_a's rpc key, then takes the
    // [segment_b, segment_c] batch and blocks inside DELETE #3.
    server.ReleaseRequest(1);
    if (!server.WaitForDeleteCount(3, std::chrono::seconds(5))) {
        cleanup();
        FAIL() << "the worker did not reach DELETE #3";
    }
    EXPECT_EQ(0u, peer.HttpMetadataCleanupQueueSizeForTesting());

    // 3. Queue one more segment, then stop while #3 is in flight.
    peer.CleanupHttpMetadataForTesting("segment_d");
    EXPECT_EQ(1u, peer.HttpMetadataCleanupQueueSizeForTesting());
    // The packaged task sets the future inside the thread runtime, which
    // removes the user-code window after Stop returns. A scheduler pause of
    // more than 150 ms after the queue clear is the only residual window;
    // the queue gate above is bounded at 5 s, and a stall past the HTTP
    // client's request timeout can make a correct run finish early.
    std::packaged_task<void()> stop_task(
        [&] { service.StopHttpMetadataCleanup(); });
    auto stop_done = stop_task.get_future();
    stop_thread = std::thread(std::move(stop_task));

    // 4. Stop drops the queue at once but keeps joining the held worker.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (peer.HttpMetadataCleanupQueueSizeForTesting() != 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(0u, peer.HttpMetadataCleanupQueueSizeForTesting())
        << "stop did not drop the queued segment";
    EXPECT_EQ(std::future_status::timeout,
              stop_done.wait_for(std::chrono::milliseconds(150)))
        << "StopHttpMetadataCleanup returned while a DELETE was in flight";

    // 5. Release #3: the worker skips segment_b's rpc key and the rest of the
    // batch, and stop returns.
    server.ReleaseRequest(3);
    stop_thread.join();
    EXPECT_EQ(std::future_status::ready,
              stop_done.wait_for(std::chrono::milliseconds(0)));
    EXPECT_EQ(3u, server.DeleteCount())
        << "the second key or the rest of the batch was sent after stop";
    const auto requests = server.Requests();
    ASSERT_EQ(3u, requests.size());
    for (const auto& request : requests) {
        EXPECT_NE(std::string::npos, request.find("DELETE "));
    }
    EXPECT_NE(std::string::npos, requests[0].find("ram"));
    EXPECT_NE(std::string::npos, requests[0].find("segment_a"));
    EXPECT_NE(std::string::npos, requests[1].find("rpc_meta"));
    EXPECT_NE(std::string::npos, requests[1].find("segment_a"));
    EXPECT_NE(std::string::npos, requests[2].find("ram"));
    EXPECT_NE(std::string::npos, requests[2].find("segment_b"));

    // 6. A late client-monitor callback must not grow the queue or send.
    peer.CleanupHttpMetadataForTesting("segment_e");
    EXPECT_EQ(0u, peer.HttpMetadataCleanupQueueSizeForTesting())
        << "a cleanup request was queued after stop";
    EXPECT_FALSE(server.WaitForDeleteCount(4, std::chrono::milliseconds(300)))
        << "a DELETE arrived after StopHttpMetadataCleanup returned";
}

TEST_F(MasterServiceTest, StopHttpMetadataCleanupIsNoOpWhenNeverStarted) {
    MasterService service;
    service.StopHttpMetadataCleanup();
    service.StopHttpMetadataCleanup();

    // Cleanup stays disabled: neither a co-located server nor a remote URL.
    MasterServiceTestPeer(service).CleanupHttpMetadataForTesting("segment_a");
    service.StopHttpMetadataCleanup();
}

TEST_F(MasterServiceTest, ActiveClientsExcludeSegmentlessDfsClients) {
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_active_clients();
    {
        MasterService service;
        const auto provider = generate_uuid();
        const auto segment = MakeSegment();
        ASSERT_TRUE(service.MountSegment(segment, provider).has_value());
        ASSERT_TRUE(service.ReMountSegment({segment}, provider).has_value());
        ASSERT_EQ(baseline + 1, metrics.get_active_clients());

        // DFS consumers register through an empty remount and still need
        // heartbeat/liveness management, even though they provide no memory.
        for (int i = 0; i < 5; ++i) {
            const auto consumer = generate_uuid();
            ASSERT_TRUE(service.ReMountSegment({}, consumer).has_value());
            ASSERT_TRUE(service.ReMountSegment({}, consumer).has_value());
            const auto ping = service.Ping(consumer);
            ASSERT_TRUE(ping.has_value());
            EXPECT_EQ(ClientStatus::OK, ping->client_status);
            EXPECT_EQ(baseline + 1, metrics.get_active_clients());
        }
    }
    EXPECT_EQ(baseline, metrics.get_active_clients());
}

TEST_F(MasterServiceTest, ActiveClientsFollowMemorySegmentLifetime) {
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_active_clients();
    MasterService service;
    const auto client = generate_uuid();
    const auto first = MakeSegment("first");
    const auto second = MakeSegment("second", 0x400000000);
    ASSERT_TRUE(service.ReMountSegment({}, client).has_value());
    ASSERT_EQ(baseline, metrics.get_active_clients());

    ASSERT_TRUE(service.MountSegment(first, client).has_value());
    ASSERT_TRUE(service.MountSegment(first, client).has_value());
    ASSERT_TRUE(service.MountSegment(second, client).has_value());
    ASSERT_TRUE(service.ReMountSegment({first, second}, client).has_value());
    EXPECT_EQ(baseline + 1, metrics.get_active_clients());

    ASSERT_TRUE(service.UnmountSegment(first.id, client).has_value());
    EXPECT_EQ(baseline + 1, metrics.get_active_clients());
    ASSERT_TRUE(service.UnmountSegment(second.id, client).has_value());
    ASSERT_TRUE(service.UnmountSegment(second.id, client).has_value());
    ASSERT_TRUE(service.ReMountSegment({}, client).has_value());
    EXPECT_EQ(baseline, metrics.get_active_clients());

    ASSERT_TRUE(service.MountSegment(first, client).has_value());
    EXPECT_EQ(baseline + 1, metrics.get_active_clients());
}

TEST_F(MasterServiceTest, ActiveClientsExpireWithoutCountingDfsConsumers) {
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_active_clients();
    auto config = MasterServiceConfig::builder()
                      .set_client_live_ttl_sec(1)
                      .build();
    MasterService service(config);
    const auto provider = generate_uuid();
    const auto consumer = generate_uuid();
    const auto segment = MakeSegment();
    ASSERT_TRUE(service.ReMountSegment({segment}, provider).has_value());
    ASSERT_TRUE(service.ReMountSegment({}, consumer).has_value());
    ASSERT_EQ(baseline + 1, metrics.get_active_clients());

    WaitUntil([&] { return metrics.get_active_clients() == baseline; });
    for (const auto& client : {provider, consumer}) {
        const auto ping = service.Ping(client);
        ASSERT_TRUE(ping.has_value());
        EXPECT_EQ(ClientStatus::NEED_REMOUNT, ping->client_status);
    }
    ASSERT_TRUE(service.ReMountSegment({}, consumer).has_value());
    EXPECT_EQ(baseline, metrics.get_active_clients());
    ASSERT_TRUE(service.ReMountSegment({segment}, provider).has_value());
    EXPECT_EQ(baseline + 1, metrics.get_active_clients());
}

TEST_F(MasterServiceTest, DfsReadFilterDoesNotBackfillMemoryReplica) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_backfill_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar allocator_type("MOONCAKE_DFS_ALLOCATOR_TYPE", "bucket");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "1048576");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "16");
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto initial = service.PutStart(context.client_id, "dfs_backfill",
                                        TenantId::Default(), 4096, config);
        ASSERT_TRUE(initial.has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_backfill",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        service.RunBatchEvictForTesting(1.0, 1.0);
        ASSERT_TRUE(service.SetDfsFilterMode(1).has_value());
        auto visible =
            service.GetReplicaList("dfs_backfill", TenantId::Default());
        ASSERT_TRUE(visible.has_value());
        ASSERT_EQ(visible->replicas.size(), 1u);
        EXPECT_TRUE(visible->replicas.front().is_dfs_replica());

        auto exists = service.ExistKey("dfs_backfill", TenantId::Default());
        ASSERT_TRUE(exists.has_value());
        EXPECT_FALSE(exists.value());
        auto batch_exists =
            service.BatchExistKey({"dfs_backfill"}, TenantId::Default());
        ASSERT_EQ(batch_exists.size(), 1u);
        ASSERT_TRUE(batch_exists.front().has_value());
        EXPECT_FALSE(batch_exists.front().value());

        auto backfill = service.PutStart(context.client_id, "dfs_backfill",
                                         TenantId::Default(), 4096, config);
        ASSERT_FALSE(backfill.has_value());
        EXPECT_EQ(backfill.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

        ASSERT_TRUE(service.SetDfsFilterMode(0).has_value());
        auto visible_after_filter =
            service.GetReplicaList("dfs_backfill", TenantId::Default());
        ASSERT_TRUE(visible_after_filter.has_value());
        ASSERT_EQ(visible_after_filter->replicas.size(), 1u);
        EXPECT_TRUE(visible_after_filter->replicas.front().is_dfs_replica());

        auto admin =
            service.GetReplicaListForAdmin("dfs_backfill", TenantId::Default());
        ASSERT_TRUE(admin.has_value());
        ASSERT_EQ(admin->replicas.size(), 1u);
        EXPECT_TRUE(admin->replicas.front().is_dfs_replica());
    }
    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, DfsReadFilterKeepsDfsOnlyGetVisible) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_filtered_not_found_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        auto start = service.PutStart(context.client_id, "dfs_filtered_only",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_filtered_only",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        service.RunBatchEvictForTesting(1.0, 1.0);
        ASSERT_TRUE(service.SetDfsFilterMode(1).has_value());

        auto single =
            service.GetReplicaList("dfs_filtered_only", TenantId::Default());
        ASSERT_TRUE(single.has_value());
        ASSERT_EQ(single->replicas.size(), 1u);
        EXPECT_TRUE(single->replicas.front().is_dfs_replica());

        auto batch = service.BatchGetReplicaList({"dfs_filtered_only"},
                                                 TenantId::Default());
        ASSERT_EQ(batch.size(), 1u);
        ASSERT_TRUE(batch.front().has_value());
        ASSERT_EQ(batch.front()->replicas.size(), 1u);
        EXPECT_TRUE(batch.front()->replicas.front().is_dfs_replica());

        auto admin = service.GetReplicaListForAdmin("dfs_filtered_only",
                                                    TenantId::Default());
        ASSERT_TRUE(admin.has_value());
        ASSERT_EQ(admin->replicas.size(), 1u);
        EXPECT_TRUE(admin->replicas.front().is_dfs_replica());
    }
    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest,
       DfsCapacityUnlimitedMetricDoesNotReuseLegacyFileStorageFlag) {
    const auto dfs_root = (std::filesystem::temp_directory_path() /
                           ("master_dfs_metric_" +
                            std::to_string(::getpid())))
                              .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator_type("MOONCAKE_DFS_ALLOCATOR_TYPE", "shard");
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    MasterService service;
    auto& metrics = MasterMetricManager::instance();
    metrics.set_dfs_capacity_unlimited(true);  // Legacy root-fs state.
    service.RefreshDfsMetrics();

    EXPECT_NE(metrics.serialize_metrics().find("master_dfs_capacity_unlimited 0"),
              std::string::npos);
    metrics.set_dfs_capacity_unlimited(false);
}

TEST_F(MasterServiceTest, MemoryEvictionContinuesWhileDfsMaintenanceIsBlocked) {
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "0");
    BlockingDfsAllocator allocator(/*blocked=*/true);
    const auto config = MasterServiceConfig::builder()
                            .set_default_kv_lease_ttl(0)
                            .set_eviction_ratio(1.0)
                            .build();
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    PutCompletedObject(service, context.client_id, "memory_during_dfs", {});

    MasterServiceTestPeer::SetDfsAllocator(service, &allocator);
    WaitUntil([&] { return allocator.checks.load() > 0; });
    MasterServiceTestPeer::RequestMemoryEviction(service);
    WaitUntil([&] {
        return !service
                    .GetReplicaListForAdmin("memory_during_dfs",
                                            TenantId::Default())
                    .has_value();
    });
    allocator.Unblock();
}

TEST_F(MasterServiceTest, DfsMaintenanceContinuesWhileMemoryKeyIsLocked) {
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "0");
    BlockingDfsAllocator allocator(/*blocked=*/false);
    const auto config =
        MasterServiceConfig::builder().set_eviction_ratio(1.0).build();
    MasterService service(config);
    MasterServiceTestPeer::SetDfsAllocator(service, &allocator);
    WaitUntil([&] { return allocator.checks.load() > 0; });

    const auto context = PrepareSimpleSegment(service);
    PutCompletedObject(service, context.client_id, "locked_memory_key", {});
    auto key_lock = MasterServiceTestPeer::WriteObject(
        service, {TenantId::Default(), "locked_memory_key"});
    ASSERT_TRUE(key_lock);
    MasterServiceTestPeer::RequestMemoryEviction(service);
    const auto checks_before = allocator.checks.load();
    // DFS maintenance continues independently while the memory key is contended.
    WaitUntil([&] { return allocator.checks.load() >= checks_before + 3; });
}

TEST_F(MasterServiceTest, DfsGetMetricsFollowFirstReadableReplica) {
    const auto root = MakeDfsTestRoot("query_metrics");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        auto& metrics = MasterMetricManager::instance();

        struct GetMetrics {
            int64_t dfs_hits;
            int64_t dfs_bytes;
            int64_t mem_hits;
            int64_t file_hits;
            int64_t mem_bytes;
            int64_t file_bytes;
            int64_t valid_gets;
            int64_t total_gets;
        };
        const auto capture_metrics = [&]() {
            const auto serialized = metrics.serialize_metrics();
            const auto read_counter = [&](const std::string& name) {
                const auto value = ReadPrometheusCounter(serialized, name);
                EXPECT_TRUE(value.has_value()) << "missing metric " << name;
                return value.value_or(-1);
            };
            return GetMetrics{
                .dfs_hits = read_counter("dfs_cache_hit_nums_"),
                .dfs_bytes = read_counter("dfs_cache_hit_bytes_total"),
                .mem_hits = read_counter("mem_cache_hit_nums_"),
                .file_hits = read_counter("file_cache_hit_nums_"),
                .mem_bytes = read_counter("mem_cache_hit_bytes_total"),
                .file_bytes = read_counter("file_cache_hit_bytes_total"),
                .valid_gets = read_counter("valid_get_nums_"),
                .total_gets = read_counter("total_get_nums_"),
            };
        };
        const GetMetrics baseline = capture_metrics();

        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto missing =
            service.GetReplicaList("dfs_metrics_missing", TenantId::Default());
        ASSERT_FALSE(missing.has_value());
        EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
        auto after_missing = capture_metrics();
        EXPECT_EQ(after_missing.dfs_hits, baseline.dfs_hits);
        EXPECT_EQ(after_missing.dfs_bytes, baseline.dfs_bytes);
        EXPECT_EQ(after_missing.valid_gets, baseline.valid_gets);
        EXPECT_EQ(after_missing.total_gets, baseline.total_gets + 1);

        ASSERT_TRUE(service
                        .PutStart(context.client_id, "dfs_metrics_not_ready",
                                  TenantId::Default(), 111, config)
                        .has_value());
        auto not_ready = service.GetReplicaList("dfs_metrics_not_ready",
                                                TenantId::Default());
        ASSERT_FALSE(not_ready.has_value());
        EXPECT_EQ(not_ready.error(), ErrorCode::REPLICA_IS_NOT_READY);
        auto after_not_ready = capture_metrics();
        EXPECT_EQ(after_not_ready.dfs_hits, baseline.dfs_hits);
        EXPECT_EQ(after_not_ready.dfs_bytes, baseline.dfs_bytes);
        EXPECT_EQ(after_not_ready.valid_gets, baseline.valid_gets);
        EXPECT_EQ(after_not_ready.total_gets, baseline.total_gets + 2);

        constexpr uint64_t kSingleDfsSize = 123;
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "dfs_metrics_single",
                                  TenantId::Default(), kSingleDfsSize, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_metrics_single",
                                TenantId::Default(), ReplicaType::DFS)
                        .has_value());
        auto single = service.GetReplicaList("dfs_metrics_single",
                                             TenantId::Default());
        ASSERT_TRUE(single.has_value());
        ASSERT_EQ(single->replicas.size(), 1u);
        EXPECT_TRUE(single->replicas.front().is_dfs_replica());

        auto after_single = capture_metrics();
        EXPECT_EQ(after_single.dfs_hits, baseline.dfs_hits + 1);
        EXPECT_EQ(after_single.dfs_bytes,
                  baseline.dfs_bytes + kSingleDfsSize);
        EXPECT_EQ(after_single.valid_gets, baseline.valid_gets + 1);
        EXPECT_EQ(after_single.total_gets, baseline.total_gets + 3);
        EXPECT_EQ(after_single.mem_hits, baseline.mem_hits);
        EXPECT_EQ(after_single.file_hits, baseline.file_hits);
        EXPECT_EQ(after_single.mem_bytes, baseline.mem_bytes);
        EXPECT_EQ(after_single.file_bytes, baseline.file_bytes);

        constexpr uint64_t kBatchDfsSize = 456;
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "dfs_metrics_batch",
                                  TenantId::Default(), kBatchDfsSize, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_metrics_batch",
                                TenantId::Default(), ReplicaType::DFS)
                        .has_value());
        ASSERT_TRUE(service
                        .PutStart(context.client_id,
                                  "dfs_metrics_batch_not_ready",
                                  TenantId::Default(), 222, config)
                        .has_value());

        auto batch = service.BatchGetReplicaList(
            {"dfs_metrics_batch", "dfs_metrics_batch_missing",
             "dfs_metrics_batch_not_ready"},
            TenantId::Default());
        ASSERT_EQ(batch.size(), 3u);
        ASSERT_TRUE(batch[0].has_value());
        ASSERT_EQ(batch[0]->replicas.size(), 1u);
        EXPECT_TRUE(batch[0]->replicas.front().is_dfs_replica());
        ASSERT_FALSE(batch[1].has_value());
        EXPECT_EQ(batch[1].error(), ErrorCode::OBJECT_NOT_FOUND);
        ASSERT_FALSE(batch[2].has_value());
        EXPECT_EQ(batch[2].error(), ErrorCode::REPLICA_IS_NOT_READY);

        auto after_batch = capture_metrics();
        EXPECT_EQ(after_batch.dfs_hits, baseline.dfs_hits + 2);
        EXPECT_EQ(after_batch.dfs_bytes,
                  baseline.dfs_bytes + kSingleDfsSize + kBatchDfsSize);
        EXPECT_EQ(after_batch.valid_gets, baseline.valid_gets + 2);
        EXPECT_EQ(after_batch.total_gets, baseline.total_gets + 6);
        EXPECT_EQ(after_batch.mem_hits, baseline.mem_hits);
        EXPECT_EQ(after_batch.file_hits, baseline.file_hits);
        EXPECT_EQ(after_batch.mem_bytes, baseline.mem_bytes);
        EXPECT_EQ(after_batch.file_bytes, baseline.file_bytes);

        constexpr uint64_t kMixedSize = 789;
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "dfs_metrics_mixed",
                                  TenantId::Default(), kMixedSize, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_metrics_mixed",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        auto mixed = service.GetReplicaList("dfs_metrics_mixed",
                                            TenantId::Default());
        ASSERT_TRUE(mixed.has_value());
        ASSERT_EQ(mixed->replicas.size(), 2u);
        EXPECT_TRUE(mixed->replicas.front().is_memory_replica());
        EXPECT_NE(FindDfsDescriptor(mixed->replicas), nullptr);

        const auto after_mixed = capture_metrics();
        EXPECT_EQ(after_mixed.dfs_hits, after_batch.dfs_hits);
        EXPECT_EQ(after_mixed.dfs_bytes, after_batch.dfs_bytes);
        EXPECT_EQ(after_mixed.mem_hits, baseline.mem_hits + 1);
        EXPECT_EQ(after_mixed.mem_bytes, baseline.mem_bytes + kMixedSize);
        EXPECT_EQ(after_mixed.file_hits, baseline.file_hits);
        EXPECT_EQ(after_mixed.file_bytes, baseline.file_bytes);
        EXPECT_EQ(after_mixed.valid_gets, baseline.valid_gets + 3);
        EXPECT_EQ(after_mixed.total_gets, baseline.total_gets + 7);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsReplicaRemovalDecrementsStatusGaugeOnce) {
    auto& metrics = MasterMetricManager::instance();
    struct ReplicaGauges {
        int64_t processing;
        int64_t complete;
    };
    const auto capture_gauges = [&]() {
        const auto serialized = metrics.serialize_metrics();
        const auto read_gauge = [&](const std::string& status) {
            const auto value = ReadDfsReplicaGauge(serialized, status);
            EXPECT_TRUE(value.has_value())
                << "missing DFS replica gauge for status=" << status;
            return value.value_or(-1);
        };
        return ReplicaGauges{.processing = read_gauge("processing"),
                             .complete = read_gauge("complete")};
    };
    const ReplicaGauges baseline = capture_gauges();
    const auto make_descriptor = [] {
        DistributedFSDescriptor descriptor;
        descriptor.file_path = "replica_metric_test";
        return descriptor;
    };

    {
        Replica processing(make_descriptor(), ReplicaStatus::PROCESSING);
        EXPECT_EQ(capture_gauges().processing, baseline.processing + 1);

        processing.mark_removed();
        EXPECT_EQ(capture_gauges().processing, baseline.processing);
        processing.mark_removed();
        EXPECT_EQ(capture_gauges().processing, baseline.processing);
    }
    EXPECT_EQ(capture_gauges().processing, baseline.processing);

    {
        Replica complete(make_descriptor(), ReplicaStatus::COMPLETE);
        EXPECT_EQ(capture_gauges().complete, baseline.complete + 1);

        complete.mark_removed();
        EXPECT_EQ(capture_gauges().complete, baseline.complete);
        complete.mark_removed();
        EXPECT_EQ(capture_gauges().complete, baseline.complete);

        complete.cancel_remove();
        EXPECT_EQ(capture_gauges().complete, baseline.complete + 1);
        complete.cancel_remove();
        EXPECT_EQ(capture_gauges().complete, baseline.complete + 1);
        complete.mark_removed();
        EXPECT_EQ(capture_gauges().complete, baseline.complete);

        Replica moved(std::move(complete));
        EXPECT_TRUE(moved.is_dfs_replica());
        EXPECT_EQ(moved.status(), ReplicaStatus::REMOVED);
        EXPECT_EQ(capture_gauges().complete, baseline.complete);
    }
    EXPECT_EQ(capture_gauges().complete, baseline.complete);

    {
        Replica removed_source(make_descriptor(), ReplicaStatus::COMPLETE);
        removed_source.mark_removed();
        Replica destination(make_descriptor(), ReplicaStatus::PROCESSING);
        EXPECT_EQ(capture_gauges().processing, baseline.processing + 1);

        destination = std::move(removed_source);
        EXPECT_EQ(destination.status(), ReplicaStatus::REMOVED);
        EXPECT_EQ(capture_gauges().processing, baseline.processing);
        EXPECT_EQ(capture_gauges().complete, baseline.complete);
    }
    const auto after_destruction = capture_gauges();
    EXPECT_EQ(after_destruction.processing, baseline.processing);
    EXPECT_EQ(after_destruction.complete, baseline.complete);
}

TEST_F(MasterServiceTest, DfsBucketModeSupportsPutEndAndQuery) {
    const auto root = MakeDfsTestRoot("put_end");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto start = service.PutStart(context.client_id, "bucket_key",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value()) << toString(start.error());
        ASSERT_EQ(start->size(), 2u);
        const auto* dfs = FindDfsDescriptor(*start);
        ASSERT_NE(dfs, nullptr);
        const auto& desc = dfs->get_dfs_descriptor();
        // Bucket mode: shard_idx is the bucket id and the path names a bucket
        // data file, not a shard file.
        EXPECT_EQ(desc.shard_idx, 0);
        EXPECT_NE(desc.file_path.find("bucket_"), std::string::npos);
        EXPECT_EQ(desc.object_size, 4096u);

        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "bucket_key",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        auto query = service.GetReplicaList("bucket_key", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 2u);
        for (const auto& replica : query->replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketSnapshotTracksVersionEpochAndHeartbeatInterval) {
    const auto root = MakeDfsTestRoot("snapshot_heartbeat");
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar ready("MOONCAKE_DFS_READY_BUCKET_COUNT", "0");
    ScopedEnvVar interval("MOONCAKE_OFFLOAD_DFS_HEARTBEAT_INTERVAL_SECONDS", "5");
    ScopedEnvVar fixed_timeout("MOONCAKE_DFS_BATCH_TIMEOUT_FIXED_MS", "54321");
    UUID old_epoch{};
    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        auto first = service.GetDfsBucketSnapshot(context.client_id, {}, 0);
        ASSERT_TRUE(first.has_value());
        EXPECT_FALSE(first->unchanged);
        EXPECT_EQ(first->heartbeat_interval_seconds, 5u);
        EXPECT_EQ(first->batch_timeout_fixed_ms, 54321u);
        old_epoch = first->epoch;

        ASSERT_TRUE(service.SetDfsHeartbeatInterval(2).has_value());
        EXPECT_FALSE(service.SetDfsHeartbeatInterval(0).has_value());
        EXPECT_EQ(service.SetDfsBatchTimeoutFixedMs(12345), 54321u);
        auto unchanged = service.GetDfsBucketSnapshot(
            context.client_id, first->epoch, first->version);
        ASSERT_TRUE(unchanged.has_value());
        EXPECT_TRUE(unchanged->unchanged);
        EXPECT_TRUE(unchanged->bucket_ids.empty());
        EXPECT_EQ(unchanged->heartbeat_interval_seconds, 2u);
        EXPECT_EQ(unchanged->batch_timeout_fixed_ms, 12345u);

        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        auto start = service.PutStart(context.client_id, "snapshot_key",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        const auto* dfs = FindDfsDescriptor(*start);
        ASSERT_NE(dfs, nullptr);
        auto changed = service.GetDfsBucketSnapshot(
            context.client_id, first->epoch, first->version);
        ASSERT_TRUE(changed.has_value());
        EXPECT_FALSE(changed->unchanged);
        EXPECT_GT(changed->version, first->version);
        EXPECT_NE(std::find(changed->bucket_ids.begin(), changed->bucket_ids.end(),
                            dfs->get_dfs_descriptor().shard_idx),
                  changed->bucket_ids.end());
    }
    {
        MasterService restarted;
        auto current = restarted.GetDfsBucketSnapshot({}, {}, 0);
        ASSERT_TRUE(current.has_value());
        EXPECT_NE(current->epoch, old_epoch);
        // Even the current version is insufficient when the epoch is stale.
        auto stale_epoch =
            restarted.GetDfsBucketSnapshot({}, old_epoch, current->version);
        ASSERT_TRUE(stale_epoch.has_value());
        EXPECT_FALSE(stale_epoch->unchanged);
    }
    std::filesystem::remove_all(root);
}

TEST_F(MasterServiceTest, DfsRootIdentityRetriesAfterRootBecomesAvailable) {
    const auto root = MakeDfsTestRoot("identity_retry_after_recovery");
    ScopedBucketDfsEnv env(root, "1048576", "16");
    std::filesystem::remove_all(root);
    {
        std::ofstream blocker(root);
        ASSERT_TRUE(blocker.good());
    }
    {
        MasterService service;
        EXPECT_FALSE(service.UsesDfsBucketAllocator());
        auto storage = service.GetStorageConfig();
        ASSERT_TRUE(storage.has_value());
        EXPECT_TRUE(storage->dfs_root_identity.empty());

        ASSERT_TRUE(std::filesystem::remove(root));
        ASSERT_TRUE(std::filesystem::create_directory(root));
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(20);
        while (storage->dfs_root_identity.empty() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            storage = service.GetStorageConfig();
            ASSERT_TRUE(storage.has_value());
        }
        ASSERT_TRUE(service.UsesDfsBucketAllocator());
        EXPECT_EQ(storage->dfs_fsdir, root);
        EXPECT_FALSE(storage->dfs_root_identity.empty());
    }
    std::filesystem::remove_all(root);
}

TEST_F(MasterServiceTest,
       DfsRootGuardRecreatesMissingProbeDirectoriesAndPinnedContents) {
    const auto root = MakeDfsTestRoot("root_guard_recreates_files");
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar guard_interval("MOONCAKE_DFS_ROOT_GUARD_INTERVAL_SECONDS",
                                "1");
    {
        MasterService service;
        auto storage = service.GetStorageConfig();
        ASSERT_TRUE(storage.has_value());
        ASSERT_FALSE(storage->dfs_root_identity.empty());

        const std::string identity_path =
            root + "/" + kDfsRootIdentityFileName;
        const std::string marker_path = root + "/" + kDfsRootMarkerFileName;
        std::array<std::string, 2> read_probe_paths;
        for (size_t probe_index = 0;
             probe_index < kDfsReadProbeDirectoryNames.size(); ++probe_index) {
            read_probe_paths[probe_index] =
                root + "/" +
                std::string(kDfsReadProbeDirectoryNames[probe_index]) + "/" +
                kDfsReadProbeFileName;
        }
        ASSERT_TRUE(std::filesystem::remove(identity_path));
        ASSERT_TRUE(std::filesystem::remove(marker_path));
        for (const auto directory_name : kDfsReadProbeDirectoryNames) {
            ASSERT_EQ(std::filesystem::remove_all(
                          std::filesystem::path(root) /
                          std::string(directory_name)),
                      2u);
        }

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(5);
        while ((!std::filesystem::exists(identity_path) ||
                !std::filesystem::exists(marker_path) ||
                !std::filesystem::exists(read_probe_paths[0]) ||
                !std::filesystem::exists(read_probe_paths[1])) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        std::ifstream identity_file(identity_path, std::ios::binary);
        const std::string restored_identity(
            (std::istreambuf_iterator<char>(identity_file)),
            std::istreambuf_iterator<char>());
        EXPECT_EQ(restored_identity, storage->dfs_root_identity);

        std::ifstream marker_file(marker_path, std::ios::binary);
        const std::string restored_marker(
            (std::istreambuf_iterator<char>(marker_file)),
            std::istreambuf_iterator<char>());
        EXPECT_EQ(restored_marker, kDfsRootMarkerPayload);

        for (size_t probe_index = 0;
             probe_index < read_probe_paths.size(); ++probe_index) {
            std::ifstream read_probe_file(read_probe_paths[probe_index],
                                          std::ios::binary);
            const std::string restored_read_probe(
                (std::istreambuf_iterator<char>(read_probe_file)),
                std::istreambuf_iterator<char>());
            EXPECT_EQ(restored_read_probe.size(), kDfsReadProbeSize);
            EXPECT_EQ(restored_read_probe,
                      BuildDfsReadProbeContents(storage->dfs_root_identity,
                                                probe_index));
        }
    }
    std::filesystem::remove_all(root);
}

TEST_F(MasterServiceTest,
       DfsRootGuardRecoversRecreatedRootWithOrWithoutManualIdentity) {
    for (const bool restore_identity_manually : {false, true}) {
        SCOPED_TRACE(restore_identity_manually);
        const auto root = MakeDfsTestRoot(
            restore_identity_manually ? "root_guard_manual_identity"
                                      : "root_guard_automatic_identity");
        ScopedBucketDfsEnv env(root, "1048576", "16");
        ScopedEnvVar guard_interval(
            "MOONCAKE_DFS_ROOT_GUARD_INTERVAL_SECONDS", "1");
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        const UUID old_epoch = service.GetDfsMasterEpoch();
        ASSERT_NE(old_epoch, UUID{});

        auto storage = service.GetStorageConfig();
        ASSERT_TRUE(storage.has_value());
        const std::string pinned_identity = storage->dfs_root_identity;
        ASSERT_FALSE(pinned_identity.empty());
        FileStorageConfig file_config;
        file_config.storage_filepath = root;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        DistributedStorageBackend backend(
            file_config, DistributedStorageConfig::FromEnvironment(),
            std::make_unique<PosixFsAdapter>(), pinned_identity);
        backend.SetMasterEpochProvider(
            [&service]() -> tl::expected<UUID, ErrorCode> {
                return service.GetDfsMasterEpoch();
            });
        ASSERT_TRUE(backend.Init().has_value());

        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        auto old_start = service.PutStart(
            context.client_id, "root_replacement_old", TenantId::Default(),
            4096, config);
        ASSERT_TRUE(old_start.has_value());
        const auto* old_dfs = FindDfsDescriptor(*old_start);
        ASSERT_NE(old_dfs, nullptr);
        auto old_descriptor = old_dfs->get_dfs_descriptor();
        old_descriptor.master_epoch = old_epoch;
        std::string old_value(4096, 'O');
        auto old_write = backend.BatchWrite({{"root_replacement_old",
                                              old_descriptor,
                                              {{old_value.data(),
                                                old_value.size()}}}});
        ASSERT_EQ(old_write.size(), 1u);
        ASSERT_TRUE(old_write.front().has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "root_replacement_old",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        ASSERT_GT(std::filesystem::remove_all(root), 0u);
        ASSERT_TRUE(std::filesystem::create_directory(root));
        if (restore_identity_manually) {
            std::ofstream identity(root + "/" + kDfsRootIdentityFileName,
                                   std::ios::binary);
            ASSERT_TRUE(identity.good());
            identity << pinned_identity;
            identity.close();
            ASSERT_TRUE(identity.good());
        }

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(8);
        UUID new_epoch{};
        while (std::chrono::steady_clock::now() < deadline) {
            new_epoch = service.GetDfsMasterEpoch();
            if (new_epoch != UUID{} && new_epoch != old_epoch) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        ASSERT_NE(new_epoch, UUID{});
        ASSERT_NE(new_epoch, old_epoch);

        std::ifstream identity_file(root + "/" + kDfsRootIdentityFileName,
                                    std::ios::binary);
        const std::string restored_identity(
            (std::istreambuf_iterator<char>(identity_file)),
            std::istreambuf_iterator<char>());
        EXPECT_EQ(restored_identity, pinned_identity);
        EXPECT_TRUE(std::filesystem::exists(root + "/" +
                                            kDfsRootMarkerFileName));
        for (const auto directory_name : kDfsReadProbeDirectoryNames) {
            EXPECT_TRUE(std::filesystem::exists(
                std::filesystem::path(root) / std::string(directory_name) /
                kDfsReadProbeFileName));
        }
        EXPECT_TRUE(backend.ProbeWrite().has_value());
        EXPECT_TRUE(backend.ProbeRead().has_value());

        auto old_query = service.GetReplicaList("root_replacement_old",
                                                TenantId::Default());
        ASSERT_TRUE(old_query.has_value());
        EXPECT_EQ(FindDfsDescriptor(old_query->replicas), nullptr);

        auto new_start = service.PutStart(
            context.client_id, "root_replacement_new", TenantId::Default(),
            4096, config);
        ASSERT_TRUE(new_start.has_value());
        const auto* new_dfs = FindDfsDescriptor(*new_start);
        ASSERT_NE(new_dfs, nullptr);
        EXPECT_TRUE(std::filesystem::exists(
            new_dfs->get_dfs_descriptor().file_path));
        EXPECT_NE(new_dfs->get_dfs_descriptor().shard_idx,
                  old_descriptor.shard_idx);
        auto new_descriptor = new_dfs->get_dfs_descriptor();
        new_descriptor.master_epoch = new_epoch;
        std::string new_value(4096, 'N');
        auto new_write = backend.BatchWrite({{"root_replacement_new",
                                              new_descriptor,
                                              {{new_value.data(),
                                                new_value.size()}}}});
        ASSERT_EQ(new_write.size(), 1u);
        ASSERT_TRUE(new_write.front().has_value());
        std::string new_output(new_value.size(), '\0');
        auto new_read = backend.BatchRead({{"root_replacement_new",
                                            new_descriptor,
                                            {{new_output.data(),
                                              new_output.size()}}}});
        ASSERT_EQ(new_read.size(), 1u);
        ASSERT_TRUE(new_read.front().has_value());
        EXPECT_EQ(new_output, new_value);
        std::filesystem::remove_all(root);
    }
}

TEST_F(MasterServiceTest,
       DfsRootGuardTransientStatFailureDoesNotTriggerRecovery) {
    const auto root = MakeDfsTestRoot("root_guard_transient_stat_failure");
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar guard_interval("MOONCAKE_DFS_ROOT_GUARD_INTERVAL_SECONDS",
                                "3600");
    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        const UUID old_epoch = service.GetDfsMasterEpoch();
        ASSERT_NE(old_epoch, UUID{});

        MasterServiceTestPeer::SetDfsRootStatErrorForTesting(service, EIO);
        MasterServiceTestPeer::ReconcileDfsRootFiles(service);

        EXPECT_TRUE(MasterServiceTestPeer::IsDfsEnabled(service));
        EXPECT_FALSE(MasterServiceTestPeer::IsDfsRootRecoveryNeeded(service));
        EXPECT_EQ(service.GetDfsMasterEpoch(), old_epoch);

        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        auto start = service.PutStart(context.client_id, "after_stat_eio",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        EXPECT_NE(FindDfsDescriptor(*start), nullptr);

        MasterServiceTestPeer::SetDfsRootStatErrorForTesting(service, 0);
        MasterServiceTestPeer::ReconcileDfsRootFiles(service);
        EXPECT_TRUE(MasterServiceTestPeer::IsDfsEnabled(service));
        EXPECT_FALSE(MasterServiceTestPeer::IsDfsRootRecoveryNeeded(service));
        EXPECT_EQ(service.GetDfsMasterEpoch(), old_epoch);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest,
       DfsRootGuardPreservesReplicasWhenOnlyEmptyReadyBucketIsMissing) {
    const auto root = MakeDfsTestRoot("root_guard_missing_ready_bucket");
    const auto old_root = root + "_old";
    const auto replacement_root = root + "_replacement";
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar guard_interval("MOONCAKE_DFS_ROOT_GUARD_INTERVAL_SECONDS",
                                "1");
    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        const UUID old_epoch = service.GetDfsMasterEpoch();
        ASSERT_NE(old_epoch, UUID{});

        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        auto start = service.PutStart(context.client_id, "preserved_dfs_key",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        const auto* dfs = FindDfsDescriptor(*start);
        ASSERT_NE(dfs, nullptr);
        const auto descriptor = dfs->get_dfs_descriptor();
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "preserved_dfs_key",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        int64_t empty_bucket_id = -1;
        const auto ready_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (empty_bucket_id < 0 &&
               std::chrono::steady_clock::now() < ready_deadline) {
            const auto tracked = MasterServiceTestPeer::GetTrackedDfsBucketIds(service);
            for (const int64_t bucket_id : tracked) {
                if (bucket_id != descriptor.shard_idx) {
                    empty_bucket_id = bucket_id;
                    break;
                }
            }
            if (empty_bucket_id < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        ASSERT_GE(empty_bucket_id, 0);
        ASSERT_NE(empty_bucket_id, descriptor.shard_idx);

        std::filesystem::copy(root, replacement_root,
                              std::filesystem::copy_options::recursive);
        const auto missing_ready_path =
            std::filesystem::path(replacement_root) /
            ("bucket_" + ImmutableBucketAllocator::FormatBucketId(
                             empty_bucket_id) +
             ".data");
        ASSERT_TRUE(std::filesystem::remove(missing_ready_path));
        std::filesystem::rename(root, old_root);
        std::filesystem::rename(replacement_root, root);

        UUID new_epoch = service.GetDfsMasterEpoch();
        const auto recovery_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(8);
        while ((new_epoch == UUID{} || new_epoch == old_epoch) &&
               std::chrono::steady_clock::now() < recovery_deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            new_epoch = service.GetDfsMasterEpoch();
        }
        ASSERT_NE(new_epoch, UUID{});
        ASSERT_NE(new_epoch, old_epoch);

        auto query = service.GetReplicaListForAdmin("preserved_dfs_key",
                                                    TenantId::Default());
        ASSERT_TRUE(query.has_value());
        const auto* preserved_dfs = FindDfsDescriptor(query->replicas);
        ASSERT_NE(preserved_dfs, nullptr);
        EXPECT_EQ(preserved_dfs->get_dfs_descriptor().shard_idx,
                  descriptor.shard_idx);
        EXPECT_EQ(preserved_dfs->status, ReplicaStatus::COMPLETE);
        const auto tracked_after_recovery = MasterServiceTestPeer::GetTrackedDfsBucketIds(service);
        EXPECT_EQ(std::find(tracked_after_recovery.begin(),
                            tracked_after_recovery.end(), empty_bucket_id),
                  tracked_after_recovery.end());
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::remove_all(old_root, ec);
    std::filesystem::remove_all(replacement_root, ec);
}

TEST_F(MasterServiceTest, DfsRootRecoveryPreparationDoesNotBlockL3Requests) {
    const auto root = MakeDfsTestRoot("root_recovery_l3_available");
    const auto old_root = root + "_old";
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar guard_interval("MOONCAKE_DFS_ROOT_GUARD_INTERVAL_SECONDS",
                                "1");
    MasterService service;
    const UUID old_epoch = service.GetDfsMasterEpoch();
    ASSERT_NE(old_epoch, UUID{});
    const auto context = PrepareSimpleSegment(service);

    ReplicateConfig with_dfs;
    with_dfs.replica_num = 1;
    with_dfs.dfs_replica_num = 1;
    ASSERT_TRUE(service
                    .PutStart(context.client_id, "dfs_only_during_recovery",
                              TenantId::Default(), 4096, with_dfs)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "dfs_only_during_recovery",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());
    service.RunBatchEvictForTesting(1.0, 1.0);
    auto dfs_only_before = service.GetReplicaList("dfs_only_during_recovery",
                                                  TenantId::Default());
    ASSERT_TRUE(dfs_only_before.has_value());
    ASSERT_EQ(dfs_only_before->replicas.size(), 1u);
    ASSERT_TRUE(dfs_only_before->replicas.front().is_dfs_replica());

    ReplicateConfig memory_only;
    memory_only.replica_num = 1;
    auto existing = service.PutStart(context.client_id, "l3_existing",
                                     TenantId::Default(), 4096, memory_only);
    ASSERT_TRUE(existing.has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "l3_existing",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    struct RecoveryHookState {
        std::mutex mutex;
        std::condition_variable cv;
        bool prepared{false};
        bool allow_commit{false};
    };
    auto hook_state = std::make_shared<RecoveryHookState>();
    MasterServiceTestPeer::SetDfsRootRecoveryBeforeCommitHook(service, [hook_state] {
        std::unique_lock<std::mutex> lock(hook_state->mutex);
        hook_state->prepared = true;
        hook_state->cv.notify_all();
        hook_state->cv.wait(lock, [&] { return hook_state->allow_commit; });
    });

    // Keep the original inode allocated so the new root cannot reuse it.
    std::error_code replace_error;
    std::filesystem::rename(root, old_root, replace_error);
    bool recreated_root = false;
    if (!replace_error) {
        recreated_root = std::filesystem::create_directory(root, replace_error);
    }
    if (!recreated_root) {
        {
            std::lock_guard<std::mutex> lock(hook_state->mutex);
            hook_state->allow_commit = true;
        }
        hook_state->cv.notify_all();
        FAIL() << "Failed to replace the DFS root for the recovery test: "
               << replace_error.message();
    }
    bool reached_commit_hook = false;
    {
        std::unique_lock<std::mutex> lock(hook_state->mutex);
        reached_commit_hook =
            hook_state->cv.wait_for(lock, std::chrono::seconds(8),
                                    [&] { return hook_state->prepared; });
    }
    if (!reached_commit_hook) {
        {
            std::lock_guard<std::mutex> lock(hook_state->mutex);
            hook_state->allow_commit = true;
        }
        hook_state->cv.notify_all();
        FAIL() << "DFS root recovery did not reach the commit hook";
    }

    const auto still_has_dfs =
        MasterServiceTestPeer::HasDfsReplicaInMetadata(service, "dfs_only_during_recovery");
    EXPECT_TRUE(still_has_dfs.has_value());
    if (still_has_dfs) EXPECT_TRUE(*still_has_dfs);

    const auto storage_during_recovery = service.GetStorageConfig();
    EXPECT_TRUE(storage_during_recovery.has_value());
    if (storage_during_recovery) {
        EXPECT_EQ(storage_during_recovery->dfs_fsdir, root);
        EXPECT_FALSE(storage_during_recovery->dfs_root_identity.empty());
    }

    auto single_during_recovery = service.GetReplicaList(
        "dfs_only_during_recovery", TenantId::Default());
    EXPECT_FALSE(single_during_recovery.has_value());
    if (!single_during_recovery) {
        EXPECT_EQ(single_during_recovery.error(),
                  ErrorCode::REPLICA_IS_NOT_READY);
    }
    auto batch_during_recovery = service.BatchGetReplicaList(
        {"dfs_only_during_recovery"}, TenantId::Default());
    EXPECT_EQ(batch_during_recovery.size(), 1u);
    if (batch_during_recovery.size() == 1u) {
        EXPECT_FALSE(batch_during_recovery.front().has_value());
        if (!batch_during_recovery.front()) {
            EXPECT_EQ(batch_during_recovery.front().error(),
                      ErrorCode::REPLICA_IS_NOT_READY);
        }
    }
    auto regex_during_recovery = service.GetReplicaListByRegex(
        "^dfs_only_during_recovery$", TenantId::Default());
    EXPECT_TRUE(regex_during_recovery.has_value());
    if (regex_during_recovery) {
        EXPECT_TRUE(regex_during_recovery->empty());
    }

    std::atomic<bool> requests_finished{false};
    std::atomic<bool> requests_succeeded{false};
    std::thread requests([&] {
        const auto exists =
            service.ExistKey("l3_existing", TenantId::Default());
        const auto replicas =
            service.GetReplicaList("l3_existing", TenantId::Default());
        auto start = service.PutStart(context.client_id, "l3_during_recovery",
                                      TenantId::Default(), 4096, memory_only);
        bool success =
            exists.value_or(false) && replicas.has_value() && start.has_value();
        if (start) {
            success =
                success && service
                               .PutEnd(context.client_id, "l3_during_recovery",
                                       TenantId::Default(), ReplicaType::ALL)
                               .has_value();
        }
        requests_succeeded.store(success, std::memory_order_release);
        requests_finished.store(true, std::memory_order_release);
    });

    const auto request_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!requests_finished.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < request_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_TRUE(requests_finished.load(std::memory_order_acquire));
    EXPECT_TRUE(requests_succeeded.load(std::memory_order_acquire));

    {
        std::lock_guard<std::mutex> lock(hook_state->mutex);
        hook_state->allow_commit = true;
    }
    hook_state->cv.notify_all();
    requests.join();

    const auto recovery_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    UUID recovered_epoch = service.GetDfsMasterEpoch();
    while ((recovered_epoch == UUID{} || recovered_epoch == old_epoch) &&
           std::chrono::steady_clock::now() < recovery_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        recovered_epoch = service.GetDfsMasterEpoch();
    }
    EXPECT_NE(recovered_epoch, UUID{});
    EXPECT_NE(recovered_epoch, old_epoch);
    MasterServiceTestPeer::SetDfsRootRecoveryBeforeCommitHook(service, {});
    EXPECT_TRUE(service.ExistKey("l3_during_recovery", TenantId::Default())
                    .value_or(false));
    std::error_code cleanup_error;
    std::filesystem::remove_all(root, cleanup_error);
    std::filesystem::remove_all(old_root, cleanup_error);
}

TEST_F(MasterServiceTest, DfsRootGuardDoesNotOverwriteMismatchedIdentity) {
    const auto root = MakeDfsTestRoot("root_guard_rejects_mismatch");
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar guard_interval("MOONCAKE_DFS_ROOT_GUARD_INTERVAL_SECONDS",
                                "1");
    const std::string invalid_identity = "not-the-pinned-identity\n";
    {
        MasterService service;
        const std::string identity_path =
            root + "/" + kDfsRootIdentityFileName;
        {
            std::ofstream identity_file(identity_path,
                                        std::ios::binary | std::ios::trunc);
            ASSERT_TRUE(identity_file.good());
            identity_file << invalid_identity;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        std::ifstream identity_file(identity_path, std::ios::binary);
        const std::string actual((std::istreambuf_iterator<char>(identity_file)),
                                 std::istreambuf_iterator<char>());
        EXPECT_EQ(actual, invalid_identity);
    }
    std::filesystem::remove_all(root);
}

TEST_F(MasterServiceTest, DfsAllocatorRetriesInitializationAfterRecovery) {
    const auto root = MakeDfsTestRoot("retry_after_recovery");
    const auto saved_root = root + "_saved";
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar recovery("MOONCAKE_DFS_BUCKET_RECOVERY_ENABLED", "true");
    std::filesystem::path persisted_meta;

    {
        MasterService initial_service;
        const auto initial_context = PrepareSimpleSegment(initial_service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        auto start = initial_service.PutStart(
            initial_context.client_id, "retry_existing", TenantId::Default(),
            4096, config);
        ASSERT_TRUE(start.has_value());
        const auto* dfs = FindDfsDescriptor(*start);
        ASSERT_NE(dfs, nullptr);
        auto storage = initial_service.GetStorageConfig();
        ASSERT_TRUE(storage.has_value());
        FileStorageConfig file_config;
        file_config.storage_filepath = root;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        DistributedStorageBackend backend(
            file_config, DistributedStorageConfig::FromEnvironment(),
            std::make_unique<PosixFsAdapter>(), storage->dfs_root_identity);
        ASSERT_TRUE(backend.Init());
        std::string value(4096, 'R');
        auto written = backend.BatchWrite({{"retry_existing",
            dfs->get_dfs_descriptor(), {{value.data(), value.size()}}}});
        ASSERT_EQ(written.size(), 1u);
        ASSERT_TRUE(written.front());
        ASSERT_TRUE(initial_service
                        .PutEnd(initial_context.client_id, "retry_existing",
                                 TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        ASSERT_TRUE(initial_service
                        .PutStart(initial_context.client_id, "retry_conflict",
                                  TenantId::Default(), 4096, config)
                        .has_value());
        ASSERT_TRUE(initial_service
                        .PutEnd(initial_context.client_id, "retry_conflict",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        // Recovery deliberately ignores an active bucket without metadata.
        // Roll over to persist the committed source bucket before restart.
        auto rollover = initial_service.PutStart(
            initial_context.client_id, "retry_rollover", TenantId::Default(),
            1048576, config);
        ASSERT_TRUE(rollover.has_value());
        const auto* rollover_dfs = FindDfsDescriptor(*rollover);
        ASSERT_NE(rollover_dfs, nullptr);
        ASSERT_NE(rollover_dfs->get_dfs_descriptor().shard_idx,
                  dfs->get_dfs_descriptor().shard_idx);
        ASSERT_TRUE(initial_service.PutRevoke(
            initial_context.client_id, "retry_rollover", TenantId::Default(),
            ReplicaType::ALL));
        persisted_meta = std::filesystem::path(
            dfs->get_dfs_descriptor().file_path);
        persisted_meta.replace_extension(".meta");
    }

    // The clean shutdown flushes sealed metadata; consuming a preallocated
    // bucket can defer that I/O until maintenance or shutdown.
    ASSERT_TRUE(std::filesystem::exists(persisted_meta));

    std::filesystem::rename(root, saved_root);
    {
        std::ofstream blocker(root);
        ASSERT_TRUE(blocker.good());
    }

    {
        MasterService service;
        EXPECT_FALSE(service.UsesDfsBucketAllocator());
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto conflict_start = service.PutStart(
            context.client_id, "retry_conflict", TenantId::Default(), 4096,
            config);
        ASSERT_TRUE(conflict_start.has_value())
            << toString(conflict_start.error());
        ASSERT_EQ(conflict_start->size(), 1u);
        EXPECT_TRUE(conflict_start->front().is_memory_replica());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "retry_conflict",
                                TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        auto degraded = service.PutStart(context.client_id, "retry_degraded",
                                         TenantId::Default(), 4096, config);
        ASSERT_TRUE(degraded.has_value()) << toString(degraded.error());
        ASSERT_EQ(degraded->size(), 1u);
        EXPECT_TRUE(degraded->front().is_memory_replica());
        ASSERT_TRUE(service.PutEnd(context.client_id, "retry_degraded",
                                   TenantId::Default(), ReplicaType::MEMORY));
        auto batch = service.BatchPutStart(
            context.client_id, {"retry_batch_0", "retry_batch_1"},
            TenantId::Default(), {4096, 4096}, config);
        ASSERT_EQ(batch.size(), 2u);
        for (const auto& result : batch) {
            ASSERT_TRUE(result.has_value());
            ASSERT_EQ(result->size(), 1u);
            EXPECT_TRUE(result->front().is_memory_replica());
        }

        std::error_code ec;
        ASSERT_TRUE(std::filesystem::remove(root, ec));
        ASSERT_FALSE(ec);
        std::filesystem::rename(saved_root, root);

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(20);
        while (!service.UsesDfsBucketAllocator() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_TRUE(service.UsesDfsBucketAllocator())
            << "master did not retry DFS allocator initialization";

        auto existing =
            service.GetReplicaList("retry_existing", TenantId::Default());
        ASSERT_TRUE(existing.has_value());
        ASSERT_EQ(existing->replicas.size(), 1u);
        EXPECT_TRUE(existing->replicas.front().is_dfs_replica());

        auto conflict_metadata =
            service.GetReplicaList("retry_conflict", TenantId::Default());
        ASSERT_TRUE(conflict_metadata.has_value());
        ASSERT_EQ(conflict_metadata->replicas.size(), 1u);
        EXPECT_TRUE(conflict_metadata->replicas.front().is_memory_replica());
        ASSERT_TRUE(service.Remove("retry_conflict", TenantId::Default(), true)
                        .has_value());
        auto conflict_rewrite = service.PutStart(
            context.client_id, "retry_conflict", TenantId::Default(), 4096,
            config);
        ASSERT_TRUE(conflict_rewrite.has_value())
            << toString(conflict_rewrite.error());
        ASSERT_EQ(conflict_rewrite->size(), 2u);
        EXPECT_NE(FindDfsDescriptor(*conflict_rewrite), nullptr);

        // Recovery leaves earlier degraded writes in memory without repair
        // tasks.
        const auto task_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        do {
            auto tasks = service.FetchTasks(context.client_id, 16);
            ASSERT_TRUE(tasks.has_value());
            EXPECT_TRUE(tasks->empty());
            auto degraded_metadata =
                service.GetReplicaList("retry_degraded", TenantId::Default());
            ASSERT_TRUE(degraded_metadata.has_value());
            ASSERT_EQ(degraded_metadata->replicas.size(), 1u);
            EXPECT_TRUE(
                degraded_metadata->replicas.front().is_memory_replica());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } while (std::chrono::steady_clock::now() < task_deadline);

        auto recovered = service.PutStart(context.client_id, "retry_recovered",
                                          TenantId::Default(), 4096, config);
        ASSERT_TRUE(recovered.has_value()) << toString(recovered.error());
        ASSERT_EQ(recovered->size(), 2u);
        EXPECT_NE(FindDfsDescriptor(*recovered), nullptr);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::remove_all(saved_root, ec);
}

TEST_F(MasterServiceTest,
       DfsUninitializedFallsBackForPutAndUpsertSingleAndBatch) {
    const auto root = MakeDfsTestRoot("uninitialized_write_fallback");
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ASSERT_TRUE(std::filesystem::remove(root));
    {
        std::ofstream blocker(root);
        ASSERT_TRUE(blocker.good());
    }

    {
        MasterService service;
        EXPECT_FALSE(service.UsesDfsBucketAllocator());
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        const auto skipped_before = ReadPrometheusCounter(
            MasterMetricManager::instance().serialize_metrics(),
            "master_dfs_replica_skipped_total{reason=\""
            "DFS_SERVICE_UNAVAILABLE\"}");

        const auto expect_memory_only = [](const auto& result) {
            ASSERT_TRUE(result.has_value()) << toString(result.error());
            ASSERT_EQ(result->size(), 1u);
            EXPECT_TRUE(result->front().is_memory_replica());
        };

        auto put = service.PutStart(context.client_id, "fallback_put",
                                    TenantId::Default(), 4096, config);
        expect_memory_only(put);
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "fallback_put",
                                TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        const std::vector<std::string> batch_put_keys{
            "fallback_batch_put_a", "fallback_batch_put_b"};
        const std::vector<uint64_t> batch_put_lengths(batch_put_keys.size(),
                                                       4096);
        auto batch_put = service.BatchPutStart(
            context.client_id, batch_put_keys, TenantId::Default(),
            batch_put_lengths, config);
        ASSERT_EQ(batch_put.size(), batch_put_keys.size());
        for (size_t i = 0; i < batch_put.size(); ++i) {
            expect_memory_only(batch_put[i]);
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, batch_put_keys[i],
                                    TenantId::Default(), ReplicaType::MEMORY)
                            .has_value());
        }

        auto upsert = service.UpsertStart(context.client_id, "fallback_upsert",
                                          TenantId::Default(), 4096, config);
        expect_memory_only(upsert);
        ASSERT_TRUE(service
                        .UpsertEnd(context.client_id, "fallback_upsert",
                                   TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        auto upsert_same_size = service.UpsertStart(
            context.client_id, "fallback_upsert", TenantId::Default(), 4096,
            config);
        expect_memory_only(upsert_same_size);
        ASSERT_TRUE(service
                        .UpsertEnd(context.client_id, "fallback_upsert",
                                   TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        auto upsert_new_size = service.UpsertStart(
            context.client_id, "fallback_upsert", TenantId::Default(), 8192,
            config);
        expect_memory_only(upsert_new_size);
        ASSERT_TRUE(service
                        .UpsertEnd(context.client_id, "fallback_upsert",
                                   TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        const std::vector<std::string> batch_upsert_keys{
            "fallback_batch_upsert_a", "fallback_batch_upsert_b"};
        const std::vector<uint64_t> batch_upsert_lengths(
            batch_upsert_keys.size(), 4096);
        auto batch_upsert = service.BatchUpsertStart(
            context.client_id, batch_upsert_keys, TenantId::Default(),
            batch_upsert_lengths, config);
        ASSERT_EQ(batch_upsert.size(), batch_upsert_keys.size());
        for (size_t i = 0; i < batch_upsert.size(); ++i) {
            expect_memory_only(batch_upsert[i]);
            ASSERT_TRUE(service
                            .UpsertEnd(context.client_id,
                                       batch_upsert_keys[i],
                                       TenantId::Default(),
                                       ReplicaType::MEMORY)
                            .has_value());
        }

        const auto skipped_after = ReadPrometheusCounter(
            MasterMetricManager::instance().serialize_metrics(),
            "master_dfs_replica_skipped_total{reason=\""
            "DFS_SERVICE_UNAVAILABLE\"}");
        ASSERT_TRUE(skipped_after.has_value());
        EXPECT_EQ(*skipped_after, skipped_before.value_or(0) + 8);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketAllocationFailureEvictsBeforeRetryingPut) {
    const auto root = MakeDfsTestRoot("forced_eviction");
    // Keep the background eviction thread asleep so the second PutStart must
    // use the allocation-failure recovery path.
    ScopedEnvVar eviction_interval("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
                                   "3600");
    ScopedBucketDfsEnv env(root, "4096", "2", "1", "0.99", "0.8");

    {
        auto service_config =
            MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
        MasterService service(service_config);
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        // Fill two buckets so the first is sealed and evictable, while the
        // second remains active. Active buckets cannot be eviction candidates.
        for (const auto* key : {"forced_eviction_old", "forced_eviction_active"}) {
            ASSERT_TRUE(service
                            .PutStart(context.client_id, key,
                                      TenantId::Default(), 4096, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }

        auto replacement = service.PutStart(
            context.client_id, "forced_eviction_new", TenantId::Default(),
            4096, config);
        ASSERT_TRUE(replacement.has_value()) << toString(replacement.error());
        ASSERT_NE(FindDfsDescriptor(*replacement), nullptr);
        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "forced_eviction_new",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest,
       DfsBucketCapacityFailureFallsBackToMemoryForSingleAndBatch) {
    const auto root = MakeDfsTestRoot("capacity_fallback");
    ScopedBucketDfsEnv env(root, "4096", "1");
    ScopedEnvVar ready_bucket_count("MOONCAKE_DFS_READY_BUCKET_COUNT", "0");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        ASSERT_TRUE(service
                        .PutStart(context.client_id, "capacity_fill",
                                  TenantId::Default(), 100, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "capacity_fill",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        const auto skipped_metric = [](const std::string& reason) {
            return ReadPrometheusCounter(
                MasterMetricManager::instance().serialize_metrics(),
                "master_dfs_replica_skipped_total{reason=\"" + reason + "\"}");
        };
        const auto before = skipped_metric("NO_AVAILABLE_HANDLE");
        ASSERT_TRUE(before.has_value());

        ScopedDfsFallbackLogCapture fallback_log_capture;

        auto single = service.PutStart(context.client_id, "capacity_single",
                                       TenantId::Default(), 100, config);
        ASSERT_TRUE(single.has_value()) << toString(single.error());
        ASSERT_EQ(single->size(), 1u);
        EXPECT_TRUE(single->front().is_memory_replica());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "capacity_single",
                                TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        const std::vector<std::string> keys{"capacity_batch_a",
                                            "capacity_batch_b"};
        const std::vector<uint64_t> lengths(keys.size(), 100);
        auto batch = service.BatchPutStart(
            context.client_id, keys, TenantId::Default(), lengths, config);
        ASSERT_EQ(batch.size(), keys.size());
        for (size_t i = 0; i < batch.size(); ++i) {
            ASSERT_TRUE(batch[i].has_value())
                << keys[i] << ": " << toString(batch[i].error());
            ASSERT_EQ(batch[i]->size(), 1u);
            EXPECT_TRUE(batch[i]->front().is_memory_replica());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, keys[i],
                                    TenantId::Default(), ReplicaType::MEMORY)
                            .has_value());
        }

        for (const auto& key :
             {"capacity_single", "capacity_batch_a", "capacity_batch_b"}) {
            auto query = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(query.has_value());
            ASSERT_EQ(query->replicas.size(), 1u);
            EXPECT_TRUE(query->replicas.front().is_memory_replica());
            EXPECT_EQ(query->replicas.front().status, ReplicaStatus::COMPLETE);
        }
        const auto after = skipped_metric("NO_AVAILABLE_HANDLE");
        ASSERT_TRUE(after.has_value());
        EXPECT_EQ(*after, *before + 3);

        const auto fallback_logs = fallback_log_capture.messages();
        EXPECT_LE(fallback_logs.size(), 1u);
        const auto allocation_errors = fallback_log_capture.errors();
        // Each site has a process-wide quota that earlier tests may consume.
        for (const auto* site : {"Failed to allocate DFS replica for key=",
                                 "DFS batch allocate active bucket failed"}) {
            EXPECT_LE(std::count_if(allocation_errors.begin(),
                                    allocation_errors.end(),
                                    [site](const std::string& message) {
                                        return message.find(site) !=
                                               std::string::npos;
                                    }),
                      1);
        }
        for (const auto& message : allocation_errors) {
            EXPECT_NE(message.find("suppressed="), std::string::npos);
        }
        if (!fallback_logs.empty()) {
            EXPECT_NE(fallback_logs.front().find("key=capacity_"),
                      std::string::npos);
            EXPECT_NE(fallback_logs.front().find("error=NO_AVAILABLE_HANDLE"),
                      std::string::npos);
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest,
       DfsBucketOpenFailureFallsBackToMemoryForSingleAndBatch) {
    const auto root = MakeDfsTestRoot("open_failure_fallback");
    const auto moved_root = root + "_moved";
    ScopedBucketDfsEnv env(root, "4096", "2");
    ScopedEnvVar ready_bucket_count("MOONCAKE_DFS_READY_BUCKET_COUNT", "0");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        ASSERT_TRUE(service
                        .PutStart(context.client_id, "open_failure_fill",
                                  TenantId::Default(), 100, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "open_failure_fill",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        std::filesystem::rename(root, moved_root);
        std::ofstream blocker(root, std::ios::binary);
        ASSERT_TRUE(blocker.good());
        blocker.close();

        const auto skipped_metric = [] {
            return ReadPrometheusCounter(
                MasterMetricManager::instance().serialize_metrics(),
                "master_dfs_replica_skipped_total{reason=\"FILE_OPEN_FAIL\"}");
        };
        const auto before = skipped_metric();
        ASSERT_TRUE(before.has_value());

        auto single = service.PutStart(context.client_id, "open_failure_single",
                                       TenantId::Default(), 100, config);
        ASSERT_TRUE(single.has_value()) << toString(single.error());
        ASSERT_EQ(single->size(), 1u);
        EXPECT_TRUE(single->front().is_memory_replica());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "open_failure_single",
                                TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        const std::vector<std::string> keys{"open_failure_batch"};
        const std::vector<uint64_t> lengths{100};
        auto batch = service.BatchPutStart(
            context.client_id, keys, TenantId::Default(), lengths, config);
        ASSERT_EQ(batch.size(), keys.size());
        ASSERT_TRUE(batch.front().has_value())
            << toString(batch.front().error());
        ASSERT_EQ(batch.front()->size(), 1u);
        EXPECT_TRUE(batch.front()->front().is_memory_replica());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, keys.front(),
                                TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        auto upsert = service.UpsertStart(
            context.client_id, "open_failure_upsert", TenantId::Default(), 100,
            config);
        ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
        ASSERT_EQ(upsert->size(), 1u);
        EXPECT_TRUE(upsert->front().is_memory_replica());
        ASSERT_TRUE(service
                        .UpsertEnd(context.client_id, "open_failure_upsert",
                                   TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        const std::vector<std::string> upsert_keys{
            "open_failure_batch_upsert"};
        auto batch_upsert = service.BatchUpsertStart(
            context.client_id, upsert_keys, TenantId::Default(), lengths,
            config);
        ASSERT_EQ(batch_upsert.size(), upsert_keys.size());
        ASSERT_TRUE(batch_upsert.front().has_value())
            << toString(batch_upsert.front().error());
        ASSERT_EQ(batch_upsert.front()->size(), 1u);
        EXPECT_TRUE(batch_upsert.front()->front().is_memory_replica());
        ASSERT_TRUE(service
                        .UpsertEnd(context.client_id, upsert_keys.front(),
                                   TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        for (const auto& key : {"open_failure_single", "open_failure_batch",
                                "open_failure_upsert",
                                "open_failure_batch_upsert"}) {
            auto query = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(query.has_value());
            ASSERT_EQ(query->replicas.size(), 1u);
            EXPECT_TRUE(query->replicas.front().is_memory_replica());
            EXPECT_EQ(query->replicas.front().status, ReplicaStatus::COMPLETE);
        }
        const auto after = skipped_metric();
        ASSERT_TRUE(after.has_value());
        EXPECT_EQ(*after, *before + 4);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::remove_all(moved_root, ec);
}

TEST_F(MasterServiceTest, DfsBucketPutRevokeReleasesSpaceAndMetadata) {
    const auto root = MakeDfsTestRoot("revoke");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto start = service.PutStart(context.client_id, "revoked",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "revoked",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        // The whole object is gone: a revoked DFS replica must not linger as
        // PROCESSING, which would keep the key in the processing set forever.
        auto query = service.GetReplicaList("revoked", TenantId::Default());
        ASSERT_FALSE(query.has_value());
        EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);

        // The key is immediately reusable.
        auto again = service.PutStart(context.client_id, "revoked",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(again.has_value()) << toString(again.error());
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketPutRevokeRejectsCompletedReplica) {
    const auto root = MakeDfsTestRoot("revoke_completed");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        ASSERT_TRUE(service
                        .PutStart(context.client_id, "partial_revoke",
                                  TenantId::Default(), 4096, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "partial_revoke",
                                TenantId::Default(), ReplicaType::MEMORY)
                        .has_value());

        auto revoke = service.PutRevoke(context.client_id, "partial_revoke",
                                        TenantId::Default(), ReplicaType::ALL);
        ASSERT_FALSE(revoke.has_value());
        EXPECT_EQ(revoke.error(), ErrorCode::INVALID_WRITE);

        auto query =
            service.GetReplicaList("partial_revoke", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 1u);
        EXPECT_TRUE(query->replicas.front().is_memory_replica());

        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "partial_revoke",
                                   TenantId::Default(), ReplicaType::DFS)
                        .has_value());
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketUpsertFilterPreservesOldReplica) {
    const auto root = MakeDfsTestRoot("upsert_filter");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "upsert_filter",
                                  TenantId::Default(), 4096, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "upsert_filter",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        ASSERT_TRUE(service.SetDfsFilterMode(1).has_value());
        auto new_key_upsert =
            service.UpsertStart(context.client_id, "new_upsert_filter",
                                TenantId::Default(), 4096, config);
        ASSERT_FALSE(new_key_upsert.has_value());
        EXPECT_EQ(new_key_upsert.error(),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);

        auto upsert = service.UpsertStart(context.client_id, "upsert_filter",
                                          TenantId::Default(), 8192, config);
        ASSERT_FALSE(upsert.has_value());
        EXPECT_EQ(upsert.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);

        auto query = service.GetReplicaListForAdmin("upsert_filter",
                                                    TenantId::Default());
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 2u);
        for (const auto& replica : query->replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketBatchPutStartAllocatesContiguously) {
    const auto root = MakeDfsTestRoot("batch");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        std::vector<std::string> keys{"batch_a", "batch_b", "batch_c"};
        std::vector<uint64_t> lengths(keys.size(), 4096);
        auto results = service.BatchPutStart(
            context.client_id, keys, TenantId::Default(), lengths, config);
        ASSERT_EQ(results.size(), keys.size());

        std::vector<DistributedFSDescriptor> descriptors;
        for (size_t i = 0; i < results.size(); ++i) {
            ASSERT_TRUE(results[i].has_value())
                << "key " << keys[i] << ": " << toString(results[i].error());
            const auto* dfs = FindDfsDescriptor(*results[i]);
            ASSERT_NE(dfs, nullptr) << "key " << keys[i];
            descriptors.push_back(dfs->get_dfs_descriptor());
        }

        // A single BatchPutStart must reserve one contiguous run in one bucket.
        for (size_t i = 1; i < descriptors.size(); ++i) {
            EXPECT_EQ(descriptors[i].shard_idx, descriptors[0].shard_idx);
            const uint64_t previous_start =
                descriptors[i - 1].offset - 8 - keys[i - 1].size();
            const uint64_t previous_end =
                previous_start + descriptors[i - 1].aligned_size;
            const uint64_t entry_start =
                descriptors[i].offset - 8 - keys[i].size();
            EXPECT_EQ(entry_start, previous_end) << "entry " << i;
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest,
       DfsBucketBatchPutStartRejectsReservationFromRetiredAllocator) {
    const auto root = MakeDfsTestRoot("batch_root_replacement");
    const auto old_root = root + "_old";
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar guard_interval("MOONCAKE_DFS_ROOT_GUARD_INTERVAL_SECONDS",
                                "1");

    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const UUID old_epoch = service.GetDfsMasterEpoch();
    ASSERT_NE(old_epoch, UUID{});

    struct ReservationHookState {
        std::mutex mutex;
        std::condition_variable cv;
        bool reserved{false};
        bool proceed{false};
    };
    auto hook_state = std::make_shared<ReservationHookState>();
    MasterServiceTestPeer::SetDfsBatchReservationHook(service, [hook_state] {
        std::unique_lock<std::mutex> lock(hook_state->mutex);
        hook_state->reserved = true;
        hook_state->cv.notify_all();
        hook_state->cv.wait(lock, [&] { return hook_state->proceed; });
    });

    ReplicateConfig config;
    config.replica_num = 1;
    config.dfs_replica_num = 1;
    using BatchResults =
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>;
    std::optional<BatchResults> batch_results;
    std::thread batch_thread([&] {
        batch_results = service.BatchPutStart(
            context.client_id, {"batch_stale_reservation"},
            TenantId::Default(), {4096}, config);
    });

    bool reached_reservation_hook = false;
    {
        std::unique_lock<std::mutex> lock(hook_state->mutex);
        reached_reservation_hook = hook_state->cv.wait_for(
            lock, std::chrono::seconds(5), [&] { return hook_state->reserved; });
    }
    if (!reached_reservation_hook) {
        {
            std::lock_guard<std::mutex> lock(hook_state->mutex);
            hook_state->proceed = true;
        }
        hook_state->cv.notify_all();
        batch_thread.join();
        FAIL() << "BatchPutStart did not reach the reservation hook";
    }

    // Keep the old directory alive so the replacement cannot reuse its inode.
    // A remove-then-create sequence can look unchanged to the root guard.
    std::error_code replace_error;
    std::filesystem::rename(root, old_root, replace_error);
    bool recreated_root = false;
    if (!replace_error) {
        recreated_root = std::filesystem::create_directory(root, replace_error);
    }
    if (!recreated_root) {
        {
            std::lock_guard<std::mutex> lock(hook_state->mutex);
            hook_state->proceed = true;
        }
        hook_state->cv.notify_all();
        batch_thread.join();
        MasterServiceTestPeer::SetDfsBatchReservationHook(service, {});
        FAIL() << "Failed to replace the DFS root for the batch race test";
    }
    UUID new_epoch = service.GetDfsMasterEpoch();
    const auto recovery_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while ((new_epoch == UUID{} || new_epoch == old_epoch) &&
           std::chrono::steady_clock::now() < recovery_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        new_epoch = service.GetDfsMasterEpoch();
    }

    {
        std::lock_guard<std::mutex> lock(hook_state->mutex);
        hook_state->proceed = true;
    }
    hook_state->cv.notify_all();
    batch_thread.join();
    MasterServiceTestPeer::SetDfsBatchReservationHook(service, {});

    ASSERT_NE(new_epoch, UUID{});
    ASSERT_NE(new_epoch, old_epoch);
    ASSERT_TRUE(batch_results.has_value());
    ASSERT_EQ(batch_results->size(), 1u);
    ASSERT_TRUE((*batch_results)[0].has_value())
        << toString((*batch_results)[0].error());
    EXPECT_EQ(FindDfsDescriptor(*(*batch_results)[0]), nullptr);

    auto has_stored_dfs =
        MasterServiceTestPeer::HasDfsReplicaInMetadata(service, "batch_stale_reservation");
    ASSERT_TRUE(has_stored_dfs.has_value());
    EXPECT_FALSE(*has_stored_dfs);

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::remove_all(old_root, ec);
}

TEST_F(MasterServiceTest, DfsBucketBatchPutStartRollsBackRejectedKeys) {
    const auto root = MakeDfsTestRoot("batch_rollback");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        // Pre-create one key so the batch entry for it is rejected, and confirm
        // the surviving entries still succeed.
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "existing",
                                  TenantId::Default(), 4096, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "existing",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        std::vector<std::string> keys{"fresh_a", "existing", "fresh_b"};
        std::vector<uint64_t> lengths(keys.size(), 4096);
        auto results = service.BatchPutStart(
            context.client_id, keys, TenantId::Default(), lengths, config);
        ASSERT_EQ(results.size(), keys.size());
        EXPECT_TRUE(results[0].has_value());
        EXPECT_FALSE(results[1].has_value());
        EXPECT_EQ(results[1].error(), ErrorCode::OBJECT_ALREADY_EXISTS);
        EXPECT_TRUE(results[2].has_value());

        // The rejected key's reservation was released and its original replica
        // is untouched.
        auto query = service.GetReplicaList("existing", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        for (const auto& replica : query->replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketRestartRestoresQueryableReplicas) {
    const auto root = MakeDfsTestRoot("restart");
    ScopedBucketDfsEnv env(root, "1048576", "16");
    ScopedEnvVar recovery("MOONCAKE_DFS_BUCKET_RECOVERY_ENABLED", "true");

    std::vector<std::string> keys{"restart_a", "restart_b"};
    std::vector<DistributedFSDescriptor> before;

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        for (const auto& key : keys) {
            auto start = service.PutStart(context.client_id, key,
                                          TenantId::Default(), 4096, config);
            ASSERT_TRUE(start.has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
            const auto* dfs = FindDfsDescriptor(*start);
            ASSERT_NE(dfs, nullptr);
            before.push_back(dfs->get_dfs_descriptor());
        }
        // Active buckets are intentionally not persisted. Seal the source
        // bucket so this fixture exercises durable metadata recovery.
        auto rollover = service.PutStart(context.client_id, "restart_rollover",
                                         TenantId::Default(), 1048576, config);
        ASSERT_TRUE(rollover.has_value());
        const auto* rollover_dfs = FindDfsDescriptor(*rollover);
        ASSERT_NE(rollover_dfs, nullptr);
        ASSERT_NE(rollover_dfs->get_dfs_descriptor().shard_idx,
                  before.front().shard_idx);
        ASSERT_TRUE(service.PutRevoke(context.client_id, "restart_rollover",
                                      TenantId::Default(), ReplicaType::ALL));
    }

    {
        // A fresh master over the same directory must recover the committed
        // entries as COMPLETE, queryable replicas, not just allocator state.
        MasterService service;
        for (size_t i = 0; i < keys.size(); ++i) {
            auto query = service.GetReplicaList(keys[i], TenantId::Default());
            ASSERT_TRUE(query.has_value())
                << "key " << keys[i] << " was not recovered";
            const auto* dfs = FindDfsDescriptor(query->replicas);
            ASSERT_NE(dfs, nullptr) << "key " << keys[i];
            const auto& desc = dfs->get_dfs_descriptor();
            // The recovered descriptor must match the original field by field.
            EXPECT_EQ(desc.file_path, before[i].file_path);
            EXPECT_EQ(desc.offset, before[i].offset);
            EXPECT_EQ(desc.object_size, before[i].object_size);
            EXPECT_EQ(desc.aligned_size, before[i].aligned_size);
            EXPECT_EQ(desc.shard_idx, before[i].shard_idx);
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketRestartCanSkipRecoveryAndRewriteOldKey) {
    const auto root = MakeDfsTestRoot("restart_no_recovery");
    ScopedBucketDfsEnv env(root, "4096", "8");
    ScopedEnvVar recovery("MOONCAKE_DFS_BUCKET_RECOVERY_ENABLED", "false");
    ScopedEnvVar ready("MOONCAKE_DFS_READY_BUCKET_COUNT", "0");
    std::string old_path;
    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        for (const auto& key : {"same_key", "rollover"}) {
            auto start = service.PutStart(context.client_id, key,
                                          TenantId::Default(), 4096, config);
            ASSERT_TRUE(start.has_value());
            ASSERT_TRUE(service.PutEnd(context.client_id, key,
                                       TenantId::Default(), ReplicaType::ALL)
                            .has_value());
            if (std::string(key) == "same_key") {
                const auto* dfs = FindDfsDescriptor(*start);
                ASSERT_NE(dfs, nullptr);
                old_path = dfs->get_dfs_descriptor().file_path;
            }
        }
    }
    {
        ScopedEnvVar budget("MOONCAKE_DFS_MAX_BUCKET_COUNT", "1");
        MasterService service;
        EXPECT_FALSE(service.GetReplicaList("same_key", TenantId::Default())
                         .has_value());
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        auto start = service.PutStart(context.client_id, "same_key",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        const auto* dfs = FindDfsDescriptor(*start);
        ASSERT_NE(dfs, nullptr);
        EXPECT_NE(dfs->get_dfs_descriptor().file_path, old_path);
        ASSERT_TRUE(service.PutEnd(context.client_id, "same_key",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        EXPECT_TRUE(service.GetReplicaList("same_key", TenantId::Default())
                        .has_value());
    }
    // Recovery was disabled for the second run, so its startup sweep reclaims
    // the first run's bucket files. The service shutdown above joins that
    // sweep, so the file is gone by the time it returns.
    EXPECT_FALSE(std::filesystem::exists(old_path));
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketRestartDoesNotRestoreRemovedKeys) {
    const auto root = MakeDfsTestRoot("restart_revoked");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        // Committed then removed: must not come back.
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "gone",
                                  TenantId::Default(), 4096, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "gone", TenantId::Default(),
                                ReplicaType::ALL)
                        .has_value());
        ASSERT_TRUE(service.Remove("gone", TenantId::Default()).has_value());

        // Never committed (no PutEnd): must not come back either.
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "never_committed",
                                  TenantId::Default(), 4096, config)
                        .has_value());
    }

    {
        MasterService service;
        auto removed = service.GetReplicaList("gone", TenantId::Default());
        EXPECT_FALSE(removed.has_value());
        auto uncommitted =
            service.GetReplicaList("never_committed", TenantId::Default());
        EXPECT_FALSE(uncommitted.has_value());
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketEvictionRemovesWholeBucket) {
    const auto root = MakeDfsTestRoot("evict");
    // One 4096-byte entry per bucket, 4 buckets total. A 0.5 high watermark
    // means eviction starts once 2 of the 4 buckets are in use.
    ScopedBucketDfsEnv env(root, "4096", "4", "0", "0.5", "0.25");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        std::vector<std::string> keys;
        for (int i = 0; i < 3; ++i) {
            keys.push_back("evict_" + std::to_string(i));
            auto start = service.PutStart(context.client_id, keys.back(),
                                          TenantId::Default(), 100, config);
            ASSERT_TRUE(start.has_value()) << "allocation " << i;
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, keys.back(),
                                    TenantId::Default(), ReplicaType::ALL)
                            .has_value());
        }

        const auto metrics_before = CaptureDfsBucketEvictionMetrics();
        service.RunDfsEvictionForTesting();
        const auto metrics_after = CaptureDfsBucketEvictionMetrics();

        EXPECT_EQ(metrics_after.at("master_dfs_bucket_eviction_attempts_total"),
                  metrics_before.at("master_dfs_bucket_eviction_attempts_total") +
                      2);
        EXPECT_EQ(metrics_after.at("master_dfs_bucket_evictions_total"),
                  metrics_before.at("master_dfs_bucket_evictions_total") + 2);
        EXPECT_EQ(
            metrics_after.at("master_dfs_bucket_evicted_replicas_total"),
            metrics_before.at("master_dfs_bucket_evicted_replicas_total") + 2);
        EXPECT_EQ(metrics_after.at("master_dfs_bucket_evicted_bytes_total"),
                  metrics_before.at("master_dfs_bucket_evicted_bytes_total") +
                      200);
        for (size_t i = 6; i < DfsBucketEvictionMetricNames().size(); ++i) {
            const auto& name = DfsBucketEvictionMetricNames()[i];
            EXPECT_EQ(metrics_after.at(name), metrics_before.at(name)) << name;
        }

        // The coldest bucket's key lost its DFS replica; its MEMORY replica
        // survives, so the object is still queryable.
        auto evicted = service.GetReplicaList(keys[0], TenantId::Default());
        ASSERT_TRUE(evicted.has_value());
        EXPECT_EQ(FindDfsDescriptor(evicted->replicas), nullptr)
            << "the evicted bucket's DFS replica must no longer be returned";

        // The newest bucket keeps its DFS replica.
        auto kept = service.GetReplicaList(keys[2], TenantId::Default());
        ASSERT_TRUE(kept.has_value());
        EXPECT_NE(FindDfsDescriptor(kept->replicas), nullptr);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketEvictionRejectsLeasedBucketWithoutChanges) {
    const auto root = MakeDfsTestRoot("evict_leased");
    ScopedBucketDfsEnv env(root, "4096", "4", "0", "0.5", "0.25");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        std::vector<std::string> keys;
        for (int i = 0; i < 3; ++i) {
            keys.push_back("leased_" + std::to_string(i));
            ASSERT_TRUE(service
                            .PutStart(context.client_id, keys.back(),
                                      TenantId::Default(), 100, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, keys.back(),
                                    TenantId::Default(), ReplicaType::ALL)
                            .has_value());
        }

        // Grant a lease on every key so no bucket can be evicted.
        for (const auto& key : keys) {
            ASSERT_TRUE(
                service.GetReplicaList(key, TenantId::Default()).has_value());
        }

        const auto metrics_before = CaptureDfsBucketEvictionMetrics();
        service.RunDfsEvictionForTesting();
        const auto metrics_after = CaptureDfsBucketEvictionMetrics();

        EXPECT_EQ(metrics_after.at("master_dfs_bucket_eviction_attempts_total"),
                  metrics_before.at("master_dfs_bucket_eviction_attempts_total") +
                      2);
        EXPECT_EQ(metrics_after.at("master_dfs_bucket_evictions_total"),
                  metrics_before.at("master_dfs_bucket_evictions_total"));
        EXPECT_EQ(
            metrics_after.at("master_dfs_bucket_evicted_replicas_total"),
            metrics_before.at("master_dfs_bucket_evicted_replicas_total"));
        EXPECT_EQ(metrics_after.at("master_dfs_bucket_evicted_bytes_total"),
                  metrics_before.at("master_dfs_bucket_evicted_bytes_total"));
        for (size_t i = 6; i < DfsBucketEvictionMetricNames().size(); ++i) {
            const auto& name = DfsBucketEvictionMetricNames()[i];
            EXPECT_EQ(metrics_after.at(name), metrics_before.at(name)) << name;
        }

        // A rejected eviction must leave metadata completely unchanged: zero
        // partial commits.
        for (const auto& key : keys) {
            auto query = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(query.has_value()) << "key " << key;
            EXPECT_EQ(query->replicas.size(), 2u) << "key " << key;
            EXPECT_NE(FindDfsDescriptor(query->replicas), nullptr)
                << "key " << key;
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketEvictionBlocksReadsAfterValidation) {
    for (const bool dfs_only : {false, true}) {
        SCOPED_TRACE(dfs_only);
        const auto root =
            MakeDfsTestRoot(dfs_only ? "evict_read_dfs" : "evict_read_mixed");
        ScopedBucketDfsEnv env(root, "4096", "4", "0", "0.5", "0.25");
        {
            MasterService service(MasterServiceConfig::builder()
                                      .set_default_kv_lease_ttl(60000)
                                      .build());
            const auto context = PrepareSimpleSegment(service);
            ReplicateConfig config;
            config.replica_num = 1;
            config.dfs_replica_num = 1;
            std::string bucket_file;
            for (const auto* key : {"read_barrier", "read_active"}) {
                auto start = service.PutStart(context.client_id, key,
                                              TenantId::Default(), 100, config);
                ASSERT_TRUE(start.has_value());
                const auto* dfs = FindDfsDescriptor(*start);
                ASSERT_NE(dfs, nullptr);
                if (std::string(key) == "read_barrier") {
                    bucket_file = dfs->get_dfs_descriptor().file_path;
                }
                ASSERT_TRUE(service
                                .PutEnd(context.client_id, key,
                                        TenantId::Default(), ReplicaType::ALL)
                                .has_value());
            }
            if (dfs_only) service.RunBatchEvictForTesting(1.0, 1.0);
            const auto lease_before =
                MasterServiceTestPeer::GetLeaseTimeoutForTest(service, "read_barrier");
            ASSERT_TRUE(lease_before.has_value());
            ASSERT_TRUE(std::filesystem::exists(bucket_file));

            EXPECT_TRUE(MasterServiceTestPeer::DuringDfsEvictionAfterValidation(service, "read_barrier", [&] {
                EXPECT_TRUE(std::filesystem::exists(bucket_file));
                auto single =
                    service.GetReplicaList("read_barrier", TenantId::Default());
                auto batch = service.BatchGetReplicaList({"read_barrier"},
                                                         TenantId::Default());
                auto regex = service.GetReplicaListByRegex("^read_barrier$",
                                                           TenantId::Default());
                ASSERT_EQ(batch.size(), 1u);
                ASSERT_TRUE(regex.has_value());
                if (dfs_only) {
                    ASSERT_FALSE(single.has_value());
                    EXPECT_EQ(single.error(), ErrorCode::REPLICA_IS_NOT_READY);
                    ASSERT_FALSE(batch[0].has_value());
                    EXPECT_EQ(batch[0].error(),
                              ErrorCode::REPLICA_IS_NOT_READY);
                    EXPECT_TRUE(regex->empty());
                } else {
                    ASSERT_TRUE(single.has_value());
                    ASSERT_EQ(single->replicas.size(), 1u);
                    EXPECT_TRUE(single->replicas.front().is_memory_replica());
                    ASSERT_TRUE(batch[0].has_value());
                    ASSERT_EQ(batch[0]->replicas.size(), 1u);
                    EXPECT_TRUE(batch[0]->replicas.front().is_memory_replica());
                    ASSERT_EQ(regex->count("read_barrier"), 1u);
                    ASSERT_EQ(regex->at("read_barrier").size(), 1u);
                    EXPECT_TRUE(
                        regex->at("read_barrier").front().is_memory_replica());
                }
                auto exists =
                    service.ExistKey("read_barrier", TenantId::Default());
                ASSERT_TRUE(exists.has_value());
                EXPECT_EQ(*exists, !dfs_only);
                auto batch_exists = service.BatchExistKey({"read_barrier"},
                                                          TenantId::Default());
                ASSERT_EQ(batch_exists.size(), 1u);
                ASSERT_TRUE(batch_exists[0].has_value());
                EXPECT_EQ(*batch_exists[0], !dfs_only);
                if (dfs_only) {
                    EXPECT_EQ(MasterServiceTestPeer::GetLeaseTimeoutForTest(service, "read_barrier"),
                              lease_before);
                }
            }));
            EXPECT_FALSE(std::filesystem::exists(bucket_file));
            auto after =
                service.GetReplicaList("read_barrier", TenantId::Default());
            if (dfs_only) {
                ASSERT_FALSE(after.has_value());
                EXPECT_EQ(after.error(), ErrorCode::OBJECT_NOT_FOUND);
            } else {
                ASSERT_TRUE(after.has_value());
                EXPECT_EQ(FindDfsDescriptor(after->replicas), nullptr);
            }
        }
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }
}

TEST_F(MasterServiceTest,
       DfsBucketEvictionRestoresReadsAfterPartialValidation) {
    for (const bool hard_pin : {false, true}) {
        SCOPED_TRACE(hard_pin);
        const auto root =
            MakeDfsTestRoot(hard_pin ? "evict_abort_pin" : "evict_abort_lease");
        ScopedBucketDfsEnv env(root, "8192", "4", "0", "0.3", "0.1");
        {
            MasterService service;
            const auto context = PrepareSimpleSegment(service);
            std::vector<std::string> keys = {"partial_a", "partial_b"};
            std::sort(keys.begin(), keys.end(),
                      [&](const auto& a, const auto& b) {
                          return RouteStripeIndex(a) <
                                 RouteStripeIndex(b);
                      });
            ASSERT_NE(RouteStripeIndex(keys[0]),
                      RouteStripeIndex(keys[1]));
            keys.push_back("partial_active");
            ReplicateConfig config;
            config.replica_num = 1;
            config.dfs_replica_num = 1;
            for (const auto& key : keys) {
                config.with_hard_pin = hard_pin && key == keys[1];
                ASSERT_TRUE(service
                                .PutStart(context.client_id, key,
                                          TenantId::Default(), 100, config)
                                .has_value());
                ASSERT_TRUE(service
                                .PutEnd(context.client_id, key,
                                        TenantId::Default(), ReplicaType::ALL)
                                .has_value());
            }
            if (!hard_pin) {
                ASSERT_TRUE(service.GetReplicaList(keys[1], TenantId::Default())
                                .has_value());
            }
            service.RunDfsEvictionForTesting();
            for (const auto& key : keys) {
                auto read = service.GetReplicaList(key, TenantId::Default());
                ASSERT_TRUE(read.has_value());
                const auto* dfs = FindDfsDescriptor(read->replicas);
                ASSERT_NE(dfs, nullptr) << key;
                EXPECT_TRUE(std::filesystem::exists(
                    dfs->get_dfs_descriptor().file_path));
            }
        }
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }
}

TEST_F(MasterServiceTest,
       DfsBucketEvictionRestoresReadsAfterRevalidationRejects) {
    const auto root = MakeDfsTestRoot("evict_read_abort");
    ScopedBucketDfsEnv env(root, "4096", "4", "0", "0.5", "0.25");
    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        for (const auto* key : {"abort_barrier", "abort_active"}) {
            ASSERT_TRUE(service
                            .PutStart(context.client_id, key,
                                      TenantId::Default(), 100, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }
        service.RunBatchEvictForTesting(1.0, 1.0);
        EXPECT_TRUE(MasterServiceTestPeer::DuringDfsEvictionAfterValidation(service, "abort_barrier", [&] {
            auto read =
                service.GetReplicaList("abort_barrier", TenantId::Default());
            ASSERT_FALSE(read.has_value());
            EXPECT_EQ(read.error(), ErrorCode::REPLICA_IS_NOT_READY);
            ASSERT_TRUE(MasterServiceTestPeer::SetProcessingKeyForTest(service, "abort_barrier", true));
        }));
        ASSERT_TRUE(MasterServiceTestPeer::SetProcessingKeyForTest(service, "abort_barrier", false));
        auto read =
            service.GetReplicaList("abort_barrier", TenantId::Default());
        ASSERT_TRUE(read.has_value());
        const auto* dfs = FindDfsDescriptor(read->replicas);
        ASSERT_NE(dfs, nullptr);
        EXPECT_TRUE(
            std::filesystem::exists(dfs->get_dfs_descriptor().file_path));
        EXPECT_GT(MasterServiceTestPeer::GetLeaseTimeoutForTest(service, "abort_barrier"),
                  std::chrono::system_clock::now());
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketEvictionCountsEmptyTombstonedBucket) {
    const auto root = MakeDfsTestRoot("evict_empty_tombstone");
    ScopedBucketDfsEnv env(root, "4096", "4", "0", "0.25", "0.1");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        for (int i = 0; i < 2; ++i) {
            const std::string key = "tombstone_" + std::to_string(i);
            ASSERT_TRUE(service
                            .PutStart(context.client_id, key,
                                      TenantId::Default(), 100, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key,
                                    TenantId::Default(), ReplicaType::ALL)
                            .has_value());
        }
        ASSERT_TRUE(service
                        .Remove("tombstone_0", TenantId::Default(),
                                /*force=*/true)
                        .has_value());

        const auto metrics_before = CaptureDfsBucketEvictionMetrics();
        service.RunDfsEvictionForTesting();
        const auto metrics_after = CaptureDfsBucketEvictionMetrics();

        EXPECT_EQ(metrics_after.at("master_dfs_bucket_eviction_attempts_total"),
                  metrics_before.at("master_dfs_bucket_eviction_attempts_total") +
                      1);
        EXPECT_EQ(metrics_after.at("master_dfs_bucket_evictions_total"),
                  metrics_before.at("master_dfs_bucket_evictions_total") + 1);
        EXPECT_EQ(
            metrics_after.at("master_dfs_bucket_evicted_replicas_total"),
            metrics_before.at("master_dfs_bucket_evicted_replicas_total"));
        EXPECT_EQ(metrics_after.at("master_dfs_bucket_evicted_bytes_total"),
                  metrics_before.at("master_dfs_bucket_evicted_bytes_total"));
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketMissingFileInvalidationIsNotEviction) {
    const auto root = MakeDfsTestRoot("missing_file_invalidation");
    ScopedBucketDfsEnv env(root, "4096", "4", "0", "0.5", "0.25");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "missing_file_key",
                                  TenantId::Default(), 100, config)
                        .has_value());
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "missing_file_key",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        const auto before =
            service.GetReplicaList("missing_file_key", TenantId::Default());
        ASSERT_TRUE(before.has_value());
        const auto* dfs = FindDfsDescriptor(before->replicas);
        ASSERT_NE(dfs, nullptr);
        const auto descriptor = dfs->get_dfs_descriptor();

        // A report must not invalidate a bucket whose data file is still
        // present on the master's DFS.
        const auto present_report = service.BatchInvalidateDfsBuckets(
            context.client_id, {{descriptor.shard_idx}}, TenantId::Default());
        ASSERT_EQ(present_report.size(), 1u);
        ASSERT_TRUE(present_report.front().has_value());
        service.ProcessInvalidatedDfsBucketsForTesting();
        const auto still_present =
            service.GetReplicaList("missing_file_key", TenantId::Default());
        ASSERT_TRUE(still_present.has_value());
        EXPECT_NE(FindDfsDescriptor(still_present->replicas), nullptr);

        ASSERT_TRUE(std::filesystem::remove(descriptor.file_path));

        const auto metrics_before = CaptureDfsBucketEvictionMetrics();
        const auto queued = service.BatchInvalidateDfsBuckets(
            context.client_id, {{descriptor.shard_idx}}, TenantId::Default());
        ASSERT_EQ(queued.size(), 1u);
        ASSERT_TRUE(queued.front().has_value());

        service.ProcessInvalidatedDfsBucketsForTesting();
        const auto metrics_after = CaptureDfsBucketEvictionMetrics();

        for (size_t i = 0; i < 6; ++i) {
            const auto& name = DfsBucketEvictionMetricNames()[i];
            EXPECT_EQ(metrics_after.at(name), metrics_before.at(name)) << name;
        }
        const auto after =
            service.GetReplicaList("missing_file_key", TenantId::Default());
        ASSERT_TRUE(after.has_value());
        EXPECT_EQ(FindDfsDescriptor(after->replicas), nullptr);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketEvictionSkipsProcessingKeys) {
    const auto root = MakeDfsTestRoot("evict_processing");
    ScopedBucketDfsEnv env(root, "4096", "4", "0", "0.5", "0.25");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        // The first key stays PROCESSING (no PutEnd), so its bucket must be
        // protected from eviction.
        ASSERT_TRUE(service
                        .PutStart(context.client_id, "processing",
                                  TenantId::Default(), 100, config)
                        .has_value());
        for (int i = 0; i < 2; ++i) {
            const std::string key = "done_" + std::to_string(i);
            ASSERT_TRUE(service
                            .PutStart(context.client_id, key,
                                      TenantId::Default(), 100, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }

        service.RunDfsEvictionForTesting();

        // The PROCESSING key must still be completable: its space was not
        // reclaimed from underneath it.
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "processing",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        auto query = service.GetReplicaList("processing", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        EXPECT_NE(FindDfsDescriptor(query->replicas), nullptr);
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsShardModeRemainsTheDefault) {
    const auto root = MakeDfsTestRoot("default_mode");
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", root.c_str());
    // MOONCAKE_DFS_ALLOCATOR_TYPE is deliberately unset.
    ScopedEnvVar allocator_type("MOONCAKE_DFS_ALLOCATOR_TYPE");
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "2");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto start = service.PutStart(context.client_id, "shard_default",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        const auto* dfs = FindDfsDescriptor(*start);
        ASSERT_NE(dfs, nullptr);
        // Without an explicit allocator type the master must stay on SHARD.
        // Match on the file name only: the temp root directory itself contains
        // "bucket_", so searching the whole path would always find it.
        const std::string file_name =
            std::filesystem::path(dfs->get_dfs_descriptor().file_path)
                .filename()
                .string();
        EXPECT_NE(file_name.find("dfs_shard_"), std::string::npos)
            << "file_name=" << file_name;
        EXPECT_EQ(file_name.find("bucket_"), std::string::npos)
            << "file_name=" << file_name;
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "shard_default",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsUnknownAllocatorTypeIsRejected) {
    const auto root = MakeDfsTestRoot("bad_mode");
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", root.c_str());
    ScopedEnvVar allocator_type("MOONCAKE_DFS_ALLOCATOR_TYPE", "not_a_mode");
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    // An unrecognized allocator type must be a hard configuration error rather
    // than a silent fallback to SHARD.
    EXPECT_THROW({ MasterService service; }, std::invalid_argument);

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, DfsBucketRejectsObjectLargerThanBucketCapacity) {
    const auto root = MakeDfsTestRoot("too_large");
    ScopedBucketDfsEnv env(root, "8192", "4");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        // Larger than one bucket: it can never be placed contiguously.
        auto start = service.PutStart(context.client_id, "huge",
                                      TenantId::Default(), 16384, config);
        ASSERT_FALSE(start.has_value());
        auto query = service.GetReplicaList("huge", TenantId::Default());
        EXPECT_FALSE(query.has_value());

        // A normal-sized object still works afterwards.
        auto ok = service.PutStart(context.client_id, "small",
                                   TenantId::Default(), 100, config);
        EXPECT_TRUE(ok.has_value()) << toString(ok.error());
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest,
       BatchExistKeyByReplicaTypeIsReadOnlyAndPreservesBatchSemantics) {
    const auto root = MakeDfsTestRoot("typed_batch_exist");
    ScopedBucketDfsEnv env(root, "1048576", "16");

    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(60000)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = context.client_id;

    const std::string group_id =
        (std::string("typed_exist_group_a") + "_group");
    ReplicateConfig grouped_memory_config;
    grouped_memory_config.replica_num = 1;
    grouped_memory_config.group_ids = std::vector<std::string>{group_id};
    const std::string group_key_a = "typed_exist_group_a";
    const std::string group_key_b = "typed_exist_group_b";
    PutCompletedObject(*service, client_id, group_key_a,
                       grouped_memory_config);
    PutCompletedObject(*service, client_id, group_key_b,
                       grouped_memory_config);

    ReplicateConfig dfs_config;
    dfs_config.replica_num = 1;
    dfs_config.dfs_replica_num = 1;
    const std::string dfs_key = "typed_exist_dfs_only";
    ASSERT_TRUE(service->PutStart(client_id, dfs_key, TenantId::Default(), 123,
                                  dfs_config)
                    .has_value());
    ASSERT_TRUE(service->PutEnd(client_id, dfs_key, TenantId::Default(),
                                ReplicaType::DFS)
                    .has_value());

    const std::string mixed_key = "typed_exist_mixed";
    ASSERT_TRUE(service->PutStart(client_id, mixed_key, TenantId::Default(),
                                  456, dfs_config)
                    .has_value());
    ASSERT_TRUE(service->PutEnd(client_id, mixed_key, TenantId::Default(),
                                ReplicaType::ALL)
                    .has_value());

    ReplicateConfig memory_config;
    memory_config.replica_num = 1;
    const std::string incomplete_key = "typed_exist_incomplete";
    ASSERT_TRUE(service->PutStart(client_id, incomplete_key,
                                  TenantId::Default(), 789, memory_config)
                    .has_value());

    const auto group_a_lease_before_opt =
        MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_a);
    const auto group_b_lease_before_opt =
        MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_b);
    ASSERT_TRUE(group_a_lease_before_opt.has_value());
    ASSERT_TRUE(group_b_lease_before_opt.has_value());
    const auto group_a_lease_before = *group_a_lease_before_opt;
    const auto group_b_lease_before = *group_b_lease_before_opt;
    auto& metrics = MasterMetricManager::instance();
    struct ReadMetrics {
        int64_t mem_hits;
        int64_t file_hits;
        int64_t dfs_hits;
        int64_t valid_gets;
        int64_t total_gets;
    };
    const auto capture_metrics = [&]() {
        const auto serialized = metrics.serialize_metrics();
        const auto value = [&](const std::string& name) {
            return ReadPrometheusCounter(serialized, name).value_or(-1);
        };
        return ReadMetrics{.mem_hits = value("mem_cache_hit_nums_"),
                           .file_hits = value("file_cache_hit_nums_"),
                           .dfs_hits = value("dfs_cache_hit_nums_"),
                           .valid_gets = value("valid_get_nums_"),
                           .total_gets = value("total_get_nums_")};
    };
    const ReadMetrics before = capture_metrics();
    ASSERT_NE(before.mem_hits, -1);
    ASSERT_NE(before.file_hits, -1);
    ASSERT_NE(before.dfs_hits, -1);
    ASSERT_NE(before.valid_gets, -1);
    ASSERT_NE(before.total_gets, -1);

    const std::vector<std::string> keys = {
        group_key_a, dfs_key, mixed_key, incomplete_key,
        "typed_exist_missing", group_key_b, group_key_a};
    const auto results = service->BatchExistKeyByReplicaType(
        keys, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value());
    }
    EXPECT_TRUE(results[0].value());
    EXPECT_FALSE(results[1].value());
    EXPECT_TRUE(results[2].value());
    EXPECT_FALSE(results[3].value());
    EXPECT_FALSE(results[4].value());
    EXPECT_TRUE(results[5].value());
    EXPECT_TRUE(results[6].value());

    const auto after_typed_exist = capture_metrics();
    EXPECT_EQ(after_typed_exist.mem_hits, before.mem_hits);
    EXPECT_EQ(after_typed_exist.file_hits, before.file_hits);
    EXPECT_EQ(after_typed_exist.dfs_hits, before.dfs_hits);
    EXPECT_EQ(after_typed_exist.valid_gets, before.valid_gets);
    EXPECT_EQ(after_typed_exist.total_gets, before.total_gets);
    EXPECT_EQ(MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_a),
              group_a_lease_before);
    EXPECT_EQ(MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_b),
              group_b_lease_before);

    const auto read = service->BatchGetReplicaList({group_key_a},
                                                   TenantId::Default());
    ASSERT_EQ(read.size(), 1u);
    ASSERT_TRUE(read[0].has_value());
    const auto after_read = capture_metrics();
    EXPECT_EQ(after_read.mem_hits, after_typed_exist.mem_hits + 1);
    EXPECT_EQ(after_read.file_hits, after_typed_exist.file_hits);
    EXPECT_EQ(after_read.dfs_hits, after_typed_exist.dfs_hits);
    EXPECT_EQ(after_read.valid_gets, after_typed_exist.valid_gets + 1);
    EXPECT_EQ(after_read.total_gets, after_typed_exist.total_gets + 1);
    const auto group_a_lease_after_read_opt =
        MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_a);
    const auto group_b_lease_after_read_opt =
        MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_b);
    ASSERT_TRUE(group_a_lease_after_read_opt.has_value());
    ASSERT_TRUE(group_b_lease_after_read_opt.has_value());
    const auto group_a_lease_after_read = *group_a_lease_after_read_opt;
    const auto group_b_lease_after_read = *group_b_lease_after_read_opt;
    EXPECT_GT(group_a_lease_after_read, group_a_lease_before);
    EXPECT_GT(group_b_lease_after_read, group_b_lease_before);

    const auto after_read_exist = service->BatchExistKeyByReplicaType(
        {group_key_a}, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_EQ(after_read_exist.size(), 1u);
    ASSERT_TRUE(after_read_exist[0].has_value());
    EXPECT_TRUE(after_read_exist[0].value());
    EXPECT_EQ(MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_a),
              group_a_lease_after_read);
    EXPECT_EQ(MasterServiceTestPeer::GetLeaseTimeoutForTest(*service, group_key_b),
              group_b_lease_after_read);
    const auto after_read_exist_metrics = capture_metrics();
    EXPECT_EQ(after_read_exist_metrics.mem_hits, after_read.mem_hits);
    EXPECT_EQ(after_read_exist_metrics.file_hits, after_read.file_hits);
    EXPECT_EQ(after_read_exist_metrics.dfs_hits, after_read.dfs_hits);
    EXPECT_EQ(after_read_exist_metrics.valid_gets, after_read.valid_gets);
    EXPECT_EQ(after_read_exist_metrics.total_gets, after_read.total_gets);
    service.reset();
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_F(MasterServiceTest, StandbyDfsRecoveryFilterPreservesMemoryAndLocalReplicas) {
    for (const bool recovery : {false, true}) {
        for (const bool batch : {false, true}) {
            SCOPED_TRACE(recovery);
            SCOPED_TRACE(batch);
            const auto root = MakeDfsTestRoot("standby_filter");
            {
                ScopedBucketDfsEnv env(root, "1048576", "16");
                ScopedEnvVar recovery_env("MOONCAKE_DFS_BUCKET_RECOVERY_ENABLED", recovery ? "1" : "0");
                MasterServiceConfig config;
                config.enable_ha = true;
                config.enable_oplog = false;
                MasterService target(config);
                const auto baseline = MasterMetricManager::instance().get_active_clients();
                const std::string endpoint = "standby_filter_memory";
                auto mixed = StandbyObjectEntry{};
                mixed.key = "mixed";
                mixed.metadata.size = 1024;
                AllocatedBuffer::Descriptor buffer;
                buffer.transport_endpoint_ = endpoint;
                buffer.buffer_address_ = 0x300000000;
                buffer.size_ = 1024;
                Replica::Descriptor memory;
                memory.id = 901;
                memory.status = ReplicaStatus::COMPLETE;
                memory.descriptor_variant = MemoryDescriptor{buffer};
                mixed.metadata.replicas.push_back(memory);
                const DistributedFSDescriptor dfs{root + "/bucket_000042.data", 0, 1024, 4096, 42};
                auto dfs_replica = Replica(dfs, ReplicaStatus::COMPLETE).get_descriptor();
                dfs_replica.id = 902;
                mixed.metadata.replicas.push_back(dfs_replica);
                auto dfs_only = mixed;
                dfs_only.key = "dfs_only";
                dfs_only.metadata.replicas = {dfs_replica};
                dfs_only.metadata.replicas.front().id = 903;
                auto local = mixed;
                local.key = "local";
                local.metadata.replicas = {Replica(generate_uuid(), 1024, endpoint, ReplicaStatus::COMPLETE).get_descriptor(), dfs_replica};
                local.metadata.replicas[0].id = 904;
                local.metadata.replicas[1].id = 905;
                const std::vector<StandbyObjectEntry> entries{mixed, dfs_only, local};
                StandbySegmentInfo segment;
                segment.segment_name = endpoint;
                segment.transport_endpoint = endpoint;
                segment.capacity = 16 * 1024 * 1024;
                segment.is_memory_segment = true;
                if (batch) {
                    BatchOpLogPromotionHandoff handoff;
                    handoff.metadata_store = std::make_unique<StandbyMetadataStore>();
                    for (const auto& entry : entries)
                        ASSERT_TRUE(handoff.metadata_store->PutMetadata(entry.tenant_id, entry.key, entry.metadata));
                    handoff.segments = {segment};
                    handoff.applied_cursor = {.batch_id = 7, .last_seq = 7};
                    handoff.max_replica_id = 905;
                    ASSERT_TRUE(target.RestoreFromBatchOpLogPromotion(handoff, 1));
                } else {
                    ASSERT_TRUE(target.RestoreFromStandbySnapshot(entries, 7, {segment}));
                }
                for (const auto& key : {"mixed", "local", "dfs_only"}) {
                    auto hold = MasterServiceTestPeer::ReadObject(target, {TenantId::Default(), key});
                    if (std::string(key) == "dfs_only" && !recovery) {
                        EXPECT_FALSE(hold);
                        continue;
                    }
                    ASSERT_TRUE(hold);
                    EXPECT_EQ(hold->metadata().CountReplicas(), std::string(key) == "dfs_only" ? 1u : (recovery ? 2u : 1u));
                    EXPECT_EQ(hold->metadata().HasReplica(&Replica::is_dfs_replica), recovery);
                }
                EXPECT_EQ(MasterMetricManager::instance().get_active_clients(), baseline);
            }
            std::error_code ec;
            std::filesystem::remove_all(root, ec);
        }
    }
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
