#include "l4/distributed_storage_internal.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/dfs_log_limiter.h"

#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <limits>
#include <mutex>
#include <sstream>
#include <sys/stat.h>
#include <system_error>
#include <thread>
#include <unordered_set>

#include "environ.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/dfs_io_timeout.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/dfs_root_identity.h"
#include "storage/distributed/dfs_read_probe.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "thread_pool.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

namespace {

constexpr char kDfsHealthProbeFilePrefix[] = ".mooncake_dfs_health_probe_";

using l4::kBucketFilePrefix;
using l4::kBucketDataSuffix;

bool IsDfsDescriptorRangeValid(const DistributedFSDescriptor& desc,
                               const DistributedStorageConfig& config) {
    if (config.alignment == 0 || desc.object_size == 0 ||
        desc.aligned_size < desc.object_size ||
        desc.offset % config.alignment != 0 ||
        desc.aligned_size % config.alignment != 0) {
        return false;
    }
    if (desc.offset > config.shard_capacity ||
        desc.aligned_size > config.shard_capacity - desc.offset) {
        return false;
    }

    constexpr uint64_t kMaxFileOffset =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return desc.offset <= kMaxFileOffset &&
           desc.aligned_size <= kMaxFileOffset - desc.offset;
}

/**
 * @brief Validate a BUCKET-mode descriptor.
 *
 * A bucket descriptor points directly at an alignment-aligned object start;
 *
 * the reserved size covers the object plus trailing alignment padding.
 */
bool IsBucketDescriptorRangeValid(const DistributedFSDescriptor& desc,
                                  const std::string& key,
                                  const DistributedStorageConfig& config) {
    if (desc.object_size == 0 || desc.shard_idx < 0) return false;
    if (key.empty()) return false;

    auto layout = RebuildBucketEntryLayout(desc.offset, desc.object_size,
                                           config.alignment);
    if (!layout) return false;
    if (layout->offset != desc.offset) return false;
    if (layout->aligned_size != desc.aligned_size) return false;
    if (layout->end() > config.bucket_capacity) return false;

    constexpr uint64_t kMaxFileOffset =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return layout->offset <= kMaxFileOffset &&
           layout->aligned_size <= kMaxFileOffset - layout->offset;
}

}  // namespace

std::optional<DfsAllocatorType> ParseDfsAllocatorType(std::string_view name) {
    if (name == "shard" || name == "SHARD") return DfsAllocatorType::SHARD;
    if (name == "bucket" || name == "BUCKET") return DfsAllocatorType::BUCKET;
    return std::nullopt;
}

const char* ToString(DfsAllocatorType type) {
    switch (type) {
        case DfsAllocatorType::SHARD:
            return "shard";
        case DfsAllocatorType::BUCKET:
            return "bucket";
    }
    return "unknown";
}

bool DistributedStorageConfig::Validate() const {
    if (!allocator_type_valid) {
        LOG(ERROR) << "DistributedStorageConfig: invalid allocator_type";
        return false;
    }
    if (fsdir.empty()) {
        LOG(ERROR) << "DistributedStorageConfig: fsdir is empty";
        return false;
    }
    if (!std::filesystem::path(fsdir).is_absolute()) {
        LOG(ERROR)
            << "DistributedStorageConfig: fsdir must be an absolute path: "
            << fsdir;
        return false;
    }
    if (fs_adapter_type != "hf3fs" && fs_adapter_type != "posix") {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (shard_count <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_count must > 0";
        return false;
    }
    if (shard_capacity == 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must > 0";
        return false;
    }
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        LOG(ERROR) << "DistributedStorageConfig: alignment must be power of 2";
        return false;
    }
    if (shard_capacity % alignment != 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must align";
        return false;
    }
    if (!single_tenant) {
        LOG(ERROR) << "DistributedStorageConfig: Currently, DFS requires "
                      "single_tenant=true";
        return false;
    }
    if (batch_get_max_inflight_batches == 0 ||
        batch_get_max_inflight_bytes == 0) {
        LOG(ERROR) << "DistributedStorageConfig: BatchGet in-flight limits "
                      "must be positive, batches="
                   << batch_get_max_inflight_batches
                   << ", bytes=" << batch_get_max_inflight_bytes;
        return false;
    }
    if (allocator_type == DfsAllocatorType::BUCKET &&
        !ValidateForBucketAllocator()) {
        return false;
    }
    return true;
}

bool DistributedStorageConfig::ValidateForAllocator() const {
    if (!Validate()) return false;

    if (eviction_low_watermark < 0.0 || eviction_low_watermark > 1.0 ||
        eviction_high_watermark < 0.0 || eviction_high_watermark > 1.0 ||
        eviction_low_watermark >= eviction_high_watermark) {
        LOG(ERROR) << "DistributedStorageConfig: eviction watermarks must "
                      "satisfy 0 <= low < high <= 1, low="
                   << eviction_low_watermark
                   << ", high=" << eviction_high_watermark;
        return false;
    }
    if (deferred_free_duration.count() < 0) {
        LOG(ERROR) << "DistributedStorageConfig: deferred_free_duration must "
                      "be non-negative, seconds="
                   << deferred_free_duration.count();
        return false;
    }
    if (eviction_enabled && eviction_check_interval.count() <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: eviction_check_interval must "
                      "be positive when eviction is enabled, seconds="
                   << eviction_check_interval.count();
        return false;
    }
    return true;
}

bool DistributedStorageConfig::IsReplicaEnabledFromEnvironment() {
    const bool enabled =
        Environ::GetBool("MOONCAKE_DFS_FORCE_ONE_REPLICA", false);
    static std::once_flag log_once;
    std::call_once(log_once, [enabled] {
        LOG(INFO) << "MOONCAKE_DFS_FORCE_ONE_REPLICA is "
                  << (enabled ? "true" : "false");
    });
    return enabled;
}

DistributedStorageConfig DistributedStorageConfig::FromEnvironment() {
    DistributedStorageConfig config;
    config.fsdir = Environ::GetString(
        "MOONCAKE_DFS_ROOT_DIR",
        Environ::GetString("MOONCAKE_DISTRIBUTED_ROOT_DIR", config.fsdir));
    if (!std::filesystem::path(config.fsdir).is_absolute()) {
        config.fsdir = std::filesystem::absolute(config.fsdir).string();
    }
    config.fs_adapter_type =
        Environ::GetString("MOONCAKE_DFS_FS_ADAPTER",
                           Environ::GetString("MOONCAKE_DISTRIBUTED_FS_TYPE",
                                              config.fs_adapter_type));
    config.enable_health_check =
        Environ::GetBool("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", false);
    config.shard_count =
        Environ::GetInt("MOONCAKE_DFS_SHARD_COUNT", config.shard_count);
    config.shard_capacity = Environ::GetUInt64("MOONCAKE_DFS_SHARD_CAPACITY",
                                               config.shard_capacity);
    config.alignment =
        Environ::GetUInt64("MOONCAKE_DFS_ALIGNMENT", config.alignment);
    config.single_tenant =
        Environ::GetBool("MOONCAKE_DFS_SINGLE_TENANT", config.single_tenant);
    config.eviction_enabled = Environ::GetBool("MOONCAKE_DFS_EVICTION_ENABLED",
                                               config.eviction_enabled);
    config.eviction_high_watermark = Environ::GetDouble(
        "MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", config.eviction_high_watermark);
    config.eviction_low_watermark = Environ::GetDouble(
        "MOONCAKE_DFS_EVICTION_LOW_WATERMARK", config.eviction_low_watermark);
    config.low_utilization_threshold = Environ::GetDouble(
        "MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_THRESHOLD",
        config.low_utilization_threshold);
    const bool valid_low_utilization_threshold =
        std::isfinite(config.low_utilization_threshold) &&
        config.low_utilization_threshold >= 0.0 &&
        config.low_utilization_threshold < 1.0;
    if (!valid_low_utilization_threshold) {
        LOG(WARNING) << "Invalid MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_THRESHOLD="
                     << config.low_utilization_threshold
                     << "; using default 0.05";
        config.low_utilization_threshold = 0.05;
    }
    config.low_utilization_evictions_per_tick = Environ::GetInt(
        "MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_EVICTIONS_PER_TICK",
        config.low_utilization_evictions_per_tick);
    if (config.low_utilization_evictions_per_tick < 0) {
        LOG(WARNING)
            << "Invalid MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_EVICTIONS_PER_TICK="
            << config.low_utilization_evictions_per_tick
            << "; using default 1";
        config.low_utilization_evictions_per_tick = 1;
    }
    config.low_utilization_eviction_attempts_per_tick = Environ::GetInt(
        "MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_ATTEMPTS_PER_TICK",
        config.low_utilization_eviction_attempts_per_tick);
    if (config.low_utilization_eviction_attempts_per_tick < 1) {
        LOG(WARNING)
            << "Invalid MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_ATTEMPTS_PER_TICK="
            << config.low_utilization_eviction_attempts_per_tick
            << "; using default 8";
        config.low_utilization_eviction_attempts_per_tick = 8;
    }
    config.deferred_free_duration = std::chrono::seconds(Environ::GetInt(
        "MOONCAKE_DFS_DEFERRED_FREE_SECONDS",
        static_cast<int>(config.deferred_free_duration.count())));
    config.eviction_check_interval = std::chrono::seconds(Environ::GetInt(
        "MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
        static_cast<int>(config.eviction_check_interval.count())));

    // An unknown allocator type name is recorded rather than corrected, so the
    // master surfaces a configuration error instead of silently using SHARD.
    const std::string allocator_type_name = Environ::GetString(
        "MOONCAKE_DFS_ALLOCATOR_TYPE", ToString(config.allocator_type));
    if (auto parsed = ParseDfsAllocatorType(allocator_type_name)) {
        config.allocator_type = *parsed;
    } else {
        LOG(ERROR) << "Unknown MOONCAKE_DFS_ALLOCATOR_TYPE '"
                   << allocator_type_name << "', expected 'shard' or 'bucket'";
        config.allocator_type_valid = false;
    }
    config.bucket_capacity = Environ::GetUInt64("MOONCAKE_DFS_BUCKET_CAPACITY",
                                                config.bucket_capacity);
    config.bucket_recovery_enabled = Environ::GetBool(
        "MOONCAKE_DFS_BUCKET_RECOVERY_ENABLED", config.bucket_recovery_enabled);
    config.max_bucket_count = static_cast<int64_t>(
        Environ::GetInt("MOONCAKE_DFS_MAX_BUCKET_COUNT",
                        static_cast<int>(config.max_bucket_count)));
    config.ready_bucket_target = Environ::GetInt(
        "MOONCAKE_DFS_READY_BUCKET_COUNT", config.ready_bucket_target);
    config.bucket_create_concurrency =
        Environ::GetInt("MOONCAKE_DFS_BUCKET_CREATE_CONCURRENCY",
                        config.bucket_create_concurrency);
    config.batch_read_threads = Environ::GetInt(
        "MOONCAKE_DFS_BATCH_READ_THREADS", config.batch_read_threads);
    config.batch_read_merge_enabled =
        Environ::GetBool("MOONCAKE_DFS_BATCH_READ_MERGE_ENABLED",
                         config.batch_read_merge_enabled);
    config.direct_read_enabled = Environ::GetBool(
        "MOONCAKE_DFS_DIRECT_READ_ENABLED", config.direct_read_enabled);
    config.batch_get_timeout_ms = Environ::GetUInt64(
        "MOONCAKE_DFS_BATCH_GET_TIMEOUT_MS", config.batch_get_timeout_ms);
    config.batch_timeout_fixed_ms = Environ::GetUInt64(
        "MOONCAKE_DFS_BATCH_TIMEOUT_FIXED_MS",
        config.batch_timeout_fixed_ms);
    config.batch_get_max_inflight_batches = Environ::GetUInt64(
        "MOONCAKE_DFS_BATCH_GET_MAX_INFLIGHT_BATCHES",
        config.batch_get_max_inflight_batches);
    config.batch_get_max_inflight_bytes = Environ::GetUInt64(
        "MOONCAKE_DFS_BATCH_GET_MAX_INFLIGHT_BYTES",
        config.batch_get_max_inflight_bytes);
    LOG(INFO) << config.FormatStr();
    return config;
}

std::string DistributedStorageConfig::FormatStr() const {
    std::ostringstream oss;
    oss << "fsdir=" << fsdir << ", fs_adapter_type=" << fs_adapter_type
        << ", enable_health_check=" << enable_health_check
        << ", shard_count=" << shard_count
        << ", shard_capacity=" << shard_capacity << ", alignment=" << alignment
        << ", single_tenant=" << single_tenant
        << ", eviction_enabled=" << eviction_enabled
        << ", eviction_high_watermark=" << eviction_high_watermark
        << ", eviction_low_watermark=" << eviction_low_watermark
        << ", low_utilization_threshold=" << low_utilization_threshold
        << ", low_utilization_evictions_per_tick="
        << low_utilization_evictions_per_tick
        << ", low_utilization_eviction_attempts_per_tick="
        << low_utilization_eviction_attempts_per_tick
        << ", deferred_free_seconds=" << deferred_free_duration.count()
        << ", eviction_check_interval_seconds="
        << eviction_check_interval.count()
        << ", allocator_type=" << ToString(allocator_type)
        << ", bucket_capacity=" << bucket_capacity
        << ", bucket_recovery_enabled=" << bucket_recovery_enabled
        << ", max_bucket_count=" << max_bucket_count
        << ", ready_bucket_target=" << ready_bucket_target
        << ", bucket_create_concurrency=" << bucket_create_concurrency
        << ", batch_read_threads=" << batch_read_threads
        << ", batch_read_merge_enabled=" << batch_read_merge_enabled
        << ", direct_read_enabled=" << direct_read_enabled
        << ", batch_get_timeout_ms=" << batch_get_timeout_ms
        << ", batch_timeout_fixed_ms=" << batch_timeout_fixed_ms
        << ", batch_get_max_inflight_batches="
        << batch_get_max_inflight_batches
        << ", batch_get_max_inflight_bytes="
        << batch_get_max_inflight_bytes;
    return oss.str();
}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter)
    : DistributedStorageBackend(file_storage_config, distributed_config,
                                std::move(fs_adapter), nullptr, {}) {}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter,
    std::string expected_root_identity)
    : DistributedStorageBackend(file_storage_config, distributed_config,
                                std::move(fs_adapter), nullptr,
                                std::move(expected_root_identity)) {}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter,
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter,
    std::string expected_root_identity)
    : StorageBackendInterface(file_storage_config),
      fs_adapter_(std::move(fs_adapter)),
      object_storage_adapter_(std::move(object_storage_adapter)),
      distributed_config_(distributed_config),
      batch_timeout_fixed_ms_(distributed_config.batch_timeout_fixed_ms),
      expected_root_identity_(std::move(expected_root_identity)),
      root_dir_(distributed_config.fsdir),
      descriptor_root_dir_(distributed_config.fsdir) {
    CHECK((fs_adapter_ != nullptr) != (object_storage_adapter_ != nullptr))
        << "DistributedStorageBackend: exactly one I/O adapter is required";
    if (object_storage_adapter_) {
        storage_mode_ = DistributedStorageMode::kObjectStorage;
    }
    health_probe_path_prefix_ = root_dir_ + "/" + kDfsHealthProbeFilePrefix +
                                UuidToString(generate_uuid());
}

DistributedStorageBackend::~DistributedStorageBackend() {
    for (auto& shard : shard_files_) {
        if (shard && fs_adapter_) {
            const int write_fd = shard->write_fd.load();
            if (write_fd >= 0) {
                fs_adapter_->CloseFile(write_fd);
            }
            if (shard->read_fd >= 0 && shard->read_fd != write_fd) {
                fs_adapter_->CloseFile(shard->read_fd);
            }
            shard->read_fd = -1;
            shard->write_fd.store(-1);
        }
    }
    if (fs_adapter_ && !active_health_probe_path_.empty()) {
        (void)fs_adapter_->DeleteFile(active_health_probe_path_);
    }
    // Drop cached bucket handles before the adapter goes away; each handle
    // closes its own fd in its destructor via the adapter pointer it holds.
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        bucket_id_cache_.clear();
        bucket_id_direct_cache_.clear();
    }
    batch_read_pool_.reset();
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::Init() {
    std::lock_guard<std::mutex> lock(init_mutex_);
    return InitLocked();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::InitLocked() {
    if (initialized_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "DistributedStorageBackend is already initialized";
        return ValidateRootIdentity();
    }

    if (UsesObjectStorage()) {
        auto init_result = object_storage_adapter_->Init();
        if (!init_result) return init_result;
        initialized_.store(true, std::memory_order_release);
        LOG(INFO) << "DistributedStorageBackend initialized, object adapter="
                  << object_storage_adapter_->GetName();
        return {};
    }

    std::error_code ec;
    if (!std::filesystem::is_directory(root_dir_, ec) || ec) {
        LOG(ERROR) << "DFS root directory is unavailable: " << root_dir_;
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    auto init_result = fs_adapter_->Init(root_dir_);
    if (!init_result) {
        const auto error = init_result.error();
        ResetFailedInitializationLocked();
        return tl::make_unexpected(error);
    }

    // Initialization is serialized and no batch I/O can run until it succeeds.
    // Retry the control plane when DFS was disabled at client startup, without
    // adding RPCs to business I/O or trusting the local marker as authority.
    if (expected_root_identity_.empty() && root_identity_provider_) {
        auto identity = root_identity_provider_();
        if (identity && !identity->empty() &&
            identity->size() <= kMaxDfsRootIdentitySize) {
            expected_root_identity_ = std::move(*identity);
        }
    }
    auto identity_result = ValidateRootIdentity();
    if (!identity_result) {
        LOG(ERROR) << "DFS root identity validation failed for " << root_dir_;
        ResetFailedInitializationLocked();
        return identity_result;
    }

    if (IsBucketMode()) {
        return InitBucketModeLocked();
    }

    shard_files_.reserve(distributed_config_.shard_count);
    for (int i = 0; i < distributed_config_.shard_count; ++i) {
        std::string path = root_dir_ + "/dfs_shard_" +
                           DfsGlobalAllocator::FormatShardIdx(
                               i, distributed_config_.shard_count) +
                           ".data";
        auto write_fd_result = fs_adapter_->OpenFile(path);
        auto read_fd_result = write_fd_result;
        if (!write_fd_result) {
            read_fd_result = fs_adapter_->OpenFileReadOnly(path);
        }
        if (!read_fd_result) {
            LOG(ERROR) << "Failed to open DFS shard " << path << ": "
                       << read_fd_result.error();
            const auto error = read_fd_result.error();
            ResetFailedInitializationLocked();
            return tl::make_unexpected(error);
        }
        auto shard = std::make_unique<ShardFile>();
        shard->path = std::move(path);
        shard->read_fd = *read_fd_result;
        shard->write_fd.store(write_fd_result ? *write_fd_result : -1);
        shard_files_.push_back(std::move(shard));
    }

    initialized_.store(true, std::memory_order_release);
    return {};
}

tl::expected<DistributedStorageBackend::ResolvedTarget, ErrorCode>
DistributedStorageBackend::ResolveTarget(
    const DistributedFSDescriptor& descriptor, const std::string& key,
    bool read_only) {
    if (!IsBucketMode()) {
        if (descriptor.shard_idx < 0 ||
            descriptor.shard_idx >= static_cast<int>(shard_files_.size())) {
            LOG(ERROR) << "Invalid DFS shard_idx " << descriptor.shard_idx
                       << " for key " << key;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto& shard = *shard_files_[descriptor.shard_idx];
        if (descriptor.file_path != shard.path) {
            LOG(ERROR) << "DFS path mismatch for key " << key
                       << ", descriptor=" << descriptor.file_path
                       << ", configured=" << shard.path;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!IsDfsDescriptorRangeValid(descriptor, distributed_config_)) {
            LOG(ERROR) << "Invalid DFS descriptor range for key " << key
                       << ", offset=" << descriptor.offset
                       << ", object_size=" << descriptor.object_size
                       << ", aligned_size=" << descriptor.aligned_size
                       << ", shard_capacity="
                       << distributed_config_.shard_capacity;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const int fd = read_only
                           ? shard.read_fd
                           : shard.write_fd.load(std::memory_order_acquire);
        if (fd < 0) {
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        return ResolvedTarget{fd, &shard.mutex, false, nullptr};
    }

    // BUCKET mode: the descriptor carries an allocator-chosen path, so it must
    // be validated before it is used to open anything.
    if (descriptor.shard_idx < 0) {
        LOG(ERROR) << "Invalid DFS bucket id " << descriptor.shard_idx
                   << " for key " << key;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const std::string expected_path =
        descriptor_root_dir_ + "/" + kBucketFilePrefix +
        ImmutableBucketAllocator::FormatBucketId(descriptor.shard_idx) +
        kBucketDataSuffix;
    // The master's mount path may not exist on this client.
    if (std::filesystem::path(descriptor.file_path).lexically_normal() !=
        std::filesystem::path(expected_path).lexically_normal()) {
        LOG(ERROR) << "DFS bucket path mismatch for key " << key
                   << ", descriptor=" << descriptor.file_path
                   << ", expected=" << expected_path;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!IsBucketDescriptorRangeValid(descriptor, key, distributed_config_)) {
        LOG(ERROR) << "Invalid DFS bucket descriptor for key " << key
                   << ", offset=" << descriptor.offset
                   << ", object_size=" << descriptor.object_size
                   << ", aligned_size=" << descriptor.aligned_size
                   << ", bucket_capacity="
                   << distributed_config_.bucket_capacity;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto handle = read_only ? GetOrOpenBucketDirect(descriptor.shard_idx,
                                                    descriptor.MasterEpoch())
                            : GetOrOpenBucket(descriptor.shard_idx,
                                              descriptor.MasterEpoch());
    if (!handle && read_only) {
        if (handle.error() != ErrorCode::NOT_SUPPORTED) {
            return tl::make_unexpected(handle.error());
        }
        handle =
            GetOrOpenBucket(descriptor.shard_idx, descriptor.MasterEpoch());
        read_only = false;
    }
    if (!handle) return tl::make_unexpected(handle.error());
    auto& shared = handle.value();
    // BUCKET mode needs no per-fd lock: the allocator hands each object a
    // unique, non-overlapping on-disk range, so concurrent writers and readers
    // never touch the same bytes.
    return ResolvedTarget{shared->fd, nullptr, read_only, shared};
}

tl::expected<int64_t, ErrorCode> DistributedStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (eviction_handler) {
        LOG_FIRST_N(WARNING, 1)
            << "DistributedStorageBackend does not support eviction, "
               "eviction_handler ignored";
    }

    std::vector<std::string> success_keys;
    std::vector<StorageObjectMetadata> success_metas;
    for (const auto& [key, slices] : batch_object) {
        if (slices.size() >
            static_cast<size_t>(std::numeric_limits<int>::max())) {
            LOG(WARNING) << "Failed to offload key " << key
                         << ": slice count exceeds INT_MAX";
            continue;
        }
        const int iovcnt = static_cast<int>(slices.size());

        std::vector<iovec> iovs;
        iovs.reserve(slices.size());
        size_t total_size = 0;
        bool total_size_overflow = false;
        for (const auto& slice : slices) {
            if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
                total_size_overflow = true;
                break;
            }
            iovs.push_back({slice.ptr, slice.size});
            total_size += slice.size;
        }
        if (total_size_overflow) {
            LOG(WARNING) << "Failed to offload key " << key
                         << ": total slice size overflows size_t";
            continue;
        }

        auto result = object_storage_adapter_->PutV(key, iovs.data(), iovcnt);
        if (!result) {
            LOG(WARNING) << "Failed to offload key " << key << ": "
                         << static_cast<int>(result.error());
            continue;
        }

        success_keys.push_back(key);
        success_metas.emplace_back(-1, 0, static_cast<int64_t>(key.size()),
                                   static_cast<int64_t>(total_size), "");
    }

    if (complete_handler && !success_keys.empty()) {
        auto err = complete_handler(success_keys, success_metas);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }
    return static_cast<int64_t>(success_keys.size());
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWrite(
    const std::vector<DfsWriteRequest>& requests) {
    return BatchWriteBlocking(requests);
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWriteBlocking(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results;

    if (UsesObjectStorage()) {
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::NOT_SUPPORTED));
        return results;
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }
    auto initialized = ValidateRootIdentity();
    if (!initialized) {
        LOG(ERROR) << "DistributedStorageBackend is unavailable: "
                   << initialized.error();
        results.assign(requests.size(),
                       tl::make_unexpected(initialized.error()));
        return results;
    }

    // Build validated entry payloads first, including the reserved zero
    // padding, then issue one write per contiguous bounded-size run. SHARD
    // keeps the original one-request path because its allocator does not
    // promise contiguity.
    if (IsBucketMode()) {
        if (requests.empty()) return results;
        const auto epoch = requests.front().descriptor.MasterEpoch();
        if (std::any_of(requests.begin(), requests.end(),
                        [&](const auto& request) {
                            return request.descriptor.MasterEpoch() != epoch;
                        })) {
            results.assign(requests.size(),
                           tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            return results;
        }
        auto guard = AcquireBucketEpoch(epoch);
        if (!guard) {
            results.assign(requests.size(), tl::make_unexpected(guard.error()));
            return results;
        }
        return BatchWriteBucket(requests);
    }
    return BatchWriteShard(requests);
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWriteShard(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto target = ResolveTarget(request.descriptor, request.key);
        if (!target) {
            results[i] = tl::make_unexpected(target.error());
            continue;
        }
        std::vector<iovec> iovs;
        uint64_t total = 0, value_size = 0;
        if (request.descriptor.object_size == 0) continue;
        for (const auto& slice : request.slices) {
            if ((!slice.ptr && slice.size > 0) ||
                slice.size > std::numeric_limits<uint64_t>::max() - total) {
                total = 0;
                break;
            }
            if (slice.size) iovs.push_back({slice.ptr, slice.size});
            total += slice.size;
            value_size += slice.size;
        }
        if (total == 0 || value_size != request.descriptor.object_size)
            continue;
        std::lock_guard<std::mutex> lock(*target->mutex);
        auto result = fs_adapter_->WriteAt(
            target->fd, iovs.data(), static_cast<int>(iovs.size()),
            static_cast<int64_t>(request.descriptor.offset));
        if (!result)
            results[i] = tl::make_unexpected(result.error());
        else if (*result == total)
            results[i] = {};
        else
            results[i] = tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchReadShard(
    const std::vector<DfsReadRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto target = ResolveTarget(request.descriptor, request.key, true);
        if (!target) {
            results[i] = tl::make_unexpected(target.error());
            continue;
        }
        if (request.descriptor.object_size >
                std::numeric_limits<size_t>::max() ||
            request.slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
            continue;
        }

        uint64_t capacity = 0;
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if ((!slice.ptr && slice.size != 0) ||
                slice.size >
                    std::numeric_limits<uint64_t>::max() - capacity) {
                invalid = true;
                break;
            }
            capacity += slice.size;
        }
        if (invalid || capacity < request.descriptor.object_size) continue;

        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        size_t remaining =
            static_cast<size_t>(request.descriptor.object_size);
        for (const auto& slice : request.slices) {
            if (remaining == 0) break;
            const size_t size = std::min(slice.size, remaining);
            if (size != 0) iovs.push_back({slice.ptr, size});
            remaining -= size;
        }

        std::lock_guard<std::mutex> lock(*target->mutex);
        auto result = fs_adapter_->ReadAt(
            target->fd, iovs.data(), static_cast<int>(iovs.size()),
            static_cast<int64_t>(request.descriptor.offset));
        if (!result)
            results[i] = tl::make_unexpected(result.error());
        else if (*result == request.descriptor.object_size)
            results[i] = {};
        else
            results[i] = tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchReadBlocking(
    const std::vector<DfsReadRequest>& requests) {
    const auto timing_start = std::chrono::steady_clock::now();
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    if (UsesObjectStorage()) {
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::NOT_SUPPORTED));
        return results;
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }
    auto initialized = ValidateRootIdentity();
    if (!initialized) {
        LOG(ERROR) << "DistributedStorageBackend is unavailable: "
                   << initialized.error();
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(initialized.error()));
        return results;
    }

    if (!IsBucketMode()) return BatchReadShard(requests);

    if (requests.empty()) return results;
    const auto epoch = requests.front().descriptor.MasterEpoch();
    if (std::any_of(requests.begin(), requests.end(), [&](const auto& request) {
            return request.descriptor.MasterEpoch() != epoch;
        })) {
        // The client can refresh this mixed batch once, without changing local
        // epoch state based on whichever request happened to come first.
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE));
        return results;
    }
    auto guard = AcquireBucketEpoch(epoch);
    if (!guard) {
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(guard.error()));
        return results;
    }

    auto tasks = PrepareReadTasks(requests, results);
    const auto prepare_done = std::chrono::steady_clock::now();
    ExecuteReadTasks(tasks, requests, results);

    if (dfs_read_trace_enabled()) {
        const int64_t prepare_us =
            std::chrono::duration_cast<std::chrono::microseconds>(prepare_done - timing_start)
                .count();
        const int64_t read_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - prepare_done)
                .count();
        uint64_t total_bytes = 0;
        for (const auto& request : requests) {
            total_bytes += request.descriptor.object_size;
        }
        double gbps = 0.0;
        if (read_us > 0) {
            // bytes / us == MB/s; /1000 -> GB/s.
            gbps = static_cast<double>(total_bytes) / read_us / 1000.0;
        }
        LOG(INFO) << "BatchRead: requests=" << requests.size()
                  << " tasks=" << tasks.size() << " bytes=" << total_bytes
                  << " prepare_us=" << prepare_us
                  << " read_us=" << read_us
                  << " bandwidth_GBps=" << gbps;
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> DistributedStorageBackend::BatchRead(
    const std::vector<DfsReadRequest>& requests) {
    if (requests.empty()) return {};
    const auto request_start = std::chrono::steady_clock::now();
    uint64_t total_bytes = 0;
    uint64_t inflight_bytes = 0;
    bool all_buffers_owned = true;
    for (const auto& request : requests) {
        SaturatingAddBytes(request.descriptor.object_size, &total_bytes);
        for (const auto& slice : request.slices) {
            SaturatingAddBytes(slice.size, &inflight_bytes);
            if (slice.size != 0 && !request.buffer_owner) {
                all_buffers_owned = false;
            }
        }
    }
    if (dfs_read_timed_out_.load(std::memory_order_acquire)) {
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet rejected by timeout latch: "
            << "reason=previous_read_timeout_latched, requests="
            << requests.size() << ", total_bytes=" << total_bytes
            << ", inflight_bytes=" << inflight_bytes
            << ", configured_timeout_ms="
            << distributed_config_.batch_get_timeout_ms
            << ", max_inflight_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_inflight_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << ErrorCode::DFS_BATCH_GET_TIMEOUT;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::DFS_BATCH_GET_TIMEOUT));
    }
    const auto base_timeout_ms = distributed_config_.batch_get_timeout_ms;
    const auto timeout_ms = DfsBatchGetTimeoutMs(
        base_timeout_ms, total_bytes,
        batch_timeout_fixed_ms_.load(std::memory_order_acquire));
    const auto admission_deadline =
        base_timeout_ms == 0
            ? std::chrono::steady_clock::time_point::max()
            : DfsIoDeadlineFromNow(timeout_ms);
    const ErrorCode admission_result =
        AcquireInflightReadUntil(inflight_bytes, admission_deadline);
    if (admission_result != ErrorCode::OK) {
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet admission rejected: reason="
            << (admission_result == ErrorCode::DFS_BATCH_GET_TIMEOUT
                    ? "dfs_read_timeout_latched"
                    : "inflight_capacity_deadline_expired")
            << ", request_bytes=" << inflight_bytes << ", max_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << admission_result;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(), tl::make_unexpected(admission_result));
    }

    if (base_timeout_ms == 0 || !all_buffers_owned) {
        if (base_timeout_ms != 0 && !all_buffers_owned) {
            LOG_EVERY_N(WARNING, 64)
                << "DFS BatchGet destination has no lifetime owner; using "
                   "blocking I/O to prevent buffer reuse while preadv is "
                   "still running";
        }
        try {
            auto results = BatchReadBlocking(requests);
            ReleaseInflightRead(inflight_bytes);
            return results;
        } catch (...) {
            ReleaseInflightRead(inflight_bytes);
            throw;
        }
    }
    // Admission waiting and DFS I/O have different failure semantics. Start
    // the I/O deadline only after capacity has been acquired so queueing behind
    // healthy reads can never set the backend-wide DFS timeout latch.
    const auto deadline = DfsIoDeadlineFromNow(timeout_ms);
    struct TimedReadContext {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        std::chrono::steady_clock::time_point completed_at;
        std::vector<DfsReadRequest> requests;
        std::vector<tl::expected<void, ErrorCode>> results;
        std::function<void()> release_inflight;

        ~TimedReadContext() {
            if (release_inflight) release_inflight();
        }
    };
    std::shared_ptr<TimedReadContext> context;
    try {
        context = std::make_shared<TimedReadContext>();
        // Preserve the request metadata for the detached worker, but read
        // directly into the caller-provided slices. This keeps aligned pinned
        // buffers on the O_DIRECT zero-copy path and avoids a private buffer
        // allocation plus a completion-time memcpy for every object. Production
        // callers attach the staging allocation through buffer_owner, and this
        // copied request quarantines it until the underlying read really ends.
        context->requests = requests;
    } catch (const std::bad_alloc&) {
        ReleaseInflightRead(inflight_bytes);
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    }

    std::shared_ptr<DistributedStorageBackend> self;
    try {
        self = shared_from_this();
    } catch (const std::bad_weak_ptr&) {
        // Test-only/legacy stack ownership cannot safely outlive this call.
        // Keep the old blocking lifetime contract in that configuration.
        try {
            auto results = BatchReadBlocking(requests);
            ReleaseInflightRead(inflight_bytes);
            return results;
        } catch (...) {
            ReleaseInflightRead(inflight_bytes);
            throw;
        }
    }
    // Keep the backend alive through quota release too; relying on the
    // destruction order of separate thread-lambda captures would leave a
    // narrow use-after-free window during shutdown.
    try {
        context->release_inflight = [owner = self, inflight_bytes]() {
            owner->ReleaseInflightRead(inflight_bytes);
        };
    } catch (const std::bad_alloc&) {
        ReleaseInflightRead(inflight_bytes);
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    }

    if (std::chrono::steady_clock::now() >= deadline) {
        SetDfsReadTimedOut(true);
        const auto elapsed_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                       request_start)
                                     .count();
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet timed out before worker launch: "
            << "reason=deadline_expired_after_admission, requests="
            << requests.size() << ", total_bytes=" << total_bytes
            << ", inflight_bytes=" << inflight_bytes
            << ", timeout_ms=" << timeout_ms
            << ", elapsed_ms=" << elapsed_ms
            << ", max_inflight_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_inflight_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << ErrorCode::DFS_BATCH_GET_TIMEOUT;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::DFS_BATCH_GET_TIMEOUT));
    }
    try {
        std::thread([self = std::move(self), context]() {
            std::vector<tl::expected<void, ErrorCode>> results;
            try {
                results = self->BatchReadBlocking(context->requests);
            } catch (const std::bad_alloc&) {
                results.assign(
                    context->requests.size(),
                    tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
            } catch (const std::exception& e) {
                LOG(ERROR) << "Timed DFS BatchGet worker failed: " << e.what();
                results.assign(
                    context->requests.size(),
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
            } catch (...) {
                LOG(ERROR) << "Timed DFS BatchGet worker failed";
                results.assign(
                    context->requests.size(),
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
            }
            {
                std::lock_guard lock(context->mutex);
                context->results = std::move(results);
                context->completed_at = std::chrono::steady_clock::now();
                context->done = true;
            }
            context->cv.notify_all();
        }).detach();
    } catch (const std::system_error&) {
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    }

    std::unique_lock lock(context->mutex);
    const bool completed_before_deadline = context->cv.wait_until(
        lock, deadline, [&context] { return context->done; });
    const auto finished_at = std::chrono::steady_clock::now();
    if (!completed_before_deadline || context->completed_at > deadline) {
        SetDfsReadTimedOut(true);
        const auto elapsed_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(finished_at - request_start)
                                     .count();
        const auto worker_elapsed_ms =
            context->done
                ? std::chrono::duration_cast<std::chrono::milliseconds>(
                      context->completed_at - request_start)
                      .count()
                : -1;
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet I/O timed out: "
            << "reason="
            << (completed_before_deadline ? "worker_completed_after_deadline"
                                          : "worker_not_completed_by_deadline")
            << ", requests=" << requests.size()
            << ", total_bytes=" << total_bytes
            << ", inflight_bytes=" << inflight_bytes
            << ", timeout_ms=" << timeout_ms
            << ", elapsed_ms=" << elapsed_ms
            << ", worker_elapsed_ms=" << worker_elapsed_ms
            << ", max_inflight_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_inflight_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << ErrorCode::DFS_BATCH_GET_TIMEOUT;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::DFS_BATCH_GET_TIMEOUT));
    }
    return context->results;
}

tl::expected<void, ErrorCode> DistributedStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    for (auto& [key, slice] : batched_slices) {
        auto result = object_storage_adapter_->Get(key, slice.ptr, slice.size);
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        if (*result != slice.size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsExist(
    const std::string& key) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return object_storage_adapter_->Exists(key);
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsEnableOffloading() {
    return UsesObjectStorage();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<std::string> batch_keys;
    std::vector<StorageObjectMetadata> batch_metas;
    const size_t batch_limit = static_cast<size_t>(std::max<int64_t>(
        1, file_storage_config_.scanmeta_iterator_keys_limit));

    auto key_infos = object_storage_adapter_->ListKeys();
    if (!key_infos) {
        LOG(ERROR) << "Failed to list keys from object storage adapter: "
                   << static_cast<int>(key_infos.error());
        return tl::make_unexpected(key_infos.error());
    }

    for (const auto& info : *key_infos) {
        batch_keys.push_back(info.logical_key);
        batch_metas.emplace_back(-1, 0,
                                 static_cast<int64_t>(info.logical_key.size()),
                                 static_cast<int64_t>(info.size), "");
        if (batch_keys.size() >= batch_limit) {
            auto err = handler(batch_keys, batch_metas);
            if (err != ErrorCode::OK) return tl::make_unexpected(err);
            batch_keys.clear();
            batch_metas.clear();
        }
    }
    if (!batch_keys.empty()) {
        auto err = handler(batch_keys, batch_metas);
        if (err != ErrorCode::OK) return tl::make_unexpected(err);
    }
    return {};
}

}  // namespace mooncake
