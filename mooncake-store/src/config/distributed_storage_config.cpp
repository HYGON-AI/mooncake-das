#include "config/distributed_storage_config.h"

#include <glog/logging.h>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <limits>
#include <mutex>
#include <sstream>
#include <string_view>

#include "environ.h"
#include "environment_variables.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/dfs_allocator_interface.h"

namespace mooncake {
namespace {
// Bound parallel bucket staging buffers and background preallocation workers.
constexpr int kMaxBatchReadThreads = 256;
constexpr int kMaxBucketCreateConcurrency = 8;

bool ValidateEvictionSettings(const DistributedStorageConfig& config) {
    if (config.eviction_low_watermark < 0.0 ||
        config.eviction_low_watermark > 1.0 ||
        config.eviction_high_watermark < 0.0 ||
        config.eviction_high_watermark > 1.0 ||
        config.eviction_low_watermark >= config.eviction_high_watermark) {
        LOG(ERROR) << "DistributedStorageConfig: eviction watermarks must "
                      "satisfy 0 <= low < high <= 1, low="
                   << config.eviction_low_watermark
                   << ", high=" << config.eviction_high_watermark;
        return false;
    }
    if (config.deferred_free_duration.count() < 0) {
        LOG(ERROR) << "DistributedStorageConfig: deferred_free_duration must "
                      "be non-negative, seconds="
                   << config.deferred_free_duration.count();
        return false;
    }
    if (config.eviction_enabled && config.eviction_check_interval.count() <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: eviction_check_interval must "
                      "be positive when eviction is enabled, seconds="
                   << config.eviction_check_interval.count();
        return false;
    }
    return true;
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
    if (UsesKvcs()) {
        if (!single_tenant) {
            LOG(ERROR) << "DistributedStorageConfig: KVCS currently requires "
                          "single_tenant=true";
            return false;
        }
        if (eviction_check_interval.count() <= 0) {
            LOG(ERROR) << "DistributedStorageConfig: KVCS capacity check "
                          "interval must be positive, seconds="
                       << eviction_check_interval.count();
            return false;
        }
        return ValidateEvictionSettings(*this);
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
    if (fs_adapter_type != "hf3fs" && fs_adapter_type != "posix" &&
        !UsesObjectStorage()) {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (UsesObjectStorage()) {
        return true;
    }
    if (!allocator_type_valid) {
        LOG(ERROR) << "DistributedStorageConfig: unsupported allocator_type: "
                   << ToString(allocator_type);
        return false;
    }
    // alignment is shared by both allocators.
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        LOG(ERROR) << "DistributedStorageConfig: alignment must be power of 2";
        return false;
    }
    if (allocator_type == DfsAllocatorType::SHARD) {
        if (shard_count <= 0) {
            LOG(ERROR) << "DistributedStorageConfig: shard_count must > 0";
            return false;
        }
        if (shard_capacity == 0) {
            LOG(ERROR) << "DistributedStorageConfig: shard_capacity must > 0";
            return false;
        }
        if (shard_capacity % alignment != 0) {
            LOG(ERROR) << "DistributedStorageConfig: shard_capacity must align";
            return false;
        }
    }
    if (batch_get_max_inflight_batches == 0 || batch_get_max_inflight_bytes == 0) {
        return false;
    }
    if (allocator_type == DfsAllocatorType::BUCKET &&
        !ValidateForBucketAllocator()) {
        return false;
    }
    if (!single_tenant) {
        LOG(ERROR) << "DistributedStorageConfig: Currently, DFS requires "
                      "single_tenant=true";
        return false;
    }
    return true;
}

bool DistributedStorageConfig::ValidateForAllocator() const {
    if (!Validate()) return false;
    if (UsesObjectStorage()) {
        LOG(ERROR) << "DistributedStorageConfig: DFS allocator requires a "
                      "filesystem adapter";
        return false;
    }

    return ValidateEvictionSettings(*this);
}

DistributedStorageConfig DistributedStorageConfig::FromEnvironment() {
    DistributedStorageConfig config;
    using Variables = DistributedStorageEnvironmentVariables;

    const auto legacy_root_dir =
        Environ::ReadOr(Variables::MOONCAKE_DISTRIBUTED_ROOT_DIR, config.fsdir);
    config.fsdir =
        Environ::ReadOr(Variables::MOONCAKE_DFS_ROOT_DIR, legacy_root_dir);
    if (!std::filesystem::path(config.fsdir).is_absolute()) {
        config.fsdir = std::filesystem::absolute(config.fsdir).string();
    }

    const auto legacy_fs_adapter = Environ::ReadOr(
        Variables::MOONCAKE_DISTRIBUTED_FS_TYPE, config.fs_adapter_type);
    config.fs_adapter_type =
        Environ::ReadOr(Variables::MOONCAKE_DFS_FS_ADAPTER, legacy_fs_adapter);
    config.object_storage_config_path = Environ::ReadOr(
        Variables::MOONCAKE_KVCS_EFC_CONFIG, config.object_storage_config_path);
    config.enable_health_check =
        Environ::ReadOr(Variables::MOONCAKE_DISTRIBUTED_HEALTH_CHECK,
                        config.enable_health_check);
    config.shard_count = Environ::ReadOr(Variables::MOONCAKE_DFS_SHARD_COUNT,
                                         config.shard_count);
    config.shard_capacity = Environ::ReadOr(
        Variables::MOONCAKE_DFS_SHARD_CAPACITY, config.shard_capacity);
    config.bucket_capacity = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BUCKET_CAPACITY, config.bucket_capacity);
    config.max_bucket_count = Environ::ReadOr(
        Variables::MOONCAKE_DFS_MAX_BUCKET_COUNT, config.max_bucket_count);
    config.alignment =
        Environ::ReadOr(Variables::MOONCAKE_DFS_ALIGNMENT, config.alignment);
    config.single_tenant = Environ::ReadOr(
        Variables::MOONCAKE_DFS_SINGLE_TENANT, config.single_tenant);
    config.eviction_enabled = Environ::ReadOr(
        Variables::MOONCAKE_DFS_EVICTION_ENABLED, config.eviction_enabled);

    // GetDouble silently falls back for an empty value; ReadOr<double> emits a
    // warning. Keep the existing diagnostics while this refactor is
    // behavior-preserving.
    config.eviction_high_watermark =
        Environ::GetDouble(Variables::MOONCAKE_DFS_EVICTION_HIGH_WATERMARK.name,
                           config.eviction_high_watermark);
    config.eviction_low_watermark =
        Environ::GetDouble(Variables::MOONCAKE_DFS_EVICTION_LOW_WATERMARK.name,
                           config.eviction_low_watermark);
    config.deferred_free_duration = std::chrono::seconds(Environ::ReadOr(
        Variables::MOONCAKE_DFS_DEFERRED_FREE_SECONDS,
        static_cast<int>(config.deferred_free_duration.count())));
    config.eviction_check_interval = std::chrono::seconds(Environ::ReadOr(
        Variables::MOONCAKE_DFS_EVICTION_CHECK_INTERVAL,
        static_cast<int>(config.eviction_check_interval.count())));
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
    config.low_utilization_evictions_per_tick = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_EVICTIONS_PER_TICK,
        config.low_utilization_evictions_per_tick);
    if (config.low_utilization_evictions_per_tick < 0) {
        LOG(WARNING)
            << "Invalid MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_EVICTIONS_PER_TICK="
            << config.low_utilization_evictions_per_tick
            << "; using default 1";
        config.low_utilization_evictions_per_tick = 1;
    }
    config.low_utilization_eviction_attempts_per_tick = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_ATTEMPTS_PER_TICK,
        config.low_utilization_eviction_attempts_per_tick);
    if (config.low_utilization_eviction_attempts_per_tick < 1) {
        LOG(WARNING)
            << "Invalid MOONCAKE_DFS_BUCKET_LOW_UTILIZATION_ATTEMPTS_PER_TICK="
            << config.low_utilization_eviction_attempts_per_tick
            << "; using default 8";
        config.low_utilization_eviction_attempts_per_tick = 8;
    }
    // An unknown allocator type name is recorded rather than corrected, so the
    // master surfaces a configuration error instead of silently using SHARD.
    const std::string allocator_type_name = Environ::ReadOr(
        Variables::MOONCAKE_DFS_ALLOCATOR_TYPE,
        Environ::ReadOr(Variables::MOONCAKE_DFS_ALLOCATOR,
                        std::string(ToString(config.allocator_type))));
    if (auto parsed = ParseDfsAllocatorType(allocator_type_name)) {
        config.allocator_type = *parsed;
    } else {
        LOG(ERROR) << "Unknown MOONCAKE_DFS_ALLOCATOR_TYPE '"
                   << allocator_type_name << "', expected 'shard' or 'bucket'";
        config.allocator_type_valid = false;
    }
    config.bucket_recovery_enabled = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BUCKET_RECOVERY_ENABLED,
        config.bucket_recovery_enabled);
    config.ready_bucket_target = Environ::ReadOr(
        Variables::MOONCAKE_DFS_READY_BUCKET_COUNT, config.ready_bucket_target);
    config.bucket_create_concurrency = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BUCKET_CREATE_CONCURRENCY,
        config.bucket_create_concurrency);
    config.batch_read_threads = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_READ_THREADS, config.batch_read_threads);
    config.batch_read_merge_enabled = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_READ_MERGE_ENABLED,
        config.batch_read_merge_enabled);
    config.direct_read_enabled = Environ::ReadOr(
        Variables::MOONCAKE_DFS_DIRECT_READ_ENABLED, config.direct_read_enabled);
    config.batch_get_timeout_ms = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_GET_TIMEOUT_MS, config.batch_get_timeout_ms);
    config.batch_timeout_fixed_ms = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_TIMEOUT_FIXED_MS,
        config.batch_timeout_fixed_ms);
    config.batch_get_max_inflight_batches = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_GET_MAX_INFLIGHT_BATCHES,
        config.batch_get_max_inflight_batches);
    config.batch_get_max_inflight_bytes = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_GET_MAX_INFLIGHT_BYTES,
        config.batch_get_max_inflight_bytes);
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
        << ", object_storage_config_path=" << object_storage_config_path
        << ", batch_get_max_inflight_bytes="
        << batch_get_max_inflight_bytes;
    return oss.str();
}

bool DistributedStorageConfig::IsReplicaEnabledFromEnvironment() {
    const bool enabled = Environ::ReadOr(
        DistributedStorageEnvironmentVariables::MOONCAKE_DFS_FORCE_ONE_REPLICA,
        false);
    static std::once_flag log_once;
    std::call_once(log_once, [enabled] {
        LOG(INFO) << "MOONCAKE_DFS_FORCE_ONE_REPLICA is "
                  << (enabled ? "true" : "false");
    });
    return enabled;
}

bool DistributedStorageConfig::ValidateForBucketAllocator() const {
    if (bucket_capacity == 0 ||
        bucket_capacity >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        LOG(ERROR) << "DistributedStorageConfig: bucket_capacity must > 0";
        return false;
    }
    if (!IsValidBucketAlignment(alignment)) {
        LOG(ERROR) << "DistributedStorageConfig: alignment must be a power of "
                      "2, alignment="
                   << alignment;
        return false;
    }
    if (bucket_capacity % alignment != 0) {
        LOG(ERROR) << "DistributedStorageConfig: bucket_capacity ("
                   << bucket_capacity << ") must be a multiple of alignment ("
                   << alignment << ")";
        return false;
    }
    if (max_bucket_count <= 0) {
        // A fixed denominator is required for stable eviction watermarks.
        LOG(ERROR) << "DistributedStorageConfig: max_bucket_count must > 0 in "
                      "bucket allocator mode, max_bucket_count="
                   << max_bucket_count;
        return false;
    }
    if (max_bucket_count > kMaxBucketId) {
        LOG(ERROR) << "DistributedStorageConfig: max_bucket_count ("
                   << max_bucket_count << ") exceeds the addressable maximum ("
                   << kMaxBucketId << ")";
        return false;
    }
    if (static_cast<uint64_t>(max_bucket_count) >
        std::numeric_limits<uint64_t>::max() / bucket_capacity) {
        LOG(ERROR) << "DistributedStorageConfig: max_bucket_count * "
                      "bucket_capacity overflows";
        return false;
    }
    if (ready_bucket_target < 0) {
        LOG(ERROR) << "DistributedStorageConfig: ready_bucket_target must be "
                      "non-negative, ready_bucket_target="
                   << ready_bucket_target;
        return false;
    }
    if (bucket_create_concurrency < 1 ||
        bucket_create_concurrency > kMaxBucketCreateConcurrency) {
        LOG(ERROR)
            << "DistributedStorageConfig: bucket_create_concurrency must be "
               "in [1, "
            << kMaxBucketCreateConcurrency
            << "], bucket_create_concurrency=" << bucket_create_concurrency;
        return false;
    }
    if (batch_read_threads < 1 || batch_read_threads > kMaxBatchReadThreads) {
        LOG(ERROR) << "DistributedStorageConfig: batch_read_threads must be in "
                      "[1, "
                   << kMaxBatchReadThreads
                   << "], batch_read_threads=" << batch_read_threads;
        return false;
    }
    return true;
}

}  // namespace mooncake
