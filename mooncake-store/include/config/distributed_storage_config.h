#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "storage/distributed/dfs_allocator_interface.h"
#include "storage/distributed/dfs_io_timeout.h"

namespace mooncake {

struct DistributedStorageConfig {
    std::string fsdir = "/mnt/3fs/mooncake";
    std::string fs_adapter_type = "hf3fs";
    bool enable_health_check = false;
    int shard_count = 64;
    uint64_t shard_capacity = 4ULL * 1024 * 1024 * 1024;
    uint64_t alignment = 4096;
    bool single_tenant = true;
    bool eviction_enabled = true;
    double eviction_high_watermark = 0.9;
    double eviction_low_watermark = 0.7;
    // BUCKET mode only: reclaim sealed buckets whose live utilization is below
    // this threshold. A value of 0 disables low-utilization cleanup.
    double low_utilization_threshold = 0.05;
    // Maximum number of low-utilization buckets reclaimed per maintenance tick.
    int low_utilization_evictions_per_tick = 1;
    // Maximum number of low-utilization candidates inspected per maintenance
    // tick. This bounds work when candidates are rejected by the Master.
    int low_utilization_eviction_attempts_per_tick = 8;
    std::chrono::seconds deferred_free_duration{30};
    std::chrono::seconds eviction_check_interval{5};

    // Which space-management strategy the master uses. SHARD is the default so
    // existing deployments are unaffected.
    DfsAllocatorType allocator_type = DfsAllocatorType::SHARD;
    // BUCKET mode only: size of each append-only bucket data file.
    uint64_t bucket_capacity = 256ULL * 1024 * 1024;
    // When false, retain old bucket files without recovering their replicas.
    // max_bucket_count then budgets only buckets created by this process.
    bool bucket_recovery_enabled = false;
    // BUCKET mode only: upper bound on live buckets. Also the fixed
    // denominator for eviction watermarks, so it must be > 0.
    int64_t max_bucket_count = 256;
    // BUCKET mode only: desired number of preallocated buckets kept ready for
    // the next rollover, capped by the available bucket count. Zero disables
    // background precreation.
    int ready_bucket_target = 4;
    // BUCKET mode only: maximum number of concurrent bucket preallocations.
    int bucket_create_concurrency = 2;
    // BUCKET mode only: number of threads for parallel batch reads across
    // buckets. Set to 1 to disable parallelism. Must be in
    // [1, kMaxBatchReadThreads].
    int batch_read_threads = 128;
    // When true, BatchRead keeps the legacy flow that groups reads by bucket
    // and merges contiguous entries. When false (default), every key is read
    // directly on its own with batch_read_threads concurrency.
    bool batch_read_merge_enabled = false;
    // When true (default), BatchRead reads through dedicated direct handles
    // (page-cache-bypassing where the adapter supports it, e.g. O_DIRECT on
    // POSIX). When false, reads go through the regular cached handles.
    bool direct_read_enabled = true;
    // Baseline for the size-scaled DFS BatchGet deadline. Zero disables it.
    uint64_t batch_get_timeout_ms = 10000;
    // Fixed budget added after size scaling. Shared with BatchPut through
    // MOONCAKE_DFS_BATCH_TIMEOUT_FIXED_MS.
    uint64_t batch_timeout_fixed_ms = kDefaultDfsBatchTimeoutFixedMs;
    // Bound DFS reads whose filesystem I/O has not actually completed. A
    // timeout keeps both charges until the background read really returns.
    uint64_t batch_get_max_inflight_batches = 8;
    uint64_t batch_get_max_inflight_bytes = 8ULL * 1024 * 1024 * 1024;
    // Cleared by FromEnvironment() when MOONCAKE_DFS_ALLOCATOR_TYPE names an
    // unknown allocator, so the master can reject the configuration instead of
    // silently defaulting to SHARD.
    bool allocator_type_valid = true;

    std::string object_storage_config_path;

    bool UsesObjectStorage() const {
        return fs_adapter_type == "oss" || fs_adapter_type == "s3" ||
               fs_adapter_type == "kvcs-lowlevel";
    }
    bool UsesKvcs() const { return fs_adapter_type == "kvcs-lowlevel"; }

    bool Validate() const;
    bool ValidateForAllocator() const;
    bool ValidateForBucketAllocator() const;
    static bool IsReplicaEnabledFromEnvironment();
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

}  // namespace mooncake
