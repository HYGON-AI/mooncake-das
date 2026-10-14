#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "fs_adapter.h"
#include "replica.h"
#include "storage/distributed/object_storage_adapter.h"
#include "storage/distributed/dfs_allocator_interface.h"
#include "storage_backend.h"

namespace mooncake {

// Filesystem mode is the descriptor-based shard/offset DFS data path. Object
// storage remains a separate, logical-key-oriented I/O mode.
enum class DistributedStorageMode {
    kFileSystem,
    kObjectStorage,
};
class ThreadPool;

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
    std::chrono::seconds deferred_free_duration{30};
    std::chrono::seconds eviction_check_interval{5};

    // Which space-management strategy the master uses. SHARD is the default so
    // existing deployments are unaffected.
    DfsAllocatorType allocator_type = DfsAllocatorType::SHARD;
    // BUCKET mode only: size of each append-only bucket data file.
    uint64_t bucket_capacity = 256ULL * 1024 * 1024;
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
    // Cleared by FromEnvironment() when MOONCAKE_DFS_ALLOCATOR_TYPE names an
    // unknown allocator, so the master can reject the configuration instead of
    // silently defaulting to SHARD.
    bool allocator_type_valid = true;

    bool Validate() const;
    bool ValidateForAllocator() const;
    bool ValidateForBucketAllocator() const;
    static bool IsReplicaEnabledFromEnvironment();
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

struct DfsWriteRequest {
    std::string key;
    DistributedFSDescriptor descriptor;
    std::vector<Slice> slices;
};

struct DfsReadRequest {
    std::string key;
    DistributedFSDescriptor descriptor;
    std::vector<Slice> slices;
};

/**
 * @brief Distributed filesystem and object storage backend.
 *
 * Uses a FileSystemAdapter for descriptor-based DFS reads and writes, or an
 * ObjectStorageAdapter for whole-object offload operations. Does not handle
 * eviction.
 */
class DistributedStorageBackend : public StorageBackendInterface {
   public:
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter);
    ~DistributedStorageBackend() override;

    // Exactly one adapter must be non-null.
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter,
        std::unique_ptr<ObjectStorageAdapter> object_storage_adapter);

    DistributedStorageMode GetStorageMode() const { return storage_mode_; }

    bool UsesObjectStorage() const {
        return storage_mode_ == DistributedStorageMode::kObjectStorage;
    }

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

    std::vector<tl::expected<void, ErrorCode>> BatchWrite(
        const std::vector<DfsWriteRequest>& requests);

    std::vector<tl::expected<void, ErrorCode>> BatchRead(
        const std::vector<DfsReadRequest>& requests);

    // Key-only storage backend operations cannot safely address DFS objects;
    // callers must use BatchRead/BatchWrite with request-scoped descriptors.
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

    /**
     * @brief Which DFS space-management mode this backend was configured for.
     *
     * The client uses this to choose between the synchronous SHARD write path
     * and the asynchronous BUCKET one.
     */
    DfsAllocatorType GetAllocatorType() const {
        return distributed_config_.allocator_type;
    }

   private:
    struct ShardFile {
        std::string path;
        int fd = -1;
        std::mutex mutex;
    };

    // BUCKET mode opens bucket data files on demand and caches the handles.
    // Handles are shared_ptr so an in-flight read/write keeps the fd alive even
    // if the cache entry is dropped (e.g. after the bucket is evicted). The
    // allocator assigns each object a unique offset, so concurrent access to a
    // bucket file never overlaps and needs no lock.
    struct OpenFileHandle {
        std::string path;
        int fd = -1;
        FileSystemAdapter* adapter = nullptr;

        ~OpenFileHandle() {
            if (fd >= 0 && adapter != nullptr) {
                (void)adapter->CloseFile(fd);
                fd = -1;
            }
        }
    };

    bool IsBucketMode() const {
        return distributed_config_.allocator_type == DfsAllocatorType::BUCKET;
    }

    /**
     * @brief A validated, ready-to-use target for one DFS request.
     *
     * `fd` and `mutex` are borrowed: in SHARD mode they belong to the
     * long-lived ShardFile, in BUCKET mode to `keepalive`, whose shared_ptr
     * guarantees the fd stays open for the duration of the I/O even if the
     * cache entry is dropped concurrently. `direct_read` records whether `fd`
     * is a page-cache-bypassing direct handle so callers can pick the right
     * read path; in BUCKET mode `mutex` is null because the allocator assigns
     * each object a non-overlapping offset.
     */
    struct ResolvedTarget {
        int fd = -1;
        std::mutex* mutex = nullptr;
        bool direct_read = false;
        std::shared_ptr<OpenFileHandle> keepalive;
    };

    /**
     * @brief Resolve `descriptor` to a validated I/O target.
     *
     * SHARD mode validates against the fixed shard table. BUCKET mode
     * canonicalizes the descriptor path, verifies it names the expected bucket
     * data file under the configured DFS root, and then opens/caches it.
     */
    tl::expected<ResolvedTarget, ErrorCode> ResolveTarget(
        const DistributedFSDescriptor& descriptor, const std::string& key,
        bool read_only = false);

    std::vector<tl::expected<void, ErrorCode>> BatchWriteShard(
        const std::vector<DfsWriteRequest>& requests);

    std::vector<tl::expected<void, ErrorCode>> BatchReadShard(
        const std::vector<DfsReadRequest>& requests);

    std::vector<tl::expected<void, ErrorCode>> BatchWriteBucket(
        const std::vector<DfsWriteRequest>& requests);

    tl::expected<std::shared_ptr<OpenFileHandle>, ErrorCode> GetOrOpenBucket(
        int64_t bucket_id);

    /**
     * @brief Open/cache the direct (page-cache-bypassing) read handle for a
     * bucket data file. Returns NOT_SUPPORTED when the adapter has no direct
     * read path; callers then fall back to the regular handle.
     */
    tl::expected<std::shared_ptr<OpenFileHandle>, ErrorCode>
    GetOrOpenBucketDirect(int64_t bucket_id);

    /**
     * @brief One contiguous read scheduled as a single task.
     *
     * `entries` references the requests covered by this task. For direct,
     * unmerged reads it contains one entry; for merged bucket reads it may
     * contain several entries that share one contiguous on-disk run.
     */
    struct ReadEntry {
        size_t request_index = 0;
        uint64_t value_offset = 0;
    };

    struct ReadTask {
        ResolvedTarget target;
        uint64_t io_offset = 0;
        uint64_t total_size = 0;
        bool merged = false;
        bool direct_read = false;
        std::vector<ReadEntry> entries;
    };

    std::vector<ReadTask> PrepareReadTasks(
        const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteReadTask(const ReadTask& task,
                         const std::vector<DfsReadRequest>& requests,
                         std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteSingleReadTask(
        const ReadTask& task, const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteMergedReadTask(
        const ReadTask& task, const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteReadTasks(
        const std::vector<ReadTask>& tasks,
        const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter_;
    DistributedStorageConfig distributed_config_;
    std::string root_dir_;
    // Canonical form of root_dir_, used to reject descriptor paths that try to
    // escape the configured DFS root.
    std::string canonical_root_dir_;
    std::vector<std::unique_ptr<ShardFile>> shard_files_;
    DistributedStorageMode storage_mode_ = DistributedStorageMode::kFileSystem;

    mutable std::mutex bucket_cache_mutex_;
    // bucket id -> handle. The int key replaces a string-keyed lookup so the
    // common PrepareReadTasks path avoids a hash + lock per request.
    // Guarded by bucket_cache_mutex_.
    std::unordered_map<int64_t, std::shared_ptr<OpenFileHandle>>
        bucket_id_cache_;
    std::unordered_map<int64_t, std::shared_ptr<OpenFileHandle>>
        bucket_id_direct_cache_;
    std::unique_ptr<ThreadPool> batch_read_pool_;

    bool initialized_ = false;
};

}  // namespace mooncake
