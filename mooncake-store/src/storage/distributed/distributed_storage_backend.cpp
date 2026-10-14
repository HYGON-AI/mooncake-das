#include "storage/distributed/distributed_storage_backend.h"

#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <limits>
#include <mutex>
#include <sstream>

#include "environ.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "thread_pool.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

namespace {

// Upper bound on MOONCAKE_DFS_BATCH_READ_THREADS. Each parallel bucket read
// can hold a staging buffer of up to kMaxMergedIo, so an unbounded thread
// count would blow up both the thread and the memory budget.
constexpr int kMaxBatchReadThreads = 256;
constexpr int kMaxBucketCreateConcurrency = 8;

// Bound temporary memory and I/O latency while still collapsing the common
// BatchAllocate output to one read. Larger batches become a few contiguous
// reads rather than one read per object.
constexpr uint64_t kMaxMergedIo = 4ULL * 1024 * 1024;

struct MergedReadScratch {
    std::vector<char> sink;
    std::vector<iovec> iovs;
    std::vector<iovec> pending;
};

thread_local MergedReadScratch merged_read_scratch;

// Must match the constants in immutable_bucket_allocator.cpp.
constexpr const char* kBucketFilePrefix = "bucket_";
constexpr const char* kBucketDataSuffix = ".data";

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

/**
 * @brief Canonicalize `path` without requiring it to exist yet.
 *
 * `weakly_canonical` collapses `..` and resolves the existing prefix through
 * symlinks, which is what we need to reject descriptor paths pointing outside
 * the configured DFS root.
 */
std::string CanonicalizePath(const std::string& path) {
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(path, ec);
    if (ec) return {};
    return canonical.lexically_normal().string();
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

bool DistributedStorageConfig::ValidateForBucketAllocator() const {
    if (bucket_capacity == 0) {
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
        << ", deferred_free_seconds=" << deferred_free_duration.count()
        << ", eviction_check_interval_seconds="
        << eviction_check_interval.count()
        << ", allocator_type=" << ToString(allocator_type)
        << ", bucket_capacity=" << bucket_capacity
        << ", max_bucket_count=" << max_bucket_count
        << ", ready_bucket_target=" << ready_bucket_target
        << ", bucket_create_concurrency=" << bucket_create_concurrency
        << ", batch_read_threads=" << batch_read_threads
        << ", batch_read_merge_enabled=" << batch_read_merge_enabled
        << ", direct_read_enabled=" << direct_read_enabled;
    return oss.str();
}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter)
    : DistributedStorageBackend(file_storage_config, distributed_config,
                                std::move(fs_adapter), nullptr) {}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter,
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter)
    : StorageBackendInterface(file_storage_config),
      fs_adapter_(std::move(fs_adapter)),
      object_storage_adapter_(std::move(object_storage_adapter)),
      distributed_config_(distributed_config),
      root_dir_(distributed_config.fsdir) {
    CHECK((fs_adapter_ != nullptr) != (object_storage_adapter_ != nullptr))
        << "DistributedStorageBackend: exactly one I/O adapter is required";
    if (object_storage_adapter_) {
        storage_mode_ = DistributedStorageMode::kObjectStorage;
    }
}

DistributedStorageBackend::~DistributedStorageBackend() {
    for (auto& shard : shard_files_) {
        if (shard && fs_adapter_) {
            if (shard->fd >= 0) {
                fs_adapter_->CloseFile(shard->fd);
                shard->fd = -1;
            }
        }
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
    if (initialized_) {
        LOG(WARNING) << "DistributedStorageBackend is already initialized";
        return {};
    }

    if (UsesObjectStorage()) {
        auto init_result = object_storage_adapter_->Init();
        if (!init_result) return init_result;
        initialized_ = true;
        LOG(INFO) << "DistributedStorageBackend initialized, object adapter="
                  << object_storage_adapter_->GetName();
        return {};
    }

    std::error_code ec;
    std::filesystem::create_directories(root_dir_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS root directory " << root_dir_
                   << ": " << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    auto init_result = fs_adapter_->Init(root_dir_);
    if (!init_result) return init_result;

    if (IsBucketMode()) {
        canonical_root_dir_ = CanonicalizePath(root_dir_);
        if (canonical_root_dir_.empty()) {
            LOG(ERROR) << "Failed to canonicalize DFS root directory "
                       << root_dir_;
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        // Bucket data files are created by the master's allocator and opened
        // on demand here, so there is no fixed shard table to preopen.
        if (!distributed_config_.ValidateForBucketAllocator()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (distributed_config_.batch_read_threads > 1) {
            batch_read_pool_ = std::make_unique<ThreadPool>(
                static_cast<size_t>(distributed_config_.batch_read_threads));
        }
        initialized_ = true;
        LOG(INFO) << "DistributedStorageBackend initialized in bucket mode, "
                     "fsdir="
                  << root_dir_ << ", bucket_capacity="
                  << distributed_config_.bucket_capacity;
        return {};
    }

    shard_files_.reserve(distributed_config_.shard_count);
    for (int i = 0; i < distributed_config_.shard_count; ++i) {
        std::string path = root_dir_ + "/dfs_shard_" +
                           DfsGlobalAllocator::FormatShardIdx(
                               i, distributed_config_.shard_count) +
                           ".data";
        auto fd_result = fs_adapter_->OpenFile(path);
        if (!fd_result) {
            LOG(ERROR) << "Failed to open DFS shard " << path << ": "
                       << fd_result.error();
            return tl::make_unexpected(fd_result.error());
        }
        auto shard = std::make_unique<ShardFile>();
        shard->path = std::move(path);
        shard->fd = *fd_result;
        shard_files_.push_back(std::move(shard));
    }

    initialized_ = true;
    return {};
}

tl::expected<std::shared_ptr<DistributedStorageBackend::OpenFileHandle>,
             ErrorCode>
DistributedStorageBackend::GetOrOpenBucket(int64_t bucket_id) {
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        auto it = bucket_id_cache_.find(bucket_id);
        if (it != bucket_id_cache_.end()) return it->second;
    }

    const std::string path = canonical_root_dir_ + "/" + kBucketFilePrefix +
                             ImmutableBucketAllocator::FormatBucketId(bucket_id) +
                             kBucketDataSuffix;
    auto fd_result = fs_adapter_->OpenFile(path);
    if (!fd_result) {
        LOG(ERROR) << "Failed to open DFS bucket file " << path << ": "
                   << fd_result.error();
        return tl::make_unexpected(fd_result.error());
    }

    auto handle = std::make_shared<OpenFileHandle>();
    handle->path = path;
    handle->fd = *fd_result;
    handle->adapter = fs_adapter_.get();

    std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
    // Another thread may have populated the cache while we were opening; keep
    // the winner and let our handle close its own fd on destruction.
    auto [it, inserted] = bucket_id_cache_.emplace(bucket_id, handle);
    (void)inserted;
    return it->second;
}

tl::expected<std::shared_ptr<DistributedStorageBackend::OpenFileHandle>,
             ErrorCode>
DistributedStorageBackend::GetOrOpenBucketDirect(int64_t bucket_id) {
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        auto it = bucket_id_direct_cache_.find(bucket_id);
        if (it != bucket_id_direct_cache_.end()) return it->second;
    }

    const std::string path = canonical_root_dir_ + "/" + kBucketFilePrefix +
                             ImmutableBucketAllocator::FormatBucketId(bucket_id) +
                             kBucketDataSuffix;
    auto fd_result = fs_adapter_->OpenFileDirect(path);
    if (!fd_result) {
        if (fd_result.error() != ErrorCode::NOT_SUPPORTED) {
            LOG(WARNING) << "Failed to open direct read handle for DFS bucket "
                            "file "
                         << path << ": " << fd_result.error();
        }
        return tl::make_unexpected(fd_result.error());
    }

    auto handle = std::make_shared<OpenFileHandle>();
    handle->path = path;
    handle->fd = *fd_result;
    handle->adapter = fs_adapter_.get();

    std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
    // Another thread may have populated the cache while we were opening; keep
    // the winner and let our handle close its own fd on destruction.
    auto [it, inserted] = bucket_id_direct_cache_.emplace(bucket_id, handle);
    (void)inserted;
    return it->second;
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
        return ResolvedTarget{shard.fd, &shard.mutex, false, nullptr};
    }

    // BUCKET mode: the descriptor carries an allocator-chosen path, so it must
    // be validated before it is used to open anything.
    if (descriptor.shard_idx < 0) {
        LOG(ERROR) << "Invalid DFS bucket id " << descriptor.shard_idx
                   << " for key " << key;
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

    auto handle = read_only ? GetOrOpenBucketDirect(descriptor.shard_idx)
                            : GetOrOpenBucket(descriptor.shard_idx);
    if (!handle && read_only) {
        if (handle.error() != ErrorCode::NOT_SUPPORTED) {
            return tl::make_unexpected(handle.error());
        }
        handle = GetOrOpenBucket(descriptor.shard_idx);
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
    if (!initialized_) {
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
    std::vector<tl::expected<void, ErrorCode>> results;

    if (UsesObjectStorage()) {
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::NOT_SUPPORTED));
        return results;
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    // Build validated entry payloads first, including the reserved zero
    // padding, then issue one write per contiguous bounded-size run. SHARD
    // keeps the original one-request path because its allocator does not
    // promise contiguity.
    if (IsBucketMode()) return BatchWriteBucket(requests);
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
        auto target = ResolveTarget(request.descriptor, request.key, false);
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
DistributedStorageBackend::BatchWriteBucket(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));

    struct Prepared {
        size_t index;
        ResolvedTarget target;
        uint64_t offset;
        std::vector<char> payload;
    };
    std::vector<Prepared> prepared;
    prepared.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto target = ResolveTarget(request.descriptor, request.key);
        if (!target) {
            results[i] = tl::make_unexpected(target.error());
            continue;
        }
        uint64_t value_size = 0;
        std::vector<char> payload(
            static_cast<size_t>(request.descriptor.aligned_size), 0);
        size_t payload_offset = 0;
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if ((!slice.ptr && slice.size > 0) ||
                slice.size > request.descriptor.object_size - value_size) {
                invalid = true;
                break;
            }
            if (slice.size) {
                std::memcpy(payload.data() + payload_offset, slice.ptr, slice.size);
                payload_offset += slice.size;
                value_size += slice.size;
            }
        }
        if (invalid || value_size != request.descriptor.object_size) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        prepared.push_back({i, std::move(*target),
                            request.descriptor.offset,
                            std::move(payload)});
    }

    auto write_run = [&](size_t begin, size_t end) {
        size_t pos = begin;
        while (pos < end) {
            size_t stop = pos;
            uint64_t total_size = 0;
            constexpr size_t kMaxIov = 1024;
            while (stop < end && stop - pos < kMaxIov &&
                   (stop == pos || total_size + prepared[stop].payload.size() <=
                                       kMaxMergedIo)) {
                total_size += prepared[stop].payload.size();
                ++stop;
            }

            std::vector<iovec> iovs;
            iovs.reserve(stop - pos);
            for (size_t j = pos; j < stop; ++j) {
                iovs.push_back(
                    {prepared[j].payload.data(), prepared[j].payload.size()});
            }

            uint64_t written = 0;
            size_t iov_index = 0;
            uint64_t iov_consumed = 0;
            ErrorCode error = ErrorCode::OK;
            while (written < total_size) {
                std::vector<iovec> pending;
                pending.reserve(iovs.size() - iov_index);
                pending.push_back(
                    {static_cast<char*>(iovs[iov_index].iov_base) +
                         iov_consumed,
                     iovs[iov_index].iov_len - iov_consumed});
                for (size_t j = iov_index + 1; j < iovs.size(); ++j) {
                    pending.push_back(iovs[j]);
                }
                auto result = fs_adapter_->WriteAt(
                    prepared[pos].target.fd, pending.data(),
                    static_cast<int>(pending.size()),
                    static_cast<int64_t>(prepared[pos].offset + written));
                if (!result) {
                    error = result.error();
                    break;
                }
                if (*result == 0) {
                    error = ErrorCode::FILE_WRITE_FAIL;
                    break;
                }
                uint64_t advanced = *result;
                written += advanced;
                while (advanced != 0 && iov_index < iovs.size()) {
                    const uint64_t available =
                        iovs[iov_index].iov_len - iov_consumed;
                    const uint64_t step =
                        std::min<uint64_t>(advanced, available);
                    iov_consumed += step;
                    advanced -= step;
                    if (iov_consumed == iovs[iov_index].iov_len) {
                        ++iov_index;
                        iov_consumed = 0;
                    }
                }
            }
            if (error == ErrorCode::OK && written == total_size) {
                for (size_t j = pos; j < stop; ++j) {
                    results[prepared[j].index] = {};
                }
            } else {
                if (error == ErrorCode::OK) error = ErrorCode::FILE_WRITE_FAIL;
                for (size_t j = pos; j < stop; ++j) {
                    results[prepared[j].index] = tl::make_unexpected(error);
                }
            }
            pos = stop;
        }
    };
    size_t run = 0;
    for (size_t i = 1; i <= prepared.size(); ++i) {
        bool contiguous =
            i < prepared.size() &&
            prepared[i - 1].target.fd == prepared[i].target.fd &&
            prepared[i - 1].target.mutex == prepared[i].target.mutex &&
            prepared[i].offset ==
                prepared[i - 1].offset + prepared[i - 1].payload.size();
        if (!contiguous) {
            if (run < i) write_run(run, i);
            run = i;
        }
    }
    return results;
}

std::vector<DistributedStorageBackend::ReadTask>
DistributedStorageBackend::PrepareReadTasks(
    const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    std::vector<ReadTask> tasks;
    std::vector<ReadTask> oversized_tasks;
    tasks.reserve(requests.size());
    oversized_tasks.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto target = ResolveTarget(request.descriptor, request.key,
                                    distributed_config_.direct_read_enabled);
        if (!target) {
            results[i] = tl::make_unexpected(target.error());
            continue;
        }

        uint64_t capacity = 0;
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if ((!slice.ptr && slice.size != 0) ||
                slice.size > std::numeric_limits<uint64_t>::max() - capacity) {
                invalid = true;
                break;
            }
            capacity += slice.size;
        }
        if (invalid || capacity < request.descriptor.object_size) continue;

        ReadTask task;
        task.target = std::move(*target);
        task.io_offset = request.descriptor.offset;
        task.total_size = IsBucketMode() ? request.descriptor.aligned_size
                                         : request.descriptor.object_size;
        task.direct_read = distributed_config_.direct_read_enabled &&
                           task.target.direct_read;
        task.entries.push_back({i, request.descriptor.offset});
        const bool merge_candidate =
            IsBucketMode() && distributed_config_.batch_read_merge_enabled &&
            task.total_size <= kMaxMergedIo;
        if (merge_candidate) {
            tasks.push_back(std::move(task));
        } else {
            oversized_tasks.push_back(std::move(task));
        }
    }

    if (!IsBucketMode() || !distributed_config_.batch_read_merge_enabled) {
        tasks.insert(tasks.end(), std::make_move_iterator(oversized_tasks.begin()),
                     std::make_move_iterator(oversized_tasks.end()));
        return tasks;
    }
    std::sort(tasks.begin(), tasks.end(), [](const ReadTask& a,
                                             const ReadTask& b) {
        if (a.target.fd != b.target.fd) {
            return a.target.fd < b.target.fd;
        }
        return a.io_offset < b.io_offset;
    });

    std::vector<ReadTask> merged;
    merged.reserve(tasks.size());
    for (auto& task : tasks) {
        const uint64_t task_end = task.io_offset + task.total_size;
        if (!merged.empty() &&
            merged.back().target.fd == task.target.fd &&
            merged.back().target.mutex == task.target.mutex &&
            merged.back().io_offset + merged.back().total_size ==
                task.io_offset &&
            task_end - merged.back().io_offset <= kMaxMergedIo) {
            merged.back().total_size = task_end - merged.back().io_offset;
            merged.back().merged = true;
            merged.back().entries.insert(merged.back().entries.end(),
                                         task.entries.begin(),
                                         task.entries.end());
            continue;
        }
        merged.push_back(std::move(task));
    }
    merged.insert(merged.end(),
                  std::make_move_iterator(oversized_tasks.begin()),
                  std::make_move_iterator(oversized_tasks.end()));
    return merged;
}

// Dispatches merged bucket tasks to the staging-buffer path; all other tasks
// contain one request and use direct scatter reads into the caller's slices.
void DistributedStorageBackend::ExecuteReadTask(
    const ReadTask& task, const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    if (task.merged || task.entries.size() > 1) {
        ExecuteMergedReadTask(task, requests, results);
    } else {
        ExecuteSingleReadTask(task, requests, results);
    }
}

void DistributedStorageBackend::ExecuteSingleReadTask(
    const ReadTask& task, const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    if (task.entries.empty()) return;
    const auto& entry = task.entries.front();
    const auto& request = requests[entry.request_index];
    // Read the full aligned size (value + padding) in bucket mode so the
    // iovec length is a multiple of the alignment, letting O_DIRECT skip the
    // adapter's staged bounce. The trailing padding lands in the arena slot but
    // is never scattered to the caller.
    const uint64_t read_size = task.total_size;

    const uint64_t alignment = distributed_config_.alignment;
    std::vector<char> sink(alignment == 0 ? 1 : alignment);
    std::vector<iovec> iovs;
    iovs.reserve(request.slices.size() + 1);
    uint64_t collected = 0;
    for (const auto& slice : request.slices) {
        if (collected >= read_size) break;
        const uint64_t size = std::min<uint64_t>(slice.size, read_size - collected);
        if (size != 0) {
            iovs.push_back({slice.ptr, static_cast<size_t>(size)});
            collected += size;
        }
    }
    // If slices don't cover the full aligned range (e.g. a non-session caller
    // that allocated object_size bytes), absorb the trailing padding with the
    // sink so the iovec total matches the on-disk reserved size.
    if (collected < read_size) {
        iovs.push_back({sink.data(), static_cast<size_t>(read_size - collected)});
    }

    constexpr size_t kMaxIovChunk = 1024;
    ErrorCode error = ErrorCode::OK;
    uint64_t done = 0;
    size_t index = 0;
    size_t consumed = 0;
    std::unique_lock<std::mutex> target_lock;
    if (task.target.mutex != nullptr) {
        target_lock = std::unique_lock<std::mutex>(*task.target.mutex);
    }

    while (done < read_size && index < iovs.size()) {
        std::vector<iovec> pending;
        pending.push_back({static_cast<char*>(iovs[index].iov_base) + consumed,
                           iovs[index].iov_len - consumed});
        for (size_t j = index + 1;
             j < iovs.size() && pending.size() < kMaxIovChunk; ++j) {
            pending.push_back(iovs[j]);
        }
        auto read_result = task.direct_read
                               ? fs_adapter_->DirectReadAt(
                                     task.target.fd, pending.data(),
                                     static_cast<int>(pending.size()),
                                     static_cast<int64_t>(entry.value_offset + done))
                               : fs_adapter_->ReadAt(
                                     task.target.fd, pending.data(),
                                     static_cast<int>(pending.size()),
                                     static_cast<int64_t>(entry.value_offset + done));
        if (!read_result) {
            error = read_result.error();
            break;
        }
        if (*read_result == 0) {
            error = ErrorCode::FILE_READ_FAIL;
            break;
        }
        uint64_t advanced = *read_result;
        done += advanced;
        while (advanced != 0 && index < iovs.size()) {
            const size_t available = iovs[index].iov_len - consumed;
            const size_t step = std::min<uint64_t>(advanced, available);
            consumed += step;
            advanced -= step;
            if (consumed == iovs[index].iov_len) {
                ++index;
                consumed = 0;
            }
        }
    }
    if (error == ErrorCode::OK && done != read_size) {
        error = ErrorCode::FILE_READ_FAIL;
    }
    results[entry.request_index] =
        error == ErrorCode::OK ? tl::expected<void, ErrorCode>{}
                               : tl::make_unexpected(error);
}

void DistributedStorageBackend::ExecuteMergedReadTask(
    const ReadTask& task, const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    if (task.entries.empty()) return;
    // Reuse worker-local storage. Determine the full padding size before adding
    // pointers into sink so later growth cannot invalidate existing iovecs.
    auto& scratch = merged_read_scratch;
    uint64_t total_padding = 0;
    for (const auto& entry : task.entries) {
        const auto& request = requests[entry.request_index];
        const uint64_t reserved = IsBucketMode()
                                      ? request.descriptor.aligned_size
                                      : request.descriptor.object_size;
        uint64_t covered = 0;
        for (const auto& slice : request.slices) {
            if (covered >= reserved) break;
            covered += std::min<uint64_t>(slice.size, reserved - covered);
        }
        if (covered < reserved) total_padding += reserved - covered;
    }

    scratch.sink.resize(static_cast<size_t>(total_padding));
    scratch.iovs.clear();
    scratch.iovs.reserve(task.entries.size() * 2);
    size_t sink_offset = 0;
    for (const auto& entry : task.entries) {
        const auto& request = requests[entry.request_index];
        const uint64_t reserved = IsBucketMode()
                                      ? request.descriptor.aligned_size
                                      : request.descriptor.object_size;
        uint64_t collected = 0;
        for (const auto& slice : request.slices) {
            if (collected >= reserved) break;
            const uint64_t size =
                std::min<uint64_t>(slice.size, reserved - collected);
            if (size != 0) {
                scratch.iovs.push_back({slice.ptr, static_cast<size_t>(size)});
                collected += size;
            }
        }
        if (collected < reserved) {
            const size_t padding = static_cast<size_t>(reserved - collected);
            scratch.iovs.push_back({scratch.sink.data() + sink_offset, padding});
            sink_offset += padding;
        }
    }

    constexpr size_t kMaxIovChunk = 1024;
    scratch.pending.clear();
    scratch.pending.reserve(kMaxIovChunk);
    ErrorCode error = ErrorCode::OK;
    uint64_t done = 0;
    size_t index = 0;
    uint64_t iov_consumed = 0;
    while (done < task.total_size && index < scratch.iovs.size()) {
        scratch.pending.clear();
        scratch.pending.push_back(
            {static_cast<char*>(scratch.iovs[index].iov_base) + iov_consumed,
             scratch.iovs[index].iov_len - iov_consumed});
        for (size_t j = index + 1;
             j < scratch.iovs.size() && scratch.pending.size() < kMaxIovChunk;
             ++j) {
            scratch.pending.push_back(scratch.iovs[j]);
        }
        auto read_result = task.direct_read
                               ? fs_adapter_->DirectReadAt(
                                     task.target.fd, scratch.pending.data(),
                                     static_cast<int>(scratch.pending.size()),
                                     static_cast<int64_t>(task.io_offset + done))
                               : fs_adapter_->ReadAt(
                                     task.target.fd, scratch.pending.data(),
                                     static_cast<int>(scratch.pending.size()),
                                     static_cast<int64_t>(task.io_offset + done));
        if (!read_result) {
            error = read_result.error();
            break;
        }
        if (*read_result == 0) {
            error = ErrorCode::FILE_READ_FAIL;
            break;
        }
        uint64_t advanced = *read_result;
        done += advanced;
        while (advanced != 0 && index < scratch.iovs.size()) {
            const uint64_t available =
                scratch.iovs[index].iov_len - iov_consumed;
            const uint64_t step = std::min<uint64_t>(advanced, available);
            iov_consumed += step;
            advanced -= step;
            if (iov_consumed == scratch.iovs[index].iov_len) {
                ++index;
                iov_consumed = 0;
            }
        }
    }
    if (error == ErrorCode::OK && done != task.total_size) {
        error = ErrorCode::FILE_READ_FAIL;
    }
    for (const auto& entry : task.entries) {
        results[entry.request_index] =
            error == ErrorCode::OK ? tl::expected<void, ErrorCode>{}
                                   : tl::make_unexpected(error);
    }
}

void DistributedStorageBackend::ExecuteReadTasks(
    const std::vector<ReadTask>& tasks,
    const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    if (batch_read_pool_ == nullptr || tasks.size() <= 1) {
        for (const auto& task : tasks) {
            ExecuteReadTask(task, requests, results);
        }
        return;
    }

    const size_t worker_count = std::min<size_t>(
        static_cast<size_t>(distributed_config_.batch_read_threads), tasks.size());
    std::atomic<size_t> next_task{0};
    std::mutex completion_mutex;
    std::condition_variable completion_cv;
    size_t pending_workers = 0;
    auto mark_done = [&completion_mutex, &completion_cv, &pending_workers]() {
        std::lock_guard<std::mutex> lock(completion_mutex);
        --pending_workers;
        completion_cv.notify_one();
    };

    auto worker_fn = [this, &tasks, &requests, &results, &mark_done,
                      &next_task]() {
        while (true) {
            const size_t task_index =
                next_task.fetch_add(1, std::memory_order_relaxed);
            if (task_index >= tasks.size()) break;
            try {
                ExecuteReadTask(tasks[task_index], requests, results);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Batch read task failed: " << e.what();
                for (const auto& entry : tasks[task_index].entries) {
                    results[entry.request_index] =
                        tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
            } catch (...) {
                LOG(ERROR) << "Batch read task failed";
                for (const auto& entry : tasks[task_index].entries) {
                    results[entry.request_index] =
                        tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                }
            }
        }
        mark_done();
    };

    for (size_t worker = 0; worker < worker_count; ++worker) {
        {
            std::lock_guard<std::mutex> lock(completion_mutex);
            ++pending_workers;
        }
        try {
            batch_read_pool_->enqueue(worker_fn);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to enqueue batch read worker: " << e.what();
            worker_fn();
        }
    }

    std::unique_lock<std::mutex> lock(completion_mutex);
    completion_cv.wait(lock, [&pending_workers] { return pending_workers == 0; });
}

std::vector<tl::expected<void, ErrorCode>> DistributedStorageBackend::BatchRead(
    const std::vector<DfsReadRequest>& requests) {
    const auto timing_start = std::chrono::steady_clock::now();
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    if (UsesObjectStorage()) {
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::NOT_SUPPORTED));
        return results;
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    if (!IsBucketMode()) return BatchReadShard(requests);

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

tl::expected<void, ErrorCode> DistributedStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
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
    if (!initialized_) {
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
    if (!initialized_) {
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
