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

std::string CanonicalizePath(const std::string& path) {
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(path, ec);
    if (ec) return {};
    return canonical.lexically_normal().string();
}

// Upper bound on MOONCAKE_DFS_BATCH_READ_THREADS. Each parallel bucket read
// can hold a staging buffer of up to kMaxMergedIo, so an unbounded thread
// count would blow up both the thread and the memory budget.
constexpr int kMaxBatchReadThreads = 256;
constexpr int kMaxBucketCreateConcurrency = 8;

// Bound temporary memory and I/O latency while still collapsing the common
// BatchAllocate output to one read. Larger batches become a few contiguous
// reads rather than one read per object.
constexpr uint64_t kMaxMergedIo = 4ULL * 1024 * 1024;
constexpr char kDfsHealthProbePayload[] = "mooncake-dfs-health-v1\n";

struct MergedReadScratch {
    std::vector<char> sink;
    std::vector<iovec> iovs;
    std::vector<iovec> pending;
};

thread_local MergedReadScratch merged_read_scratch;


}  // namespace

using l4::kBucketFilePrefix;
using l4::kBucketDataSuffix;

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

tl::expected<void, ErrorCode> DistributedStorageBackend::EnsureInitialized() {
    if (initialized_.load(std::memory_order_acquire)) {
        if (UsesObjectStorage()) return {};
        return ValidateRootIdentity();
    }
    return Init();
}

tl::expected<void, ErrorCode>
DistributedStorageBackend::TryEnsureInitialized() {
    if (initialized_.load(std::memory_order_acquire)) {
        if (UsesObjectStorage()) return {};
        return ValidateRootIdentity();
    }
    std::unique_lock<std::mutex> lock(init_mutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    return InitLocked();
}

tl::expected<void, ErrorCode>
DistributedStorageBackend::ValidateRootIdentity() const {
    if (UsesObjectStorage()) return {};
    if (expected_root_identity_.empty() ||
        expected_root_identity_.size() > kMaxDfsRootIdentitySize ||
        !fs_adapter_) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    std::vector<char> contents(expected_root_identity_.size() + 1);
    const std::string marker_path =
        root_dir_ + "/" + kDfsRootIdentityFileName;
    auto read_result = fs_adapter_->ReadFile(
        marker_path, contents.data(), contents.size());
    if (!read_result || *read_result != expected_root_identity_.size() ||
        std::memcmp(contents.data(), expected_root_identity_.data(),
                    expected_root_identity_.size()) != 0) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    return {};
}

void DistributedStorageBackend::ResetFailedInitializationLocked() {
    for (auto& shard : shard_files_) {
        if (shard && fs_adapter_) {
            const int write_fd = shard->write_fd.load();
            if (write_fd >= 0) {
                (void)fs_adapter_->CloseFile(write_fd);
            }
            if (shard->read_fd >= 0 && shard->read_fd != write_fd) {
                (void)fs_adapter_->CloseFile(shard->read_fd);
            }
            shard->read_fd = -1;
            shard->write_fd.store(-1);
        }
    }
    shard_files_.clear();
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        bucket_id_cache_.clear();
        bucket_id_direct_cache_.clear();
    }
    batch_read_pool_.reset();
    canonical_root_dir_.clear();
    if (fs_adapter_) (void)fs_adapter_->Shutdown();
    initialized_.store(false, std::memory_order_release);
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ProbeRead() {
    auto initialized = TryEnsureInitialized();
    if (!initialized) return initialized;
    if (UsesObjectStorage()) return {};
    void* raw = nullptr;
    if (::posix_memalign(&raw, kDfsReadProbeBlockSize, kDfsReadProbeSize) !=
        0) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    std::unique_ptr<void, decltype(&::free)> buffer(raw, &::free);

    for (size_t probe_index = 0;
         probe_index < kDfsReadProbeDirectoryNames.size(); ++probe_index) {
        const std::string probe_path =
            root_dir_ + "/" +
            std::string(kDfsReadProbeDirectoryNames[probe_index]) + "/" +
            kDfsReadProbeFileName;
        auto size = fs_adapter_->GetFileSize(probe_path);
        if (!size) return tl::make_unexpected(size.error());
        if (*size != kDfsReadProbeSize) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }

        // Open and close on every probe. Strict open has no buffered fallback
        // and PosixFsAdapter uses O_RDONLY|O_DIRECT|O_CLOEXEC|O_NOFOLLOW.
        auto fd = fs_adapter_->OpenFileDirectStrict(probe_path);
        if (!fd) return tl::make_unexpected(fd.error());
        size_t total_read = 0;
        while (total_read < kDfsReadProbeSize) {
            iovec iov{static_cast<char*>(buffer.get()) + total_read,
                      kDfsReadProbeSize - total_read};
            auto read = fs_adapter_->DirectReadAt(
                *fd, &iov, 1, static_cast<int64_t>(total_read));
            if (!read || *read == 0) {
                const auto error =
                    read ? ErrorCode::FILE_READ_FAIL : read.error();
                (void)fs_adapter_->CloseFile(*fd);
                return tl::make_unexpected(error);
            }
            total_read += *read;
        }
        const auto close = fs_adapter_->CloseFile(*fd);
        if (!close) return tl::make_unexpected(close.error());

        const auto expected =
            BuildDfsReadProbeContents(expected_root_identity_, probe_index);
        if (std::memcmp(buffer.get(), expected.data(), kDfsReadProbeSize) != 0) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    SetDfsReadTimedOut(false);
    return {};
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ProbeWrite() {
    auto initialized = TryEnsureInitialized();
    if (!initialized) return initialized;
    if (UsesObjectStorage()) return {};

    std::lock_guard probe_lock(health_probe_write_mutex_);
    // Use a new path each round: deleting the previous file first could free
    // just enough space for the probe to pass on an otherwise full filesystem.
    const std::string new_probe_path =
        health_probe_path_prefix_ + "_" +
        std::to_string(++health_probe_generation_);
    auto write_result = fs_adapter_->WriteFile(
        new_probe_path,
        std::span<const char>(kDfsHealthProbePayload,
                              sizeof(kDfsHealthProbePayload) - 1));
    if (!write_result) {
        (void)fs_adapter_->DeleteFile(new_probe_path);
        return tl::make_unexpected(write_result.error());
    }
    if (*write_result != sizeof(kDfsHealthProbePayload) - 1) {
        (void)fs_adapter_->DeleteFile(new_probe_path);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    auto sync_result = fs_adapter_->SyncFile(new_probe_path);
    if (!sync_result) {
        (void)fs_adapter_->DeleteFile(new_probe_path);
        return sync_result;
    }

    if (!active_health_probe_path_.empty()) {
        auto delete_result = fs_adapter_->DeleteFile(active_health_probe_path_);
        if (!delete_result &&
            delete_result.error() != ErrorCode::FILE_NOT_FOUND) {
            (void)fs_adapter_->DeleteFile(new_probe_path);
            return tl::make_unexpected(delete_result.error());
        }
    }
    active_health_probe_path_ = new_probe_path;

    if (!IsBucketMode()) {
        std::vector<int> new_write_fds;
        new_write_fds.reserve(shard_files_.size());
        for (const auto& shard : shard_files_) {
            if (shard->write_fd.load(std::memory_order_acquire) >= 0) {
                new_write_fds.push_back(-1);
                continue;
            }
            auto fd = fs_adapter_->OpenFile(shard->path);
            if (!fd) {
                for (int opened_fd : new_write_fds) {
                    if (opened_fd >= 0) (void)fs_adapter_->CloseFile(opened_fd);
                }
                return tl::make_unexpected(fd.error());
            }
            new_write_fds.push_back(*fd);
        }
        for (size_t i = 0; i < shard_files_.size(); ++i) {
            if (new_write_fds[i] >= 0) {
                shard_files_[i]->write_fd.store(new_write_fds[i],
                                                std::memory_order_release);
            }
        }
    }
    return {};
}

tl::expected<std::shared_ptr<DistributedStorageBackend::OpenFileHandle>,
             ErrorCode>
DistributedStorageBackend::GetOrOpenBucket(int64_t bucket_id,
                                           const UUID& epoch) {
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        auto it = bucket_id_cache_.find(bucket_id);
        if (it != bucket_id_cache_.end()) return it->second;
    }

    const std::string path = canonical_root_dir_ + "/" + kBucketFilePrefix +
                             ImmutableBucketAllocator::FormatBucketId(bucket_id) +
                             kBucketDataSuffix;
    auto fd_result = fs_adapter_->OpenFileReadWriteExisting(path);
    if (!fd_result) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to open DFS bucket file " << path << ": "
                       << fd_result.error()
                       << ", suppressed=" << *suppressed;
        }
        return tl::make_unexpected(fd_result.error());
    }

    auto handle = std::make_shared<OpenFileHandle>();
    handle->path = path;
    handle->fd = *fd_result;
    handle->adapter = fs_adapter_.get();

    // A queued operation may still carry the old epoch before this client has
    // observed a restart. Confirm AFTER opening: an old location must never
    // acquire a newly recreated bucket file. Cached handles need no RPC.
    if (master_epoch_provider_) {
        auto current_epoch = master_epoch_provider_();
        if (!current_epoch) return tl::make_unexpected(current_epoch.error());
        if (*current_epoch != epoch) {
            return tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE);
        }
    }

    std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
    // Another thread may have populated the cache while we were opening; keep
    // the winner and let our handle close its own fd on destruction.
    auto [it, inserted] = bucket_id_cache_.emplace(bucket_id, handle);
    (void)inserted;
    return it->second;
}

tl::expected<bool, ErrorCode>
DistributedStorageBackend::CleanupBucketFd(int64_t bucket_id) {
    if (!IsBucketMode() || bucket_id < 0) return false;

    std::shared_ptr<OpenFileHandle> regular_handle;
    std::shared_ptr<OpenFileHandle> direct_handle;
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        auto regular_it = bucket_id_cache_.find(bucket_id);
        if (regular_it != bucket_id_cache_.end()) regular_handle = regular_it->second;
        auto direct_it = bucket_id_direct_cache_.find(bucket_id);
        if (direct_it != bucket_id_direct_cache_.end()) direct_handle = direct_it->second;
    }
    if (!regular_handle && !direct_handle) return false;

    const auto& handle = regular_handle ? regular_handle : direct_handle;
    struct stat path_stat {};
    if (::stat(handle->path.c_str(), &path_stat) == 0) return false;
    if (errno != ENOENT && errno != ESTALE) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
    bool erased = false;
    auto regular_it = bucket_id_cache_.find(bucket_id);
    if (regular_it != bucket_id_cache_.end() &&
        regular_it->second == regular_handle) {
        bucket_id_cache_.erase(regular_it);
        erased = true;
    }
    auto direct_it = bucket_id_direct_cache_.find(bucket_id);
    if (direct_it != bucket_id_direct_cache_.end() &&
        direct_it->second == direct_handle) {
        bucket_id_direct_cache_.erase(direct_it);
        erased = true;
    }
    return erased;
}

tl::expected<std::shared_lock<std::shared_mutex>, ErrorCode>
DistributedStorageBackend::AcquireBucketEpoch(const UUID& epoch) {
    std::shared_lock read_lock(bucket_epoch_mutex_);
    if (epoch == bucket_epoch_) {
        if (epoch == UUID{} && master_epoch_provider_) {
            return tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE);
        }
        return read_lock;
    }
    if (!master_epoch_provider_ || epoch == UUID{} ||
        retired_bucket_epochs_.contains(epoch)) {
        return tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE);
    }
    read_lock.unlock();

    // Serialize confirmations without excluding current-epoch cache hits.
    // Cold opens do not take this mutex: they already hold a shared I/O guard
    // that a confirmed transition must wait for before clearing the caches.
    std::unique_lock confirmation_lock(bucket_epoch_confirmation_mutex_);
    read_lock.lock();
    if (epoch == bucket_epoch_) return read_lock;
    if (retired_bucket_epochs_.contains(epoch)) {
        return tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE);
    }
    read_lock.unlock();
    if (bucket_epoch_confirmation_error_ &&
        std::chrono::steady_clock::now() <
            bucket_epoch_confirmation_retry_after_) {
        return tl::make_unexpected(*bucket_epoch_confirmation_error_);
    }
    // A delayed response is not evidence of a newer master: UUIDs have no
    // order. Network confirmation must not hold the exclusive I/O lock.
    auto current_epoch = master_epoch_provider_();
    if (!current_epoch) {
        // Waiting confirmations share this failure instead of each consuming
        // another RPC timeout while the master is unavailable.
        bucket_epoch_confirmation_error_ = current_epoch.error();
        bucket_epoch_confirmation_retry_after_ =
            std::chrono::steady_clock::now() + std::chrono::seconds(1);
        return tl::make_unexpected(current_epoch.error());
    }
    bucket_epoch_confirmation_error_.reset();
    if (*current_epoch != epoch) {
        return tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE);
    }
    {
        std::unique_lock write_lock(bucket_epoch_mutex_);
        if (retired_bucket_epochs_.contains(epoch)) {
            return tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE);
        }
        auto invalidated = InvalidateAllBucketFds();
        if (!invalidated) return tl::make_unexpected(invalidated.error());
        if (bucket_epoch_ != UUID{})
            retired_bucket_epochs_.insert(bucket_epoch_);
        bucket_epoch_ = epoch;
        LOG(INFO) << "Invalidated " << *invalidated
                  << " DFS bucket handles before using a changed Master epoch";
    }
    read_lock.lock();
    return read_lock;
}

tl::expected<size_t, ErrorCode>
DistributedStorageBackend::InvalidateAllBucketFds() {
    if (!IsBucketMode()) return size_t{0};

    std::vector<std::shared_ptr<OpenFileHandle>> handles;
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        handles.reserve(bucket_id_cache_.size() +
                        bucket_id_direct_cache_.size());
        for (auto& [bucket_id, handle] : bucket_id_cache_) {
            handles.push_back(std::move(handle));
        }
        for (auto& [bucket_id, handle] : bucket_id_direct_cache_) {
            handles.push_back(std::move(handle));
        }
        bucket_id_cache_.clear();
        bucket_id_direct_cache_.clear();
    }
    return handles.size();
}

tl::expected<size_t, ErrorCode>
DistributedStorageBackend::CleanupStaleBucketFds(
    const std::vector<int64_t>& active_bucket_ids) {
    if (!IsBucketMode()) return size_t{0};

    std::unordered_set<int64_t> active_ids(active_bucket_ids.begin(),
                                           active_bucket_ids.end());
    std::vector<int64_t> bucket_ids;
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        bucket_ids.reserve(bucket_id_cache_.size() + bucket_id_direct_cache_.size());
        for (const auto& [bucket_id, handle] : bucket_id_cache_) {
            if (!active_ids.contains(bucket_id)) bucket_ids.push_back(bucket_id);
        }
        for (const auto& [bucket_id, handle] : bucket_id_direct_cache_) {
            if (!active_ids.contains(bucket_id)) bucket_ids.push_back(bucket_id);
        }
    }
    std::sort(bucket_ids.begin(), bucket_ids.end());
    bucket_ids.erase(std::unique(bucket_ids.begin(), bucket_ids.end()),
                     bucket_ids.end());
    if (bucket_ids.empty()) return size_t{0};

    std::ostringstream bucket_id_list;
    for (size_t i = 0; i < bucket_ids.size(); ++i) {
        if (i != 0) bucket_id_list << ',';
        bucket_id_list << bucket_ids[i];
    }
    LOG(INFO) << "DFS bucket FD cleanup candidates, size=" << bucket_ids.size()
              << ", bucket_ids=" << bucket_id_list.str();

    size_t cleaned = 0;
    std::vector<int64_t> failed_bucket_ids;
    for (int64_t bucket_id : bucket_ids) {
        auto result = CleanupBucketFd(bucket_id);
        if (!result) {
            failed_bucket_ids.push_back(bucket_id);
            continue;
        }
        if (*result) ++cleaned;
    }
    if (!failed_bucket_ids.empty()) {
        std::ostringstream failed_id_list;
        for (size_t i = 0; i < failed_bucket_ids.size(); ++i) {
            if (i != 0) failed_id_list << ',';
            failed_id_list << failed_bucket_ids[i];
        }
        LOG(WARNING) << "Failed to inspect DFS bucket FDs, size="
                     << failed_bucket_ids.size()
                     << ", bucket_ids=" << failed_id_list.str();
    }
    return cleaned;
}

tl::expected<std::shared_ptr<DistributedStorageBackend::OpenFileHandle>,
             ErrorCode>
DistributedStorageBackend::GetOrOpenBucketDirect(int64_t bucket_id,
                                                 const UUID& epoch) {
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

    // A queued operation may still carry the old epoch before this client has
    // observed a restart. Confirm AFTER opening: an old location must never
    // acquire a newly recreated bucket file. Cached handles need no RPC.
    if (master_epoch_provider_) {
        auto current_epoch = master_epoch_provider_();
        if (!current_epoch) return tl::make_unexpected(current_epoch.error());
        if (*current_epoch != epoch) {
            return tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE);
        }
    }

    std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
    // Another thread may have populated the cache while we were opening; keep
    // the winner and let our handle close its own fd on destruction.
    auto [it, inserted] = bucket_id_direct_cache_.emplace(bucket_id, handle);
    (void)inserted;
    return it->second;
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
        auto target = ResolveTarget(
            request.descriptor, request.key,
            !IsBucketMode() || distributed_config_.direct_read_enabled);
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

ErrorCode DistributedStorageBackend::AcquireInflightReadUntil(
    uint64_t bytes, std::chrono::steady_clock::time_point deadline) {
    if (bytes > distributed_config_.batch_get_max_inflight_bytes) {
        return ErrorCode::DFS_BATCH_GET_ADMISSION_TIMEOUT;
    }

    std::unique_lock lock(inflight_read_mutex_);
    const auto capacity_available = [this, bytes] {
        return inflight_read_batches_ <
                   distributed_config_.batch_get_max_inflight_batches &&
               inflight_read_bytes_ <=
                   distributed_config_.batch_get_max_inflight_bytes - bytes;
    };
    const auto capacity_available_or_dfs_timed_out =
        [this, &capacity_available] {
            return capacity_available() ||
                   dfs_read_timed_out_.load(std::memory_order_acquire);
        };
    while (true) {
        if (dfs_read_timed_out_.load(std::memory_order_acquire)) {
            return ErrorCode::DFS_BATCH_GET_TIMEOUT;
        }
        if (capacity_available()) {
            ++inflight_read_batches_;
            inflight_read_bytes_ += bytes;
            return ErrorCode::OK;
        }
        if (deadline == std::chrono::steady_clock::time_point::max()) {
            inflight_read_cv_.wait(lock,
                                   capacity_available_or_dfs_timed_out);
        } else if (!inflight_read_cv_.wait_until(
                       lock, deadline, capacity_available_or_dfs_timed_out)) {
            return ErrorCode::DFS_BATCH_GET_ADMISSION_TIMEOUT;
        }
    }
}

void DistributedStorageBackend::ReleaseInflightRead(uint64_t bytes) {
    {
        std::lock_guard lock(inflight_read_mutex_);
        CHECK_GT(inflight_read_batches_, 0);
        CHECK_GE(inflight_read_bytes_, bytes);
        --inflight_read_batches_;
        inflight_read_bytes_ -= bytes;
    }
    inflight_read_cv_.notify_all();
}

void DistributedStorageBackend::SetDfsReadTimedOut(bool timed_out) {
    {
        // Serialize latch changes with admission so a request cannot acquire a
        // slot between observing the old latch value and incrementing the
        // in-flight counters.
        std::lock_guard lock(inflight_read_mutex_);
        dfs_read_timed_out_.store(timed_out, std::memory_order_release);
    }
    inflight_read_cv_.notify_all();
}

tl::expected<void, ErrorCode>
DistributedStorageBackend::InitBucketModeLocked() {
    canonical_root_dir_ = CanonicalizePath(root_dir_);
    if (canonical_root_dir_.empty()) {
        LOG(ERROR) << "Failed to canonicalize DFS root directory "
                   << root_dir_;
        ResetFailedInitializationLocked();
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    // Bucket data files are created by the master's allocator and opened
    // on demand here, so there is no fixed shard table to preopen.
    if (!distributed_config_.ValidateForBucketAllocator()) {
        ResetFailedInitializationLocked();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (distributed_config_.batch_read_threads > 1) {
        batch_read_pool_ = std::make_unique<ThreadPool>(
            static_cast<size_t>(distributed_config_.batch_read_threads));
    }
    initialized_.store(true, std::memory_order_release);
    LOG(INFO) << "DistributedStorageBackend initialized in bucket mode, "
                 "fsdir="
              << root_dir_ << ", bucket_capacity="
              << distributed_config_.bucket_capacity;
    return {};
}

}  // namespace mooncake
