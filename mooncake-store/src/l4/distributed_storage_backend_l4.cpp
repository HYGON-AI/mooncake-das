#include "l4/distributed_storage_internal.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/dfs_log_limiter.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <exception>
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
#include "bool_parser.h"
#include "ext/dfe_loader.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/dfs_io_timeout.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/dfs_root_identity.h"
#include "storage/distributed/dfs_read_probe.h"
#include "storage/distributed/shard_allocator.h"
#include "thread_pool.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

namespace {

// Owns a raw fd for the duration of a cold open so an exception or early
// return before the fd is handed to its cached OpenFileHandle cannot leak it.
struct FdGuard {
    FileSystemAdapter* adapter = nullptr;
    int fd = -1;
    ~FdGuard() {
        if (fd >= 0 && adapter != nullptr) {
            (void)adapter->CloseFile(fd);
        }
    }
    int release() {
        const int handed_off = fd;
        fd = -1;
        return handed_off;
    }
};

std::string CanonicalizePath(const std::string& path) {
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(path, ec);
    if (ec) return {};
    return canonical.lexically_normal().string();
}

constexpr char kDfsHealthProbePayload[] = "mooncake-dfs-health-v1\n";

// The private data-plane extension owns the read merge algorithm. This helper
// only decides whether the current configuration may use it: the provider
// issues raw positional I/O, so it is valid only for the POSIX adapter.
bool DfeDataPlaneAvailable(const DistributedStorageConfig& config) {
    return config.fs_adapter_type == "posix" &&
           mooncake::ext::DfeProvider::Instance().available();
}

// Only shape/availability rejections are worth redoing on the baseline path.
// A real I/O error is final: retrying it would repeat the same failed syscall
// for every object in the batch.
bool ExtStatusNeedsBaselineFallback(int32_t status) {
    return status == MOONCAKE_EXT_STATUS_INVALID_ARGUMENT ||
           status == MOONCAKE_EXT_STATUS_UNAVAILABLE;
}

ErrorCode MapExtReadFailure(int32_t status, int32_t error_code) {
    switch (status) {
        case MOONCAKE_EXT_STATUS_NO_SPACE:
            return ErrorCode::NO_AVAILABLE_HANDLE;
        case MOONCAKE_EXT_STATUS_NOT_SUPPORTED:
            return ErrorCode::NOT_SUPPORTED;
        case MOONCAKE_EXT_STATUS_IO_ERROR:
            // The provider reports the errno of the failing syscall, so the
            // baseline mapping for a missing file still applies.
            return error_code == ENOENT ? ErrorCode::FILE_NOT_FOUND
                                        : ErrorCode::FILE_READ_FAIL;
        case MOONCAKE_EXT_STATUS_DISABLED:
            return ErrorCode::NOT_SUPPORTED;
        default:
            return ErrorCode::FILE_READ_FAIL;
    }
}

ErrorCode MapExtWriteFailure(int32_t status, int32_t error_code) {
    (void)error_code;
    switch (status) {
        case MOONCAKE_EXT_STATUS_NO_SPACE:
            return ErrorCode::NO_AVAILABLE_HANDLE;
        case MOONCAKE_EXT_STATUS_NOT_SUPPORTED:
            return ErrorCode::NOT_SUPPORTED;
        case MOONCAKE_EXT_STATUS_IO_ERROR:
            // The baseline write path (PosixFsAdapter::WriteAt) maps every
            // pwritev failure, including ENOENT, to FILE_WRITE_FAIL. Match it
            // here so a missing bucket file reports the same error class with
            // or without the extension, restoring the pre-change behaviour.
            return ErrorCode::FILE_WRITE_FAIL;
        case MOONCAKE_EXT_STATUS_DISABLED:
            return ErrorCode::NOT_SUPPORTED;
        default:
            return ErrorCode::FILE_WRITE_FAIL;
    }
}

// Runs `count` independent items across up to `max_threads` workers. Both the
// baseline batch reader and the extension fallback used to be strictly serial,
// which turned a large batch into a per-object latency pile-up.
constexpr size_t kBaselineReadThreads = 128;
constexpr size_t kFallbackThreads = 32;

template <typename F>
void RunParallel(size_t count, size_t max_threads, F&& fn) {
    if (count == 0) return;
    const size_t workers = std::min(max_threads == 0 ? size_t{1} : max_threads,
                                    count);
    if (workers <= 1) {
        for (size_t i = 0; i < count; ++i) fn(i);
        return;
    }
    std::atomic<size_t> next{0};
    // An exception escaping a pool worker would call std::terminate, which in
    // this backend runs inside the inference process. Capture the first one
    // and rethrow it on the caller's thread only after every worker has been
    // joined; the caller (or the detached timed-read worker) already maps a
    // thrown batch to a whole-batch failure.
    std::exception_ptr eptr;
    std::mutex eptr_mutex;
    const auto worker = [&] {
        try {
            for (size_t i = next.fetch_add(1, std::memory_order_relaxed);
                 i < count;
                 i = next.fetch_add(1, std::memory_order_relaxed)) {
                fn(i);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(eptr_mutex);
            if (!eptr) eptr = std::current_exception();
        }
    };
    std::vector<std::thread> pool;
    pool.reserve(workers);
    for (size_t i = 0; i < workers; ++i) {
        try {
            pool.emplace_back(worker);
        } catch (...) {
            // Thread creation can fail under resource pressure. Stop spawning;
            // the caller's thread below picks up whatever is left.
            break;
        }
    }
    // Run the caller's share under the same barrier so an exception here does
    // not leave the pool threads unjoined (which would terminate on their
    // destruction during stack unwinding).
    try {
        worker();
    } catch (...) {
        std::lock_guard<std::mutex> lock(eptr_mutex);
        if (!eptr) eptr = std::current_exception();
    }
    for (auto& thread : pool) thread.join();
    if (eptr) std::rethrow_exception(eptr);
}

void LogDfeBaselineFallback(const char* direction, size_t count) {
    static std::atomic<uint64_t> batches{0};
    static std::atomic<uint64_t> requests{0};
    const uint64_t total_batches =
        batches.fetch_add(1, std::memory_order_relaxed) + 1;
    const uint64_t total_requests =
        requests.fetch_add(count, std::memory_order_relaxed) + count;
    static DfsLogLimiter limiter;
    if (auto suppressed = limiter.ShouldLog()) {
        LOG(WARNING) << "DFS " << direction
                     << " batch rejected by the extension; redoing only the "
                        "rejected items on the baseline path: requests="
                     << count << ", fallback_batches=" << total_batches
                     << ", fallback_requests=" << total_requests
                     << ", suppressed=" << *suppressed;
    }
}

}  // namespace

using l4::kBucketFilePrefix;
using l4::kBucketDataSuffix;

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
    for (auto& [_, shard] : shard_files_) {
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
        bucket_id_read_cache_.clear();
    }
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
        std::vector<ShardFile*> shards;
        {
            std::lock_guard cache_lock(shard_files_mutex_);
            for (const auto& [_, shard] : shard_files_) {
                shards.push_back(shard.get());
            }
        }
        std::vector<std::pair<ShardFile*, int>> opened;
        for (auto* shard : shards) {
            std::lock_guard shard_lock(shard->mutex);
            if (shard->path.empty() || shard->write_fd.load() >= 0) continue;
            auto fd = ValidateAndOpenShardFile(shard->path);
            if (!fd) {
                for (const auto& [_, new_fd] : opened) {
                    (void)fs_adapter_->CloseFile(new_fd);
                }
                return tl::make_unexpected(fd.error());
            }
            opened.emplace_back(shard, *fd);
        }
        for (const auto& [shard, fd] : opened) {
            std::lock_guard shard_lock(shard->mutex);
            if (shard->write_fd.load() < 0) {
                shard->write_fd.store(fd);
            } else {
                (void)fs_adapter_->CloseFile(fd);
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
    int fd = *fd_result;
    // Own the fd immediately: the capability read and the write-context probe
    // below can throw (e.g. bad_alloc) and used to leak it.
    FdGuard fd_guard{fs_adapter_.get(), fd};

    // Write-context ioctl is a pure optimization. If the filesystem does not
    // implement it, or rejects it for any other reason, fall back to the plain
    // locked write path instead of failing the write. Once it fails the process
    // stops probing it, so an unsupported filesystem never induces an
    // open+ioctl+close storm for every cold bucket.
    //
    // The switch is also a safety escape hatch: when the operator turns it off
    // both the client and the master must keep the filesystem mtime update.
    // Parse it with the shared rules so FALSE/Off/disable behave like 0.
    static const bool write_context_configured = [] {
        const char* raw =
            std::getenv("MOONCAKE_DFS_POSIX_WRITE_CONTEXT_ENABLED");
        if (raw == nullptr) return true;
        return TryParseBool(raw).value_or(true);
    }();
    // Split deployments can run the master without the private extension.
    // Skipping the per-write mtime update is only safe when the master
    // advertised that it refreshes the mtime on seal; otherwise the value
    // would freeze at the last real update. Cache the answer, but drop it on
    // every Master epoch change (see AcquireBucketEpoch): a restarted master
    // may run a different private-extension build and flip the capability, so
    // a stale cache could keep skipping per-write mtime updates against a
    // master that no longer compensates on seal.
    const auto master_refreshes_mtime = [&]() {
        const int cached =
            master_mtime_refresh_support_.load(std::memory_order_relaxed);
        if (cached >= 0) return cached != 0;
        bool supported = false;
        const std::string capability_path =
            root_dir_ + "/" + kDfsWriteContextCapabilityFileName;
        std::vector<char> contents(kMaxDfsWriteContextCapabilitySize);
        auto read = fs_adapter_->ReadFile(capability_path, contents.data(),
                                          contents.size());
        if (read && *read <= kMaxDfsWriteContextCapabilitySize) {
            (void)ParseDfsWriteContextCapability(
                std::string_view(contents.data(), *read), &supported);
        }
        master_mtime_refresh_support_.store(supported ? 1 : 0,
                                            std::memory_order_relaxed);
        return supported;
    };
    if (distributed_config_.fs_adapter_type == "posix" &&
        mooncake::ext::DfeProvider::Instance().available()) {
        // Config-off and a master that cannot refresh mtime are re-evaluated
        // on every cold open from cheap sources (a process-lifetime constant
        // and the epoch-fresh capability cache above), so they are not latched
        // here. Only a filesystem ioctl failure is permanent for this client
        // and survives a Master generation change.
        if (!write_context_configured) {
            LOG_FIRST_N(INFO, 1)
                << "DFS bucket write context disabled by configuration "
                   "(MOONCAKE_DFS_POSIX_WRITE_CONTEXT_ENABLED); keeping the "
                   "per-write mtime update";
        } else if (!master_refreshes_mtime()) {
            LOG_FIRST_N(INFO, 1)
                << "DFS bucket write context disabled: the master does not "
                   "advertise sealed-bucket mtime refresh (private extension "
                   "missing on the master?); keeping the per-write mtime "
                   "update";
        } else if (!posix_write_context_unavailable_.load(
                       std::memory_order_relaxed)) {
            auto context = fs_adapter_->SetBucketWriteContext(
                fd_guard.fd, distributed_config_.bucket_capacity);
            if (context) {
                // One INFO per process; the ioctl is otherwise silent.
                LOG_FIRST_N(INFO, 1)
                    << "DFS bucket write context enabled: client skips "
                       "per-write mtime updates (range locks retained)";
            } else {
                posix_write_context_unavailable_.store(
                    true, std::memory_order_relaxed);
                static DfsLogLimiter log_limiter;
                if (auto suppressed = log_limiter.ShouldLog()) {
                    LOG(WARNING)
                        << "DFS bucket write-context ioctl failed for " << path
                        << " (error=" << context.error()
                        << "); disabling it for this process and falling back "
                           "to the legacy write path from now on, suppressed="
                        << *suppressed;
                }
                if (context.error() != ErrorCode::NOT_SUPPORTED) {
                    // A hard error may have left the fd in an unknown state.
                    // Replace it with a clean handle before writing through it.
                    (void)fs_adapter_->CloseFile(fd_guard.fd);
                    fd_guard.fd = -1;  // avoid a double-close if reopen fails
                    auto reopened =
                        fs_adapter_->OpenFileReadWriteExisting(path);
                    if (!reopened) {
                        return tl::make_unexpected(reopened.error());
                    }
                    fd_guard.fd = *reopened;
                }
            }
        }
    }

    auto handle = std::make_shared<OpenFileHandle>();
    handle->adapter = fs_adapter_.get();
    handle->fd = fd_guard.release();
    handle->path = path;

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

tl::expected<std::shared_ptr<DistributedStorageBackend::OpenFileHandle>,
             ErrorCode>
DistributedStorageBackend::GetOrOpenBucketForRead(int64_t bucket_id,
                                                  const UUID& epoch) {
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        auto it = bucket_id_read_cache_.find(bucket_id);
        if (it != bucket_id_read_cache_.end()) return it->second;
    }

    const std::string path = canonical_root_dir_ + "/" + kBucketFilePrefix +
                             ImmutableBucketAllocator::FormatBucketId(bucket_id) +
                             kBucketDataSuffix;
    // A plain read handle: no write-context ioctl, so reads keep the default
    // metadata and range-lock behavior even while writes opt out of per-write
    // mtime updates.
    auto fd_result = fs_adapter_->OpenFileReadOnly(path);
    if (!fd_result) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to open DFS bucket file for read " << path
                       << ": " << fd_result.error()
                       << ", suppressed=" << *suppressed;
        }
        return tl::make_unexpected(fd_result.error());
    }
    int read_fd = *fd_result;
    FdGuard fd_guard{fs_adapter_.get(), read_fd};

    auto handle = std::make_shared<OpenFileHandle>();
    handle->adapter = fs_adapter_.get();
    handle->fd = fd_guard.release();
    handle->path = path;

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
    auto [it, inserted] = bucket_id_read_cache_.emplace(bucket_id, handle);
    (void)inserted;
    return it->second;
}

tl::expected<bool, ErrorCode>
DistributedStorageBackend::CleanupBucketFd(int64_t bucket_id) {
    if (!IsBucketMode() || bucket_id < 0) return false;

    std::shared_ptr<OpenFileHandle> regular_handle;
    std::shared_ptr<OpenFileHandle> direct_handle;
    std::shared_ptr<OpenFileHandle> read_handle;
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        auto regular_it = bucket_id_cache_.find(bucket_id);
        if (regular_it != bucket_id_cache_.end()) regular_handle = regular_it->second;
        auto direct_it = bucket_id_direct_cache_.find(bucket_id);
        if (direct_it != bucket_id_direct_cache_.end()) direct_handle = direct_it->second;
        auto read_it = bucket_id_read_cache_.find(bucket_id);
        if (read_it != bucket_id_read_cache_.end()) read_handle = read_it->second;
    }
    if (!regular_handle && !direct_handle && !read_handle) return false;

    const auto& handle = regular_handle ? regular_handle
                          : direct_handle ? direct_handle
                                          : read_handle;
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
    auto read_it = bucket_id_read_cache_.find(bucket_id);
    if (read_it != bucket_id_read_cache_.end() &&
        read_it->second == read_handle) {
        bucket_id_read_cache_.erase(read_it);
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
        // A restarted Master may run a different private-extension build, so
        // its sealed-bucket mtime-refresh capability can flip. Drop the cached
        // answer so the next cold write re-reads the capability file. The
        // ioctl latch (posix_write_context_unavailable_) is a filesystem
        // property and survives the generation change.
        master_mtime_refresh_support_.store(-1, std::memory_order_relaxed);
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
                        bucket_id_direct_cache_.size() +
                        bucket_id_read_cache_.size());
        for (auto& [bucket_id, handle] : bucket_id_cache_) {
            handles.push_back(std::move(handle));
        }
        for (auto& [bucket_id, handle] : bucket_id_direct_cache_) {
            handles.push_back(std::move(handle));
        }
        for (auto& [bucket_id, handle] : bucket_id_read_cache_) {
            handles.push_back(std::move(handle));
        }
        bucket_id_cache_.clear();
        bucket_id_direct_cache_.clear();
        bucket_id_read_cache_.clear();
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
        bucket_ids.reserve(bucket_id_cache_.size() +
                           bucket_id_direct_cache_.size() +
                           bucket_id_read_cache_.size());
        for (const auto& [bucket_id, handle] : bucket_id_cache_) {
            if (!active_ids.contains(bucket_id)) bucket_ids.push_back(bucket_id);
        }
        for (const auto& [bucket_id, handle] : bucket_id_direct_cache_) {
            if (!active_ids.contains(bucket_id)) bucket_ids.push_back(bucket_id);
        }
        for (const auto& [bucket_id, handle] : bucket_id_read_cache_) {
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

    // Baseline write for one prepared object. Shared by the all-baseline path
    // and by the fallback that only redoes objects the provider rejected.
    const auto write_one = [&](size_t prepared_index) {
        const auto& item = prepared[prepared_index];
        const auto& payload = item.payload;
        uint64_t written = 0;
        ErrorCode error = ErrorCode::OK;
        while (written < payload.size()) {
            iovec iov{const_cast<char*>(payload.data()) + written,
                      static_cast<size_t>(payload.size() - written)};
            auto result = fs_adapter_->WriteAt(
                item.target.fd, &iov, 1,
                static_cast<int64_t>(item.offset + written));
            if (!result) {
                error = result.error();
                break;
            }
            if (*result == 0) {
                error = ErrorCode::FILE_WRITE_FAIL;
                break;
            }
            written += *result;
        }
        if (error == ErrorCode::OK) {
            results[item.index] = {};
        } else {
            results[item.index] = tl::make_unexpected(error);
        }
    };

    // The private data-plane extension owns the write-side run merge and
    // parallelism. Every payload stays owned here; only the positional write
    // is delegated. A shape the provider cannot take falls back below.
    if (!prepared.empty() && DfeDataPlaneAvailable(distributed_config_)) {
        std::vector<mooncake_ext_iovec> iovecs(prepared.size());
        std::vector<mooncake_ext_io_request> io_requests(prepared.size());
        for (size_t i = 0; i < prepared.size(); ++i) {
            iovecs[i].base = prepared[i].payload.data();
            iovecs[i].length = prepared[i].payload.size();
            auto& request = io_requests[i];
            request.fd = prepared[i].target.fd;
            request.offset = prepared[i].offset;
            request.total_size = prepared[i].payload.size();
            request.op = MOONCAKE_EXT_IO_WRITE;
            request.mergeable = 1;
            request.iovecs = &iovecs[i];
            request.iovec_count = 1;
        }
        std::vector<mooncake_ext_io_result> io_results(prepared.size());
        if (mooncake::ext::DfeProvider::Instance().IoBatch(
                io_requests.data(), io_results.data(), io_requests.size())) {
            std::vector<size_t> fallback;
            for (size_t i = 0; i < io_results.size(); ++i) {
                const auto& item = prepared[i];
                const bool ok =
                    io_results[i].status == MOONCAKE_EXT_STATUS_OK &&
                    io_results[i].bytes_done == item.payload.size();
                if (ok) {
                    results[item.index] = tl::expected<void, ErrorCode>{};
                    continue;
                }
                if (ExtStatusNeedsBaselineFallback(io_results[i].status)) {
                    // Shape/availability rejection: redo only this object on
                    // the baseline path, never the objects already served.
                    fallback.push_back(i);
                    continue;
                }
                // A real I/O error is final. Retrying it would repeat the
                // same failing write, so map it through the provider's errno.
                results[item.index] = tl::make_unexpected(
                    MapExtWriteFailure(io_results[i].status,
                                       io_results[i].error_code));
            }
            if (!fallback.empty()) {
                LogDfeBaselineFallback("write", fallback.size());
                RunParallel(fallback.size(), kFallbackThreads,
                            [&](size_t k) { write_one(fallback[k]); });
            }
            return results;
        }
    }

    // Baseline write path: one positional write per object. Run merging and
    // parallel dispatch belong to the private extension above.
    for (size_t i = 0; i < prepared.size(); ++i) write_one(i);
    return results;
}

std::vector<DistributedStorageBackend::ReadTask>
DistributedStorageBackend::PrepareReadTasks(
    const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    std::vector<ReadTask> tasks;
    tasks.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto target = ResolveTarget(
            request.descriptor, request.key, /*for_read=*/true,
            /*prefer_direct=*/!IsBucketMode() ||
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
        uint64_t value_offset = 0;
        uint64_t read_bytes = request.descriptor.object_size;
        if (request.range_read) {
            value_offset = request.value_offset;
            if (value_offset > request.descriptor.object_size ||
                capacity > request.descriptor.object_size - value_offset) {
                invalid = true;
            }
            read_bytes = capacity;
            auto size = fs_adapter_->GetFileSize(target->keepalive->path);
            if (!size || *size < request.descriptor.offset +
                                    request.descriptor.object_size) {
                results[i] = tl::make_unexpected(
                    size ? ErrorCode::FILE_READ_FAIL : size.error());
                continue;
            }
        }
        if (invalid || capacity < read_bytes) continue;

        ReadTask task;
        task.target = std::move(*target);
        task.io_offset = request.descriptor.offset + value_offset;
        task.total_size = request.range_read
                              ? read_bytes
                              : (IsBucketMode() ? request.descriptor.aligned_size
                                                : request.descriptor.object_size);
        task.direct_read = distributed_config_.direct_read_enabled &&
                           task.target.direct_read;
        task.entries.push_back({i, task.io_offset});
        tasks.push_back(std::move(task));
    }
    return tasks;
}

// Each task now carries exactly one request. Reads scatter straight into the
// caller's slices; the optional private extension may merge and parallelize
// them across equally-shaped tasks.
void DistributedStorageBackend::ExecuteReadTask(
    const ReadTask& task, const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    ExecuteSingleReadTask(task, requests, results);
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
    // Slices may cover only object_size while the read covers the whole reserved
    // range, so the shortfall is absorbed by a sink. Size it from the actual gap
    // rather than one alignment unit, so a reservation larger than the
    // configured alignment cannot overflow the sink.
    std::vector<char> sink;
    if (collected < read_size) {
        sink.resize(static_cast<size_t>(read_size - collected));
        iovs.push_back({sink.data(), sink.size()});
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

void DistributedStorageBackend::ExecuteReadTasks(
    const std::vector<ReadTask>& tasks,
    const std::vector<DfsReadRequest>& requests,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    const auto run_serial = [&]() {
        for (const auto& task : tasks) ExecuteReadTask(task, requests, results);
    };
    if (tasks.empty()) return;

    // A single request has nothing to merge. Keep it on the baseline path so
    // the common small-GET case never pays the provider handshake.
    if (tasks.size() <= 1 || !IsBucketMode() ||
        !DfeDataPlaneAvailable(distributed_config_)) {
        // Without the extension there is no merge to do, but the reads are
        // still independent: run them concurrently instead of serially, which
        // matches the pre-change batch reader's concurrency.
        if (tasks.size() <= 1) {
            run_serial();
        } else {
            RunParallel(tasks.size(), kBaselineReadThreads, [&](size_t k) {
                ExecuteReadTask(tasks[k], requests, results);
            });
        }
        return;
    }

    // One provider request per task. The provider merges adjacent requests
    // that share an fd and parallelizes the resulting runs. Buffers and sinks
    // stay owned here and outlive the synchronous call.
    std::vector<std::vector<char>> sinks(tasks.size());
    std::vector<std::vector<mooncake_ext_iovec>> iov_storage(tasks.size());
    std::vector<mooncake_ext_io_request> io_requests;
    std::vector<size_t> task_indices;
    io_requests.reserve(tasks.size());
    task_indices.reserve(tasks.size());

    for (size_t task_index = 0; task_index < tasks.size(); ++task_index) {
        const auto& task = tasks[task_index];
        if (task.entries.empty()) continue;
        const auto& request = requests[task.entries.front().request_index];
        auto& iovs = iov_storage[task_index];
        uint64_t collected = 0;
        iovs.reserve(request.slices.size() + 1);
        for (const auto& slice : request.slices) {
            if (collected >= task.total_size) break;
            const uint64_t size =
                std::min<uint64_t>(slice.size, task.total_size - collected);
            if (size != 0) {
                iovs.push_back({slice.ptr, size});
                collected += size;
            }
        }
        auto& sink = sinks[task_index];
        if (collected < task.total_size) {
            sink.resize(static_cast<size_t>(task.total_size - collected));
            iovs.push_back({sink.data(), sink.size()});
        }
        if (iovs.empty()) {
            if (task.total_size == 0) {
                results[task.entries.front().request_index] =
                    tl::expected<void, ErrorCode>{};
            }
            continue;
        }

        mooncake_ext_io_request io_request{};
        io_request.fd = task.target.fd;
        io_request.offset = task.io_offset;
        io_request.total_size = task.total_size;
        io_request.op = MOONCAKE_EXT_IO_READ;
        io_request.direct_io = task.direct_read ? 1u : 0u;
        io_request.mergeable = 1;
        io_request.worker_count = 0;
        io_request.iovecs = iovs.data();
        io_request.iovec_count = iovs.size();
        io_requests.push_back(io_request);
        task_indices.push_back(task_index);
    }

    if (io_requests.empty()) return;
    std::vector<mooncake_ext_io_result> io_results(io_requests.size());
    // Pre-seed as failure: the enum value of OK is 0, so a value-initialised
    // slot would otherwise look like a successful zero-length read.
    for (auto& result : io_results) {
        result.status = MOONCAKE_EXT_STATUS_IO_ERROR;
        result.bytes_done = 0;
    }
    if (!mooncake::ext::DfeProvider::Instance().IoBatch(
            io_requests.data(), io_results.data(), io_requests.size())) {
        // The provider failed before returning per-request results. Redo the
        // batch on the baseline path in parallel: a serial redo would multiply
        // the batch latency by the number of objects.
        LogDfeBaselineFallback("read", task_indices.size());
        RunParallel(task_indices.size(), kFallbackThreads, [&](size_t k) {
            ExecuteReadTask(tasks[task_indices[k]], requests, results);
        });
        return;
    }
    // Only a shape/availability rejection is rerun on the baseline path; an
    // I/O error is final and is mapped through the provider's errno.
    std::vector<size_t> fallback;
    for (size_t i = 0; i < io_results.size(); ++i) {
        const auto& task = tasks[task_indices[i]];
        const bool ok =
            io_results[i].status == MOONCAKE_EXT_STATUS_OK &&
            io_results[i].bytes_done == task.total_size;
        if (ok) {
            results[task.entries.front().request_index] =
                tl::expected<void, ErrorCode>{};
            continue;
        }
        if (ExtStatusNeedsBaselineFallback(io_results[i].status)) {
            fallback.push_back(i);
            continue;
        }
        results[task.entries.front().request_index] = tl::make_unexpected(
            MapExtReadFailure(io_results[i].status, io_results[i].error_code));
    }
    if (!fallback.empty()) {
        LogDfeBaselineFallback("read", fallback.size());
        RunParallel(fallback.size(), kFallbackThreads, [&](size_t k) {
            ExecuteReadTask(tasks[task_indices[fallback[k]]], requests, results);
        });
    }
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
    const auto& dfe = mooncake::ext::DfeProvider::Instance();
    LOG(INFO) << "DFS private data-plane extension: state=" << dfe.state()
              << " detail=" << dfe.detail();
    initialized_.store(true, std::memory_order_release);
    LOG(INFO) << "DistributedStorageBackend initialized in bucket mode, "
                 "fsdir="
              << root_dir_ << ", bucket_capacity="
              << distributed_config_.bucket_capacity;
    return {};
}

}  // namespace mooncake
