#include "master_service.h"
#include "master_metric_manager.h"
#include "environ.h"
#include "config/distributed_storage_config.h"
#include "random.h"
#include "storage/distributed/dfs_io_timeout.h"
#include "storage/distributed/dfs_log_limiter.h"
#include "storage/distributed/dfs_root_identity.h"
#include "storage/distributed/dfs_read_probe.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/shard_allocator.h"
#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mooncake {

namespace {

enum class DfsRootFileStatus {
    kMatch,
    kMissing,
    kInvalid,
    kIoError,
};

DfsRootFileStatus ReadExpectedDfsRootFile(const std::string& path,
                                          std::string_view expected) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return errno == ENOENT ? DfsRootFileStatus::kMissing
                              : DfsRootFileStatus::kIoError;
    }

    struct stat st {};
    bool valid = ::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) &&
                 static_cast<uint64_t>(st.st_size) == expected.size();
    std::string contents;
    if (valid) {
        contents.resize(expected.size());
        size_t offset = 0;
        while (offset < contents.size()) {
            const ssize_t count =
                ::read(fd, contents.data() + offset, contents.size() - offset);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) {
                valid = false;
                break;
            }
            offset += static_cast<size_t>(count);
        }
    }
    const int close_result = ::close(fd);
    if (close_result != 0) return DfsRootFileStatus::kIoError;
    if (!valid || std::string_view(contents) != expected) {
        return DfsRootFileStatus::kInvalid;
    }
    return DfsRootFileStatus::kMatch;
}

bool ReadDfsRootIdentity(const std::string& marker_path,
                         const std::string& cluster_id,
                         std::string& identity, bool& missing) {
    missing = false;
    const int fd = ::open(marker_path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        missing = errno == ENOENT;
        return false;
    }

    struct stat st {};
    bool valid = ::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0 &&
                 static_cast<uint64_t>(st.st_size) <= kMaxDfsRootIdentitySize;
    std::string contents;
    if (valid) {
        contents.resize(static_cast<size_t>(st.st_size));
        size_t offset = 0;
        while (offset < contents.size()) {
            const ssize_t count =
                ::read(fd, contents.data() + offset, contents.size() - offset);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) {
                valid = false;
                break;
            }
            offset += static_cast<size_t>(count);
        }
    }
    const int close_result = ::close(fd);
    if (close_result != 0 || !valid ||
        !ParseDfsRootIdentity(contents, cluster_id, identity)) {
        identity.clear();
        return false;
    }
    return true;
}

bool SyncDfsRootDirectory(const std::string& root) {
    const int fd = ::open(root.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        return errno == EINVAL || errno == EOPNOTSUPP;
    }
    const int result = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    return result == 0 || saved_errno == EINVAL ||
           saved_errno == EOPNOTSUPP;
}

bool EnsureDfsReadProbeDirectory(const std::string& root,
                                 std::string_view directory_name) {
    const std::string path = root + "/" + std::string(directory_name);
    struct stat st {};
    if (::lstat(path.c_str(), &st) == 0) return S_ISDIR(st.st_mode);
    if (errno != ENOENT) return false;

    if (::mkdir(path.c_str(), 0755) != 0) {
        if (errno != EEXIST || ::lstat(path.c_str(), &st) != 0 ||
            !S_ISDIR(st.st_mode)) {
            return false;
        }
    }
    return SyncDfsRootDirectory(root);
}

bool WriteAll(int fd, const std::string& contents) {
    size_t offset = 0;
    while (offset < contents.size()) {
        const ssize_t count =
            ::write(fd, contents.data() + offset, contents.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        offset += static_cast<size_t>(count);
    }
    return ::fsync(fd) == 0;
}

bool PublishExpectedDfsRootFile(const std::string& root,
                                const std::string& path,
                                const std::string& contents) {
    const UUID suffix = generate_uuid();
    const std::string temp_path =
        path + "." + std::to_string(::getpid()) + "." +
        UuidToString(suffix) + ".tmp";
    const int temp_fd = ::open(temp_path.c_str(),
                               O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC |
                                   O_NOFOLLOW,
                               0644);
    if (temp_fd < 0) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to create DFS root temp file " << temp_path
                       << ": " << std::strerror(errno)
                       << ", suppressed=" << *suppressed;
        }
        return false;
    }

    const bool temp_written = WriteAll(temp_fd, contents);
    const int close_result = ::close(temp_fd);
    if (!temp_written || close_result != 0) {
        const int saved_errno = errno;
        ::unlink(temp_path.c_str());
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to write DFS root temp file " << temp_path
                       << ": " << std::strerror(saved_errno)
                       << ", suppressed=" << *suppressed;
        }
        return false;
    }

    const int link_result = ::link(temp_path.c_str(), path.c_str());
    const int link_errno = errno;
    if (link_result != 0 && link_errno != EEXIST) {
        ::unlink(temp_path.c_str());
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to publish DFS root file " << path << ": "
                       << std::strerror(link_errno)
                       << ", suppressed=" << *suppressed;
        }
        return false;
    }
    ::unlink(temp_path.c_str());

    // EEXIST means another writer won the race, not that it published the
    // expected bytes. Re-read in both cases before declaring success.
    if (ReadExpectedDfsRootFile(path, contents) !=
        DfsRootFileStatus::kMatch) {
        return false;
    }
    if (!SyncDfsRootDirectory(root)) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to sync DFS root after publishing " << path
                       << ", suppressed=" << *suppressed;
        }
        return false;
    }
    return true;
}

bool EnsureDfsRootIdentity(const std::string& root,
                           const std::string& cluster_id,
                           std::string& identity) {
    std::error_code ec;
    if (!std::filesystem::is_directory(root, ec) || ec) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "DFS root must already exist as a directory; "
                          "refusing to create it: "
                       << root << ", suppressed=" << *suppressed;
        }
        return false;
    }
    if (!IsValidClusterIdComponent(cluster_id)) {
        LOG(ERROR) << "Invalid cluster id for DFS root identity: " << cluster_id;
        return false;
    }

    const std::string marker_path =
        root + "/" + kDfsRootIdentityFileName;
    bool missing = false;
    if (ReadDfsRootIdentity(marker_path, cluster_id, identity, missing)) {
        return true;
    }
    if (!missing) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "DFS root identity marker is invalid or belongs to "
                          "a different cluster: "
                       << marker_path << ", suppressed=" << *suppressed;
        }
        return false;
    }

    const bool restoring_cached_identity = !identity.empty();
    std::string candidate = identity;
    if (restoring_cached_identity) {
        std::string parsed;
        if (!ParseDfsRootIdentity(candidate, cluster_id, parsed) ||
            parsed != candidate) {
            LOG(ERROR) << "Refusing to restore an invalid cached DFS root "
                          "identity for "
                       << marker_path;
            return false;
        }
    } else {
        const UUID token{
            randomUniform<uint64_t>(0, std::numeric_limits<uint64_t>::max()),
            randomUniform<uint64_t>(0, std::numeric_limits<uint64_t>::max())};
        candidate = SerializeDfsRootIdentity(cluster_id, token);
        if (candidate.empty()) return false;
    }

    if (!PublishExpectedDfsRootFile(root, marker_path, candidate)) {
        // Concurrent first-time initializers legitimately generate different
        // candidates. The atomic link chooses one; initializers that lose the
        // race adopt the complete, valid winner. Recovery from a cached
        // identity must remain exact and may never adopt a different value.
        if (!restoring_cached_identity &&
            ReadDfsRootIdentity(marker_path, cluster_id, identity, missing)) {
            return true;
        }
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to publish or validate DFS root identity "
                       << marker_path << ", suppressed=" << *suppressed;
        }
        return false;
    }
    identity = std::move(candidate);
    return true;
}

bool ProbeDfsRootWritable(const std::string& root, std::string& error) {
    constexpr size_t kProbeSize = 4096;
    const std::string path =
        root + "/.mooncake_dfs_init_probe." + std::to_string(::getpid()) +
        "." + UuidToString(generate_uuid());
    const int fd = ::open(
        path.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) {
        error = "create " + path + ": " + std::strerror(errno);
        return false;
    }

    // Exercise allocation and persistence rather than treating a successful
    // zero-byte create as proof that the DFS can accept ready-bucket writes.
    std::array<char, kProbeSize> contents{};
    size_t offset = 0;
    int failure_errno = 0;
    while (offset < contents.size()) {
        const ssize_t written =
            ::write(fd, contents.data() + offset, contents.size() - offset);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) {
            failure_errno = written < 0 ? errno : EIO;
            break;
        }
        offset += static_cast<size_t>(written);
    }
    if (failure_errno == 0 && ::fdatasync(fd) != 0) failure_errno = errno;
    if (::close(fd) != 0 && failure_errno == 0) failure_errno = errno;

    if (::unlink(path.c_str()) != 0 && failure_errno == 0) {
        failure_errno = errno;
    }
    if (failure_errno == 0 && !SyncDfsRootDirectory(root)) {
        failure_errno = EIO;
    }
    if (failure_errno != 0) {
        // Best-effort cleanup also covers write/fsync failures.
        ::unlink(path.c_str());
        error = "write/sync/remove " + path + ": " +
                std::strerror(failure_errno);
        return false;
    }
    return true;
}

}  // namespace

void MasterService::DfsAllocatorRetryThreadFunc() {
    constexpr auto kInitialRetryInterval = std::chrono::seconds(5);
    constexpr auto kMaxRetryInterval = std::chrono::seconds(60);
    auto retry_interval = kInitialRetryInterval;
    while (dfs_retry_running_.load(std::memory_order_acquire)) {
        const int jitter_percent = randomUniform<int>(-10, 10);
        const auto wait_interval = std::chrono::milliseconds(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                retry_interval)
                .count() *
            (100 + jitter_percent) / 100);
        std::unique_lock lock(dfs_retry_mutex_);
        if (dfs_retry_cv_.wait_for(lock, wait_interval, [this] {
                return !dfs_retry_running_.load(std::memory_order_acquire);
            })) {
            break;
        }
        lock.unlock();

        if (!dfs_retry_needed_.load(std::memory_order_acquire)) break;
        try {
            MasterServiceConfig retry_config;
            // The runtime snapshot policy is fixed at construction. Retry the
            // same configured allocator against the current DFS environment.
            retry_config.enable_ha = !dfs_restore_from_bucket_metadata_;
            const DfsInitResult result =
                InitDfsAllocatorFromEnvironment(retry_config);
            if (result != DfsInitResult::kRetryableFailure) break;
        } catch (const std::exception& e) {
            static DfsLogLimiter retry_log_limiter;
            if (auto suppressed = retry_log_limiter.ShouldLog()) {
                LOG(WARNING) << "DFS allocator retry threw an exception, error="
                             << e.what() << ", suppressed=" << *suppressed;
            }
        }
        if (!dfs_retry_needed_.load(std::memory_order_acquire)) break;
        retry_interval = std::min(retry_interval * 2, kMaxRetryInterval);
    }
}

bool MasterService::InitializeDfsRootGuard(const std::string& root,
                                           const std::string& identity) {
    struct stat st {};
    if (::stat(root.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
        return false;
    }

    const std::string identity_path = root + "/" + kDfsRootIdentityFileName;
    if (ReadExpectedDfsRootFile(identity_path, identity) !=
        DfsRootFileStatus::kMatch) {
        return false;
    }
    for (size_t probe_index = 0;
         probe_index < kDfsReadProbeDirectoryNames.size(); ++probe_index) {
        const auto directory_name =
            kDfsReadProbeDirectoryNames[probe_index];
        if (!EnsureDfsReadProbeDirectory(root, directory_name)) return false;
        const std::string probe_directory =
            root + "/" + std::string(directory_name);
        const std::string read_probe_path =
            probe_directory + "/" + kDfsReadProbeFileName;
        const std::string read_probe =
            BuildDfsReadProbeContents(identity, probe_index);
        const auto probe_status =
            ReadExpectedDfsRootFile(read_probe_path, read_probe);
        if (probe_status == DfsRootFileStatus::kMissing) {
            if (!PublishExpectedDfsRootFile(probe_directory, read_probe_path,
                                            read_probe)) {
                return false;
            }
        } else if (probe_status != DfsRootFileStatus::kMatch) {
            return false;
        }
    }

    std::lock_guard lock(dfs_root_guard_mutex_);
    dfs_root_guard_path_ = root;
    dfs_root_guard_identity_ = identity;
    dfs_root_guard_device_ = static_cast<uint64_t>(st.st_dev);
    dfs_root_guard_inode_ = static_cast<uint64_t>(st.st_ino);
    dfs_root_guard_ready_ = true;
    dfs_root_recovery_needed_ = false;
    dfs_root_replacement_lost_data_ = false;
    return true;
}

size_t MasterService::DropDfsReplicasAfterRootReplacement() {
    size_t removed_replicas = 0;
    VisitNamespaces([&](const TenantId& tenant_id, metadata::Namespace& ns) {
        std::vector<route::ObjectRef> objects;
        for (auto object : ns.objects.ReadCursor()) objects.push_back(object.ref());
        for (const auto& ref : objects) {
            MetadataAccessorRW accessor(this, tenant_id, ref);
            if (!accessor.IsPublished()) continue;
            auto& metadata = accessor.Get();
            const auto previous_media = KvMediaSnapshot(metadata);
            const auto removed = EraseReplicasWithCacheTotalAccounting(
                metadata, [](const Replica& replica) { return replica.is_dfs_replica(); });
            removed_replicas += removed;
            if (!removed) continue;
            if (!metadata.IsValid()) {
                accessor.Erase();
            } else {
                // A prior completed DFS replica may already have consumed its
                // backfill candidate. This lost generation needs a fresh one.
                if (!accessor.GetState().dfs_backfill) ScheduleDfsBackfill(accessor);
                SyncKvObjectState(ref.key, metadata, tenant_id, previous_media);
            }
        }
    });
    return removed_replicas;
}

bool MasterService::RecoverDfsRootAfterReplacement(
    const std::string& root, const std::string& identity) {
    bool expected = false;
    if (!dfs_root_recovery_in_progress_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        return false;
    }
    enable_dfs_.store(false, std::memory_order_release);
    struct RecoveryFlagReset {
        std::atomic<bool>& flag;
        ~RecoveryFlagReset() { flag.store(false, std::memory_order_release); }
    } recovery_flag_reset{dfs_root_recovery_in_progress_};

    // Everything through allocator construction and guard publication is
    // deliberately outside master_state_mutex_. DFS publication is gated by
    // dfs_root_recovery_in_progress_, while L3-only metadata requests remain
    // available throughout slow DFS inspection and bucket creation.
    const std::string identity_path = root + "/" + kDfsRootIdentityFileName;
    const auto identity_status =
        ReadExpectedDfsRootFile(identity_path, identity);
    if (identity_status == DfsRootFileStatus::kInvalid) {
        LOG(ERROR) << "Replacement DFS root carries a conflicting identity; "
                      "refusing recovery, root="
                   << root;
        return false;
    }
    if (identity_status == DfsRootFileStatus::kIoError) {
        LOG(ERROR) << "Failed to inspect replacement DFS root identity, root="
                   << root;
        return false;
    }

    const std::string marker_path = root + "/" + kDfsRootMarkerFileName;
    const std::string marker_contents(kDfsRootMarkerPayload,
                                      sizeof(kDfsRootMarkerPayload) - 1);
    const auto marker_status =
        ReadExpectedDfsRootFile(marker_path, marker_contents);
    if (marker_status == DfsRootFileStatus::kInvalid) {
        LOG(ERROR) << "Replacement DFS root carries a conflicting marker; "
                      "refusing recovery, root="
                   << root;
        return false;
    }
    if (marker_status == DfsRootFileStatus::kIoError) {
        LOG(ERROR) << "Failed to inspect replacement DFS root marker, root="
                   << root;
        return false;
    }

    // The root marker is created by the allocator, independently of the
    // identity. If only the identity was copied into a newly created root,
    // the missing marker proves that the old bucket generation is gone.
    bool root_lost_data = identity_status == DfsRootFileStatus::kMissing ||
                          marker_status == DfsRootFileStatus::kMissing;
    if (!root_lost_data && bucket_allocator_ != nullptr) {
        const auto snapshot = bucket_allocator_->GetBucketIdSnapshot();
        if (!snapshot) {
            LOG(ERROR) << "Failed to inspect bucket generation while "
                          "recovering replacement DFS root, root="
                       << root;
            return false;
        }
        for (const int64_t bucket_id : snapshot->bucket_ids) {
            const auto present =
                bucket_allocator_->IsBucketDataFilePresent(bucket_id);
            if (!present) {
                LOG(ERROR) << "Failed to verify bucket " << bucket_id
                           << " while recovering replacement DFS root, root="
                           << root << ", error=" << present.error();
                return false;
            }
            if (!*present) {
                // Ready buckets are only preallocated capacity. Recheck under
                // the allocator lock because SetMaxBucketCount may have
                // removed one after this snapshot, or an in-flight allocation
                // may have promoted it and made the missing file significant.
                if (bucket_allocator_->DiscardMissingBucketIfEmpty(bucket_id)) {
                    LOG(WARNING)
                        << "Replacement DFS root is missing empty bucket "
                        << bucket_id
                        << "; discarded it without invalidating live DFS "
                           "replicas, root="
                        << root;
                    continue;
                }
                root_lost_data = true;
                LOG(WARNING) << "Replacement DFS root is missing bucket "
                             << bucket_id
                             << "; treating the old storage generation as "
                                "lost, root="
                             << root;
                break;
            }
        }
    }
    {
        std::lock_guard lock(dfs_root_guard_mutex_);
        root_lost_data =
            root_lost_data || dfs_root_replacement_lost_data_;
        dfs_root_replacement_lost_data_ = root_lost_data;
    }
    if (identity_status == DfsRootFileStatus::kMissing &&
        !PublishExpectedDfsRootFile(root, identity_path, identity)) {
        LOG(ERROR) << "Failed to restore identity in replacement DFS root "
                   << root;
        return false;
    }

    std::unique_ptr<ImmutableBucketAllocator> replacement_allocator;
    if (root_lost_data) {
        if (bucket_allocator_ == nullptr) {
            LOG(ERROR) << "Automatic DFS root replacement recovery currently "
                          "requires the bucket allocator, root="
                       << root;
            return false;
        }
        auto config = DistributedStorageConfig::FromEnvironment();
        if (config.allocator_type != DfsAllocatorType::BUCKET ||
            config.fsdir != root) {
            LOG(ERROR) << "DFS configuration changed during root recovery, "
                          "root="
                       << root << ", config={" << config.FormatStr() << "}";
            return false;
        }
        replacement_allocator =
            bucket_allocator_->RetireAndCreateReplacementAllocator();
        auto initialized = replacement_allocator->Init(config);
        if (!initialized) {
            LOG(ERROR) << "Failed to rebuild DFS bucket allocator after root "
                          "replacement, root="
                       << root << ", error=" << initialized.error();
            return false;
        }
        // A replacement root is a new storage generation. It contains no
        // trustworthy old DFS replicas; consume any recovery candidates that
        // may have raced into the directory without publishing them.
        (void)replacement_allocator->TakeRecoveredReplicas();
        // Keep vector growth, and therefore heap allocation, out of the
        // snapshot commit barrier below.
        retired_dfs_allocators_.reserve(retired_dfs_allocators_.size() + 1);
    }

    if (!InitializeDfsRootGuard(root, identity)) {
        LOG(ERROR) << "Failed to rebuild DFS guard files after root "
                      "replacement, root="
                   << root;
        return false;
    }
    UUID new_epoch = generate_uuid();
    if (dfs_root_recovery_before_commit_hook_) {
        dfs_root_recovery_before_commit_hook_();
    }

    size_t removed_replicas = 0;
    {
        // The commit barrier excludes metadata requests only while DFS replicas
        // are removed and the fully initialized allocator generation is
        // published. No DFS filesystem I/O or bucket creation occurs here.
        std::unique_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
        if (root_lost_data) {
            removed_replicas = DropDfsReplicasAfterRootReplacement();

            auto old_allocator = std::move(dfs_allocator_owner_);
            auto* replacement_ptr = replacement_allocator.get();
            dfs_allocator_owner_ = std::move(replacement_allocator);
            bucket_allocator_.store(replacement_ptr);
            dfs_allocator_.store(replacement_ptr);
            retired_dfs_allocators_.push_back(std::move(old_allocator));
        }
        std::lock_guard<std::mutex> epoch_lock(dfs_epoch_mutex_);
        dfs_snapshot_epoch_ = new_epoch;
        enable_dfs_.store(true, std::memory_order_release);
    }
    RefreshDfsMetrics();
    LOG(WARNING) << "Recovered replacement DFS root, root=" << root
                 << ", data_generation_lost=" << root_lost_data
                 << ", removed_dfs_replicas=" << removed_replicas
                 << ", new_epoch=" << new_epoch;
    return true;
}

void MasterService::ReconcileDfsRootFiles() {
    std::string root;
    std::string identity;
    uint64_t expected_device = 0;
    uint64_t expected_inode = 0;
    bool recovery_needed = false;
    {
        std::lock_guard lock(dfs_root_guard_mutex_);
        if (!dfs_root_guard_ready_ ||
            (!enable_dfs_.load(std::memory_order_acquire) &&
             !dfs_root_recovery_needed_)) {
            return;
        }
        root = dfs_root_guard_path_;
        identity = dfs_root_guard_identity_;
        expected_device = dfs_root_guard_device_;
        expected_inode = dfs_root_guard_inode_;
        recovery_needed = dfs_root_recovery_needed_;
    }

    struct stat st {};
    const int injected_stat_error =
        dfs_root_stat_error_for_testing_.load(std::memory_order_acquire);
    int stat_result = 0;
    int stat_error = 0;
    if (injected_stat_error != 0) {
        stat_result = -1;
        stat_error = injected_stat_error;
    } else {
        stat_result = ::stat(root.c_str(), &st);
        if (stat_result != 0) stat_error = errno;
    }
    if (stat_result != 0 && stat_error != ENOENT && stat_error != ENOTDIR) {
        // EIO, ETIMEDOUT, ESTALE, and similar failures do not prove that the
        // mounted generation changed. Keep serving DFS and retry on the next
        // guard pass; otherwise one transient metadata-server error forces an
        // unnecessary epoch change and rejects in-flight client writes.
        static DfsLogLimiter stat_log_limiter;
        if (auto suppressed = stat_log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to inspect DFS root; keeping the current "
                          "generation active, root="
                       << root << ", errno=" << stat_error
                       << ", error=" << std::strerror(stat_error)
                       << ", suppressed=" << *suppressed;
        }
        return;
    }
    if (stat_result != 0 || !S_ISDIR(st.st_mode)) {
        enable_dfs_.store(false, std::memory_order_release);
        {
            std::lock_guard lock(dfs_root_guard_mutex_);
            dfs_root_recovery_needed_ = true;
        }
        static DfsLogLimiter mount_log_limiter;
        if (auto suppressed = mount_log_limiter.ShouldLog()) {
            LOG(ERROR) << "DFS root is unavailable; waiting for recovery, "
                          "root="
                       << root
                       << ", suppressed=" << *suppressed;
        }
        return;
    }
    const uint64_t actual_device = static_cast<uint64_t>(st.st_dev);
    const uint64_t actual_inode = static_cast<uint64_t>(st.st_ino);
    if (actual_device != expected_device) {
        enable_dfs_.store(false, std::memory_order_release);
        {
            std::lock_guard lock(dfs_root_guard_mutex_);
            dfs_root_recovery_needed_ = true;
        }
        static DfsLogLimiter mount_log_limiter;
        if (auto suppressed = mount_log_limiter.ShouldLog()) {
            LOG(ERROR) << "DFS root device changed; refusing automatic "
                          "recovery, root="
                       << root << ", expected_device=" << expected_device
                       << ", actual_device=" << actual_device
                       << ", suppressed=" << *suppressed;
        }
        return;
    }
    if (recovery_needed || actual_inode != expected_inode) {
        enable_dfs_.store(false, std::memory_order_release);
        {
            std::lock_guard lock(dfs_root_guard_mutex_);
            dfs_root_recovery_needed_ = true;
        }
        (void)RecoverDfsRootAfterReplacement(root, identity);
        return;
    }

    const auto reconcile = [](const std::string& parent, const char* filename,
                              const std::string& expected) {
        const std::string path = parent + "/" + filename;
        switch (ReadExpectedDfsRootFile(path, expected)) {
            case DfsRootFileStatus::kMatch:
                return true;
            case DfsRootFileStatus::kMissing:
                if (PublishExpectedDfsRootFile(parent, path, expected)) {
                    LOG(WARNING) << "Recreated missing DFS root file " << path;
                    return true;
                }
                LOG(ERROR) << "Failed to recreate missing DFS root file "
                           << path;
                return false;
            case DfsRootFileStatus::kInvalid:
                LOG(ERROR) << "DFS root file has unexpected contents; "
                              "refusing to overwrite "
                           << path;
                return false;
            case DfsRootFileStatus::kIoError:
                LOG(ERROR) << "Failed to inspect DFS root file " << path;
                return false;
        }
        return false;
    };

    // Identity is the safety boundary. Do not recreate the read-probe files
    // until the root still carries (or has atomically recovered) the exact
    // identity pinned when the allocator was initialized.
    if (!reconcile(root, kDfsRootIdentityFileName, identity)) {
        return;
    }
    reconcile(root, kDfsRootMarkerFileName,
              std::string(kDfsRootMarkerPayload,
                          sizeof(kDfsRootMarkerPayload) - 1));
    for (size_t probe_index = 0;
         probe_index < kDfsReadProbeDirectoryNames.size(); ++probe_index) {
        const auto directory_name =
            kDfsReadProbeDirectoryNames[probe_index];
        if (!EnsureDfsReadProbeDirectory(root, directory_name)) {
            LOG(ERROR) << "Failed to create DFS read-probe directory "
                       << root << "/" << directory_name;
            continue;
        }
        const std::string probe_directory =
            root + "/" + std::string(directory_name);
        reconcile(probe_directory, kDfsReadProbeFileName,
                  BuildDfsReadProbeContents(identity, probe_index));
    }
}

void MasterService::RefreshDfsMetrics() const {
    uint64_t capacity = 0;
    uint64_t used = 0;
    if (dfs_allocator_ && dfs_allocator_->IsInitialized()) {
        capacity = dfs_allocator_->GetTotalCapacity();
        used = dfs_allocator_->GetUsedBytes();
    }
    // The new DFS allocators are always finite: SHARD uses the configured
    // shard_count * shard_capacity and BUCKET uses max_bucket_count *
    // bucket_capacity.  Do not reuse the legacy root-fs/global-file flag.
    MasterMetricManager::instance().set_dfs_storage_usage(capacity, used,
                                                           false);
}

void MasterService::ProcessInvalidatedDfsBucketsForTesting() {
    ProcessInvalidatedDfsBuckets();
}

tl::expected<int64_t, ErrorCode> MasterService::SetDfsMaxBucketCount(
    int64_t new_max_bucket_count) {
    if (!bucket_allocator_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return bucket_allocator_->SetMaxBucketCount(new_max_bucket_count);
}

tl::expected<uint32_t, ErrorCode> MasterService::SetDfsHeartbeatInterval(
    uint32_t new_interval_seconds) {
    if (new_interval_seconds == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return dfs_heartbeat_interval_seconds_.exchange(
        new_interval_seconds, std::memory_order_acq_rel);
}

uint64_t MasterService::SetDfsBatchTimeoutFixedMs(uint64_t new_timeout_ms) {
    const uint64_t old_timeout_ms = dfs_batch_timeout_fixed_ms_.exchange(
        new_timeout_ms, std::memory_order_acq_rel);
    LOG(INFO) << "Updated DFS batch timeout fixed budget from "
              << old_timeout_ms << "ms to " << new_timeout_ms << "ms";
    return old_timeout_ms;
}

tl::expected<uint8_t, ErrorCode> MasterService::SetDfsFilterMode(
    uint8_t new_filter_mode) {
    if (new_filter_mode > 1) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint8_t old_value =
        dfs_filter_mode_.exchange(new_filter_mode, std::memory_order_acq_rel);
    LOG(INFO) << "action=set_dfs_filter_mode, old_value="
              << static_cast<unsigned int>(old_value)
              << ", new_value="
              << static_cast<unsigned int>(new_filter_mode);
    return old_value;
}

bool MasterService::IsReplicaReadableForGet(const Replica& replica) const {
    if (!replica.is_completed() || !replica.is_available() ||
        replica.is_eviction_pending() || IsAwaitingRemount(replica)) return false;
    return !replica.is_dfs_replica() ||
           (enable_dfs_.load(std::memory_order_acquire) &&
            !dfs_root_recovery_in_progress_.load(std::memory_order_acquire));
}

std::unordered_map<std::string, MasterService::DfsBatchReservation>
MasterService::ReserveDfsSpaceForBatch(
    const std::vector<std::string>& keys,
    const std::vector<uint64_t>& value_lengths,
    const std::vector<bool>& needs_dfs,
    std::unordered_map<std::string, ErrorCode>& errors_out) {
    std::unordered_map<std::string, DfsBatchReservation> reservations;
    auto* source_allocator = bucket_allocator_.get();
    if (source_allocator == nullptr || !source_allocator->IsInitialized()) {
        return reservations;
    }

    std::vector<BatchAllocateRequest> requests;
    requests.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!needs_dfs[i]) continue;
        requests.push_back(BatchAllocateRequest{keys[i], value_lengths[i]});
    }
    if (requests.empty()) return reservations;

    auto results = source_allocator->BatchAllocate(requests);
    if (results.size() != requests.size()) {
        LOG(ERROR) << "DFS BatchAllocate returned " << results.size()
                   << " results for " << requests.size() << " requests";
        for (const auto& request : requests) {
            errors_out[request.key] = ErrorCode::INTERNAL_ERROR;
        }
        return reservations;
    }

    // Per-key outcome: the allocator rejects individual keys (for example one
    // that already holds a live allocation) without failing the whole batch, so
    // release only what was reserved when *nothing* usable came back.
    bool any_ok = false;
    for (const auto& result : results) {
        if (result.success) {
            any_ok = true;
            continue;
        }
        errors_out[result.key] = result.error == ErrorCode::OK
                                     ? ErrorCode::NO_AVAILABLE_HANDLE
                                     : result.error;
    }
    if (!any_ok) return reservations;

    for (auto& result : results) {
        if (!result.success) continue;
        reservations.emplace(
            result.key,
            DfsBatchReservation{std::move(result.descriptor), source_allocator});
    }
    return reservations;
}

tl::expected<bool, ErrorCode> MasterService::IsDfsBucketDataFilePresent(
    int64_t bucket_id) const {
    if (bucket_allocator_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return bucket_allocator_->IsBucketDataFilePresent(bucket_id);
}

std::vector<tl::expected<void, ErrorCode>>
MasterService::BatchInvalidateDfsBuckets(
    const UUID& client_id, const std::vector<DfsMissingBucketReport>& reports,
    const TenantId& tenant_id) {
    (void)client_id;
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(reports.size());
    if (bucket_allocator_ == nullptr || !tenant_id.IsDefault()) {
        results.assign(reports.size(),
                       tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        return results;
    }

    std::unordered_set<int64_t> bucket_ids;
    for (const auto& report : reports) {
        if (report.bucket_id < 0 ||
            !bucket_allocator_->HasBucket(report.bucket_id)) {
            results.emplace_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        const auto file_present = IsDfsBucketDataFilePresent(report.bucket_id);
        if (!file_present) {
            LOG(WARNING) << "Cannot verify DFS bucket " << report.bucket_id
                         << " data file: " << file_present.error();
            results.emplace_back(tl::make_unexpected(file_present.error()));
            continue;
        }
        if (*file_present) {
            LOG(INFO) << "Ignoring DFS bucket invalidation report for bucket "
                      << report.bucket_id
                      << ": master data file is present";
            results.emplace_back();
            continue;
        }
        bucket_ids.insert(report.bucket_id);
        results.emplace_back();
    }

    if (!bucket_ids.empty()) {
        std::lock_guard<std::mutex> lock(invalidated_dfs_buckets_mutex_);
        invalidated_dfs_buckets_.insert(bucket_ids.begin(), bucket_ids.end());
    }
    return results;
}

void MasterService::ProcessInvalidatedDfsBuckets() {
    std::unordered_set<int64_t> pending;
    {
        std::lock_guard<std::mutex> lock(invalidated_dfs_buckets_mutex_);
        pending.swap(invalidated_dfs_buckets_);
    }
    for (const int64_t bucket_id : pending) {
        const auto file_present = IsDfsBucketDataFilePresent(bucket_id);
        if (!file_present) {
            LOG(WARNING) << "Deferring DFS bucket invalidation for bucket "
                         << bucket_id << ": " << file_present.error();
            std::lock_guard<std::mutex> lock(invalidated_dfs_buckets_mutex_);
            invalidated_dfs_buckets_.insert(bucket_id);
            continue;
        }
        if (*file_present) {
            LOG(INFO) << "Ignoring DFS bucket invalidation for bucket "
                      << bucket_id << ": master data file is present";
            continue;
        }
        if (!RunBucketDfsEvictionInternal(/*force_one=*/false, bucket_id)) {
            if (bucket_allocator_ != nullptr &&
                bucket_allocator_->HasBucket(bucket_id)) {
                std::lock_guard<std::mutex> lock(
                    invalidated_dfs_buckets_mutex_);
                invalidated_dfs_buckets_.insert(bucket_id);
            }
        }
    }
}

tl::expected<CopyStartResponse, ErrorCode> MasterService::DfsBackfillStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const std::string& src_segment, const UUID& task_id) {
    auto tenant = ResolveTenantIdForWrite(tenant_id);
    if (!tenant) return tl::make_unexpected(tenant.error());
    const auto record = FindClientRecord(client_id);
    auto retaining_guard = record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    std::shared_lock<std::shared_mutex> lock(master_state_mutex_);
    MetadataAccessorRW accessor(this, ObjectIdentity{*tenant, key});
    if (!accessor.Exists()) return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    auto& state = accessor.GetState();
    if (!state.dfs_backfill || task_id == UUID{} || state.dfs_backfill->task_id != task_id)
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    auto assignment = task_manager_.get_read_access().find_task_by_id(task_id);
    if (!assignment || assignment->assigned_client != client_id ||
        assignment->type != TaskType::DFS_BACKFILL || assignment->status != TaskStatus::PROCESSING)
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    auto& metadata = accessor.Get();
    if (accessor.InProcessing() || accessor.HasReplicationTask() || state.dynamic_replication_pending)
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    if (metadata.HasReplica(&Replica::is_dfs_replica)) return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    UUID owner;
    if (!source || !source->is_memory_replica() || !source->is_completed() || !source->is_available() ||
        segment_manager_.getSegmentAccess().GetClientIdBySegmentName(src_segment, owner) != ErrorCode::OK || owner != client_id)
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    if ((dfs_filter_mode_.load(std::memory_order_acquire) & 1) != 0 ||
        dfs_root_recovery_in_progress_.load(std::memory_order_acquire))
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    if (!dfs_allocator_ || !dfs_allocator_->IsInitialized())
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    auto allocated = dfs_allocator_->Allocate(key, metadata.GetPart(0).length());
    if (!allocated) return tl::make_unexpected(allocated.error());
    std::vector<Replica> replicas;
    replicas.emplace_back(std::move(*allocated), ReplicaStatus::PROCESSING);
    CopyStartResponse response;
    response.source = source->get_descriptor();
    response.targets.push_back(replicas.front().get_descriptor());
    state.replication_task.emplace(ReplicationTask{
        client_id, std::chrono::system_clock::now(), ReplicationTask::Type::COPY,
        source->id(), {replicas.front().id()}, task_id, 0, false});
    source->inc_refcnt();
    metadata.AddReplicas(std::move(replicas));
    return response;
}

void MasterService::RestoreRecoveredDfsReplicas(bool rebuild_metadata) {
    auto* allocator = bucket_allocator_.get();
    if (!allocator) return;
    auto recovered = allocator->TakeRecoveredReplicas();
    std::vector<ImmutableBucketAllocator::RecoveredReplica> stale;
    size_t restored = 0;
    if (rebuild_metadata) {
        const TenantId tenant = TenantId::Default();
        std::shared_lock<std::shared_mutex> lock(master_state_mutex_);
        for (auto& entry : recovered) {
            MetadataAccessorRW accessor(this, {tenant, entry.key},
                MetadataAccessorRW::CreateKey{.cleanup_invalid = false});
            if (accessor.IsPublished()) {
                const auto& metadata = accessor.Get();
                if (metadata.client_id == UUID{} && metadata.CountReplicas() == 1 &&
                    metadata.HasReplica(&Replica::fn_is_dfs_replica)) {
                    accessor.Erase();
                } else {
                    stale.push_back(std::move(entry));
                    continue;
                }
            }
            if (!accessor.Grow(0)) {
                stale.push_back(std::move(entry));
                continue;
            }
            std::vector<Replica> replicas;
            replicas.emplace_back(entry.descriptor, ReplicaStatus::COMPLETE);
            accessor.Publish(UUID{}, std::chrono::system_clock::now(),
                entry.descriptor.object_size, std::move(replicas), std::nullopt,
                false, ObjectDataType::UNKNOWN, std::string(), tenant, entry.key);
            ++restored;
        }
    }
    // Tombstone stale extents after every key and the state barrier are released.
    for (const auto& entry : stale) allocator->Free(entry.key, entry.descriptor);
    LOG(INFO) << "Restored " << restored << " DFS replicas from bucket metadata ("
              << recovered.size() << " candidates, " << stale.size()
              << " live entries preserved, metadata_rebuilt=" << rebuild_metadata << ")";
}

UUID MasterService::GetDfsMasterEpoch() const {
    if (!enable_dfs_.load(std::memory_order_acquire) ||
        bucket_allocator_ == nullptr) {
        return {};
    }
    std::lock_guard<std::mutex> lock(dfs_epoch_mutex_);
    return dfs_snapshot_epoch_;
}

auto MasterService::GetDfsBucketSnapshot(
    const UUID& client_id, const UUID& last_epoch, uint64_t last_version)
    -> tl::expected<DfsBucketSnapshotResponse, ErrorCode> {
    (void)client_id;
    std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    const UUID current_epoch = GetDfsMasterEpoch();
    if (current_epoch == UUID{} || bucket_allocator_ == nullptr) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    auto snapshot = bucket_allocator_->GetBucketIdSnapshot();
    if (!snapshot) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    DfsBucketSnapshotResponse response;
    response.epoch = current_epoch;
    response.version = snapshot->version;
    response.heartbeat_interval_seconds =
        dfs_heartbeat_interval_seconds_.load(std::memory_order_acquire);
    response.batch_timeout_fixed_ms =
        dfs_batch_timeout_fixed_ms_.load(std::memory_order_acquire);
    response.unchanged = last_epoch == current_epoch &&
                         last_version == snapshot->version;
    if (!response.unchanged) response.bucket_ids = snapshot->bucket_ids;
    LOG(INFO) << "Returned DFS bucket snapshot, version=" << response.version
              << ", unchanged=" << response.unchanged
              << ", bucket_count=" << response.bucket_ids.size();
    return response;
}

void MasterService::ScheduleDfsBackfill(MetadataAccessorRW& accessor) {
    if (accessor.GetState().dfs_backfill) return;
    accessor.GetState().dfs_backfill.emplace();
    std::lock_guard lock(dfs_backfill_mutex_);
    dfs_backfill_candidates_.push_back({accessor.GetTenantId(), accessor.GetRef()});
}

void MasterService::RunDfsBackfill() {
    if ((dfs_filter_mode_.load(std::memory_order_acquire) & 1) != 0 ||
        dfs_root_recovery_in_progress_.load(std::memory_order_acquire) ||
        !dfs_allocator_ || !dfs_allocator_->IsInitialized()) return;
    size_t count;
    {
        std::lock_guard lock(dfs_backfill_mutex_);
        count = std::min<size_t>(256, dfs_backfill_candidates_.size());
    }
    const auto now = std::chrono::steady_clock::now();
    std::shared_lock<std::shared_mutex> lock(master_state_mutex_);
    for (size_t i = 0; i < count; ++i) {
        DfsBackfillCandidate candidate;
        {
            std::lock_guard queue_lock(dfs_backfill_mutex_);
            candidate = std::move(dfs_backfill_candidates_.front());
            dfs_backfill_candidates_.pop_front();
        }
        MetadataAccessorRW accessor(this, candidate.tenant_id, candidate.ref);
        if (!accessor.IsPublished() || !accessor.GetState().dfs_backfill) continue;
        auto& metadata = accessor.Get();
        auto& state = accessor.GetState();
        if (metadata.HasReplica([](const Replica& replica) {
                return replica.is_dfs_replica() && replica.is_completed();
            })) {
            state.dfs_backfill.reset();
            continue;
        }
        // Requeue only this publication; a recreated key never inherits its task.
        {
            std::lock_guard queue_lock(dfs_backfill_mutex_);
            dfs_backfill_candidates_.push_back(candidate);
        }
        auto& retry = *state.dfs_backfill;
        if (retry.retry_after > now || state.is_processing || state.replication_task ||
            metadata.HasReplica(&Replica::is_dfs_replica)) continue;
        if (retry.task_id != UUID{}) {
            auto task = task_manager_.get_read_access().find_task_by_id(retry.task_id);
            if (task && !task->is_finished()) continue;
        }
        retry.retry_after = now + std::chrono::seconds(5);
        for (const auto& source : metadata.GetReplicaSegmentNames()) {
            auto replica = metadata.GetReplicaBySegmentName(source);
            UUID owner;
            if (!replica || !replica->is_memory_replica() || !replica->is_completed() ||
                !replica->is_available() || segment_manager_.getSegmentAccess()
                .GetClientIdBySegmentName(source, owner) != ErrorCode::OK) continue;
            auto task = task_manager_.get_write_access().submit_task_typed<TaskType::DFS_BACKFILL>(
                owner, {candidate.tenant_id.value(), candidate.ref.key, source});
            if (task) retry.task_id = *task;
            else if (task.error() == ErrorCode::TASK_PENDING_LIMIT_EXCEEDED) return;
            break;
        }
    }
}

bool MasterService::PrepareDfsRootForInitialization(
    const DistributedStorageConfig& dfs_config) {
    if (!EnsureDfsRootIdentity(dfs_config.fsdir, cluster_id_,
                               dfs_root_identity_)) {
        enable_dfs_ = false;
        dfs_retry_needed_.store(true, std::memory_order_release);
        return false;
    }

    std::string probe_error;
    if (!ProbeDfsRootWritable(dfs_config.fsdir, probe_error)) {
        static DfsLogLimiter probe_log_limiter;
        if (auto suppressed = probe_log_limiter.ShouldLog()) {
            LOG(ERROR) << "DFS root write preflight failed; skipping allocator "
                          "recovery, root="
                       << dfs_config.fsdir << ", error=" << probe_error
                       << ", suppressed=" << *suppressed;
        }
        enable_dfs_ = false;
        dfs_retry_needed_.store(true, std::memory_order_release);
        return false;
    }
    return true;
}

void MasterService::RunLowUtilizationDfsEviction() {
    const bool low_utilization_enabled =
        dfs_bucket_low_utilization_threshold_ > 0.0 &&
        dfs_bucket_low_utilization_evictions_per_tick_ > 0;
    if (low_utilization_enabled) {
        (void)RunBucketDfsEvictionInternal(
            /*force_one=*/false, std::nullopt,
            dfs_bucket_low_utilization_threshold_,
            dfs_bucket_low_utilization_evictions_per_tick_,
            dfs_bucket_low_utilization_attempts_per_tick_);
    }
}

}  // namespace mooncake
