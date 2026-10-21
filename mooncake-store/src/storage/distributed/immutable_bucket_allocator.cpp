#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/dfs_log_limiter.h"

#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <unordered_set>
#include <utility>

#include <ylt/struct_pb.hpp>

#include "crc32c.h"
#include "master_metric_manager.h"
#include "thread_pool.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {

namespace {

constexpr const char* kBucketFilePrefix = "bucket_";
constexpr const char* kBucketDataSuffix = ".data";
constexpr const char* kBucketMetaSuffix = ".meta";
constexpr auto kBucketCreationRetryDelay = std::chrono::seconds(1);

int64_t CurrentTimeNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool IsLive(BucketEntryState state) {
    return state == BucketEntryState::PENDING ||
           state == BucketEntryState::COMMITTED;
}

bool IsKnownEntryState(int32_t state) {
    return state == static_cast<int32_t>(BucketEntryState::PENDING) ||
           state == static_cast<int32_t>(BucketEntryState::COMMITTED) ||
           state == static_cast<int32_t>(BucketEntryState::TOMBSTONE);
}

uint32_t ComputeMetadataChecksum(PersistedBucketMetadata snapshot) {
    // The checksum covers the serialized form with the checksum field zeroed,
    // so verification can recompute it the same way after loading.
    snapshot.checksum = 0;
    std::string payload;
    struct_pb::to_pb(snapshot, payload);
    return Crc32cValue(payload.data(), payload.size());
}

/**
 * @brief Extract the bucket id from a `bucket_<id><suffix>` file name.
 */
std::optional<int64_t> ParseBucketFileName(const std::string& name,
                                           const char* suffix) {
    const std::string prefix(kBucketFilePrefix);
    const std::string tail(suffix);
    if (name.size() <= prefix.size() + tail.size()) return std::nullopt;
    if (name.compare(0, prefix.size(), prefix) != 0) return std::nullopt;
    if (name.compare(name.size() - tail.size(), tail.size(), tail) != 0) {
        return std::nullopt;
    }

    const std::string digits =
        name.substr(prefix.size(), name.size() - prefix.size() - tail.size());
    if (digits.empty() ||
        !std::all_of(digits.begin(), digits.end(),
                     [](unsigned char c) { return std::isdigit(c) != 0; })) {
        return std::nullopt;
    }
    try {
        const long long value = std::stoll(digits);
        if (value < 0 || value > kMaxBucketId) return std::nullopt;
        return static_cast<int64_t>(value);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

/**
 * @brief Resolve the configured recovery worker count.
 *
 * A configured value of 0 means auto: min(8, hardware_concurrency()), clamped
 * to at least one so recovery always makes progress on a host whose hardware
 * concurrency cannot be determined.
 */
size_t ResolveRecoveryWorkerCount(int configured) {
    if (configured > 0) return static_cast<size_t>(configured);
    const unsigned hw = std::thread::hardware_concurrency();
    return std::max<size_t>(1, std::min<size_t>(8, hw));
}

}  // namespace

ImmutableBucketAllocator::PendingEviction::~PendingEviction() {
    // An unresolved transaction must not leave the bucket frozen forever.
    // Note: `owner_` must still be set when AbortEviction is entered, because
    // that is how it recognises the transaction as its own; clearing it here
    // first would make the abort a silent no-op. AbortEviction clears it.
    if (owner_ != nullptr) {
        owner_->AbortEviction(std::move(*this), /*demote=*/false);
    }
}

ImmutableBucketAllocator::PendingEviction::PendingEviction(
    PendingEviction&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      bucket_id_(std::exchange(other.bucket_id_, -1)),
      bucket_identity_(std::move(other.bucket_identity_)),
      candidates_(std::move(other.candidates_)) {
    other.candidates_.clear();
}

ImmutableBucketAllocator::PendingEviction&
ImmutableBucketAllocator::PendingEviction::operator=(
    PendingEviction&& other) noexcept {
    if (this != &other) {
        if (owner_ != nullptr) {
            PendingEviction discarded;
            discarded.owner_ = std::exchange(owner_, nullptr);
            discarded.bucket_id_ = bucket_id_;
            discarded.bucket_identity_ = std::move(bucket_identity_);
            discarded.candidates_ = std::move(candidates_);
            discarded.owner_->AbortEviction(std::move(discarded),
                                            /*demote=*/false);
        }
        owner_ = std::exchange(other.owner_, nullptr);
        bucket_id_ = std::exchange(other.bucket_id_, -1);
        bucket_identity_ = std::move(other.bucket_identity_);
        candidates_ = std::move(other.candidates_);
        other.candidates_.clear();
    }
    return *this;
}

ImmutableBucketAllocator::~ImmutableBucketAllocator() {
    initialized_.store(false, std::memory_order_release);
    StopBucketWorkers();

    // A clean shutdown must not leave a sealed bucket's metadata unwritten.
    // The still-active bucket is deliberately left unpersisted: its data is
    // discarded on the next start, which is the accepted trade-off for keeping
    // the hot path free of metadata I/O.
    FlushDirtyMetadata();

    std::vector<int64_t> ready_bucket_ids;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ready_bucket_ids = DetachReadyBucketsLocked();
        UpdateBucketPoolMetricsLocked();
    }
    for (const int64_t bucket_id : ready_bucket_ids) {
        DeleteBucketFiles(bucket_id);
    }
    // The cleanup thread deletes files through `fs_adapter_`, so it must finish
    // before the adapter is shut down.
    if (recovery_cleanup_thread_.joinable()) {
        recovery_cleanup_thread_.join();
    }
    if (fs_adapter_) fs_adapter_->Shutdown();
}

std::string ImmutableBucketAllocator::FormatBucketId(int64_t bucket_id) {
    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << bucket_id;
    return oss.str();
}

std::string ImmutableBucketAllocator::BucketDataPath(int64_t bucket_id) const {
    return fsdir_ + "/" + kBucketFilePrefix + FormatBucketId(bucket_id) +
           kBucketDataSuffix;
}

std::string ImmutableBucketAllocator::BucketMetaPath(int64_t bucket_id) const {
    return fsdir_ + "/" + kBucketFilePrefix + FormatBucketId(bucket_id) +
           kBucketMetaSuffix;
}

tl::expected<void, ErrorCode> ImmutableBucketAllocator::Init(
    const DistributedStorageConfig& config) {
    if (initialized_.load(std::memory_order_acquire)) return {};

    if (!config.ValidateForAllocator()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!config.ValidateForBucketAllocator()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    fsdir_ = config.fsdir;
    fs_adapter_type_ = config.fs_adapter_type;
    bucket_capacity_ = config.bucket_capacity;
    alignment_ = config.alignment;
    max_bucket_count_ = config.max_bucket_count;
    ready_bucket_target_ = static_cast<size_t>(config.ready_bucket_target);
    bucket_create_concurrency_ =
        static_cast<size_t>(config.bucket_create_concurrency);
    recovery_worker_count_ = ResolveRecoveryWorkerCount(
        config.recovery_worker_count);
    recovery_max_buckets_ = static_cast<size_t>(config.recovery_max_buckets);
    recovery_max_duration_seconds_ = config.recovery_max_duration_seconds;
    eviction_enabled_ = config.eviction_enabled;
    eviction_high_watermark_ = config.eviction_high_watermark;
    eviction_low_watermark_ = config.eviction_low_watermark;
    eviction_check_interval_ = config.eviction_check_interval;

    std::error_code ec;
    if (!std::filesystem::is_directory(fsdir_, ec) || ec) {
        LOG(ERROR) << "DFS bucket directory must already exist: " << fsdir_;
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    if (config.fs_adapter_type == "posix") {
        fs_adapter_ = std::make_unique<PosixFsAdapter>();
    } else if (config.fs_adapter_type == "hf3fs") {
#ifdef USE_3FS
        fs_adapter_ = std::make_unique<Hf3fsAdapter>();
#else
        LOG(ERROR) << "The hf3fs DFS adapter requires Mooncake to be built "
                      "with the USE_3FS compile-time option (-DUSE_3FS=ON)";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
    }
    if (!fs_adapter_) {
        LOG(ERROR) << "Unsupported DFS fs adapter type "
                   << config.fs_adapter_type;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto adapter_init = fs_adapter_->Init(fsdir_);
    if (!adapter_init) {
        LOG(ERROR) << "Failed to initialize DFS fs adapter "
                   << config.fs_adapter_type << " for fsdir=" << fsdir_
                   << ", error=" << adapter_init.error();
        fs_adapter_.reset();
        return tl::make_unexpected(adapter_init.error());
    }

    const std::string root_marker_path = fsdir_ + "/" + kDfsRootMarkerFileName;
    auto marker_result = EnsureDfsRootMarker(*fs_adapter_, root_marker_path);
    if (!marker_result) {
        LOG(ERROR) << "Failed to ensure DFS root marker " << root_marker_path
                   << ": " << marker_result.error();
        fs_adapter_->Shutdown();
        fs_adapter_.reset();
        return marker_result;
    }

    // Enumerate bucket files by name only and defer their `.meta` contents to
    // RecoverBucketsProgressively(). Parsing later, one bucket at a time, is
    // what keeps recovery temporary memory bounded by the worker count instead
    // of the total key count.
    {
        auto files = fs_adapter_->ListFiles(fsdir_);
        if (!files) {
            if (files.error() != ErrorCode::FILE_NOT_FOUND) {
                LOG(ERROR) << "Failed to list DFS bucket directory " << fsdir_
                           << ", error=" << files.error();
                fs_adapter_->Shutdown();
                fs_adapter_.reset();
                return tl::make_unexpected(files.error());
            }
            // No directory yet: there is nothing to recover.
        } else {
            pending_recovery_meta_ids_.clear();
            pending_recovery_data_ids_.clear();
            for (const auto& name : *files) {
                if (auto id = ParseBucketFileName(name, kBucketMetaSuffix)) {
                    pending_recovery_meta_ids_.push_back(*id);
                } else if (auto data_id =
                               ParseBucketFileName(name, kBucketDataSuffix)) {
                    pending_recovery_data_ids_.push_back(*data_id);
                }
                // Anything else - including `.meta.0`/`.meta.1` snapshot slots,
                // `.meta.log` and `.meta.tmp.*` leftovers from older layouts -
                // is deliberately ignored.
            }
            // Recover newest buckets first: a higher id means the bucket was
            // sealed later, so its entries are newer. If the recovery budget
            // cuts this short, the buckets most likely to hold current data
            // have already been restored.
            std::sort(pending_recovery_meta_ids_.begin(),
                      pending_recovery_meta_ids_.end(),
                      std::greater<int64_t>());
            std::sort(pending_recovery_data_ids_.begin(),
                      pending_recovery_data_ids_.end());
        }

        // The ready pool creates new buckets from next_bucket_id_ before
        // recovery publishes any recovered bucket, so it must be advanced past
        // every id seen on disk here.
        int64_t max_seen_id = -1;
        for (const int64_t id : pending_recovery_meta_ids_) {
            max_seen_id = std::max(max_seen_id, id);
        }
        for (const int64_t id : pending_recovery_data_ids_) {
            max_seen_id = std::max(max_seen_id, id);
        }
        next_bucket_id_ = max_seen_id + 1;

        if (!config.bucket_recovery_enabled) {
            // Recovery is disabled: the files found above are invisible to
            // this process's allocation and eviction state, so nothing would
            // ever reclaim them and they would keep occupying the DFS
            // directory. Delete exactly the scanned ids off the startup path
            // so a large legacy directory does not stall initialization.
            std::unordered_set<int64_t> legacy_ids(
                pending_recovery_meta_ids_.begin(),
                pending_recovery_meta_ids_.end());
            legacy_ids.insert(pending_recovery_data_ids_.begin(),
                              pending_recovery_data_ids_.end());
            std::vector<int64_t> sweep(legacy_ids.begin(), legacy_ids.end());
            pending_recovery_meta_ids_.clear();
            pending_recovery_data_ids_.clear();
            if (sweep.empty()) {
                LOG(INFO) << "DFS bucket recovery disabled; no legacy bucket "
                             "files to delete, next_bucket_id="
                          << next_bucket_id_;
            } else {
                LOG(INFO) << "DFS bucket recovery disabled; deleting "
                          << sweep.size()
                          << " legacy bucket(s) in the background, next_bucket_id="
                          << next_bucket_id_;
                try {
                    // `sweep` is passed as an lvalue so the catch block below
                    // can still reclaim the files if the worker thread cannot
                    // be started.
                    legacy_cleanup_worker_ = std::thread(
                        &ImmutableBucketAllocator::LegacyCleanupWorker, this,
                        sweep);
                } catch (const std::system_error& e) {
                    LOG(WARNING)
                        << "Failed to start DFS legacy bucket cleanup worker: "
                        << e.what() << ", deleting synchronously";
                    for (const int64_t bucket_id : sweep) {
                        DeleteBucketFiles(bucket_id);
                    }
                }
            }
        }
    }
    size_t initial_ready_target = 0;
    ErrorCode initial_creation_error = ErrorCode::OK;
    if (ready_bucket_target_ > 0) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_bucket_workers_ = false;
            bucket_creation_retry_after_ =
                std::chrono::steady_clock::time_point::min();
            // Recovery has not published any bucket into `buckets_` yet, so
            // fold the pending count in explicitly. Discarded buckets only ever
            // shrink the target, which is the safe direction.
            initial_ready_target =
                EffectiveReadyTargetLocked(pending_recovery_meta_ids_.size());
            initialization_creations_remaining_ = initial_ready_target;
            UpdateBucketPoolMetricsLocked();
        }

        try {
            bucket_create_workers_.reserve(bucket_create_concurrency_);
            for (size_t i = 0; i < bucket_create_concurrency_; ++i) {
                bucket_create_workers_.emplace_back(
                    &ImmutableBucketAllocator::BucketCreateWorker, this);
            }
        } catch (const std::system_error& e) {
            LOG(ERROR) << "Failed to start DFS bucket creation workers: "
                       << e.what();
            StopBucketWorkers();
            std::vector<int64_t> ready_bucket_ids;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                ready_bucket_ids = DetachReadyBucketsLocked();
                UpdateBucketPoolMetricsLocked();
            }
            for (const int64_t bucket_id : ready_bucket_ids) {
                DeleteBucketFiles(bucket_id);
            }
            fs_adapter_->Shutdown();
            fs_adapter_.reset();
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        if (initial_ready_target > 0) {
            std::unique_lock<std::mutex> lock(mutex_);
            bucket_pool_cv_.notify_all();
            bucket_pool_cv_.wait(lock, [this, initial_ready_target] {
                return stop_bucket_workers_ ||
                       ready_bucket_ids_.size() >= initial_ready_target ||
                       (initialization_creations_remaining_ == 0 &&
                        bucket_creations_in_flight_ == 0);
            });
            initialization_creations_remaining_ = 0;
            if (ready_bucket_ids_.size() < initial_ready_target) {
                initial_creation_error = last_bucket_creation_error_;
                if (!ready_bucket_ids_.empty()) {
                    LOG(WARNING)
                        << "DFS ready bucket pool initialized below target, "
                        << "ready_count=" << ready_bucket_ids_.size()
                        << ", target=" << initial_ready_target
                        << ", last_error=" << initial_creation_error;
                }
            }
            bucket_pool_cv_.notify_all();
        }

        if (initial_ready_target > 0) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (ready_bucket_ids_.empty()) {
                if (initial_creation_error == ErrorCode::OK) {
                    initial_creation_error = ErrorCode::FILE_WRITE_FAIL;
                }
            } else {
                initial_creation_error = ErrorCode::OK;
            }
        }
    } else {
        std::lock_guard<std::mutex> lock(mutex_);
        UpdateBucketPoolMetricsLocked();
    }

    if (initial_creation_error != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize DFS ready bucket pool, target="
                   << initial_ready_target
                   << ", error=" << initial_creation_error;
        StopBucketWorkers();
        std::vector<int64_t> ready_bucket_ids;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ready_bucket_ids = DetachReadyBucketsLocked();
            UpdateBucketPoolMetricsLocked();
        }
        for (const int64_t bucket_id : ready_bucket_ids) {
            DeleteBucketFiles(bucket_id);
        }
        fs_adapter_->Shutdown();
        fs_adapter_.reset();
        return tl::make_unexpected(initial_creation_error);
    }

    initialized_.store(true, std::memory_order_release);

    // Bucket metadata is only read in RecoverBucketsProgressively(), which runs
    // after this returns. Any state it rewrites (PENDING entries demoted to
    // tombstones, duplicate losers demoted) is persisted at the end of that
    // pass, so Init itself performs no recovery I/O and needs no flush.
    size_t ready_bucket_count = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ready_bucket_count = ready_bucket_ids_.size();
    }
    LOG(INFO) << "DFS bucket allocator initialized, fsdir=" << fsdir_
              << ", bucket_capacity=" << bucket_capacity_
              << ", alignment=" << alignment_
              << ", max_bucket_count=" << max_bucket_count_
              << ", ready_bucket_target=" << ready_bucket_target_
              << ", bucket_create_concurrency=" << bucket_create_concurrency_
              << ", recovery_workers=" << recovery_worker_count_
              << ", recovery_pending_buckets="
              << pending_recovery_meta_ids_.size()
              << ", next_bucket_id=" << next_bucket_id_
              << ", ready_buckets=" << ready_bucket_count;
    return {};
}

PersistedBucketMetadata ImmutableBucketAllocator::SnapshotLocked(
    BucketState& bucket) {
    PersistedBucketMetadata snapshot;
    snapshot.version = kBucketMetadataVersion;
    snapshot.bucket_id = bucket.bucket_id;
    snapshot.capacity = bucket.capacity;
    snapshot.alignment = alignment_;
    snapshot.append_offset = bucket.append_offset;
    // `placed` is in append order, so the last reservation for a key is the
    // current one and any earlier one is dead: they collapse to a single entry
    // exactly as the old per-key map did by overwriting.
    snapshot.entries.reserve(bucket.placed.size());
    std::unordered_map<std::string, size_t> slot_of_key;
    for (const auto& placed : bucket.placed) {
        PersistedBucketEntry persisted;
        persisted.key = placed.key;
        persisted.entry_offset = placed.entry_offset;
        persisted.key_size = placed.key.size();
        persisted.value_size = placed.object_size;
        persisted.reserved_size = placed.reserved_size;
        persisted.generation = placed.generation;
        auto index_it = key_index_.find(placed.key);
        persisted.state =
            (index_it != key_index_.end() &&
             index_it->second.bucket_id == bucket.bucket_id)
                ? static_cast<int32_t>(index_it->second.state)
                : static_cast<int32_t>(BucketEntryState::TOMBSTONE);
        auto slot_it = slot_of_key.find(placed.key);
        if (slot_it == slot_of_key.end()) {
            slot_of_key.emplace(placed.key, snapshot.entries.size());
            snapshot.entries.push_back(std::move(persisted));
        } else {
            snapshot.entries[slot_it->second] = std::move(persisted);
        }
    }
    // Deterministic order keeps the serialized bytes (and hence the checksum)
    // stable for identical logical state, which makes tests reproducible.
    std::sort(
        snapshot.entries.begin(), snapshot.entries.end(),
        [](const PersistedBucketEntry& lhs, const PersistedBucketEntry& rhs) {
            if (lhs.entry_offset != rhs.entry_offset) {
                return lhs.entry_offset < rhs.entry_offset;
            }
            return lhs.key < rhs.key;
        });
    snapshot.checksum = ComputeMetadataChecksum(snapshot);
    return snapshot;
}

std::optional<PersistedBucketMetadata>
ImmutableBucketAllocator::ReadBucketMetadata(int64_t bucket_id) const {
    if (!fs_adapter_) return std::nullopt;
    const std::string meta_path = BucketMetaPath(bucket_id);
    auto file_size = fs_adapter_->GetFileSize(meta_path);
    if (!file_size) return std::nullopt;
    std::string payload(*file_size, '\0');
    if (*file_size > 0) {
        auto read =
            fs_adapter_->ReadFile(meta_path, payload.data(), payload.size());
        if (!read || *read != payload.size()) return std::nullopt;
    }
    PersistedBucketMetadata snapshot;
    try {
        struct_pb::from_pb(snapshot, payload);
    } catch (...) {
        return std::nullopt;
    }
    if (snapshot.version != kBucketMetadataVersion) return std::nullopt;
    if (snapshot.bucket_id != bucket_id) return std::nullopt;
    if (ComputeMetadataChecksum(snapshot) != snapshot.checksum) {
        return std::nullopt;
    }
    return snapshot;
}

void ImmutableBucketAllocator::ApplyKeyIndexStatesLocked(
    PersistedBucketMetadata& snapshot) const {
    // Offsets and sizes are immutable once an entry is placed, so only the
    // three-state field can differ between the file and memory. A key not live
    // in this bucket (freed, reallocated elsewhere, or never committed) is
    // recorded as a tombstone so recovery will not revive it.
    for (auto& entry : snapshot.entries) {
        auto index_it = key_index_.find(entry.key);
        entry.state =
            (index_it != key_index_.end() &&
             index_it->second.bucket_id == snapshot.bucket_id)
                ? static_cast<int32_t>(index_it->second.state)
                : static_cast<int32_t>(BucketEntryState::TOMBSTONE);
    }
    snapshot.checksum = ComputeMetadataChecksum(snapshot);
}

std::optional<DfsAllocatorInterface::EvictionCandidate>
ImmutableBucketAllocator::MakeEvictionCandidate(int64_t bucket_id,
                                                const std::string& key,
                                                uint64_t entry_offset,
                                                uint64_t value_size) const {
    auto layout =
        RebuildBucketEntryLayout(entry_offset, value_size, alignment_);
    if (!layout) return std::nullopt;
    DfsAllocatorInterface::EvictionCandidate candidate;
    candidate.key = key;
    candidate.shard_idx = static_cast<int>(bucket_id);
    candidate.offset = layout->offset;
    candidate.descriptor =
        MakeBucketDescriptor(BucketDataPath(bucket_id), *layout, bucket_id);
    return candidate;
}

bool ImmutableBucketAllocator::CollectCandidatesFromPlacedLocked(
    const BucketState& bucket,
    std::vector<DfsAllocatorInterface::EvictionCandidate>& candidates) const {
    // Fold duplicate keys the same way SnapshotLocked does: the last
    // reservation for a key is the one that can still be live.
    std::unordered_map<std::string, const PlacedEntry*> latest;
    for (const auto& placed : bucket.placed) {
        latest[placed.key] = &placed;
    }
    for (const auto& [key, placed] : latest) {
        auto index_it = key_index_.find(key);
        if (index_it == key_index_.end() ||
            index_it->second.bucket_id != bucket.bucket_id ||
            !IsLive(index_it->second.state)) {
            continue;  // dead or owned by another bucket: nothing to validate
        }
        auto candidate = MakeEvictionCandidate(
            bucket.bucket_id, key, placed->entry_offset, placed->object_size);
        if (!candidate) {
            LOG(ERROR) << "Skipping DFS eviction of bucket " << bucket.bucket_id
                       << ": entry for key " << key
                       << " has an inconsistent layout";
            return false;
        }
        candidates.push_back(std::move(*candidate));
    }
    return true;
}

bool ImmutableBucketAllocator::CollectCandidatesFromSnapshotLocked(
    int64_t bucket_id, const PersistedBucketMetadata& snapshot,
    std::vector<DfsAllocatorInterface::EvictionCandidate>& candidates) const {
    for (const auto& persisted : snapshot.entries) {
        auto index_it = key_index_.find(persisted.key);
        if (index_it == key_index_.end() ||
            index_it->second.bucket_id != bucket_id ||
            !IsLive(index_it->second.state)) {
            continue;  // tombstoned or reallocated: nothing to evict
        }
        auto candidate = MakeEvictionCandidate(
            bucket_id, persisted.key, persisted.entry_offset,
            persisted.value_size);
        if (!candidate) {
            LOG(ERROR) << "Skipping DFS eviction of bucket " << bucket_id
                       << ": entry for key " << persisted.key
                       << " has an inconsistent layout";
            return false;
        }
        candidates.push_back(std::move(*candidate));
    }
    return true;
}

void ImmutableBucketAllocator::MarkMetaDirtyLocked(BucketState& bucket) {
    // An active bucket has no `.meta` file yet and is not supposed to get one,
    // so there is nothing to keep in sync until it is sealed. Sealing sets the
    // flag itself, which is what makes the first write happen.
    if (bucket.sealed) bucket.meta_dirty = true;
}

void ImmutableBucketAllocator::SealActiveBucketLocked() {
    if (active_bucket_id_ < 0) return;
    auto it = buckets_.find(active_bucket_id_);
    active_bucket_id_ = -1;
    if (it == buckets_.end()) return;
    BucketState& bucket = *it->second;
    if (bucket.sealed) return;
    bucket.sealed = true;
    // The bucket accepts no further allocations, so this is the moment its
    // metadata becomes worth persisting. The write itself happens outside the
    // lock, in FlushDirtyMetadata().
    bucket.meta_dirty = true;
}

tl::expected<void, ErrorCode> ImmutableBucketAllocator::PersistMetadata(
    const PersistedBucketMetadata& snapshot) {
    if (!fs_adapter_) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    std::string payload;
    try {
        struct_pb::to_pb(snapshot, payload);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize bucket metadata, bucket_id="
                   << snapshot.bucket_id << ", error=" << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const std::string meta_path = BucketMetaPath(snapshot.bucket_id);
    bool meta_existed = false;
    auto meta_exists_result = fs_adapter_->FileExists(meta_path);
    if (meta_exists_result) meta_existed = *meta_exists_result;

    // Overwrite the bucket's single `.meta` file in place: one file per bucket
    // is the point, so no temporary file and no rename are involved. A torn
    // rewrite loses this bucket's metadata entirely, and recovery then removes
    // its data file - the accepted failure mode.
    auto write_result = fs_adapter_->WriteFile(
        meta_path, std::span<const char>(payload.data(), payload.size()));
    if (!write_result) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to write bucket metadata " << meta_path
                       << ", error=" << write_result.error()
                       << ", suppressed=" << *suppressed;
        }
        return tl::make_unexpected(write_result.error());
    }
    if (*write_result != payload.size()) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Short write of bucket metadata " << meta_path
                       << ", expected=" << payload.size()
                       << ", actual=" << *write_result
                       << ", suppressed=" << *suppressed;
        }
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    auto sync_file = fs_adapter_->SyncFile(meta_path);
    if (!sync_file) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to fsync bucket metadata " << meta_path
                       << ", error=" << sync_file.error()
                       << ", suppressed=" << *suppressed;
        }
        return tl::make_unexpected(sync_file.error());
    }

    // Creating a directory entry needs one directory sync. Rewriting an
    // existing file changes no namespace metadata and deliberately avoids that
    // DFS lock.
    if (!meta_existed) {
        auto sync_dir = fs_adapter_->SyncDirectory(fsdir_);
        if (!sync_dir) {
            static DfsLogLimiter log_limiter;
            if (auto suppressed = log_limiter.ShouldLog()) {
                LOG(ERROR) << "Failed to fsync DFS bucket directory " << fsdir_
                           << ", error=" << sync_dir.error()
                           << ", suppressed=" << *suppressed;
            }
            return tl::make_unexpected(sync_dir.error());
        }
    }
    return {};
}

void ImmutableBucketAllocator::DeleteBucketFiles(int64_t bucket_id) {
    if (!fs_adapter_) return;
    auto meta_result = fs_adapter_->DeleteFile(BucketMetaPath(bucket_id));
    if (!meta_result && meta_result.error() != ErrorCode::FILE_NOT_FOUND) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to delete bucket metadata file for bucket_id="
                       << bucket_id << ", error=" << meta_result.error()
                       << ", suppressed=" << *suppressed;
        }
    }
    auto data_result = fs_adapter_->DeleteFile(BucketDataPath(bucket_id));
    if (!data_result && data_result.error() != ErrorCode::FILE_NOT_FOUND) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(ERROR) << "Failed to delete bucket data file for bucket_id="
                       << bucket_id << ", error=" << data_result.error()
                       << ", suppressed=" << *suppressed;
        }
    }
}

// === bucket lifecycle ===

size_t ImmutableBucketAllocator::EffectiveReadyTargetLocked() const {
    return EffectiveReadyTargetLocked(0);
}

size_t ImmutableBucketAllocator::EffectiveReadyTargetLocked(
    size_t recovered_count) const {
    const size_t ready_count = ready_bucket_ids_.size();
    // `recovered_count` folds in buckets that are still on disk and about to be
    // published by RecoverBucketsProgressively(), so the ready pool does not
    // over-create during startup and leave the allocator above its budget.
    const size_t non_ready_count =
        buckets_.size() - ready_count + recovered_count;
    if (max_bucket_count_ > 0 &&
        non_ready_count >= static_cast<size_t>(max_bucket_count_)) {
        return 0;
    }
    const size_t available =
        max_bucket_count_ > 0
            ? static_cast<size_t>(max_bucket_count_) - non_ready_count
            : ready_bucket_target_;
    return std::min(ready_bucket_target_, available);
}

bool ImmutableBucketAllocator::CanReserveBucketCreationLocked() const {
    if (stop_bucket_workers_ || next_bucket_id_ > kMaxBucketId) return false;
    return max_bucket_count_ <= 0 ||
           buckets_.size() + bucket_creations_in_flight_ <
               static_cast<size_t>(max_bucket_count_);
}

std::optional<ImmutableBucketAllocator::BucketCreationReservation>
ImmutableBucketAllocator::ReserveBucketCreationLocked() {
    if (!CanReserveBucketCreationLocked()) return std::nullopt;
    BucketCreationReservation reservation;
    reservation.bucket_id = next_bucket_id_++;
    ++bucket_creations_in_flight_;
    try {
        UpdateBucketPoolMetricsLocked();
    } catch (...) {
        --bucket_creations_in_flight_;
        throw;
    }
    return reservation;
}

void ImmutableBucketAllocator::FinishBucketCreationLocked(bool synchronous) {
    CHECK_GT(bucket_creations_in_flight_, 0);
    --bucket_creations_in_flight_;
    if (synchronous) synchronous_creation_in_flight_ = false;
    bucket_pool_cv_.notify_all();
}

void ImmutableBucketAllocator::RollbackBucketPublicationLocked(
    int64_t bucket_id, const BucketPtr& bucket) {
    auto ready_it =
        std::find(ready_bucket_ids_.begin(), ready_bucket_ids_.end(), bucket_id);
    if (ready_it != ready_bucket_ids_.end()) ready_bucket_ids_.erase(ready_it);
    RemoveFromLruLocked(bucket_id);
    if (active_bucket_id_ == bucket_id) active_bucket_id_ = -1;
    auto it = buckets_.find(bucket_id);
    if (it != buckets_.end() && it->second == bucket) {
        buckets_.erase(it);
        PublishBucketIdSnapshotLocked();
    }
}

bool ImmutableBucketAllocator::ActiveBucketHasSpaceLocked(
    uint64_t required, BucketPtr* bucket) const {
    if (active_bucket_id_ < 0) return false;
    auto it = buckets_.find(active_bucket_id_);
    if (it == buckets_.end() || it->second->ready || it->second->sealed ||
        it->second->frozen) {
        return false;
    }
    auto entry_start = CheckedAlignUp(it->second->append_offset, alignment_);
    if (!entry_start || *entry_start > it->second->capacity ||
        required > it->second->capacity - *entry_start) {
        return false;
    }
    if (bucket != nullptr) *bucket = it->second;
    return true;
}

void ImmutableBucketAllocator::RequestRefillLocked() {
    // Waiting allocations must not bypass an existing failure backoff.
    bucket_pool_cv_.notify_all();
}

void ImmutableBucketAllocator::RecordBucketCreationFailureLocked(ErrorCode error) {
    ++bucket_creation_failure_sequence_;
    last_bucket_creation_error_ = error;
    bucket_creation_retry_after_ =
        std::chrono::steady_clock::now() + bucket_creation_retry_delay_;
    bucket_creation_retry_delay_ =
        std::min(bucket_creation_retry_delay_ * 2, std::chrono::seconds(30));
}

ImmutableBucketAllocator::BucketPtr
ImmutableBucketAllocator::PromoteReadyBucketLocked() {
    while (!ready_bucket_ids_.empty()) {
        const int64_t bucket_id = ready_bucket_ids_.front();
        ready_bucket_ids_.pop_front();
        auto it = buckets_.find(bucket_id);
        if (it == buckets_.end() || !it->second->ready) continue;

        SealActiveBucketLocked();
        it->second->ready = false;
        active_bucket_id_ = bucket_id;
        TouchLruLocked(bucket_id, CurrentTimeNs());
        UpdateBucketPoolMetricsLocked();
        RequestRefillLocked();
        return it->second;
    }
    UpdateBucketPoolMetricsLocked();
    return nullptr;
}

std::vector<int64_t> ImmutableBucketAllocator::DetachReadyBucketsLocked() {
    std::vector<int64_t> bucket_ids;
    bucket_ids.reserve(ready_bucket_ids_.size());
    for (const int64_t bucket_id : ready_bucket_ids_) {
        auto it = buckets_.find(bucket_id);
        if (it == buckets_.end() || !it->second->ready) continue;
        bucket_ids.push_back(bucket_id);
        buckets_.erase(it);
    }
    ready_bucket_ids_.clear();
    return bucket_ids;
}

void ImmutableBucketAllocator::UpdateBucketPoolMetricsLocked() const {
    MasterMetricManager::instance().set_dfs_bucket_pool_state(
        ready_bucket_ids_.size(), bucket_creations_in_flight_,
        static_cast<uint64_t>(ready_bucket_ids_.size()) * bucket_capacity_);
}

void ImmutableBucketAllocator::StopBucketWorkers() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_bucket_workers_ = true;
        initialization_creations_remaining_ = 0;
        bucket_pool_cv_.notify_all();
    }
    for (auto& worker : bucket_create_workers_) {
        if (worker.joinable()) worker.join();
    }
    bucket_create_workers_.clear();
    // The legacy sweep deletes files through the fs adapter, so it must finish
    // before the adapter is shut down.
    if (legacy_cleanup_worker_.joinable()) {
        legacy_cleanup_worker_.join();
    }
}

void ImmutableBucketAllocator::LegacyCleanupWorker(
    std::vector<int64_t> bucket_ids) {
    // Deleting these files needs no allocator lock: every id is below
    // next_bucket_id_, so no bucket this process creates or evicts reuses a
    // name in this list, and DeleteBucketFiles() touches only the fs adapter.
    // DeleteBucketFiles() tolerates an already-absent `.meta` or `.data`, so an
    // id that was recorded from only one of the two files - or whose file was
    // removed before the sweep reached it - is not a failure.
    for (const int64_t bucket_id : bucket_ids) {
        DeleteBucketFiles(bucket_id);
    }
    LOG(INFO) << "DFS legacy bucket cleanup finished, buckets="
              << bucket_ids.size();
}

void ImmutableBucketAllocator::BucketCreateWorker() {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto needs_creation = [this] {
        return CanReserveBucketCreationLocked() &&
               ready_bucket_ids_.size() + bucket_creations_in_flight_ <
                   EffectiveReadyTargetLocked();
    };

    while (!stop_bucket_workers_) {
        while (!stop_bucket_workers_ && !needs_creation()) {
            bucket_pool_cv_.wait(lock);
        }
        if (stop_bucket_workers_) break;

        const auto now = std::chrono::steady_clock::now();
        if (initialization_creations_remaining_ == 0 &&
            now < bucket_creation_retry_after_) {
            const auto retry_after = bucket_creation_retry_after_;
            bucket_pool_cv_.wait_until(
                lock, retry_after, [this, retry_after, &needs_creation] {
                    return stop_bucket_workers_ ||
                           initialization_creations_remaining_ > 0 ||
                           bucket_creation_retry_after_ < retry_after ||
                           !needs_creation();
                });
            continue;
        }

        std::optional<BucketCreationReservation> reservation;
        int64_t reserved_bucket_id = -1;
        BucketPtr bucket;
        const bool initialization_attempt =
            initialization_creations_remaining_ > 0;
        bool initialization_consumed = false;
        try {
            reservation = ReserveBucketCreationLocked();
            if (!reservation) continue;
            reserved_bucket_id = reservation->bucket_id;
            if (initialization_attempt) {
                --initialization_creations_remaining_;
                initialization_consumed = true;
            }

            // After a failure, allow only one worker to retry per deadline.
            if (!initialization_attempt &&
                last_bucket_creation_error_ != ErrorCode::OK) {
                bucket_creation_retry_after_ = std::chrono::steady_clock::now() +
                                               bucket_creation_retry_delay_;
            }

            lock.unlock();
            const auto create_started = std::chrono::steady_clock::now();
            auto preallocated = fs_adapter_->PreallocateFile(
                BucketDataPath(reserved_bucket_id), bucket_capacity_);
            const auto create_latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - create_started)
                    .count();
            MasterMetricManager::instance()
                .observe_dfs_bucket_create_latency_us(create_latency_us);
            lock.lock();

            const bool within_capacity =
                max_bucket_count_ <= 0 ||
                buckets_.size() + bucket_creations_in_flight_ <=
                    static_cast<size_t>(max_bucket_count_);
            const bool within_target =
                ready_bucket_ids_.size() + bucket_creations_in_flight_ <=
                EffectiveReadyTargetLocked();
            const bool publish = preallocated && within_capacity &&
                                 within_target && !stop_bucket_workers_;
            bool delete_file = false;
            const char* result = "failure";
            if (publish) {
                bucket = std::make_shared<BucketState>();
                bucket->bucket_id = reserved_bucket_id;
                bucket->capacity = bucket_capacity_;
                bucket->last_access_ns = CurrentTimeNs();
                bucket->ready = true;
                const bool inserted =
                    buckets_.emplace(bucket->bucket_id, bucket).second;
                if (!inserted) throw std::runtime_error("duplicate bucket id");
                ready_bucket_ids_.push_back(bucket->bucket_id);
                PublishBucketIdSnapshotLocked();
                bucket_creation_retry_after_ =
                    std::chrono::steady_clock::time_point::min();
                bucket_creation_retry_delay_ = kBucketCreationRetryDelay;
                last_bucket_creation_error_ = ErrorCode::OK;
                result = "success";
            } else if (preallocated) {
                delete_file = true;
                result = "discarded";
            } else {
                delete_file = preallocated.error() != ErrorCode::FILE_OPEN_FAIL;
                RecordBucketCreationFailureLocked(preallocated.error());
            }

            FinishBucketCreationLocked(/*synchronous=*/false);
            reservation.reset();
            const size_t ready_count = ready_bucket_ids_.size();
            const size_t in_flight = bucket_creations_in_flight_;
            const int64_t max_bucket_count = max_bucket_count_;
            UpdateBucketPoolMetricsLocked();
            lock.unlock();

            MasterMetricManager::instance().inc_dfs_bucket_create_total(result);
            if (!preallocated) {
                static DfsLogLimiter precreate_log_limiter;
                if (auto suppressed = precreate_log_limiter.ShouldLog()) {
                    LOG(WARNING)
                        << "Failed to precreate DFS bucket, bucket_id="
                        << reserved_bucket_id
                        << ", error=" << preallocated.error()
                        << ", ready_count=" << ready_count
                        << ", in_flight=" << in_flight
                        << ", max_bucket_count=" << max_bucket_count
                        << ", suppressed=" << *suppressed;
                }
            } else if (!publish) {
                VLOG(1)
                    << "Discarded precreated DFS bucket after state change, "
                    << "bucket_id=" << reserved_bucket_id;
            }
            if (delete_file) DeleteBucketFiles(reserved_bucket_id);

            lock.lock();
        } catch (...) {
            if (!lock.owns_lock()) lock.lock();
            if (initialization_attempt && !initialization_consumed) {
                --initialization_creations_remaining_;
            }
            if (reservation) {
                RollbackBucketPublicationLocked(reserved_bucket_id, bucket);
                FinishBucketCreationLocked(/*synchronous=*/false);
            }
            RecordBucketCreationFailureLocked(ErrorCode::INTERNAL_ERROR);
            bucket_pool_cv_.notify_all();
            lock.unlock();
            if (reservation) DeleteBucketFiles(reserved_bucket_id);
            LOG(ERROR) << "Unexpected exception while creating DFS bucket; "
                          "the ready bucket pool will retry";
            lock.lock();
        }
    }
}

tl::expected<ImmutableBucketAllocator::BucketPtr, ErrorCode>
ImmutableBucketAllocator::CreateBucketUnlocked(
    std::unique_lock<std::mutex>& lock) {
    std::optional<BucketCreationReservation> reservation;
    try {
        reservation = ReserveBucketCreationLocked();
    } catch (...) {
        LOG(ERROR) << "Unexpected exception while reserving a DFS bucket";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (!reservation) {
        if (next_bucket_id_ > kMaxBucketId) {
            LOG(ERROR) << "DFS bucket id space exhausted, next_bucket_id="
                       << next_bucket_id_;
        }
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    synchronous_creation_in_flight_ = true;

    ErrorCode error = ErrorCode::OK;
    BucketPtr bucket;
    bool preallocated = false;
    bool publish = false;
    try {
        lock.unlock();
        const auto create_started = std::chrono::steady_clock::now();
        auto result = fs_adapter_->PreallocateFile(
            BucketDataPath(reservation->bucket_id), bucket_capacity_);
        preallocated = result.has_value();
        if (!result) error = result.error();
        const auto create_latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - create_started)
                .count();
        MasterMetricManager::instance().observe_dfs_bucket_create_latency_us(
            create_latency_us);
        lock.lock();

        const bool within_capacity =
            max_bucket_count_ <= 0 ||
            buckets_.size() + bucket_creations_in_flight_ <=
                static_cast<size_t>(max_bucket_count_);
        publish = preallocated && within_capacity && !stop_bucket_workers_;
        if (publish) {
            bucket = std::make_shared<BucketState>();
            bucket->bucket_id = reservation->bucket_id;
            bucket->capacity = bucket_capacity_;
            bucket->last_access_ns = CurrentTimeNs();
            const bool inserted =
                buckets_.emplace(bucket->bucket_id, bucket).second;
            if (!inserted) throw std::runtime_error("duplicate bucket id");
            SealActiveBucketLocked();
            active_bucket_id_ = bucket->bucket_id;
            TouchLruLocked(bucket->bucket_id, bucket->last_access_ns);
            PublishBucketIdSnapshotLocked();
        } else if (preallocated) {
            error = ErrorCode::NO_AVAILABLE_HANDLE;
        }
    } catch (...) {
        if (!lock.owns_lock()) lock.lock();
        RollbackBucketPublicationLocked(reservation->bucket_id, bucket);
        FinishBucketCreationLocked(/*synchronous=*/true);
        UpdateBucketPoolMetricsLocked();
        lock.unlock();
        DeleteBucketFiles(reservation->bucket_id);
        lock.lock();
        LOG(ERROR) << "Unexpected exception while publishing DFS bucket "
                   << reservation->bucket_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    FinishBucketCreationLocked(/*synchronous=*/true);
    UpdateBucketPoolMetricsLocked();

    lock.unlock();
    MasterMetricManager::instance().inc_dfs_bucket_create_total(
        publish ? "success" : (preallocated ? "discarded" : "failure"));
    if (!publish && (preallocated || error != ErrorCode::FILE_OPEN_FAIL)) {
        DeleteBucketFiles(reservation->bucket_id);
    }
    if (publish) FlushDirtyMetadata();
    lock.lock();

    if (!publish) {
        if (!preallocated) {
            static DfsLogLimiter log_limiter;
            if (auto suppressed = log_limiter.ShouldLog()) {
                LOG(ERROR) << "Failed to create DFS bucket "
                           << reservation->bucket_id << ", error=" << error
                           << ", suppressed=" << *suppressed;
            }
        }
        return tl::make_unexpected(error);
    }
    return bucket;
}

tl::expected<ImmutableBucketAllocator::BucketPtr, ErrorCode>
ImmutableBucketAllocator::EnsureActiveBucket(std::unique_lock<std::mutex>& lock,
                                          uint64_t required) {
    if (required > bucket_capacity_) {
        // Refuse rather than spill across buckets: the caller asked for one
        // contiguous region and no bucket can ever satisfy it.
        LOG(ERROR) << "DFS EnsureActiveBucket rejected: object exceeds "
                   << "bucket capacity, required=" << required
                   << ", bucket_capacity=" << bucket_capacity_;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::optional<std::chrono::steady_clock::time_point> rollover_started;
    bool pool_exhaustion_recorded = false;
    const auto observe_rollover = [&rollover_started] {
        if (!rollover_started) return;
        const auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - *rollover_started)
                .count();
        MasterMetricManager::instance().observe_dfs_bucket_rollover_latency_us(
            latency_us);
    };

    while (true) {
        BucketPtr active;
        if (ActiveBucketHasSpaceLocked(required, &active)) {
            observe_rollover();
            return active;
        }
        if (!rollover_started && active_bucket_id_ >= 0) {
            rollover_started = std::chrono::steady_clock::now();
        }
        if (stop_bucket_workers_) {
            observe_rollover();
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }

        if (ready_bucket_target_ == 0) {
            if (synchronous_creation_in_flight_) {
                bucket_pool_cv_.wait(lock, [this] {
                    return stop_bucket_workers_ ||
                           !synchronous_creation_in_flight_;
                });
                continue;
            }
            auto created = CreateBucketUnlocked(lock);
            if (!created) {
                observe_rollover();
                return tl::make_unexpected(created.error());
            }
            continue;
        }

        if (!ready_bucket_ids_.empty()) {
            auto promoted = PromoteReadyBucketLocked();
            if (promoted) {
                // Persist the bucket just sealed by the promotion before the
                // allocation returns, preserving the existing crash boundary.
                lock.unlock();
                FlushDirtyMetadata();
                lock.lock();
                continue;
            }
        }

        if (!CanReserveBucketCreationLocked() &&
            bucket_creations_in_flight_ == 0) {
            observe_rollover();
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        if (!pool_exhaustion_recorded) {
            pool_exhaustion_recorded = true;
            MasterMetricManager::instance()
                .inc_dfs_bucket_pool_exhausted_total();
            static DfsLogLimiter pool_log_limiter;
            if (auto suppressed = pool_log_limiter.ShouldLog()) {
                LOG(WARNING)
                    << "DFS ready bucket pool exhausted, ready_count="
                    << ready_bucket_ids_.size()
                    << ", in_flight=" << bucket_creations_in_flight_
                    << ", waiting_allocators=" << waiting_allocators_
                    << ", active_bucket_id=" << active_bucket_id_
                    << ", max_bucket_count=" << max_bucket_count_
                    << ", suppressed=" << *suppressed;
            }
        }

        if (bucket_creations_in_flight_ == 0 &&
            std::chrono::steady_clock::now() < bucket_creation_retry_after_) {
            observe_rollover();
            return tl::make_unexpected(last_bucket_creation_error_);
        }
        const uint64_t failure_sequence = bucket_creation_failure_sequence_;
        RequestRefillLocked();
        ++waiting_allocators_;
        const auto wait_started = std::chrono::steady_clock::now();
        bucket_pool_cv_.wait(lock, [this, required, failure_sequence] {
            return stop_bucket_workers_ ||
                   ActiveBucketHasSpaceLocked(required) ||
                   !ready_bucket_ids_.empty() ||
                   (bucket_creation_failure_sequence_ != failure_sequence &&
                    bucket_creations_in_flight_ == 0) ||
                   (!CanReserveBucketCreationLocked() &&
                    bucket_creations_in_flight_ == 0);
        });
        --waiting_allocators_;
        const auto wait_latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - wait_started)
                .count();
        MasterMetricManager::instance().observe_dfs_bucket_pool_wait_latency_us(
            wait_latency_us);

        if (stop_bucket_workers_) {
            observe_rollover();
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        if (ready_bucket_ids_.empty() &&
            !ActiveBucketHasSpaceLocked(required) &&
            bucket_creations_in_flight_ == 0 &&
            bucket_creation_failure_sequence_ != failure_sequence) {
            const ErrorCode error = last_bucket_creation_error_ == ErrorCode::OK
                                        ? ErrorCode::FILE_WRITE_FAIL
                                        : last_bucket_creation_error_;
            observe_rollover();
            return tl::make_unexpected(error);
        }
    }
}

void ImmutableBucketAllocator::TouchLruLocked(int64_t bucket_id, int64_t now_ns) {
    auto index_it = lru_index_.find(bucket_id);
    if (index_it != lru_index_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_, index_it->second);
    } else {
        lru_list_.push_front(bucket_id);
        try {
            lru_index_.emplace(bucket_id, lru_list_.begin());
        } catch (...) {
            lru_list_.pop_front();
            throw;
        }
    }
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it != buckets_.end()) {
        bucket_it->second->last_access_ns = now_ns;
    }
}

void ImmutableBucketAllocator::RemoveFromLruLocked(int64_t bucket_id) {
    auto index_it = lru_index_.find(bucket_id);
    if (index_it == lru_index_.end()) return;
    lru_list_.erase(index_it->second);
    lru_index_.erase(index_it);
}

tl::expected<DistributedFSDescriptor, ErrorCode>
ImmutableBucketAllocator::ReserveInBucketLocked(BucketState& bucket,
                                             const std::string& key,
                                             uint64_t size) {
    auto layout =
        ComputeBucketEntryLayout(bucket.append_offset, size, alignment_);
    if (!layout) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (layout->end() > bucket.capacity) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // A live entry for this key already sits somewhere - either in this bucket
    // or in another one. key_index_ is the single source of truth for liveness,
    // so one lookup rejects both cases and no orphan can be created: the guard
    // must live here because a concurrent batch can reserve the key during an
    // EnsureActiveBucket unlock window.
    if (key_index_.find(key) != key_index_.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    PlacedEntry placed;
    placed.key = key;
    placed.entry_offset = layout->offset;
    placed.object_size = size;
    placed.reserved_size = layout->aligned_size;
    placed.generation = next_generation_++;

    bucket.placed.push_back(std::move(placed));
    bucket.append_offset = layout->end();
    ++bucket.pending_count;
    bucket.live_bytes += layout->aligned_size;
    key_index_[key] =
        LiveKeyRecord{bucket.bucket_id, BucketEntryState::PENDING};

    // Reservations in the active bucket touch no file at all. A reservation in
    // an already sealed bucket cannot happen (it accepts no allocations), but
    // marking it keeps the invariant local instead of implicit.
    MarkMetaDirtyLocked(bucket);

    auto descriptor = MakeBucketDescriptor(BucketDataPath(bucket.bucket_id),
                                          *layout, bucket.bucket_id);
    return descriptor;
}

void ImmutableBucketAllocator::UnreserveInBucketLocked(
    BucketState& bucket, const std::string& key,
    const DistributedFSDescriptor& descriptor) {
    auto matches = [&key, &descriptor](const PlacedEntry& placed) {
        return placed.key == key &&
               placed.object_size == descriptor.object_size &&
               placed.reserved_size == descriptor.aligned_size;
    };
    // BatchAllocate unwinds in reverse order, so the reservation being undone
    // is normally the most recent placement and the reverse scan below matches
    // on its first step. A concurrent batch can interleave its own reservation
    // into the same bucket during an EnsureActiveBucket unlock window, though;
    // silently skipping a non-tail unreserve would leak a PENDING index record
    // and strand the bucket's pending_count forever. Scanning from the back
    // also picks the live placement when a key was freed and re-placed.
    auto placed_it = bucket.placed.end();
    for (auto rit = bucket.placed.rbegin(); rit != bucket.placed.rend();
         ++rit) {
        if (matches(*rit)) {
            placed_it = std::prev(rit.base());
            break;
        }
    }
    if (placed_it == bucket.placed.end()) return;

    const uint64_t reserved_size = placed_it->reserved_size;
    const uint64_t entry_end = placed_it->entry_offset + reserved_size;
    if (std::next(placed_it) == bucket.placed.end() &&
        bucket.append_offset == entry_end) {
        // Only the most recent reservation can give its space back; an entry
        // erased from the middle stays reserved (the bucket is append-only).
        bucket.append_offset = placed_it->entry_offset;
    }

    // The reservation existed only in memory, so undoing it means dropping the
    // placement and its index record. If the bucket happens to be sealed
    // already, its `.meta` file has to be rewritten without this entry.
    MarkMetaDirtyLocked(bucket);

    bucket.placed.erase(placed_it);
    if (bucket.pending_count > 0) --bucket.pending_count;
    // The reservation no longer holds live bytes. The space itself stays
    // reserved in the append-only bucket until the whole bucket is evicted.
    if (bucket.live_bytes >= reserved_size) {
        bucket.live_bytes -= reserved_size;
    } else {
        bucket.live_bytes = 0;
    }

    auto index_it = key_index_.find(key);
    if (index_it != key_index_.end() &&
        index_it->second.bucket_id == bucket.bucket_id) {
        key_index_.erase(index_it);
    }
}

ImmutableBucketAllocator::LiveKeyRecord*
ImmutableBucketAllocator::FindMatchingEntryLocked(
    const std::string& key, const DistributedFSDescriptor& desc,
    bool verify_placement_offset, BucketPtr* out_bucket) {
    auto fail = [&key, &desc](const char* reason) -> LiveKeyRecord* {
        LOG(ERROR) << "DFS entry match failed: reason=" << reason
                   << ", key=" << key << ", bucket_id=" << desc.shard_idx
                   << ", offset=" << desc.offset;
        return nullptr;
    };

    if (desc.shard_idx < 0) return fail("invalid_bucket_id");
    const int64_t bucket_id = static_cast<int64_t>(desc.shard_idx);

    // Identity check only: the key must be live and owned by the bucket the
    // descriptor names. A durable bucket no longer keeps its per-key layout in
    // memory, so the descriptor's own fields are the only reference left.
    auto index_it = key_index_.find(key);
    if (index_it == key_index_.end()) return fail("key_not_indexed");
    if (index_it->second.bucket_id != bucket_id) {
        return fail("key_index_bucket_mismatch");
    }
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it == buckets_.end()) return fail("bucket_not_found");

    // Reject a descriptor whose size/offset are internally inconsistent, so a
    // truncated or superseded descriptor cannot address a valid entry.
    auto layout =
        RebuildBucketEntryLayout(desc.offset, desc.object_size, alignment_);
    if (!layout) return fail("invalid_descriptor_layout");
    if (layout->aligned_size != desc.aligned_size) {
        return fail("aligned_size_mismatch");
    }
    if (layout->end() > bucket_it->second->capacity) {
        return fail("offset_out_of_bounds");
    }

    // Stale-descriptor guard: a key can only be re-placed into the same bucket
    // while that bucket is still active, and for such a bucket the placements
    // are still in memory, so a superseded generation's descriptor can be told
    // apart by its offset. Once the bucket is durable the re-placement window
    // is closed and the identity check alone is sufficient. Mutation callers
    // (commit/free) must enable this; read-only callers may skip it.
    if (verify_placement_offset && !bucket_it->second->placed_durable) {
        // The last placement for a key is the live one; duplicates arise when
        // a key is freed and re-placed before the first snapshot.
        const PlacedEntry* live_placement = nullptr;
        for (const auto& placed : bucket_it->second->placed) {
            if (placed.key == key) live_placement = &placed;
        }
        if (live_placement == nullptr) {
            return fail("stale_descriptor_no_placement");
        }
        if (live_placement->entry_offset != desc.offset) {
            return fail("stale_descriptor_offset");
        }
        // The offset alone does not identify a placement: a descriptor whose
        // size fields were tampered with (or carried over from a superseded
        // operation) must not address the live entry either.
        if (live_placement->object_size != desc.object_size) {
            return fail("object_size_mismatch");
        }
        if (live_placement->reserved_size != desc.aligned_size) {
            return fail("aligned_size_mismatch");
        }
    }

    if (out_bucket) *out_bucket = bucket_it->second;
    return &index_it->second;
}

// === allocation ===

tl::expected<DistributedFSDescriptor, ErrorCode>
ImmutableBucketAllocator::Allocate(const std::string& key, uint64_t size) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (key.empty() || size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    BatchAllocateRequest request{key, size};
    auto results = BatchAllocate({request});
    if (results.size() != 1) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (!results[0].success) {
        return tl::make_unexpected(results[0].error);
    }
    return results[0].descriptor;
}

std::vector<BatchAllocateResult> ImmutableBucketAllocator::BatchAllocate(
    const std::vector<BatchAllocateRequest>& requests) {
    std::vector<BatchAllocateResult> results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        results.push_back(
            BatchAllocateResult{request.key, {}, false, ErrorCode::OK});
    }
    if (requests.empty()) return results;

    auto fail_all = [&results](ErrorCode error) {
        for (auto& result : results) {
            result.success = false;
            result.error = error;
            result.descriptor = DistributedFSDescriptor{};
        }
    };
    if (!initialized_.load(std::memory_order_acquire)) {
        fail_all(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        return results;
    }

    // Validate request shape before changing allocator state. A batch may span
    // buckets, but one object must always fit in one bucket. Any failure here is
    // terminal for the whole batch, so the reserve loop below can iterate every
    // request that survived.
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.key.empty() || request.size == 0) {
            fail_all(ErrorCode::INVALID_PARAMS);
            return results;
        }
        for (size_t j = 0; j < i; ++j) {
            if (requests[j].key == request.key) {
                LOG(ERROR) << "Duplicate key " << request.key
                           << " in DFS batch allocate request";
                fail_all(ErrorCode::INVALID_PARAMS);
                return results;
            }
        }
        auto layout = ComputeBucketEntryLayout(0, request.size, alignment_);
        if (!layout || layout->aligned_size > bucket_capacity_) {
            LOG(ERROR) << "DFS object for key " << request.key
                       << " exceeds bucket capacity, object_size="
                       << request.size << ", reserved_size="
                       << (layout ? layout->aligned_size : 0)
                       << ", bucket_capacity=" << bucket_capacity_;
            fail_all(ErrorCode::INVALID_PARAMS);
            return results;
        }
    }

    std::unique_lock<std::mutex> lock(mutex_);
    struct Reservation {
        size_t request_index = 0;
        int64_t bucket_id = -1;
        BucketPtr bucket_identity;
    };
    std::vector<Reservation> reserved;
    reserved.reserve(requests.size());
    size_t already_exists_count = 0;

    // Mark every not-yet-resolved request as a generic failure so a later break
    // leaves no slot at its default OK.
    auto fail_remaining = [&](size_t from_index) {
        for (size_t i = from_index; i < requests.size(); ++i) {
            results[i].success = false;
            results[i].descriptor = DistributedFSDescriptor{};
            if (results[i].error == ErrorCode::OK) {
                results[i].error = ErrorCode::NO_AVAILABLE_HANDLE;
            }
        }
    };

    // Append-pack in request order. EnsureActiveBucket receives only the
    // current object's reserved size, so a partially filled active bucket is
    // used before a new bucket is created. A bucket boundary can therefore
    // occur only between two objects, never inside one object.
    for (size_t index = 0; index < requests.size(); ++index) {
        const auto& request = requests[index];
        // Dedup right before the reserve, under the same lock hold. A key can
        // be live either from a prior generation still PENDING in this bucket
        // or from a concurrent batch that reserved it during the previous
        // iteration's EnsureActiveBucket window; either way, skip it.
        if (key_index_.count(request.key) != 0) {
            results[index].success = false;
            results[index].descriptor = DistributedFSDescriptor{};
            results[index].error = ErrorCode::OBJECT_ALREADY_EXISTS;
            ++already_exists_count;
            continue;
        }
        auto object_layout =
            ComputeBucketEntryLayout(0, request.size, alignment_);
        if (!object_layout) {
            // Per-key failure: this key's shape is invalid, but subsequent
            // keys may still allocate successfully.
            LOG(ERROR) << "DFS batch allocate layout failed: key="
                       << request.key << ", size=" << request.size
                       << ", error=invalid_layout";
            results[index].success = false;
            results[index].descriptor = DistributedFSDescriptor{};
            results[index].error = ErrorCode::INVALID_PARAMS;
            fail_remaining(index + 1);
            break;
        }
        auto bucket_result =
            EnsureActiveBucket(lock, object_layout->aligned_size);
        if (!bucket_result) {
            // Global failure: no active bucket can be created or found, so
            // remaining keys will fail the same way. Mark them and stop.
            static DfsLogLimiter log_limiter;
            if (auto suppressed = log_limiter.ShouldLog()) {
                LOG(ERROR) << "DFS batch allocate active bucket failed: key="
                           << request.key << ", reserved_size="
                           << object_layout->aligned_size
                           << ", error=" << bucket_result.error()
                           << ", suppressed=" << *suppressed;
            }
            results[index].success = false;
            results[index].descriptor = DistributedFSDescriptor{};
            results[index].error = bucket_result.error();
            fail_remaining(index + 1);
            break;
        }
        auto bucket = bucket_result.value();
        auto descriptor =
            ReserveInBucketLocked(*bucket, request.key, request.size);
        if (!descriptor) {
            // Per-key failure: this key does not fit in the current active
            // bucket, but subsequent keys (which may be smaller) might.
            // OBJECT_ALREADY_EXISTS means a concurrent batch reserved the key
            // during this iteration's EnsureActiveBucket window; record it and
            // keep packing the rest instead of unwinding the whole batch.
            results[index].success = false;
            results[index].descriptor = DistributedFSDescriptor{};
            results[index].error = descriptor.error();
            if (descriptor.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                // A concurrent batch reserved the key during this iteration's
                // EnsureActiveBucket window; keep packing the rest instead of
                // unwinding the whole batch.
                ++already_exists_count;
                continue;
            }
            LOG(ERROR) << "DFS batch allocate reserve failed: key="
                       << request.key << ", size=" << request.size
                       << ", bucket_id=" << bucket->bucket_id
                       << ", append_offset=" << bucket->append_offset
                       << ", error=" << descriptor.error();
            fail_remaining(index + 1);
            break;
        }
        results[index].descriptor = std::move(descriptor.value());
        results[index].success = true;
        results[index].error = ErrorCode::OK;
        reserved.push_back({index, bucket->bucket_id, bucket});
        TouchLruLocked(bucket->bucket_id, CurrentTimeNs());
    }

    if (reserved.size() + already_exists_count != requests.size()) {
        // Roll back in reverse reservation order, across every bucket touched
        // by this batch. Nothing was written to disk, so undoing the in-memory
        // state is all it takes.
        for (auto it = reserved.rbegin(); it != reserved.rend(); ++it) {
            auto bucket_it = buckets_.find(it->bucket_id);
            if (bucket_it != buckets_.end() &&
                bucket_it->second.get() == it->bucket_identity.get()) {
                UnreserveInBucketLocked(*bucket_it->second,
                                        requests[it->request_index].key,
                                        results[it->request_index].descriptor);
            }
            // The reservation was rolled back, so this key did not succeed
            // after all; surface a failure so the caller does not treat a
            // descriptor that no longer resolves as valid.
            results[it->request_index].success = false;
            results[it->request_index].descriptor = DistributedFSDescriptor{};
            if (results[it->request_index].error == ErrorCode::OK) {
                results[it->request_index].error =
                    ErrorCode::NO_AVAILABLE_HANDLE;
            }
        }
        lock.unlock();
        return results;
    }

    // Reservations live in memory only: no metadata I/O happens on this path,
    // which is the whole point of deferring persistence to the moment a bucket
    // is sealed.
    return results;
}

bool ImmutableBucketAllocator::MarkCommitted(
    const std::string& key, const DistributedFSDescriptor& descriptor) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DFS commit rejected: reason=allocator_not_initialized"
                   << ", key=" << key << ", bucket_id=" << descriptor.shard_idx;
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    BucketPtr bucket;
    auto* record = FindMatchingEntryLocked(
        key, descriptor, /*verify_placement_offset=*/true, &bucket);
    if (record == nullptr) return false;
    if (record->state == BucketEntryState::COMMITTED) {
        // Idempotent: a duplicate PutEnd for the same generation succeeds.
        return true;
    }
    if (record->state != BucketEntryState::PENDING) {
        LOG(ERROR) << "DFS commit rejected: reason=invalid_entry_state"
                   << ", key=" << key << ", bucket_id=" << descriptor.shard_idx
                   << ", state=" << static_cast<int32_t>(record->state);
        return false;
    }
    record->state = BucketEntryState::COMMITTED;
    if (bucket->pending_count > 0) --bucket->pending_count;
    // PutEnd never touches a file. For the active bucket the transition is
    // simply part of the state that gets written when the bucket is sealed; for
    // an already sealed bucket (a reservation that committed after the switch)
    // the flag schedules a rewrite on the maintenance path.
    MarkMetaDirtyLocked(*bucket);
    return true;
}

void ImmutableBucketAllocator::Free(const std::string& key,
                                 const DistributedFSDescriptor& descriptor) {
    if (!initialized_.load(std::memory_order_acquire)) return;

    std::lock_guard<std::mutex> lock(mutex_);
    BucketPtr bucket;
    auto* record = FindMatchingEntryLocked(
        key, descriptor, /*verify_placement_offset=*/true, &bucket);
    if (record == nullptr) {
        // Stale Free from a superseded generation: the placement offset check
        // rejects it whenever the replacement could have landed in the same
        // bucket, so it cannot drop the allocation that replaced it.
        return;
    }
    if (!IsLive(record->state)) return;

    // Buckets are append-only, so freeing never reclaims the middle of the
    // file. Dropping the key from key_index_ is what turns it into a TOMBSTONE
    // in the next snapshot; the space comes back only when the whole bucket is
    // evicted.
    if (record->state == BucketEntryState::PENDING &&
        bucket->pending_count > 0) {
        --bucket->pending_count;
    }
    // The entry stops counting as live; the descriptor's aligned slot size is
    // exactly the `reserved_size` recorded when it was placed.
    if (bucket->live_bytes >= descriptor.aligned_size) {
        bucket->live_bytes -= descriptor.aligned_size;
    } else {
        bucket->live_bytes = 0;
    }
    key_index_.erase(key);
    // The tombstone must survive a restart so the key cannot come back, but the
    // master calls Free() while holding a metadata shard lock, so no I/O may
    // happen here. Mark the bucket instead and let FlushDirtyMetadata() rewrite
    // its `.meta` file. For the still-active bucket there is nothing to rewrite
    // yet: the tombstone is simply part of what gets written when it is sealed.
    //
    // Losing a tombstone in a crash before the flush is safe: recovery only
    // revives COMMITTED entries, and the master's own metadata (which no longer
    // references the key) is the authority on visibility.
    MarkMetaDirtyLocked(*bucket);
}

size_t ImmutableBucketAllocator::FlushDirtyMetadata() {
    if (!fs_adapter_) return 0;

    // Snapshot every dirty bucket under the lock, then do the writes without
    // it.
    struct PendingWrite {
        int64_t bucket_id = -1;
        BucketPtr bucket_identity;
        // Set when the bucket's placements have never been written: the
        // snapshot is built entirely from memory and no read-back is needed.
        // Empty when the rewrite must merge the on-disk snapshot with the
        // current key states instead.
        std::optional<PersistedBucketMetadata> in_memory_snapshot;
    };
    std::vector<PendingWrite> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [bucket_id, bucket] : buckets_) {
            // An unsealed bucket is deliberately never written: it is still
            // being appended to, and its data is discarded on the next start.
            if (!bucket->sealed || !bucket->meta_dirty) continue;
            PendingWrite write;
            write.bucket_id = bucket_id;
            write.bucket_identity = bucket;
            if (!bucket->placed_durable) {
                write.in_memory_snapshot = SnapshotLocked(*bucket);
            }
            pending.push_back(std::move(write));
            // Clear the flag together with taking the snapshot. A change that
            // lands while we are unlocked sets it again and is picked up by the
            // next flush, instead of being swallowed by a late clear. It also
            // keeps a concurrent flush from writing the same file.
            bucket->meta_dirty = false;
        }
    }
    if (pending.empty()) return 0;

    // Deterministic order keeps concurrent flushes from interleaving writes to
    // the same bucket in an unpredictable sequence.
    std::sort(pending.begin(), pending.end(),
              [](const PendingWrite& lhs, const PendingWrite& rhs) {
                  return lhs.bucket_id < rhs.bucket_id;
              });

    size_t flushed = 0;
    for (auto& item : pending) {
        std::lock_guard<std::mutex> metadata_lock(metadata_io_mutex_);
        auto mark_dirty_again = [&]() {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = buckets_.find(item.bucket_id);
            if (it != buckets_.end() &&
                it->second.get() == item.bucket_identity.get()) {
                it->second->meta_dirty = true;
            }
        };
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = buckets_.find(item.bucket_id);
            if (it == buckets_.end() ||
                it->second.get() != item.bucket_identity.get()) {
                continue;
            }
        }

        PersistedBucketMetadata snapshot;
        if (item.in_memory_snapshot) {
            snapshot = std::move(*item.in_memory_snapshot);
        } else {
            // The bucket's per-key layout now lives only on disk, so the
            // rewrite starts from the existing `.meta` and refreshes the states
            // from key_index_. Offsets are immutable, so nothing else changes.
            auto loaded = ReadBucketMetadata(item.bucket_id);
            if (!loaded) {
                LOG(WARNING) << "Cannot rewrite DFS bucket metadata, bucket_id="
                             << item.bucket_id
                             << ": existing .meta is missing or invalid";
                mark_dirty_again();
                continue;
            }
            snapshot = std::move(*loaded);
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = buckets_.find(item.bucket_id);
            if (it == buckets_.end() ||
                it->second.get() != item.bucket_identity.get()) {
                continue;
            }
            ApplyKeyIndexStatesLocked(snapshot);
        }

        auto persisted = PersistMetadata(snapshot);
        if (persisted) {
            ++flushed;
            if (item.in_memory_snapshot) {
                // The first durable snapshot now holds every placement, so the
                // in-memory list is redundant and later rewrites read the file.
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = buckets_.find(item.bucket_id);
                if (it != buckets_.end() &&
                    it->second.get() == item.bucket_identity.get()) {
                    it->second->placed.clear();
                    it->second->placed_durable = true;
                }
            }
            continue;
        }
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(WARNING) << "Failed to write DFS bucket metadata, bucket_id="
                         << item.bucket_id << ", error=" << persisted.error()
                         << ", suppressed=" << *suppressed;
        }
        // Put the flag back so the next maintenance tick retries, unless the
        // bucket changed identity meanwhile - then it is not ours to mark.
        mark_dirty_again();
    }
    return flushed;
}

void ImmutableBucketAllocator::UpdateAccess(
    const std::string& key, const DistributedFSDescriptor& descriptor) {
    if (!initialized_.load(std::memory_order_acquire)) return;

    std::lock_guard<std::mutex> lock(mutex_);
    BucketPtr bucket;
    auto* entry = FindMatchingEntryLocked(
        key, descriptor, /*verify_placement_offset=*/false, &bucket);
    if (entry == nullptr || !IsLive(entry->state)) return;
    // A frozen bucket is mid-eviction; refreshing it would fight the
    // transaction the master is currently resolving.
    if (bucket->frozen) return;
    TouchLruLocked(bucket->bucket_id, CurrentTimeNs());
}

uint64_t ImmutableBucketAllocator::GetTotalCapacity() const {
    // A fixed denominator: watermarks must not move as buckets come and go,
    // or deleting a bucket would shrink capacity in lockstep with usage and
    // eviction would never converge.
    std::lock_guard<std::mutex> lock(mutex_);
    if (max_bucket_count_ > 0) {
        return static_cast<uint64_t>(max_bucket_count_) * bucket_capacity_;
    }
    return static_cast<uint64_t>(buckets_.size()) * bucket_capacity_;
}

uint64_t ImmutableBucketAllocator::UsedBytesLocked() const {
    // Physical reservation, not live bytes: an append-only bucket keeps its
    // whole reserved prefix until the bucket itself is evicted.
    uint64_t used = 0;
    for (const auto& [bucket_id, bucket] : buckets_) {
        (void)bucket_id;
        used += bucket->append_offset;
    }
    return used;
}

uint64_t ImmutableBucketAllocator::GetUsedBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return UsedBytesLocked();
}

int64_t ImmutableBucketAllocator::SetMaxBucketCount(int64_t new_max_bucket_count) {
    std::vector<int64_t> removed_ready_buckets;
    int64_t old = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        old = max_bucket_count_;
        max_bucket_count_ = new_max_bucket_count;

        while (ready_bucket_ids_.size() > EffectiveReadyTargetLocked()) {
            const int64_t bucket_id = ready_bucket_ids_.back();
            ready_bucket_ids_.pop_back();
            auto it = buckets_.find(bucket_id);
            if (it == buckets_.end() || !it->second->ready) continue;
            buckets_.erase(it);
            removed_ready_buckets.push_back(bucket_id);
        }
        if (!removed_ready_buckets.empty()) PublishBucketIdSnapshotLocked();
        if (new_max_bucket_count > old) {
            bucket_creation_retry_after_ =
                std::chrono::steady_clock::time_point::min();
        }
        UpdateBucketPoolMetricsLocked();
    }

    for (const int64_t bucket_id : removed_ready_buckets) {
        DeleteBucketFiles(bucket_id);
    }
    bucket_pool_cv_.notify_all();
    LOG(INFO) << "Dynamic max_bucket_count changed from " << old << " to "
              << new_max_bucket_count
              << ", removed_ready_buckets=" << removed_ready_buckets.size();
    return old;
}

size_t ImmutableBucketAllocator::GetBucketCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return buckets_.size();
}

std::shared_ptr<const ImmutableBucketAllocator::BucketIdSnapshot>
ImmutableBucketAllocator::GetBucketIdSnapshot() const {
    return std::atomic_load_explicit(&bucket_id_snapshot_,
                                     std::memory_order_acquire);
}

void ImmutableBucketAllocator::PublishBucketIdSnapshotLocked() {
    auto snapshot = std::make_shared<BucketIdSnapshot>();
    snapshot->version = bucket_snapshot_version_.load(
                            std::memory_order_relaxed) +
                        1;
    snapshot->bucket_ids.reserve(buckets_.size());
    for (const auto& [bucket_id, bucket] : buckets_) {
        snapshot->bucket_ids.push_back(bucket_id);
    }
    bucket_snapshot_version_.store(snapshot->version,
                                   std::memory_order_relaxed);
    std::atomic_store_explicit(
        &bucket_id_snapshot_,
        std::shared_ptr<const BucketIdSnapshot>(std::move(snapshot)),
        std::memory_order_release);
}

std::optional<int64_t> ImmutableBucketAllocator::GetBucketIdForKey(
    const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = key_index_.find(key);
    if (it == key_index_.end()) return std::nullopt;
    return it->second.bucket_id;
}

// === eviction ===

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEviction() {
    return PrepareEvictionInternal(/*force_one=*/false);
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareInvalidation(int64_t bucket_id) {
    return PrepareInvalidationInternal(bucket_id);
}

bool ImmutableBucketAllocator::HasBucket(int64_t bucket_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return buckets_.contains(bucket_id);
}

tl::expected<bool, ErrorCode>
ImmutableBucketAllocator::IsBucketDataFilePresent(int64_t bucket_id) const {
    if (bucket_id < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto root_present = fs_adapter_->FileExists(fsdir_);
    if (!root_present || !*root_present) {
        LOG(WARNING) << "Cannot verify DFS bucket directory " << fsdir_
                     << ": filesystem is unavailable";
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return fs_adapter_->FileExists(BucketDataPath(bucket_id));
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareInvalidationInternal(int64_t bucket_id) {
    PendingEviction pending;
    if (!initialized_.load(std::memory_order_acquire) || bucket_id < 0) {
        return pending;
    }

    bool need_disk_candidates = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto bucket_it = buckets_.find(bucket_id);
        if (bucket_it == buckets_.end() || bucket_it->second->frozen) {
            return pending;
        }
        const bool was_active = active_bucket_id_ == bucket_id;
        const BucketPtr& victim = bucket_it->second;
        if (victim->pending_count != 0) return pending;

        std::vector<DfsAllocatorInterface::EvictionCandidate> candidates;
        if (!victim->placed_durable) {
            if (!CollectCandidatesFromPlacedLocked(*victim, candidates)) {
                return pending;
            }
        } else {
            need_disk_candidates = true;
        }

        victim->frozen = true;
        RemoveFromLruLocked(bucket_id);
        if (!victim->sealed) {
            victim->sealed = true;
            victim->meta_dirty = true;
        }
        if (was_active) {
            active_bucket_id_ = -1;
            RequestRefillLocked();
        }
        pending.owner_ = this;
        pending.bucket_id_ = bucket_id;
        pending.bucket_identity_ = victim;
        pending.candidates_ = std::move(candidates);
    }

    if (need_disk_candidates) {
        // The per-key layout lives only in the durable `.meta`; rebuild
        // the candidate list from it, keeping only keys still live.
        auto loaded = ReadBucketMetadata(bucket_id);
        bool usable = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = buckets_.find(bucket_id);
            if (loaded && it != buckets_.end() &&
                it->second.get() == pending.bucket_identity_.get()) {
                std::vector<DfsAllocatorInterface::EvictionCandidate> candidates;
                if (CollectCandidatesFromSnapshotLocked(bucket_id, *loaded,
                                                        candidates)) {
                    pending.candidates_ = std::move(candidates);
                    usable = true;
                }
            }
        }
        if (!usable) {
            // The bucket's entries cannot be enumerated safely. Abandon the
            // transaction (the destructor restores the bucket to service)
            // rather than reporting it as having no live entries.
            return PendingEviction{};
        }
    }

    return pending;
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEvictionForAllocationFailure() {
    return PrepareEvictionInternal(/*force_one=*/true);
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEvictionForLowUtilization(double threshold) {
    if (threshold <= 0.0 || threshold >= 1.0) {
        return {};
    }
    return PrepareEvictionInternal(/*force_one=*/false, threshold);
}

ImmutableBucketAllocator::PendingEviction
ImmutableBucketAllocator::PrepareEvictionInternal(
    bool force_one, double low_utilization_threshold) {
    PendingEviction pending;
    if (!initialized_.load(std::memory_order_acquire)) return pending;

    bool need_disk_candidates = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        const uint64_t capacity =
            max_bucket_count_ > 0
                ? static_cast<uint64_t>(max_bucket_count_) * bucket_capacity_
                : static_cast<uint64_t>(buckets_.size()) * bucket_capacity_;
        if (capacity == 0) return pending;

        const double usage = static_cast<double>(UsedBytesLocked()) /
                             static_cast<double>(capacity);
        const bool prioritize_low_utilization =
            low_utilization_threshold > 0.0 &&
            low_utilization_threshold < 1.0;
        if (!force_one && !prioritize_low_utilization) {
            if (usage >= eviction_high_watermark_) {
                eviction_active_ = true;
            }
            if (!eviction_active_) return pending;
            if (usage <= eviction_low_watermark_) {
                eviction_active_ = false;
                return pending;
            }
        }

        // Walk the LRU from the cold end and take the first bucket that is
        // neither active, frozen, nor still waiting for an async write to
        // finish.
        BucketPtr victim;
        for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
            const int64_t bucket_id = *it;
            if (bucket_id == active_bucket_id_) continue;
            auto bucket_it = buckets_.find(bucket_id);
            if (bucket_it == buckets_.end()) continue;
            if (bucket_it->second->frozen) continue;
            const auto& candidate = bucket_it->second;
            const bool is_low_utilization =
                candidate->capacity > 0 &&
                static_cast<double>(candidate->live_bytes) /
                        static_cast<double>(candidate->capacity) <
                    low_utilization_threshold;
            if (prioritize_low_utilization && !is_low_utilization) continue;
            if (candidate->pending_count != 0) continue;
            victim = candidate;
            break;
        }
        if (!victim) return pending;

        std::vector<DfsAllocatorInterface::EvictionCandidate> candidates;
        if (!victim->placed_durable) {
            // Build from the in-memory placements before freezing. An
            // inconsistent layout aborts without touching the bucket.
            if (!CollectCandidatesFromPlacedLocked(*victim, candidates)) {
                return pending;
            }
        } else {
            need_disk_candidates = true;
        }

        victim->frozen = true;
        RemoveFromLruLocked(victim->bucket_id);

        // If the master rejects eviction, normal maintenance can persist this
        // sealed state; preparing eviction itself does no metadata I/O.
        if (!victim->sealed) {
            victim->sealed = true;
            victim->meta_dirty = true;
        }

        pending.owner_ = this;
        pending.bucket_id_ = victim->bucket_id;
        pending.bucket_identity_ = victim;
        pending.candidates_ = std::move(candidates);
    }

    if (need_disk_candidates) {
        // The per-key layout lives only in the durable `.meta`; rebuild
        // the candidate list from it, keeping only keys still live.
        auto loaded = ReadBucketMetadata(pending.bucket_id_);
        bool usable = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = buckets_.find(pending.bucket_id_);
            if (loaded && it != buckets_.end() &&
                it->second.get() == pending.bucket_identity_.get()) {
                std::vector<DfsAllocatorInterface::EvictionCandidate> candidates;
                if (CollectCandidatesFromSnapshotLocked(pending.bucket_id_,
                                                        *loaded, candidates)) {
                    pending.candidates_ = std::move(candidates);
                    usable = true;
                }
            }
        }
        if (!usable) {
            // The bucket's entries cannot be enumerated safely. Abandon the
            // transaction (the destructor restores the bucket to service)
            // rather than reporting it as having no live entries.
            return PendingEviction{};
        }
    }

    return pending;
}

bool ImmutableBucketAllocator::CommitEviction(PendingEviction&& pending) {
    // Detach first so the destructor of `pending` cannot abort what we commit.
    auto* owner = std::exchange(pending.owner_, nullptr);
    if (owner != this) return false;

    const int64_t bucket_id = pending.bucket_id_;
    auto bucket_identity = std::move(pending.bucket_identity_);
    auto candidates = std::move(pending.candidates_);
    if (bucket_id < 0 || !bucket_identity) return false;

    std::lock_guard<std::mutex> metadata_lock(metadata_io_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = buckets_.find(bucket_id);
        if (it == buckets_.end() || it->second.get() != bucket_identity.get()) {
            return false;
        }
        if (it->second->pending_count != 0) {
            it->second->frozen = false;
            // Restore cold LRU position so the next round can reconsider it
            // after the pending write completes or times out.
            if (lru_index_.find(bucket_id) == lru_index_.end()) {
                lru_list_.push_back(bucket_id);
                lru_index_[bucket_id] = std::prev(lru_list_.end());
            }
            LOG(WARNING) << "Aborted DFS bucket eviction at commit: bucket "
                        << bucket_id << " has a PENDING entry";
            return false;
        }
        // Prepare already enumerated the frozen bucket's live keys. Reuse
        // that list without reading or rewriting metadata at commit time.
        for (const auto& candidate : candidates) {
            auto index_it = key_index_.find(candidate.key);
            if (index_it != key_index_.end() &&
                index_it->second.bucket_id == bucket_id) {
                key_index_.erase(index_it);
            }
        }
        buckets_.erase(it);
        PublishBucketIdSnapshotLocked();
        RemoveFromLruLocked(bucket_id);
        if (active_bucket_id_ == bucket_id) active_bucket_id_ = -1;
    }

    // shortcut: interrupted deletion may restore cached entries, add a durable
    // eviction intent if recovery must remember logical evictions.
    // The master has already dropped all replicas before file deletion.
    DeleteBucketFiles(bucket_id);
    // The deleted bucket's quota slot is reusable only after its files are no
    // longer present. Wake both refill workers and allocation waiters now.
    bucket_pool_cv_.notify_all();
    LOG(INFO) << "Evicted DFS bucket " << bucket_id;
    return true;
}

void ImmutableBucketAllocator::AbortEviction(PendingEviction&& pending) {
    AbortEviction(std::move(pending), /*demote=*/true);
}

void ImmutableBucketAllocator::AbortEviction(PendingEviction&& pending,
                                          bool demote) {
    auto* owner = std::exchange(pending.owner_, nullptr);
    if (owner != this) return;

    const int64_t bucket_id = pending.bucket_id_;
    auto bucket_identity = std::move(pending.bucket_identity_);
    pending.candidates_.clear();
    if (bucket_id < 0 || !bucket_identity) return;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = buckets_.find(bucket_id);
    if (it == buckets_.end() || it->second.get() != bucket_identity.get()) {
        return;
    }
    it->second->frozen = false;
    if (lru_index_.find(bucket_id) != lru_index_.end()) return;

    if (demote) {
        // The master actively rejected this bucket, so put it back at the warm
        // end: PrepareEviction scans from the cold end, and reinserting there
        // would hand back the same rejected bucket on every subsequent round,
        // never letting the scan reach another candidate.
        lru_list_.push_front(bucket_id);
        lru_index_[bucket_id] = lru_list_.begin();
    } else {
        // The transaction was dropped without a verdict (destructor or move
        // assignment). Nothing judged the bucket, so restore its original cold
        // position and let the next round reconsider it.
        lru_list_.push_back(bucket_id);
        lru_index_[bucket_id] = std::prev(lru_list_.end());
    }
}

// === recovery ===

ImmutableBucketAllocator::ParsedBucket ImmutableBucketAllocator::ParseBucketMeta(
    int64_t bucket_id) {
    ParsedBucket parsed;

    // Reading through the shared helper keeps the version, bucket-id and
    // checksum validation identical to every other metadata read. It performs
    // DFS I/O only, so it is safe to run on a recovery worker.
    auto loaded = ReadBucketMetadata(bucket_id);
    if (!loaded) {
        LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                   << ": no valid metadata snapshot, so none of its data can"
                      " be proven correct";
        parsed.discarded = true;
        return parsed;
    }
    PersistedBucketMetadata snapshot = std::move(*loaded);

    if (snapshot.alignment != alignment_) {
        LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                   << ": metadata alignment " << snapshot.alignment
                   << " does not match configured " << alignment_
                   << ", so entry offsets cannot be recomputed";
        parsed.discarded = true;
        return parsed;
    }
    if (snapshot.capacity == 0 || snapshot.capacity > bucket_capacity_) {
        LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                   << ": metadata capacity " << snapshot.capacity
                   << " is incompatible with configured " << bucket_capacity_;
        parsed.discarded = true;
        return parsed;
    }
    if (snapshot.append_offset > snapshot.capacity) {
        LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                   << ": append_offset " << snapshot.append_offset
                   << " exceeds capacity " << snapshot.capacity;
        parsed.discarded = true;
        return parsed;
    }
    if (snapshot.evicting) {
        // The marker says the data file was being deleted; finish the job
        // rather than exposing entries whose data may already be gone.
        LOG(INFO) << "Completing interrupted eviction of DFS bucket "
                  << bucket_id;
        parsed.discarded = true;
        return parsed;
    }

    auto data_size = fs_adapter_->GetFileSize(BucketDataPath(bucket_id));
    if (!data_size) {
        // Either the data file is gone or it cannot even be sized. Both leave
        // the metadata describing bytes nobody can verify, so the whole bucket
        // goes away instead of being reported every restart.
        LOG(WARNING) << "Discarding DFS bucket " << bucket_id
                     << ": data file is missing or unreadable, error="
                     << data_size.error();
        parsed.discarded = true;
        return parsed;
    }
    auto bucket = std::make_shared<BucketState>();
    bucket->bucket_id = bucket_id;
    bucket->capacity = snapshot.capacity;
    bucket->append_offset = 0;
    bucket->live_bytes = 0;
    bucket->last_access_ns = CurrentTimeNs();
    // Everything on disk was written at seal time, so a recovered bucket is
    // sealed by definition, its `.meta` already matches this state and its
    // per-key layout stays on disk rather than in memory.
    bucket->sealed = true;
    bucket->meta_dirty = false;
    bucket->placed_durable = true;

    bool bucket_ok = true;
    // The largest end any entry implies. The persisted append_offset is
    // authoritative when it is at least this large (it also covers rolled-back
    // space), so both are combined below.
    uint64_t implied_append_offset = 0;
    // One candidate per key even if a snapshot repeated it: the layout of the
    // last placement is the live one.
    std::unordered_set<std::string> seen_keys;
    for (const auto& persisted : snapshot.entries) {
        // Accumulate even if this bucket is later rejected, matching the
        // original single-pass recovery so next_generation_ still advances past
        // every generation ever written to disk.
        parsed.max_generation =
            std::max(parsed.max_generation, persisted.generation + 1);
        if (!IsKnownEntryState(persisted.state)) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": entry for key " << persisted.key
                       << " has unknown state " << persisted.state;
            bucket_ok = false;
            break;
        }
        if (persisted.key.empty() ||
            persisted.key_size != persisted.key.size()) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": entry key size mismatch";
            bucket_ok = false;
            break;
        }
        auto layout = RebuildBucketEntryLayout(
            persisted.entry_offset, persisted.value_size, alignment_);
        if (!layout || layout->aligned_size != persisted.reserved_size ||
            layout->end() > snapshot.capacity) {
            LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                       << ": entry for key " << persisted.key
                       << " has an out-of-range or inconsistent layout";
            bucket_ok = false;
            break;
        }
        implied_append_offset =
            std::max(implied_append_offset, layout->end());

        if (persisted.state !=
            static_cast<int32_t>(BucketEntryState::COMMITTED)) {
            // A PENDING entry's data was never confirmed durable, so it must
            // not come back as readable. The rewrite that records it as a
            // TOMBSTONE happens at flush: the key is absent from key_index_,
            // which is exactly what makes ApplyKeyIndexStatesLocked() demote it.
            if (persisted.state ==
                static_cast<int32_t>(BucketEntryState::PENDING)) {
                bucket->meta_dirty = true;
            }
            continue;
        }
        if (layout->end() > static_cast<uint64_t>(*data_size)) {
            LOG(ERROR) << "Dropping committed DFS entry for key "
                       << persisted.key << " in bucket " << bucket_id
                       << ": it extends past the data file end";
            bucket->meta_dirty = true;
            continue;
        }

        // A surviving COMMITTED entry is a candidate winner. Which duplicate
        // actually wins is decided under `mutex_` at commit time, so the worker
        // stays free of allocator state. Building the descriptor here spreads
        // that work across the worker pool instead of doing it at commit.
        if (seen_keys.insert(persisted.key).second) {
            // Count the slot as live; a duplicate that loses the cross-bucket
            // race is subtracted again in ResolveRecoveryConflicts().
            bucket->live_bytes += persisted.reserved_size;
            parsed.candidates.push_back(RecoveryCandidate{
                RecoveredReplica{persisted.key,
                                 MakeBucketDescriptor(
                                     BucketDataPath(bucket_id), *layout,
                                     bucket_id)},
                persisted.generation, persisted.entry_offset});
        }
    }
    if (!bucket_ok) {
        parsed.discarded = true;
        return parsed;
    }

    bucket->append_offset =
        std::max(snapshot.append_offset, implied_append_offset);
    if (bucket->append_offset > bucket->capacity) {
        LOG(ERROR) << "Discarding DFS bucket " << bucket_id
                   << ": recovered append_offset " << bucket->append_offset
                   << " exceeds capacity " << bucket->capacity;
        parsed.discarded = true;
        return parsed;
    }

    parsed.bucket = std::move(bucket);
    return parsed;
}

void ImmutableBucketAllocator::CommitParsedBucketLocked(
    int64_t bucket_id, ParsedBucket& parsed,
    std::vector<RecoveredReplica>& winners) {
    for (auto& candidate : parsed.candidates) {
        const std::string& key = candidate.replica.key;
        auto index_it = key_index_.find(key);
        if (index_it == key_index_.end()) {
            // First bucket to claim the key: it is the running winner and its
            // replica is streamed at once. keep-max is commutative, so a later
            // bucket only ever replaces it by re-streaming a greater tuple.
            key_index_[key] =
                LiveKeyRecord{bucket_id, BucketEntryState::COMMITTED};
            winners.push_back(std::move(candidate.replica));
            continue;
        }

        // A duplicate. This model keeps no per-bucket entry table, so the
        // running winner's generation and offset cannot be read here to decide
        // keep-max. Record the pair instead and settle every duplicate in one
        // final pass; both buckets are marked dirty so the loser's `.meta` gets
        // rewritten (a key not live in its bucket is written back as TOMBSTONE).
        recovery_conflicts_.push_back(RecoveryConflict{key, bucket_id});
        auto winner_it = buckets_.find(index_it->second.bucket_id);
        if (winner_it != buckets_.end()) winner_it->second->meta_dirty = true;
        parsed.bucket->meta_dirty = true;
    }

    buckets_.emplace(bucket_id, std::move(parsed.bucket));
}

void ImmutableBucketAllocator::ResolveRecoveryConflicts(
    const RecoveredBatchConsumer& consumer) {
    if (recovery_conflicts_.empty()) return;

    struct Group {
        std::vector<int64_t> bucket_ids;
        int64_t first_bucket_id = -1;
    };
    std::unordered_map<std::string, Group> groups;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& conflict : recovery_conflicts_) {
            auto& group = groups[conflict.key];
            group.bucket_ids.push_back(conflict.bucket_id);
            auto it = key_index_.find(conflict.key);
            if (it != key_index_.end()) {
                group.first_bucket_id = it->second.bucket_id;
            }
        }
    }

    for (auto& [key, group] : groups) {
        // The bucket key_index_ still points at claimed the key first, so its
        // replica already reached the consumer. Include it among the candidates
        // so the true winner can be found, but remember it to avoid re-emitting
        // an unchanged winner.
        if (group.first_bucket_id >= 0 &&
            std::find(group.bucket_ids.begin(), group.bucket_ids.end(),
                      group.first_bucket_id) == group.bucket_ids.end()) {
            group.bucket_ids.push_back(group.first_bucket_id);
        }

        // Decide by the stored (generation, bucket_id, entry_offset) tuple,
        // re-read from each member bucket's `.meta` because this model keeps no
        // per-bucket entry table in memory. The location tie-breaker keeps the
        // outcome independent of traversal order.
        uint64_t best_generation = 0;
        uint64_t best_entry_offset = 0;
        uint64_t best_value_size = 0;
        int64_t best_bucket_id = -1;
        bool have_best = false;
        // Slot size each member bucket counted as live for this key; every
        // member but the winner has to give it back.
        std::unordered_map<int64_t, uint64_t> member_live_bytes;
        for (const int64_t member : group.bucket_ids) {
            auto snapshot = ReadBucketMetadata(member);
            if (!snapshot) continue;
            for (const auto& entry : snapshot->entries) {
                if (entry.key != key) continue;
                if (entry.state !=
                    static_cast<int32_t>(BucketEntryState::COMMITTED)) {
                    continue;
                }
                auto layout = RebuildBucketEntryLayout(
                    entry.entry_offset, entry.value_size, alignment_);
                if (!layout) continue;
                member_live_bytes[member] = entry.reserved_size;
                if (!have_best ||
                    std::tie(entry.generation, member, entry.entry_offset) >
                        std::tie(best_generation, best_bucket_id,
                                 best_entry_offset)) {
                    best_generation = entry.generation;
                    best_entry_offset = entry.entry_offset;
                    best_value_size = entry.value_size;
                    best_bucket_id = member;
                    have_best = true;
                }
            }
        }
        if (!have_best) continue;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            key_index_[key] =
                LiveKeyRecord{best_bucket_id, BucketEntryState::COMMITTED};
            auto it = buckets_.find(best_bucket_id);
            if (it != buckets_.end()) it->second->meta_dirty = true;
            // Only the winner's bucket keeps these bytes counted as live.
            for (const auto& [member, bytes] : member_live_bytes) {
                if (member == best_bucket_id) continue;
                auto loser_it = buckets_.find(member);
                if (loser_it == buckets_.end()) continue;
                if (loser_it->second->live_bytes >= bytes) {
                    loser_it->second->live_bytes -= bytes;
                } else {
                    loser_it->second->live_bytes = 0;
                }
            }
        }

        // Only a change of winner needs a corrected batch. The master applies
        // recovery batches last-wins, so the corrected replica overrides the
        // one streamed when the key was first claimed.
        if (best_bucket_id == group.first_bucket_id) continue;
        auto layout = RebuildBucketEntryLayout(best_entry_offset,
                                               best_value_size, alignment_);
        if (!layout) continue;
        std::vector<RecoveredReplica> corrected;
        corrected.push_back(RecoveredReplica{
            key, MakeBucketDescriptor(BucketDataPath(best_bucket_id), *layout,
                                      best_bucket_id)});
        if (consumer) consumer(std::move(corrected));
    }

    recovery_conflicts_.clear();
}

tl::expected<void, ErrorCode>
ImmutableBucketAllocator::RecoverBucketsProgressively(
    const RecoveredBatchConsumer& consumer) {
    if (!fs_adapter_) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    // Take ownership of the ids Init discovered, so the members stop pinning
    // this memory once recovery has consumed them.
    std::vector<int64_t> meta_ids = std::move(pending_recovery_meta_ids_);
    std::vector<int64_t> data_ids = std::move(pending_recovery_data_ids_);
    pending_recovery_meta_ids_.clear();
    pending_recovery_data_ids_.clear();
    recovery_conflicts_.clear();

    const size_t workers = std::max<size_t>(1, recovery_worker_count_);

    // Recovery stops at the first batch boundary where either budget is spent,
    // so a huge backlog cannot stall startup; a zero value disables that
    // dimension.
    const bool has_deadline = recovery_max_duration_seconds_ > 0;
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(recovery_max_duration_seconds_);
    // Ids the budget left unparsed, and where the cut happened. The cut index
    // is kept so a failed cleanup-thread start can still delete them here.
    std::unordered_set<int64_t> skipped_ids;
    size_t skip_start = meta_ids.size();
    const char* limit_reason = nullptr;

    std::unordered_set<int64_t> discarded_ids;
    uint64_t max_generation = 0;
    size_t recovered_bucket_count = 0;
    size_t recovered_replica_count = 0;
    size_t discarded_count = 0;

    const size_t total = meta_ids.size();
    ThreadPool pool(workers);
    const size_t max_buckets = recovery_max_buckets_;
    for (size_t batch_start = 0; batch_start < total; batch_start += workers) {
        // Cut over only at a batch boundary and only once at least one batch
        // has committed, so even a tiny budget still restores the newest
        // buckets and an in-flight batch is never left half-published.
        const bool bucket_limit_reached =
            max_buckets > 0 && recovered_bucket_count >= max_buckets;
        const bool time_limit_reached =
            has_deadline && std::chrono::steady_clock::now() >= deadline;
        if (batch_start > 0 && (bucket_limit_reached || time_limit_reached)) {
            limit_reason = bucket_limit_reached ? "bucket_limit" : "time_limit";
            skip_start = batch_start;
            break;
        }
        const size_t batch_end = std::min(total, batch_start + workers);
        const size_t batch_size = batch_end - batch_start;

        // One parsed bucket per worker in flight: this bounds recovery memory
        // by `workers`, not by the total key count.
        std::vector<ParsedBucket> parsed(batch_size);
        std::atomic<size_t> remaining{batch_size};
        std::mutex batch_mutex;
        std::condition_variable batch_cv;
        std::exception_ptr worker_error;

        for (size_t i = 0; i < batch_size; ++i) {
            const int64_t bucket_id = meta_ids[batch_start + i];
            pool.enqueue([this, bucket_id, &parsed, i, &remaining, &batch_mutex,
                          &batch_cv, &worker_error]() {
                // Parsing runs on a pool thread, so an exception here would
                // otherwise terminate the process. Capture it and rethrow on
                // the committing thread, preserving the single-threaded
                // behaviour where it propagated to the caller.
                try {
                    parsed[i] = ParseBucketMeta(bucket_id);
                } catch (...) {
                    std::lock_guard<std::mutex> lock(batch_mutex);
                    if (!worker_error) worker_error = std::current_exception();
                }
                {
                    std::lock_guard<std::mutex> lock(batch_mutex);
                    --remaining;
                }
                batch_cv.notify_one();
            });
        }
        {
            std::unique_lock<std::mutex> lock(batch_mutex);
            batch_cv.wait(lock,
                          [&remaining] { return remaining.load() == 0; });
        }
        if (worker_error) {
            std::rethrow_exception(worker_error);
        }

        // Commit serially, in ascending bucket_id order. The consumer is
        // invoked with the allocator mutex released, so the allocator mutex and
        // the master's metadata shard mutex are never nested.
        for (size_t i = 0; i < batch_size; ++i) {
            ParsedBucket& pb = parsed[i];
            max_generation = std::max(max_generation, pb.max_generation);
            const int64_t bucket_id = meta_ids[batch_start + i];
            if (pb.discarded) {
                DeleteBucketFiles(bucket_id);
                discarded_ids.insert(bucket_id);
                ++discarded_count;
                continue;
            }

            std::vector<RecoveredReplica> winners;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                CommitParsedBucketLocked(bucket_id, pb, winners);
            }
            ++recovered_bucket_count;
            recovered_replica_count += winners.size();
            if (consumer) consumer(std::move(winners));
        }
        // `parsed` is destroyed here, releasing this batch before the next.
    }

    // Buckets the budget cut off keep their files for now; an asynchronous
    // cleanup thread removes them so a large backlog never delays startup.
    if (limit_reason != nullptr) {
        for (size_t i = skip_start; i < meta_ids.size(); ++i) {
            skipped_ids.insert(meta_ids[i]);
        }
        LOG(WARNING) << "DFS bucket recovery stopped early (" << limit_reason
                     << "), recovered=" << recovered_bucket_count
                     << ", skipped=" << skipped_ids.size()
                     << ", max_buckets=" << recovery_max_buckets_
                     << ", max_duration_seconds="
                     << recovery_max_duration_seconds_;
    }

    // Settle every duplicated key. The running winner's replica was already
    // streamed; a corrected one is streamed only when the true winner differs.
    ResolveRecoveryConflicts(consumer);

    // A data file with no `.meta` at all is the signature of a crash while the
    // bucket was still active: its metadata only lived in memory, so nothing
    // can ever address the data again. Reclaim it. Buckets rejected above were
    // already deleted, so they are skipped here to avoid a duplicate log line.
    // Ready-pool workers may still be running, so decide under `mutex_` and do
    // the file I/O outside it.
    std::vector<int64_t> orphan_data_ids;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const int64_t data_id : data_ids) {
            if (buckets_.count(data_id) > 0) continue;
            if (discarded_ids.count(data_id) > 0) continue;
            // Skipped buckets belong to the cleanup thread; deleting them here
            // as well would race with it.
            if (skipped_ids.count(data_id) > 0) continue;
            orphan_data_ids.push_back(data_id);
        }
    }
    for (const int64_t data_id : orphan_data_ids) {
        LOG(WARNING) << "Removing orphaned DFS bucket data file for bucket_id="
                     << data_id << " (no metadata, lost on an unclean restart)";
        DeleteBucketFiles(data_id);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        next_generation_ = std::max<uint64_t>(1, max_generation);

        // Every recovered bucket is sealed, so none of them is resumed: the
        // next allocation opens a fresh bucket. This is what keeps a sealed
        // bucket's `.meta` final, written once when the bucket stopped growing.
        active_bucket_id_ = -1;

        // Seed the LRU newest-first so recovered buckets have a defined order.
        // Ready (preallocated, never used) buckets stay out of the LRU, exactly
        // as they do when the pool creates them.
        std::vector<int64_t> ordered;
        ordered.reserve(buckets_.size());
        for (const auto& [bucket_id, bucket] : buckets_) {
            if (bucket->ready) continue;
            ordered.push_back(bucket_id);
        }
        std::sort(ordered.begin(), ordered.end());
        for (const int64_t bucket_id : ordered) {
            lru_list_.push_front(bucket_id);
            lru_index_[bucket_id] = lru_list_.begin();
        }

        // The ready pool reads the published id set, so publish it while the
        // lock guarding `buckets_` is still held.
        PublishBucketIdSnapshotLocked();
    }

    // Recovery may have rewritten in-memory state (PENDING entries demoted to
    // tombstones, duplicate losers demoted). Persist that now so the next start
    // reads exactly what this process believes.
    const size_t rewritten = FlushDirtyMetadata();

    // Delete the skipped buckets off the startup path. `skipped_ids` is moved
    // into the thread; `meta_ids`/`skip_start` still let the catch fall back to
    // deleting here if the thread cannot start.
    const size_t skipped_count = skipped_ids.size();
    if (skipped_count > 0) {
        // The cleanup worker takes a plain vector; `skipped_ids` stays a set
        // for the orphan dedup above, so hand it a converted copy.
        std::vector<int64_t> skipped(skipped_ids.begin(), skipped_ids.end());
        bool cleanup_started = false;
        try {
            std::thread cleanup(
                &ImmutableBucketAllocator::CleanupSkippedBuckets, this,
                std::move(skipped));
            recovery_cleanup_thread_ = std::move(cleanup);
            cleanup_started = true;
        } catch (const std::system_error& e) {
            LOG(WARNING) << "Failed to start skipped-bucket cleanup thread: "
                         << e.what() << "; deleting skipped buckets synchronously";
            for (size_t i = skip_start; i < meta_ids.size(); ++i) {
                DeleteBucketFiles(meta_ids[i]);
            }
        }
        if (cleanup_started) {
            LOG(INFO) << "DFS bucket recovery started asynchronous cleanup for "
                      << skipped_count << " skipped buckets";
        }
    }

    LOG(INFO) << "DFS bucket recovery complete, buckets="
              << recovered_bucket_count << ", replicas="
              << recovered_replica_count << ", discarded=" << discarded_count
              << ", skipped=" << skipped_count
              << ", workers=" << workers
              << ", next_bucket_id=" << next_bucket_id_
              << ", next_generation=" << next_generation_
              << ", rewritten_buckets=" << rewritten;
    return {};
}

void ImmutableBucketAllocator::CleanupSkippedBuckets(
    std::vector<int64_t> skipped_ids) {
    size_t deleted = 0;
    for (const int64_t bucket_id : skipped_ids) {
        LOG(WARNING) << "Deleting DFS bucket skipped by the recovery budget, "
                     << "bucket_id=" << bucket_id;
        DeleteBucketFiles(bucket_id);
        ++deleted;
    }
    LOG(INFO) << "DFS skipped-bucket cleanup complete, deleted=" << deleted;
}

}  // namespace mooncake
