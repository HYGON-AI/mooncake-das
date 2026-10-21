#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/dfs_allocator_interface.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

/**
 * @brief Persisted state of one entry inside a bucket.
 *
 * `entry_offset` is both the aligned start of the entry and the value offset;

 * * the data file stores no key header.
 */
struct PersistedBucketEntry {
    std::string key;
    uint64_t entry_offset = 0;
    uint64_t key_size = 0;
    uint64_t value_size = 0;
    uint64_t reserved_size = 0;
    uint64_t generation = 0;
    // 0 = PENDING (space reserved, data not known to be durable)
    // 1 = COMMITTED (data fully written; safe to recover)
    // 2 = TOMBSTONE (freed/removed; must not be revived on restart)
    int32_t state = 0;
    YLT_REFL(PersistedBucketEntry, key, entry_offset, key_size, value_size,
             reserved_size, generation, state);
};

/**
 * @brief On-disk `.meta` snapshot of one bucket.
 *
 * A CRC-32C over the serialized payload (with `checksum` zeroed) detects torn
 * or corrupt metadata; `version` rejects formats this build cannot parse.
 *
 * There is exactly one `.meta` file per bucket, holding the bucket's full state
 * at the moment it was written. It is written when the bucket stops being the
 * active one (it is sealed) and rewritten only when the sealed bucket's state
 * changed afterwards, never once per key.
 */
struct PersistedBucketMetadata {
    uint32_t version = 0;
    uint32_t checksum = 0;
    int64_t bucket_id = 0;
    uint64_t capacity = 0;
    uint64_t alignment = 0;
    uint64_t append_offset = 0;
    // Legacy marker: new writes leave this false, but recovery still discards
    // buckets marked for eviction by older versions.
    bool evicting = false;
    std::vector<PersistedBucketEntry> entries;
    YLT_REFL(PersistedBucketMetadata, version, checksum, bucket_id, capacity,
             alignment, append_offset, evicting, entries);
};

// Bump when the layout of PersistedBucketMetadata changes incompatibly.
// Version 5 removes the bucket-level identity counter and describes data files
// whose entries contain only object bytes followed by padding. Older versions
// are rejected.
inline constexpr uint32_t kBucketMetadataVersion = 5;

enum class BucketEntryState : int32_t {
    PENDING = 0,
    COMMITTED = 1,
    TOMBSTONE = 2,
};

/**
 * @brief Append-only, bucket-based DFS space allocator.
 *
 * Space is handed out by appending into the currently active bucket file. Each
 * bucket has a fixed capacity; when a request no longer fits, a new bucket is
 * created. Buckets carry persisted metadata so committed entries can be
 * recovered after a master restart, and eviction works at whole-bucket
 * granularity through a two-phase protocol driven by the master.
 *
 * Metadata durability
 * -------------------
 * Each bucket owns exactly two files:
 *
 *   bucket_NNNNNN.data     preallocated data file
 *   bucket_NNNNNN.meta     the bucket's single full metadata snapshot
 *
 * The active bucket keeps its metadata in memory only. The `.meta` file is
 * written when the bucket is sealed, i.e. when allocation moves on to a new
 * bucket, and rewritten afterwards only if the sealed bucket's state changed
 * (a tail reservation committing, or an entry being freed). Rewrites happen in
 * place: the hot path never writes metadata, never renames and never appends to
 * a log, so storing a key costs no metadata I/O at all.
 *
 * `FlushDirtyMetadata()` performs the deferred writes for every bucket marked
 * dirty. It runs on the master's DFS maintenance tick and at shutdown.
 * Preparing and committing eviction do not persist a marker or flush metadata.
 * If deletion is interrupted and both files survive, recovery may restore the
 * cached entries; they can be evicted again after restart.
 *
 * The accepted failure mode: if the master dies before a bucket is sealed, that
 * bucket has no `.meta` file and recovery deletes its `.data` file, so at most
 * the newest bucket's contents are lost. Because an in-place rewrite that is
 * torn destroys both the old and the new content, a crash during a rewrite has
 * the same effect for that one bucket.
 *
 * Lock ownership
 * --------------
 * `mutex_` guards every mutable allocator member: `buckets_`, `key_index_`,
 * `active_bucket_id_`, `next_bucket_id_`, `lru_list_`/`lru_index_` and the
 * eviction bookkeeping. Bucket state lives behind shared_ptr and is only
 * mutated while `mutex_` is held, so no per-bucket mutex is needed.
 *
 * Slow DFS I/O (preallocation, metadata writes, deletes) is always performed
 * with `mutex_` released: the caller snapshots the state it needs under the
 * lock, does the I/O, then reacquires the lock and re-validates the captured
 *
 * BucketState object identity before publishing the result. No RPC, callback
 * or
 * filesystem call ever happens while `mutex_` is held.
 */
class ImmutableBucketAllocator final : public DfsAllocatorInterface {
   public:
    ImmutableBucketAllocator() = default;
    ~ImmutableBucketAllocator() override;

    ImmutableBucketAllocator(const ImmutableBucketAllocator&) = delete;
    ImmutableBucketAllocator& operator=(const ImmutableBucketAllocator&) = delete;

    DfsAllocatorType Type() const override { return DfsAllocatorType::BUCKET; }

    tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig& config) override;

    bool IsInitialized() const override {
        return initialized_.load(std::memory_order_acquire);
    }

    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) override;

    std::vector<BatchAllocateResult> BatchAllocate(
        const std::vector<BatchAllocateRequest>& requests) override;

    void Free(const std::string& key,
              const DistributedFSDescriptor& descriptor) override;

    void UpdateAccess(const std::string& key,
                      const DistributedFSDescriptor& descriptor) override;

    bool IsEvictionEnabled() const override { return eviction_enabled_; }

    std::chrono::seconds GetEvictionCheckInterval() const override {
        return eviction_check_interval_;
    }

    uint64_t GetTotalCapacity() const override;

    uint64_t GetUsedBytes() const override;

    /**
     * @brief Dynamically change the maximum number of buckets at runtime.
     *
     * A value of 0 means unlimited. The new limit takes effect on the next
     * allocation: no new bucket is created once the count is reached. It does
     * not force eviction of already-existing buckets, so it can be set below
     * the current bucket count without immediate effect.
     *
     * @return the previous value of max_bucket_count_.
     */
    int64_t SetMaxBucketCount(int64_t new_max_bucket_count);

    /**
     * @brief Mark a reservation as durable so restart recovery may revive it.
     *
     * Called by the master once the client reports the DFS data write finished
     * (PutEnd). A mismatching descriptor or generation is ignored, so a late
     * completion from a superseded operation cannot commit the allocation that
     * replaced it.
     *
     * @return true when the entry is COMMITTED and that fact is durable.
     */
    bool MarkCommitted(const std::string& key,
                       const DistributedFSDescriptor& descriptor);

    /**
     * @brief One bucket's worth of winners, handed to the master as recovery
     * advances.
     *
     * Recovery streams these batches instead of collecting the whole set, which
     * is what keeps the restart peak independent of the total key count.
     */
    struct RecoveredReplica {
        std::string key;
        DistributedFSDescriptor descriptor;
    };
    using RecoveredBatchConsumer =
        std::function<void(std::vector<RecoveredReplica>&&)>;

    /**
     * @brief Rebuild allocator state from the buckets previous runs left on
     * disk, streaming each bucket's winners to `consumer` as it is committed.
     *
     * Runs exactly once after Init() and before the allocator serves traffic.
     * `consumer` is invoked with `mutex_` released, so the allocator mutex and
     * the master's metadata shard mutex are never nested. A null consumer still
     * rebuilds the allocator but reports no replicas, which is how the
     * snapshot-restore path keeps its own metadata source.
     */
    tl::expected<void, ErrorCode> RecoverBucketsProgressively(
        const RecoveredBatchConsumer& consumer);

    /**
     * @brief Bucket-granular eviction transaction.
     *
     * Prepare() freezes one non-active bucket and lists its live entries. The
     * master validates every candidate without mutating metadata; only if all
     * are acceptable does it Commit(), which removes the replicas and then
     * deletes the bucket files. Any rejection leads to Abort(), which restores
     * the bucket untouched.
     *
     * Move-only, and Commit/Abort are each idempotent: whichever runs first
     * wins and the destructor aborts an unresolved transaction.
     */
    class PendingEviction {
       public:
        PendingEviction() = default;
        ~PendingEviction();

        PendingEviction(const PendingEviction&) = delete;
        PendingEviction& operator=(const PendingEviction&) = delete;
        PendingEviction(PendingEviction&& other) noexcept;
        PendingEviction& operator=(PendingEviction&& other) noexcept;

        // A transaction is empty only when no bucket was selected. A selected
        // bucket may legitimately have no live entries left (every key in it
        // was freed), and such a bucket must still be evicted, so this cannot
        // be derived from candidates_.empty().
        bool Empty() const { return bucket_id_ < 0; }
        int64_t bucket_id() const { return bucket_id_; }
        const std::vector<EvictionCandidate>& Candidates() const {
            return candidates_;
        }

       private:
        friend class ImmutableBucketAllocator;

        ImmutableBucketAllocator* owner_ = nullptr;
        int64_t bucket_id_ = -1;
        // Keeps the exact BucketState captured by PrepareEviction alive. The
        // allocator compares pointer identity before Commit/Abort can act.
        std::shared_ptr<void> bucket_identity_;
        std::vector<EvictionCandidate> candidates_;
    };

    /**
     * @brief Freeze an eligible bucket and return its live entries.
     *
     * Watermark eviction selects the coldest eligible bucket. The dedicated
     * low-utilization entry point restricts selection to sparse buckets.
     * Returns an empty transaction when nothing can be evicted.
     */
    PendingEviction PrepareEviction();

    /**
     * @brief Prepare one cold bucket regardless of byte watermarks.
     *
     * Used only after allocation reports that the bucket-count limit has been
     * reached. It allows the master to reclaim one bucket and retry without
     * turning low-utilization bucket tails into a permanent allocation
     * deadlock.
     */
    PendingEviction PrepareEvictionForAllocationFailure();

    /**
     * @brief Freeze one non-active bucket whose live utilization is below the
     * supplied threshold.
     *
     * The caller must still validate the returned candidates with the master
     * before committing the eviction.
     */
    PendingEviction PrepareEvictionForLowUtilization(double threshold);

    /**
     * @brief Freeze one specific bucket after its data file is confirmed missing.
     */
    PendingEviction PrepareInvalidation(int64_t bucket_id);

    bool HasBucket(int64_t bucket_id) const;

    /**
     * @brief Check whether the bucket data file exists on the master's DFS.
     *
     * The error result is preserved so callers can ask the client to retry
     * instead of treating an inconclusive check as a missing file.
     */
    tl::expected<bool, ErrorCode> IsBucketDataFilePresent(
        int64_t bucket_id) const;

    /**
     * @brief Accept the eviction: drop the bucket and delete its files.
     * Must only be called once the master has removed every candidate replica.
     * @return True when the allocator removed the logical bucket. Physical file
     * deletion is still best-effort.
     */
    bool CommitEviction(PendingEviction&& pending);

    /**
     * @brief Reject the eviction and return the bucket to service unchanged.
     *
     * Used when the master declined the candidates. The bucket goes back at the
     * warm end of the LRU so the next round can reach a different candidate
     * instead of re-offering this one forever.
     */
    void AbortEviction(PendingEviction&& pending);

    /**
     * @brief Write the `.meta` file of every bucket whose state is dirty.
     *
     * The hot paths - BatchAllocate, MarkCommitted, Free - only update memory
     * and mark the bucket dirty, because the master calls `Free()` while
     * holding a metadata shard lock and DFS I/O must never happen under that
     * lock. This method performs the deferred writes, so it must be called with
     * no master lock held: the master's DFS maintenance tick drives it, and the
     * destructor runs it once more so a clean shutdown leaves no unwritten
     * metadata.
     *
     * @return the number of buckets whose metadata was made durable.
     */
    size_t FlushDirtyMetadata();

    /**
     * @brief Number of buckets currently tracked (test/metrics helper).
     */
    size_t GetBucketCount() const;

    struct BucketIdSnapshot {
        uint64_t version = 0;
        std::vector<int64_t> bucket_ids;
    };
    std::shared_ptr<const BucketIdSnapshot> GetBucketIdSnapshot() const;

    /**
     * @brief Bucket id an existing key currently lives in, if any.
     */
    std::optional<int64_t> GetBucketIdForKey(const std::string& key) const;

    static std::string FormatBucketId(int64_t bucket_id);

   private:
    friend class PendingEviction;

    void PublishBucketIdSnapshotLocked();

    // Shared implementation for watermark-driven and allocation-failure-driven
    // eviction. The latter bypasses only the watermark gate and still observes
    // active/frozen state plus the master's full validation protocol.
    PendingEviction PrepareEvictionInternal(
        bool force_one, double low_utilization_threshold = -1.0);
    PendingEviction PrepareInvalidationInternal(int64_t bucket_id);

    // Shared implementation of AbortEviction. `demote` distinguishes an
    // explicit master rejection (return the bucket at the warm end so the scan
    // can move on) from a transaction dropped without a verdict by the
    // destructor or move assignment (restore its cold position).
    void AbortEviction(PendingEviction&& pending, bool demote);

    // A key that currently has a live entry (PENDING or COMMITTED) in some
    // bucket. `key_index_` maps every such key to where it lives; a key absent
    // from it is either never allocated or already recorded as TOMBSTONE on
    // disk.
    struct LiveKeyRecord {
        int64_t bucket_id = -1;
        BucketEntryState state = BucketEntryState::PENDING;
    };

    // One space reservation made into a bucket, in append order. Only kept for
    // a bucket whose `.meta` file has never been written successfully: once the
    // first snapshot is durable, the per-key layout lives on disk and `placed`
    // is cleared. `state` is deliberately absent - the authoritative state is
    // always `key_index_[key].state`.
    struct PlacedEntry {
        std::string key;
        uint64_t entry_offset = 0;
        uint64_t object_size = 0;
        uint64_t reserved_size = 0;
        uint64_t generation = 0;
    };

    struct BucketState {
        int64_t bucket_id = 0;
        uint64_t capacity = 0;
        uint64_t append_offset = 0;
        // Bytes reserved by entries that are still live (PENDING or
        // COMMITTED). A failed PutStart must Free() its PENDING entry before
        // this value can represent reclaimable live capacity.
        uint64_t live_bytes = 0;
        int64_t last_access_ns = 0;
        // Set between PrepareEviction and Commit/Abort. A frozen bucket
        // accepts no new allocations and cannot be selected again.
        bool frozen = false;
        // A ready bucket owns a fully preallocated data file but has never
        // accepted an allocation. Ready buckets are absent from the LRU.
        bool ready = false;
        // True once the bucket has been sealed (it is no longer the active
        // bucket), which is when its `.meta` file starts to exist.
        bool sealed = false;
        // Set when the in-memory state differs from what the `.meta` file
        // holds, cleared once the file has been rewritten. Only meaningful for
        // a sealed bucket: an unsealed one is deliberately not persisted at
        // all.
        bool meta_dirty = false;
        // Reservations not yet folded into a durable `.meta` snapshot.
        // push_back on reserve, pop_back on batch rollback, cleared once the
        // first snapshot has been written.
        std::vector<PlacedEntry> placed;
        // Number of entries in this bucket still awaiting PutEnd (PENDING). An
        // eviction must not touch a bucket that has any.
        uint64_t pending_count = 0;
        // True once this bucket's `.meta` file was written successfully at
        // least once. From then on the per-key layout is read back from disk
        // for later rewrites instead of being kept in `placed`.
        bool placed_durable = false;
    };

    using BucketPtr = std::shared_ptr<BucketState>;

    struct BucketCreationReservation {
        int64_t bucket_id = -1;
    };

    // --- helpers, called with mutex_ held only where the name says Locked ---

    std::string BucketDataPath(int64_t bucket_id) const;
    std::string BucketMetaPath(int64_t bucket_id) const;

    // Serializes `bucket`'s not-yet-durable `placed` list into a snapshot,
    // folding duplicate keys and taking every entry's state from key_index_.
    // Only valid while `placed_durable` is false. Taken under `mutex_`; the
    // file write happens outside it.
    PersistedBucketMetadata SnapshotLocked(BucketState& bucket);

    // Reads and validates one bucket's `.meta` file. Performs DFS I/O, so it
    // must be called with `mutex_` released. Returns nullopt when the file is
    // missing, unreadable, or fails the version/checksum check.
    std::optional<PersistedBucketMetadata> ReadBucketMetadata(
        int64_t bucket_id) const;

    // Overwrites every entry's state in `snapshot` from key_index_: a key still
    // live in `snapshot`'s bucket keeps its recorded state, anything else
    // becomes TOMBSTONE. Refreshes the checksum. Requires `mutex_`.
    void ApplyKeyIndexStatesLocked(PersistedBucketMetadata& snapshot) const;

    // Builds one eviction candidate for a live entry, or nullopt when the
    // recorded layout is inconsistent.
    std::optional<EvictionCandidate> MakeEvictionCandidate(
        int64_t bucket_id, const std::string& key, uint64_t entry_offset,
        uint64_t value_size) const;

    // Collects eviction candidates for the keys still live in a bucket, from
    // its in-memory `placed` list (a bucket whose first snapshot is not durable
    // yet) or from a persisted snapshot. Both require `mutex_` and return false
    // when an entry's layout is inconsistent.
    bool CollectCandidatesFromPlacedLocked(
        const BucketState& bucket,
        std::vector<EvictionCandidate>& candidates) const;
    bool CollectCandidatesFromSnapshotLocked(
        int64_t bucket_id, const PersistedBucketMetadata& snapshot,
        std::vector<EvictionCandidate>& candidates) const;

    // Overwrites `bucket_id`'s single `.meta` file in place and syncs it.
    tl::expected<void, ErrorCode> PersistMetadata(
        const PersistedBucketMetadata& snapshot);

    // Marks `bucket` as needing its `.meta` file (re)written. A bucket that is
    // still active is never persisted, so the flag is only set once sealed.
    static void MarkMetaDirtyLocked(BucketState& bucket);

    // Seals the currently active bucket - it stops accepting allocations - and
    // marks it for persistence. Requires `mutex_`.
    void SealActiveBucketLocked();

    // Synchronous compatibility path used only when ready_bucket_target_ is 0.
    // It reserves identity under the lock, preallocates outside it, then
    // publishes the bucket as active without rolling back the monotonic id.
    tl::expected<BucketPtr, ErrorCode> CreateBucketUnlocked(
        std::unique_lock<std::mutex>& lock);

    bool CanReserveBucketCreationLocked() const;
    std::optional<BucketCreationReservation> ReserveBucketCreationLocked();
    void FinishBucketCreationLocked(bool synchronous);
    void RollbackBucketPublicationLocked(int64_t bucket_id,
                                         const BucketPtr& bucket);
    size_t EffectiveReadyTargetLocked() const;
    // Same computation, but counts `recovered_count` buckets as already
    // committed to the pool. Recovery runs after Init() has started the worker
    // pool, so at that point `buckets_` is still empty and the plain overload
    // would let the pool over-create.
    size_t EffectiveReadyTargetLocked(size_t recovered_count) const;
    BucketPtr PromoteReadyBucketLocked();
    bool ActiveBucketHasSpaceLocked(uint64_t required,
                                    BucketPtr* bucket = nullptr) const;
    void RequestRefillLocked();
    void RecordBucketCreationFailureLocked(ErrorCode error);
    void BucketCreateWorker();
    void StopBucketWorkers();

    // One-shot background sweep that reclaims the bucket files recorded by the
    // disabled-recovery scan. Their ids are all below `next_bucket_id_`, so no
    // bucket this process creates reuses a name in the list. Joined by
    // StopBucketWorkers() before the fs adapter shuts down.
    void LegacyCleanupWorker(std::vector<int64_t> bucket_ids);
    std::vector<int64_t> DetachReadyBucketsLocked();
    void UpdateBucketPoolMetricsLocked() const;

    // Ensures an active bucket exists with at least `required` bytes free.
    // May temporarily release `lock` to create a bucket.
    tl::expected<BucketPtr, ErrorCode> EnsureActiveBucket(
        std::unique_lock<std::mutex>& lock, uint64_t required);

    void TouchLruLocked(int64_t bucket_id, int64_t now_ns);
    void RemoveFromLruLocked(int64_t bucket_id);

    // Applies one reservation to `bucket` and returns the descriptor.
    tl::expected<DistributedFSDescriptor, ErrorCode> ReserveInBucketLocked(
        BucketState& bucket, const std::string& key, uint64_t size);

    // Rolls a reservation back out of `bucket` (used when a later entry of the
    // same batch fails). BatchAllocate unwinds in reverse order, so the target
    // is normally the most recent placement; a concurrent batch's interleaved
    // reservation is handled by scanning for the key instead of assuming the
    // tail. Only a tail rollback returns its space to `append_offset`.
    void UnreserveInBucketLocked(BucketState& bucket, const std::string& key,
                                 const DistributedFSDescriptor& descriptor);

    // Resolves `key` to its live key_index_ record and validates that the
    // descriptor addresses that same live entry. Returns nullptr when the key
    // is not live, is owned by another bucket, or the descriptor is stale.
    // `verify_placement_offset` additionally compares the descriptor's offset
    // against the in-memory placement while the bucket has no durable
    // snapshot, which is the only window in which a superseded generation
    // could name the same bucket; mutation paths must set it, read-only
    // paths may skip it.
    LiveKeyRecord* FindMatchingEntryLocked(const std::string& key,
                                           const DistributedFSDescriptor& desc,
                                           bool verify_placement_offset,
                                           BucketPtr* out_bucket);

    // One live entry a worker found in a bucket's `.meta`. The descriptor is
    // built on the worker so that committing stays a bookkeeping step;
    // `generation` and `entry_offset` are what the duplicate-key comparison
    // needs.
    struct RecoveryCandidate {
        RecoveredReplica replica;
        uint64_t generation = 0;
        uint64_t entry_offset = 0;
    };

    // A bucket as parsed from disk by one recovery worker: the state to publish
    // plus the COMMITTED entries that may win a key. Nothing here reads or
    // writes allocator state, which is what keeps workers free of `mutex_`.
    struct ParsedBucket {
        BucketPtr bucket;
        std::vector<RecoveryCandidate> candidates;
        uint64_t max_generation = 0;
        bool discarded = false;
    };

    // A key that turned up in more than one bucket. The winner is deliberately
    // not decided while committing: that needs the running winner's generation,
    // and this model keeps no per-bucket entry table to read it from. Recording
    // it instead keeps the bookkeeping proportional to the number of duplicated
    // keys - which is rare - rather than to the number of keys.
    struct RecoveryConflict {
        std::string key;
        int64_t bucket_id = -1;
    };

    // Parses one bucket's `.meta` without touching allocator state, so it is
    // safe to run on a worker thread.
    ParsedBucket ParseBucketMeta(int64_t bucket_id);

    // Publishes one parsed bucket under `mutex_`. A candidate whose key is not
    // in `key_index_` is the final winner and is appended to `winners`; a
    // duplicate is recorded in `recovery_conflicts_` for the final pass.
    void CommitParsedBucketLocked(int64_t bucket_id, ParsedBucket& parsed,
                                  std::vector<RecoveredReplica>& winners);

    // Decides every recorded duplicate by the persisted
    // (generation, bucket_id, entry_offset) tuple and streams the corrected
    // winner. Single-threaded and called with no lock held, so the bucket reads
    // it performs are ordinary DFS I/O.
    void ResolveRecoveryConflicts(const RecoveredBatchConsumer& consumer);

    // Deletes the files of buckets recovery gave up on, one bucket at a time,
    // on the dedicated `recovery_cleanup_thread_`. Runs after recovery has
    // finished so the startup path never blocks on deleting a large backlog.
    void CleanupSkippedBuckets(std::vector<int64_t> skipped_ids);

    void DeleteBucketFiles(int64_t bucket_id);

    uint64_t UsedBytesLocked() const;

    std::string fsdir_;
    std::string fs_adapter_type_;
    std::unique_ptr<FileSystemAdapter> fs_adapter_;

    uint64_t bucket_capacity_ = 0;
    uint64_t alignment_ = 4096;
    int64_t max_bucket_count_ = 0;
    size_t ready_bucket_target_ = 4;
    size_t bucket_create_concurrency_ = 2;

    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::chrono::seconds eviction_check_interval_{5};

    mutable std::mutex mutex_;
    // Serializes `.meta` writes and eviction deletion. Flush and eviction both
    // revalidate BucketState pointer identity after taking this mutex so a
    // captured operation cannot recreate or overwrite metadata for a bucket
    // that has since been removed.
    std::mutex metadata_io_mutex_;
    std::deque<int64_t> ready_bucket_ids_;
    size_t bucket_creations_in_flight_ = 0;
    size_t waiting_allocators_ = 0;
    bool synchronous_creation_in_flight_ = false;
    bool stop_bucket_workers_ = false;
    size_t initialization_creations_remaining_ = 0;
    uint64_t bucket_creation_failure_sequence_ = 0;
    ErrorCode last_bucket_creation_error_ = ErrorCode::OK;
    std::chrono::steady_clock::time_point bucket_creation_retry_after_{};
    std::chrono::seconds bucket_creation_retry_delay_{1};
    std::condition_variable bucket_pool_cv_;
    std::vector<std::thread> bucket_create_workers_;
    // One-shot sweep of legacy bucket files, spawned only when metadata
    // recovery is disabled. Joined by StopBucketWorkers().
    std::thread legacy_cleanup_worker_;
    std::unordered_map<int64_t, BucketPtr> buckets_;
    std::unordered_map<std::string, LiveKeyRecord> key_index_;
    int64_t next_bucket_id_ = 0;
    int64_t active_bucket_id_ = -1;
    uint64_t next_generation_ = 1;
    // Once the high watermark is crossed, keep evicting until usage falls
    // below the low watermark; protected buckets may make that span several
    // prepare/resolve rounds.
    bool eviction_active_ = false;

    std::list<int64_t> lru_list_;
    std::unordered_map<int64_t, std::list<int64_t>::iterator> lru_index_;

    // Bucket ids Init() discovered but has not parsed yet. Recovery consumes
    // them one batch at a time, so no two buckets' `.meta` contents coexist
    // beyond the in-flight batch.
    std::vector<int64_t> pending_recovery_meta_ids_;
    std::vector<int64_t> pending_recovery_data_ids_;
    // Resolved at Init(): the configured worker count, or the auto value when
    // the configured one is 0.
    size_t recovery_worker_count_ = 0;
    // Resolved at Init(): the number of buckets recovery may restore before it
    // gives up and skips the rest, or 0 for no bucket-count limit.
    size_t recovery_max_buckets_ = 0;
    // Resolved at Init(): the wall-clock budget in seconds for recovery,
    // or 0 for no time limit.
    int recovery_max_duration_seconds_ = 0;
    // Duplicate keys seen while committing, resolved in one final pass.
    std::vector<RecoveryConflict> recovery_conflicts_;
    // Joins in the destructor before `fs_adapter_` is torn down: the cleanup
    // thread deletes files through the adapter.
    std::thread recovery_cleanup_thread_;

    std::shared_ptr<const BucketIdSnapshot> bucket_id_snapshot_;
    std::atomic<uint64_t> bucket_snapshot_version_{0};

    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake
