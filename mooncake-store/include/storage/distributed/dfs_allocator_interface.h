#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "replica.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

/**
 * @brief Selects which DFS space-management strategy the master uses.
 *
 * SHARD keeps the pre-existing DfsGlobalAllocator behaviour: a fixed number
 * of preallocated shard files whose space is handed out by an OffsetAllocator
 * and reclaimed per key.
 *
 * BUCKET uses ImmutableBucketAllocator: a dynamic set of append-only bucket
 * files with persisted `.meta` sidecars, whole-bucket LRU eviction and
 * restart recovery.
 */
enum class DfsAllocatorType {
    SHARD,
    BUCKET,
};

/**
 * @brief Parse an allocator type name.
 *
 * Returns std::nullopt for unknown names so callers can surface a
 * configuration error instead of silently falling back to SHARD.
 */
std::optional<DfsAllocatorType> ParseDfsAllocatorType(std::string_view name);

const char* ToString(DfsAllocatorType type);

/**
 * @brief One entry of a batch space reservation request.
 */
struct BatchAllocateRequest {
    std::string key;
    uint64_t size = 0;
};

/**
 * @brief Per-key outcome of BatchAllocate.
 *
 * Exactly one result is produced for every request, in request order. On
 * success `descriptor` is filled and `error` is ErrorCode::OK; on failure
 * `descriptor` is left default-constructed and must not be used.
 */
struct BatchAllocateResult {
    std::string key;
    DistributedFSDescriptor descriptor;
    bool success = false;
    ErrorCode error = ErrorCode::OK;
};

/**
 * @brief Common interface for DFS space allocators.
 *
 * The master owns exactly one implementation, chosen by configuration, and
 * only ever talks to it through this interface. Both implementations are
 * expected to be safe for concurrent use from RPC handler threads and the
 * master eviction thread.
 *
 * Descriptor conventions shared by all implementations:
 *  - `file_path`    absolute path of the backing data file
 *  - `offset`       byte offset at which the *value* starts
 *  - `object_size`  value length in bytes
 *  - `aligned_size` bytes reserved for the whole entry (>= object_size)
 *  - `shard_idx`    SHARD: shard index; BUCKET: bucket id
 *
 * Note that BUCKET mode reuses `shard_idx` as the bucket id, so consumers
 * must not assume `shard_idx < shard_count`.
 */
class DfsAllocatorInterface {
   public:
    /**
     * @brief A single allocation selected for eviction.
     *
     * `descriptor` is byte-for-byte the descriptor originally handed out by
     * Allocate()/BatchAllocate(), so the master can match it against replica
     * metadata without reconstructing offsets itself.
     */
    struct EvictionCandidate {
        std::string key;
        int shard_idx = 0;
        uint64_t offset = 0;
        DistributedFSDescriptor descriptor;
    };

    virtual ~DfsAllocatorInterface() = default;

    virtual DfsAllocatorType Type() const = 0;

    virtual tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig& config) = 0;

    virtual bool IsInitialized() const = 0;

    /**
     * @brief Reserve space for a single key.
     *
     * Used by non-batch paths (PutStart, UpsertStart, Copy). Returns
     * INVALID_PARAMS for an empty key or zero size, NO_AVAILABLE_HANDLE when
     * space is exhausted and DFS_SERVICE_UNAVAILABLE before Init().
     */
    virtual tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) = 0;

    /**
     * @brief Reserve space for a whole batch.
     *
     * Implementations that can do so should place the whole batch
     * contiguously so the client can write it with sequential I/O. The
     * returned vector always has exactly `requests.size()` entries in
     * request order.
     */
    virtual std::vector<BatchAllocateResult> BatchAllocate(
        const std::vector<BatchAllocateRequest>& requests) = 0;

    /**
     * @brief Release the space described by `descriptor` for `key`.
     *
     * Must be a no-op when the descriptor no longer matches the allocator's
     * current state for that key (i.e. a stale free from a superseded
     * generation), so late completions cannot drop a newer allocation.
     */
    virtual void Free(const std::string& key,
                      const DistributedFSDescriptor& descriptor) = 0;

    /**
     * @brief Mark `key`/`descriptor` as recently used for LRU purposes.
     * Stale descriptors are ignored.
     */
    virtual void UpdateAccess(const std::string& key,
                              const DistributedFSDescriptor& descriptor) = 0;

    virtual bool IsEvictionEnabled() const = 0;

    virtual std::chrono::seconds GetEvictionCheckInterval() const = 0;

    virtual uint64_t GetTotalCapacity() const = 0;

    virtual uint64_t GetUsedBytes() const = 0;
};

}  // namespace mooncake
