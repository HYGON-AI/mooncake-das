#pragma once

/**
 * @brief C ABI shared between the public tree and the private extension
 *        shared objects.
 *
 * The public tree only describes the shape of the data that crosses the
 * boundary. No algorithm, threshold, or tuning constant of the private
 * optimization belongs here: the provider owns every such decision.
 *
 * Two independent providers exist:
 *   - libmooncake_dfe.so.1 (DFS data-plane extension, pure POSIX, no device
 *     runtime). Loaded by client and, for the batch-placement hook, the master.
 *   - libmooncake_ahe.so.1 (accelerator host-to-device extension, links HIP).
 *     Loaded by the client only.
 *
 * Both use the same handshake: the public side fills abi_version and
 * struct_size, calls the exported entry point, and the provider fills the
 * remaining fields (user_data plus function pointers). A provider returns
 * MOONCAKE_EXT_STATUS_INVALID_ARGUMENT when the two sides disagree, and the
 * public side then keeps its own baseline behavior.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOONCAKE_DFE_ABI_VERSION 3u
#define MOONCAKE_AHE_ABI_VERSION 1u

enum {
    MOONCAKE_EXT_STATUS_OK = 0,
    MOONCAKE_EXT_STATUS_UNAVAILABLE = 1,
    MOONCAKE_EXT_STATUS_INVALID_ARGUMENT = 2,
    MOONCAKE_EXT_STATUS_IO_ERROR = 3,
    MOONCAKE_EXT_STATUS_NOT_SUPPORTED = 4,
    MOONCAKE_EXT_STATUS_NO_SPACE = 5,
    /* The provider was asked to act but its feature is switched off by
     * configuration. Distinct from NOT_SUPPORTED so the caller logs the real
     * reason instead of reporting an unsupported filesystem. */
    MOONCAKE_EXT_STATUS_DISABLED = 6,
};

typedef struct mooncake_ext_iovec {
    void* base;
    uint64_t length;
} mooncake_ext_iovec;

typedef enum mooncake_ext_io_op {
    MOONCAKE_EXT_IO_READ = 0,
    MOONCAKE_EXT_IO_WRITE = 1,
} mooncake_ext_io_op;

/**
 * One logical I/O transfer. The public side always owns the buffers addressed
 * by `iovecs`; the provider must not retain any pointer after `io_batch`
 * returns. `offset` is the file offset of the first byte of `iovecs[0]`.
 *
 * `direct_io` requests the page-cache-bypassing semantics the caller would get
 * from a dedicated direct handle; the provider is responsible for any bounce
 * buffering that those semantics require.
 *
 * `mergeable` tells the provider that this transfer may be coalesced with an
 * adjacent one that shares `fd` and a contiguous byte range. `worker_count` is
 * the caller's upper bound on parallelism for one call; zero means "provider
 * default".
 */
typedef struct mooncake_ext_io_request {
    int64_t fd;
    uint64_t offset;
    uint64_t total_size;
    uint32_t op;
    uint32_t direct_io;
    uint32_t mergeable;
    uint32_t worker_count;
    mooncake_ext_iovec* iovecs;
    uint64_t iovec_count;
    uint64_t reserved;
} mooncake_ext_io_request;

typedef struct mooncake_ext_io_result {
    int32_t status;
    /* errno reported by the provider when status != OK (0 otherwise), so the
     * caller can map ENOENT to FILE_NOT_FOUND exactly like the baseline. */
    int32_t error_code;
    uint64_t bytes_done;
} mooncake_ext_io_result;

/**
 * Capacity and chunk sizes of the unified DFS buffer the caller is about to
 * configure. The caller only reports the resources it owns; how much of that
 * capacity each class of traffic may hold back is the provider's decision.
 */
typedef struct mooncake_ext_buffer_policy_query {
    uint64_t capacity_bytes;
    uint64_t read_chunk_bytes;
    uint64_t write_chunk_bytes;
    uint64_t reserved;
} mooncake_ext_buffer_policy_query;

/**
 * Admission budgets for the unified buffer. All byte values are absolute and
 * clamped by the caller to the capacity above; `high_watermark` is the
 * utilization fraction that raises the high-usage warning.
 */
typedef struct mooncake_ext_buffer_policy {
    uint64_t read_min_protection_bytes;
    uint64_t write_min_protection_bytes;
    uint64_t prefetch_limit_bytes;
    uint32_t high_watermark_numerator;
    uint32_t high_watermark_denominator;
} mooncake_ext_buffer_policy;

/**
 * @brief DFS data-plane extension.
 *
 * All function pointers are optional. A caller must check for null before use
 * and fall back to its own implementation otherwise.
 */
typedef struct mooncake_dfe_ext {
    uint32_t abi_version;
    uint32_t struct_size;
    void* user_data;

    /** Batched read/write. `requests[i].op` selects the direction. */
    int (*io_batch)(void* user_data, const mooncake_ext_io_request* requests,
                    mooncake_ext_io_result* results, size_t request_count);

    /**
     * Choose the start offset of one batch inside a bucket.
     *
     * @param append_offset current append offset inside the bucket
     * @param capacity      bucket capacity in bytes
     * @param object_size   bytes this allocation needs
     * @param out_offset    chosen start offset
     * @param out_alignment alignment the caller must pass when it asks for a
     *                      replacement bucket; zero means "no alignment"
     * @param fits          non-zero when out_offset + object_size fits
     * @return MOONCAKE_EXT_STATUS_OK on success
     */
    int (*align_batch_offset)(void* user_data, uint64_t append_offset,
                              uint64_t capacity, uint64_t object_size,
                              uint64_t* out_offset, uint64_t* out_alignment,
                              uint32_t* fits);

    /**
     * Configure one bucket write handle. Returns MOONCAKE_EXT_STATUS_OK when
     * the handle now skips the metadata work the provider is allowed to skip,
     * MOONCAKE_EXT_STATUS_NOT_SUPPORTED when the filesystem has no such
     * extension, or MOONCAKE_EXT_STATUS_IO_ERROR on a hard failure.
     */
    int (*configure_write_fd)(void* user_data, int fd,
                              uint64_t max_write_bytes);

    /** Refresh a sealed bucket file's mtime on `fd`. */
    int (*refresh_file_mtime)(void* user_data, int fd);

    /**
     * Derive the unified DFS buffer admission budgets. The provider owns the
     * protection depth, the prefetch cap and the high-watermark fraction; the
     * caller only supplies the capacity and the size of one aligned chunk.
     * Returns MOONCAKE_EXT_STATUS_OK on success.
     */
    int (*buffer_policy)(void* user_data,
                         const mooncake_ext_buffer_policy_query* query,
                         mooncake_ext_buffer_policy* result);
} mooncake_dfe_ext;

/**
 * @brief Accelerator host-to-device extension.
 *
 * The provider owns the returned handle and every pointer produced by
 * `alloc_mapped`; the public side must only pass them back to this provider.
 */
typedef struct mooncake_ahe_copy_range {
    const void* src;
    /**
     * Device-visible alias of `src` when it is mapped host memory, or NULL for
     * ordinary pinned memory. A batch that contains a NULL alias is copied
     * with plain DMA instead of a scatter kernel.
     */
    const void* src_device;
    uint64_t dst;
    uint64_t size;
} mooncake_ahe_copy_range;

typedef struct mooncake_ahe_ext {
    uint32_t abi_version;
    uint32_t struct_size;
    void* user_data;

    /** Create a submit handle for one device ordinal, or NULL on failure. */
    void* (*create)(void* user_data, int device_ordinal, uint32_t stream_count);
    /**
     * Queue ranges on the stream selected by `stream_key`. Ranges sharing a
     * key keep their submission order.
     */
    int (*submit)(void* user_data, void* handle,
                  const mooncake_ahe_copy_range* ranges, size_t range_count,
                  uint32_t stream_key);
    /** Wait for every queued range of `handle` to complete. */
    int (*sync)(void* user_data, void* handle);
    void (*destroy)(void* user_data, void* handle);

    /** Allocate host (mapped pinned) memory plus its device alias. */
    int (*alloc_mapped)(void* user_data, int device_ordinal, uint64_t size,
                        void** host_ptr, void** device_ptr);
    void (*free_mapped)(void* user_data, void* host_ptr);
} mooncake_ahe_ext;

int mooncake_dfe_extension_get(mooncake_dfe_ext* extension);
int mooncake_ahe_extension_get(mooncake_ahe_ext* extension);

#ifdef __cplusplus
}  // extern "C"
#endif
