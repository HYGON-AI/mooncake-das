// A deliberately tiny DFE provider used by extension_loader_test to exercise
// the public contract without the real private implementation.
//
// The behavior is deterministic and stateless so the test can assert on the
// exact values the public loader passes across the ABI boundary.

#include <cstdlib>
#include <cstring>

#include "ext/mooncake_private_ext_abi.h"

namespace {

constexpr uint64_t kFakeAlignment = 65536;
constexpr uint64_t kFakeChunk = 4096;

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    return (value + alignment - 1) / alignment * alignment;
}

int FakeIoBatch(void* /*user_data*/, const mooncake_ext_io_request* requests,
                mooncake_ext_io_result* results, size_t request_count) {
    for (size_t i = 0; i < request_count; ++i) {
        const mooncake_ext_io_request& request = requests[i];
        if (request.iovecs == nullptr || request.iovec_count == 0) {
            results[i].status = MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
            results[i].bytes_done = 0;
            continue;
        }
        // Fill every byte with a value derived from the offset so the caller
        // can tell whether the provider wrote the buffers.
        const unsigned char pattern =
            static_cast<unsigned char>((request.offset / kFakeChunk) & 0xFF);
        for (uint64_t v = 0; v < request.iovec_count; ++v) {
            unsigned char* base =
                static_cast<unsigned char*>(request.iovecs[v].base);
            std::memset(base, pattern, request.iovecs[v].length);
        }
        results[i].status = MOONCAKE_EXT_STATUS_OK;
        results[i].bytes_done = request.total_size;
    }
    return MOONCAKE_EXT_STATUS_OK;
}

int FakeAlignBatchOffset(void* /*user_data*/, uint64_t append_offset,
                         uint64_t capacity, uint64_t object_size,
                         uint64_t* out_offset, uint64_t* out_alignment,
                         uint32_t* fits) {
    if (out_offset == nullptr || out_alignment == nullptr || fits == nullptr) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    *out_alignment = kFakeAlignment;
    // Pure geometry: a bucket is full only when the object does not fit at the
    // aligned start. The real provider no longer rejects placements once the
    // bucket is over half full, so the fake must not reintroduce that rule.
    const uint64_t aligned = AlignUp(append_offset, kFakeAlignment);
    *out_offset = aligned;
    *fits = (aligned + object_size <= capacity) ? 1u : 0u;
    return MOONCAKE_EXT_STATUS_OK;
}

int FakeConfigureWriteFd(void* /*user_data*/, int fd,
                         uint64_t /*max_write_bytes*/) {
    return fd >= 0 ? MOONCAKE_EXT_STATUS_OK
                   : MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
}

int FakeRefreshFileMtime(void* /*user_data*/, int fd) {
    return fd >= 0 ? MOONCAKE_EXT_STATUS_OK
                   : MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
}

int FakeBufferPolicy(void* /*user_data*/,
                     const mooncake_ext_buffer_policy_query* query,
                     mooncake_ext_buffer_policy* result) {
    if (query == nullptr || result == nullptr) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    result->read_min_protection_bytes = 3 * query->read_chunk_bytes;
    result->write_min_protection_bytes = 2 * query->write_chunk_bytes;
    result->prefetch_limit_bytes = query->capacity_bytes / 3;
    result->high_watermark_numerator = 7;
    result->high_watermark_denominator = 10;
    return MOONCAKE_EXT_STATUS_OK;
}

}  // namespace

extern "C" __attribute__((visibility("default"))) int
mooncake_dfe_extension_get(mooncake_dfe_ext* extension) {
    if (extension == nullptr) return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    if (std::getenv("FAKE_DFE_BAD_ABI") != nullptr) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    if (extension->abi_version != MOONCAKE_DFE_ABI_VERSION ||
        extension->struct_size < sizeof(mooncake_dfe_ext)) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    extension->user_data = nullptr;
    extension->io_batch = &FakeIoBatch;
    extension->align_batch_offset = &FakeAlignBatchOffset;
    extension->configure_write_fd = &FakeConfigureWriteFd;
    extension->refresh_file_mtime = &FakeRefreshFileMtime;
    extension->buffer_policy = &FakeBufferPolicy;
    return MOONCAKE_EXT_STATUS_OK;
}
