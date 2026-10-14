// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

// mc_copy_kernel.cpp
#include <hip/hip_runtime.h>
#include <stdint.h>

extern "C" __global__ __launch_bounds__(1024) void MCCopyKernel(uint64_t dst_addr, uint64_t src_addr, uint64_t size) {
    /**
     * CRITICAL: Addresses are passed as uint64_t instead of pointers.
     * This bypasses the HIP Runtime (UMD) memory object validation, which would
     * otherwise crash when encountering addresses not allocated via hipMalloc.
     */
    uint4* dst_v = (uint4*)dst_addr;
    uint4* src_v = (uint4*)src_addr;
    uint64_t n_v4 = size / 16;

    uint64_t tid = (uint64_t)blockIdx.x * blockDim.x + threadIdx.x;
    uint64_t stride = (uint64_t)blockDim.x * gridDim.x;

    for (uint64_t i = tid; i < n_v4; i += stride) {
        dst_v[i] = src_v[i];
    }

    uint64_t remaining_bytes = size % 16;
    if (tid == 0 && remaining_bytes > 0) {
        uint64_t processed_bytes = n_v4 * 16;
        uint8_t* d_tail = (uint8_t*)dst_addr + processed_bytes;
        uint8_t* s_tail = (uint8_t*)src_addr + processed_bytes;
        for (uint64_t i = 0; i < remaining_bytes; ++i) {
            d_tail[i] = s_tail[i];
        }
    }
    return;
}


// One block handles one independent range. The source address must be the
// device-visible address returned by hipHostGetDevicePointer for mapped pinned
// host memory. Addresses are represented as integers for the same reason as
// MCCopyKernel above: the Hygon runtime must not validate them as hipMalloc
// allocations when the source is mapped host memory.
struct MCStoreH2DCopyRange {
    uint64_t dst_addr;
    uint64_t src_addr;
    uint64_t size;
};

extern "C" __global__ __launch_bounds__(256) void MCStoreH2DScatterKernel(
    const MCStoreH2DCopyRange* ranges, uint64_t range_count) {
    const uint64_t range_index = static_cast<uint64_t>(blockIdx.x);
    if (range_index >= range_count) return;

    const MCStoreH2DCopyRange range = ranges[range_index];
    uint8_t* dst = reinterpret_cast<uint8_t*>(range.dst_addr);
    const uint8_t* src = reinterpret_cast<const uint8_t*>(range.src_addr);

    const bool aligned = ((range.dst_addr | range.src_addr | range.size) & 15) == 0;
    if (aligned) {
        uint4* dst_v = reinterpret_cast<uint4*>(dst);
        const uint4* src_v = reinterpret_cast<const uint4*>(src);
        const uint64_t n_v4 = range.size / 16;
        for (uint64_t i = static_cast<uint64_t>(threadIdx.x); i < n_v4;
             i += static_cast<uint64_t>(blockDim.x)) {
            dst_v[i] = src_v[i];
        }
    } else {
        for (uint64_t i = static_cast<uint64_t>(threadIdx.x); i < range.size;
             i += static_cast<uint64_t>(blockDim.x)) {
            dst[i] = src[i];
        }
    }

}
