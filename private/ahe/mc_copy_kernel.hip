// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <hip/hip_runtime.h>
#include <stdint.h>

// One block copies one range from mapped pinned host memory.
// src_addr is the device-visible alias returned by hipHostGetDevicePointer.
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

    const bool aligned =
        ((range.dst_addr | range.src_addr | range.size) & 15) == 0;
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
