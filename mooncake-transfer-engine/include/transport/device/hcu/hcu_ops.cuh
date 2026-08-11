// HCU/DTK implementations of the device-side primitives consumed by the
// transport Device API. The source Hygon kernels used system-scope HIP
// atomics for GPU/CPU/NIC-visible state and explicit HDP flushing before
// ringing an IBGDA doorbell; keep those semantics behind the shared mc_* API.
#pragma once

#include <hip/hip_runtime.h>

namespace mooncake {
namespace device {

__device__ __forceinline__ uint16_t mc_ld_acquire_u16(
    const uint16_t* ptr) {
    return __hip_atomic_load(ptr, __ATOMIC_ACQUIRE,
                             __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ int mc_ld_acquire(const int* ptr) {
    return __hip_atomic_load(ptr, __ATOMIC_ACQUIRE,
                             __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ uint32_t mc_ld_acquire_u32(const uint32_t* ptr) {
    return __hip_atomic_load(ptr, __ATOMIC_ACQUIRE,
                             __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ uint64_t mc_ld_acquire_u64(
    const uint64_t* ptr) {
    return __hip_atomic_load(ptr, __ATOMIC_ACQUIRE,
                             __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ void mc_st_release(const int* ptr, int val) {
    __hip_atomic_store(const_cast<int*>(ptr), val, __ATOMIC_RELEASE,
                       __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ void mc_st_release_u32(const uint32_t* ptr,
                                                  uint32_t val) {
    __hip_atomic_store(const_cast<uint32_t*>(ptr), val, __ATOMIC_RELEASE,
                       __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ void mc_st_release_u64(const uint64_t* ptr,
                                                  uint64_t val) {
    __hip_atomic_store(const_cast<uint64_t*>(ptr), val, __ATOMIC_RELEASE,
                       __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ int mc_atomic_add_release(const int* ptr, int val) {
    return __hip_atomic_fetch_add(const_cast<int*>(ptr), val, __ATOMIC_RELEASE,
                                  __HIP_MEMORY_SCOPE_SYSTEM);
}

__device__ __forceinline__ int4 mc_ld_nc(const int4* ptr) {
    using HcuInt4 = int __attribute__((ext_vector_type(4)));
    auto value =
        __builtin_nontemporal_load(reinterpret_cast<const HcuInt4*>(ptr));
    return *reinterpret_cast<int4*>(&value);
}

__device__ __forceinline__ int mc_ld_nc_s32(const int* ptr) {
    return __builtin_nontemporal_load(ptr);
}

__device__ __forceinline__ float mc_ld_nc_f32(const float* ptr) {
    return __builtin_nontemporal_load(ptr);
}

__device__ __forceinline__ int64_t mc_ld_nc_s64(const int64_t* ptr) {
    return __builtin_nontemporal_load(ptr);
}

__device__ __forceinline__ void mc_st_na(const int4* ptr, const int4& val) {
    *const_cast<int4*>(ptr) = val;
}

__device__ __forceinline__ void mc_bar_init() {}

// HCU has no PTX named sub-CTA barrier. EP marks HCU as a split-kernel
// platform and makes every thread in the CTA execute the same barrier sites,
// so a full workgroup barrier is the correct implementation.
__device__ __forceinline__ void mc_bar_sync(int /*bar_id*/,
                                            int /*num_threads*/) {
    __syncthreads();
}

__device__ __forceinline__ void mc_grid_sync() {}

__device__ __forceinline__ void mc_fence() { __threadfence_system(); }

__device__ __forceinline__ void mc_fence_barrier_fence() {
    mc_fence();
    __syncthreads();
    mc_fence();
}

__device__ __forceinline__ void mc_flush_hdp(uint32_t* hdp_flush) {
    mc_fence();
    if (hdp_flush != nullptr) {
        __hip_atomic_store(hdp_flush, 1u, __ATOMIC_SEQ_CST,
                           __HIP_MEMORY_SCOPE_SYSTEM);
        mc_fence();
    }
}

__device__ __forceinline__ uint16_t mc_bswap16(uint16_t x) {
    return __builtin_bswap16(x);
}

__device__ __forceinline__ uint32_t mc_bswap32(uint32_t x) {
    return __builtin_bswap32(x);
}

__device__ __forceinline__ uint64_t mc_bswap64(uint64_t x) {
    return __builtin_bswap64(x);
}

__device__ __forceinline__ void mc_trap() { __builtin_trap(); }

}  // namespace device
}  // namespace mooncake
