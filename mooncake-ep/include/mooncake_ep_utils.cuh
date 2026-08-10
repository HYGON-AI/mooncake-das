#pragma once

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>

#define UNROLLED_WARP_COPY(UNROLL_FACTOR, LANE_ID, N, DST, SRC, LD_FUNC, ST_FUNC) \
    {                                                                              \
        constexpr int stride = kWarpSize * (UNROLL_FACTOR);                        \
        auto src_ = (SRC);                                                         \
        auto dst_ = (DST);                                                         \
        for (int i_ = (LANE_ID); i_ < (N); i_ += stride) {                        \
            _Pragma("unroll") for (int j_ = 0; j_ < (UNROLL_FACTOR); ++j_) {       \
                int idx_ = i_ + j_ * kWarpSize;                                    \
                if (idx_ < (N)) ST_FUNC(dst_ + idx_, LD_FUNC(src_ + idx_));        \
            }                                                                      \
        }                                                                          \
    }

namespace mooncake {

template <int N>
struct VecInt {};
template <>
struct VecInt<1> { using vec_t = int8_t; };
template <>
struct VecInt<2> { using vec_t = int16_t; };
template <>
struct VecInt<4> { using vec_t = int; };
template <>
struct VecInt<8> { using vec_t = int64_t; };
template <>
struct VecInt<16> { using vec_t = int __attribute__((ext_vector_type(4))); };

template <typename T>
__device__ __forceinline__ T shfl(T value, int src, int width = kWarpSize) {
    return __shfl(value, src, width);
}
template <typename T>
__device__ __forceinline__ T shfl_xor(T value, int mask,
                                      int width = kWarpSize) {
    return __shfl_xor(value, mask, width);
}
__device__ __forceinline__ void syncwarp() {
    __builtin_amdgcn_fence(__ATOMIC_RELEASE, "wavefront");
    __builtin_amdgcn_wave_barrier();
    __builtin_amdgcn_fence(__ATOMIC_ACQUIRE, "wavefront");
}
__device__ __forceinline__ void trap() { __builtin_trap(); }
__device__ __forceinline__ void memory_fence() { __threadfence_system(); }
__device__ __forceinline__ void memory_fence_gpu() { __threadfence(); }
__device__ __forceinline__ void memory_fence_cta() { __threadfence_block(); }

#define MOONCAKE_HIP_LOAD(name, order, scope)                                  \
    template <typename T>                                                      \
    __device__ __forceinline__ T name(const T* ptr) {                          \
        return __hip_atomic_load(ptr, order, scope);                            \
    }
#define MOONCAKE_HIP_STORE(name, order, scope)                                 \
    template <typename T>                                                      \
    __device__ __forceinline__ void name(const T* ptr, T value) {              \
        __hip_atomic_store(const_cast<T*>(ptr), value, order, scope);           \
    }

MOONCAKE_HIP_LOAD(ld_na_relaxed, __ATOMIC_RELAXED, __HIP_MEMORY_SCOPE_AGENT)
MOONCAKE_HIP_LOAD(ld_volatile_global, __ATOMIC_RELAXED,
                  __HIP_MEMORY_SCOPE_SYSTEM)
MOONCAKE_HIP_LOAD(ld_acquire_sys_global, __ATOMIC_ACQUIRE,
                  __HIP_MEMORY_SCOPE_SYSTEM)
MOONCAKE_HIP_LOAD(ld_acquire_global, __ATOMIC_ACQUIRE, __HIP_MEMORY_SCOPE_AGENT)
MOONCAKE_HIP_LOAD(ld_acquire_cta, __ATOMIC_ACQUIRE,
                  __HIP_MEMORY_SCOPE_WORKGROUP)
MOONCAKE_HIP_STORE(st_na_relaxed, __ATOMIC_RELAXED, __HIP_MEMORY_SCOPE_AGENT)
MOONCAKE_HIP_STORE(st_na_release, __ATOMIC_RELEASE, __HIP_MEMORY_SCOPE_AGENT)
MOONCAKE_HIP_STORE(st_release_sys_global, __ATOMIC_RELEASE,
                   __HIP_MEMORY_SCOPE_SYSTEM)
MOONCAKE_HIP_STORE(st_release_cta, __ATOMIC_RELEASE,
                   __HIP_MEMORY_SCOPE_WORKGROUP)

__device__ __forceinline__ void st_relaxed_sys_global(const int* ptr, int value) {
    __hip_atomic_store(const_cast<int*>(ptr), value, __ATOMIC_RELAXED,
                       __HIP_MEMORY_SCOPE_SYSTEM);
}
__device__ __forceinline__ int atomic_add_release_sys_global(const int* ptr,
                                                              int value) {
    return __hip_atomic_fetch_add(const_cast<int*>(ptr), value, __ATOMIC_RELEASE,
                                  __HIP_MEMORY_SCOPE_SYSTEM);
}
__device__ __forceinline__ int atomic_add_release_global(const int* ptr,
                                                          int value) {
    return __hip_atomic_fetch_add(const_cast<int*>(ptr), value, __ATOMIC_RELEASE,
                                  __HIP_MEMORY_SCOPE_AGENT);
}
__device__ __forceinline__ void st_na_relaxed(const int4* ptr, int4 value) {
    auto out = const_cast<int4*>(ptr);
    st_na_relaxed(&out->x, value.x);
    st_na_relaxed(&out->y, value.y);
    st_na_relaxed(&out->z, value.z);
    st_na_relaxed(&out->w, value.w);
}
template <typename T>
__device__ __forceinline__ T ld_nc_global(const T* ptr) {
    using V = typename VecInt<sizeof(T)>::vec_t;
    auto value = __builtin_nontemporal_load(reinterpret_cast<const V*>(ptr));
    return *reinterpret_cast<T*>(&value);
}
template <typename T>
__device__ __forceinline__ void st_na_global(const T* ptr, const T& value) {
    *const_cast<T*>(ptr) = value;
}

template <typename T>
__host__ __device__ T cell_div(T a, T b) { return (a + b - 1) / b; }
template <typename T>
__host__ __device__ T align(T a, T b) { return cell_div(a, b) * b; }
__forceinline__ __device__ void get_channel_task_range(
    int num_tokens, int num_sms, int sm_id, int& begin, int& end) {
    int per_sm = cell_div(num_tokens, num_sms);
    begin = min(per_sm * sm_id, num_tokens);
    end = min(begin + per_sm, num_tokens);
}
template <typename A, typename B>
__device__ __forceinline__ B pack2(const A& x, const A& y) {
    B packed;
    auto values = reinterpret_cast<A*>(&packed);
    values[0] = x;
    values[1] = y;
    return packed;
}
template <typename A, typename B>
__device__ __forceinline__ void unpack2(const B& packed, A& x, A& y) {
    auto values = reinterpret_cast<const A*>(&packed);
    x = values[0];
    y = values[1];
}
template <typename T>
__device__ __forceinline__ T broadcast(T& value, int src) {
    auto input = reinterpret_cast<int*>(&value);
    int output[sizeof(T) / sizeof(int)];
#pragma unroll
    for (int i = 0; i < sizeof(T) / sizeof(int); ++i) output[i] = shfl(input[i], src);
    return *reinterpret_cast<T*>(output);
}
__forceinline__ __device__ int warp_reduce_sum(int value) {
    value += shfl_xor(value, 32);
    value += shfl_xor(value, 16);
    value += shfl_xor(value, 8);
    value += shfl_xor(value, 4);
    value += shfl_xor(value, 2);
    return value + shfl_xor(value, 1);
}
__forceinline__ __device__ float quant_group_reduce_max(float value) {
    constexpr int kQuantGroupThreads = 16;
    value = fmaxf(value, shfl_xor(value, 8, kQuantGroupThreads));
    value = fmaxf(value, shfl_xor(value, 4, kQuantGroupThreads));
    value = fmaxf(value, shfl_xor(value, 2, kQuantGroupThreads));
    return fmaxf(value, shfl_xor(value, 1, kQuantGroupThreads));
}
__forceinline__ __device__ int get_lane_id() { return threadIdx.x % kWarpSize; }
template <int N>
__forceinline__ __device__ void move_fifo_slots(int& head) {
    head = (head + N) % NUM_MAX_FIFO_SLOTS;
}
template <int N>
__device__ __forceinline__ bool not_finished(int* task, int expected) {
    int lane = get_lane_id();
    return (__ballot(lane < N && ld_volatile_global(task + lane) != expected) &
            kFullWarpMask) != 0;
}
template <int N>
__forceinline__ __device__ void timeout_check(int** fifos, int head, int rank,
                                              int expected, int tag = 0) {
    auto start = clock64();
    while (not_finished<N>(fifos[rank] + head, expected)) {
        if (clock64() - start > NUM_TIMEOUT_CYCLES && threadIdx.x == 0) {
            printf("Timeout check failed: %d (rank = %d)\n", tag, rank);
            trap();
        }
    }
}
template <int N>
__forceinline__ __device__ void barrier_device(int** fifos, int head, int rank,
                                               int tag = 0) {
    int tid = threadIdx.x;
    EP_DEVICE_ASSERT(N <= kWarpSize);
    if (tid < N) {
        atomicAdd_system(fifos[rank] + head + tid, FINISHED_SUM_TAG);
        memory_fence();
        atomicSub_system(fifos[tid] + head + rank, FINISHED_SUM_TAG);
    }
    timeout_check<N>(fifos, head, rank, 0, tag);
}

}  // namespace mooncake
