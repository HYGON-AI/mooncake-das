#pragma once

#include <mooncake_ep_configs.cuh>
#include <cstdio>
#include <memory>
#include <utility>

namespace mooncake::hip_launch {
struct LaunchConfig {
    dim3 grid;
    dim3 block;
    unsigned int shared_mem;
    hipStream_t stream;
};

template <typename T>
inline void fill_args(void** args, size_t index, T&& arg) {
    args[index] = static_cast<void*>(std::addressof(arg));
    return;
}

template <typename Head, typename... Tail>
inline void fill_args(void** args, size_t index, Head&& head, Tail&&... tail) {
    args[index] = static_cast<void*>(std::addressof(head));
    fill_args(args, index + 1, std::forward<Tail>(tail)...);
    return;
}

template <typename Kernel, typename... Args>
inline void launch(const LaunchConfig& config, Kernel kernel, Args&&... args) {
    void* kernel_args[sizeof...(args)];
    fill_args(kernel_args, 0, std::forward<Args>(args)...);
    const auto result = hipLaunchCooperativeKernel(
        reinterpret_cast<const void*>(kernel), config.grid, config.block,
        kernel_args, config.shared_mem, config.stream);
    if (result != hipSuccess) {
        hipFuncAttributes attr{};
        int device = 0;
        int cooperative = 0;
        hipGetDevice(&device);
        hipDeviceGetAttribute(&cooperative, hipDeviceAttributeCooperativeLaunch,
                              device);
        hipFuncGetAttributes(&attr, reinterpret_cast<const void*>(kernel));
        std::fprintf(stderr,
                     "[EP] HIP cooperative launch failed: grid=%u block=%u "
                     "max_threads=%d regs=%d static_smem=%zu cooperative=%d\n",
                     config.grid.x, config.block.x, attr.maxThreadsPerBlock,
                     attr.numRegs, attr.sharedSizeBytes, cooperative);
    }
    HIP_CHECK(result);
}
}  // namespace mooncake::hip_launch

#define SETUP_LAUNCH_CONFIG(num_sms, num_threads, stream) \
    mooncake::hip_launch::LaunchConfig cfg = {            \
        dim3(num_sms), dim3(num_threads), 0, stream}
#define LAUNCH_KERNEL(config, kernel, ...) \
    mooncake::hip_launch::launch(*(config), kernel, ##__VA_ARGS__)

#define SWITCH_RANKS(case_macro)                           \
    switch (num_ranks) {                                   \
        case 2:                                            \
            case_macro(2);                                 \
        case 4:                                            \
            case_macro(4);                                 \
        case 8:                                            \
            case_macro(8);                                 \
        default:                                           \
            EP_HOST_ASSERT(false and "Unsupported ranks"); \
    }                                                      \
    while (false)

#define SWITCH_RDMA_RANKS(case_macro)                           \
    switch (num_ranks / NUM_MAX_NVL_PEERS) {                    \
        case 2:                                                 \
            case_macro(2);                                      \
        case 3:                                                 \
            case_macro(3);                                      \
        case 4:                                                 \
            case_macro(4);                                      \
        case 8:                                                 \
            case_macro(8);                                      \
        case 16:                                                \
            case_macro(16);                                     \
        case 18:                                                \
            case_macro(18);                                     \
        case 20:                                                \
            case_macro(20);                                     \
        default:                                                \
            EP_HOST_ASSERT(false and "Unsupported RDMA ranks"); \
    }                                                           \
    while (false)

#define SWITCH_RANKS_WITH_DTYPE(dtype, case_macro)        \
    switch (num_ranks) {                                  \
        case 2:                                           \
            case_macro(dtype, 2);                         \
        case 4:                                           \
            case_macro(dtype, 4);                         \
        case 8:                                           \
            case_macro(dtype, 8);                         \
        default:                                          \
            EP_HOST_ASSERT(false && "Unsupported ranks"); \
    }                                                     \
    while (false)

#define SWITCH_TYPES(case_macro)                         \
    switch (type) {                                      \
        case HIP_R_16BF:                                 \
            case_macro(hip_bfloat16);                    \
        case HIP_R_32F:                                  \
            case_macro(float);                           \
        default:                                         \
            EP_HOST_ASSERT(false && "Unsupported type"); \
    }                                                    \
    while (false)

#define SWITCH_HIDDEN(case_macro)                          \
    switch (hidden) {                                      \
        case 2048:                                         \
            case_macro(2048);                              \
        case 2560:                                         \
            case_macro(2560);                              \
        case 3072:                                         \
            case_macro(3072); /* for gpt-oss */            \
        case 4096:                                         \
            case_macro(4096);                              \
        case 5120:                                         \
            case_macro(5120);                              \
        case 6144:                                         \
            case_macro(6144); /* For qwen3 coder */        \
        case 7168:                                         \
            case_macro(7168);                              \
        case 8192:                                         \
            case_macro(8192);                              \
        default:                                           \
            EP_HOST_ASSERT(false && "Unsupported hidden"); \
    }                                                      \
    while (false)
