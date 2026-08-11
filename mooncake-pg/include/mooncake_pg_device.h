#pragma once

// PG host-runtime facade.  PyTorch exposes ROCm streams through CUDA-shaped
// tensors/events, but the concrete stream wrapper differs from CUDA.  Keep
// that distinction here so the collective algorithms use one shared source.
#ifdef MOONCAKE_PG_USE_HCU
#include <ATen/hip/HIPContext.h>
#include <ATen/hip/HIPGraphsUtils.cuh>
#include <ATen/hip/impl/HIPStreamMasqueradingAsCUDA.h>
#include <c10/hip/HIPStream.h>
#else
#include <ATen/cuda/CUDAContext.h>
#include <ATen/cuda/CUDAGraphsUtils.cuh>
#endif

namespace mooncake {

#ifdef MOONCAKE_PG_USE_HCU
using PgStream =
    decltype(c10::hip::getCurrentHIPStreamMasqueradingAsCUDA());

inline PgStream pgCurrentStream(int device_index = -1) {
    return device_index < 0
               ? c10::hip::getCurrentHIPStreamMasqueradingAsCUDA()
               : c10::hip::getCurrentHIPStreamMasqueradingAsCUDA(device_index);
}

inline int pgCurrentDevice() { return c10::hip::current_device(); }

inline bool pgStreamCaptureActive() {
    // DTK's hipified HIPGraphsUtils.cuh intentionally keeps the ATen helper
    // in at::cuda, while its result type comes from c10::hip.
    return at::cuda::currentStreamCaptureStatus() !=
           c10::hip::CaptureStatus::None;
}
#else
using PgStream = at::cuda::CUDAStream;

inline PgStream pgCurrentStream(int device_index = -1) {
    return device_index < 0 ? at::cuda::getCurrentCUDAStream()
                            : at::cuda::getCurrentCUDAStream(device_index);
}

inline int pgCurrentDevice() { return at::cuda::current_device(); }

inline bool pgStreamCaptureActive() {
    return at::cuda::currentStreamCaptureStatus() !=
           c10::cuda::CaptureStatus::None;
}
#endif

// Acquire a non-blocking enqueue stream from the platform's PyTorch pool.
PgStream pgTaskEnqueueStream(int device_index);

}  // namespace mooncake
