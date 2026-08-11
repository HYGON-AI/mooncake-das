#pragma once

// HCU uses the DTK HIP runtime, while the shared Transfer Engine host sources
// are written against a CUDA-shaped API surface.
#include "gpu_vendor/hip.h"

#define cudaDeviceSynchronize hipDeviceSynchronize
#define cudaErrorNotReady hipErrorNotReady
#define cudaError_t hipError_t
#define cudaEvent_t hipEvent_t
#define cudaEventCreateWithFlags hipEventCreateWithFlags
#define cudaEventDestroy hipEventDestroy
#define cudaEventDisableTiming hipEventDisableTiming
#define cudaEventQuery hipEventQuery
#define cudaEventRecord hipEventRecord
#define cudaEventSynchronize hipEventSynchronize
#define cudaFree hipFree
#define cudaFreeHost hipHostFree
#define cudaFuncAttributes hipFuncAttributes
#define cudaFuncGetAttributes hipFuncGetAttributes
#define cudaGetDevice hipGetDevice
#define cudaGetDeviceCount hipGetDeviceCount
#define cudaGetErrorString hipGetErrorString
#define cudaHostAlloc hipHostMalloc
#define cudaHostAllocMapped hipHostMallocMapped
#define cudaHostGetDevicePointer hipHostGetDevicePointer
#define cudaMalloc hipMalloc
#define cudaMemcpy hipMemcpy
#define cudaMemcpyAsync hipMemcpyAsync
#define cudaMemcpyDeviceToDevice hipMemcpyDeviceToDevice
#define cudaMemsetAsync hipMemsetAsync
#define cudaSetDevice hipSetDevice
#define cudaStream_t hipStream_t
#define cudaStreamSynchronize hipStreamSynchronize
#define cudaSuccess hipSuccess
