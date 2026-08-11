// Host-side DTK HIP aliases used only by the HCU Device API implementation.
//
// The CUDA/MUSA Device API implementation is intentionally written against a
// CUDA-shaped runtime surface. Keep that implementation shared for HCU while
// confining the compatibility names to transport/device instead of exposing
// them through the project-wide gpu_vendor/hip.h header.
#pragma once

#include <hip/hip_runtime.h>

#define cudaError_t hipError_t
#define cudaStream_t hipStream_t
#define cudaIpcMemHandle_t hipIpcMemHandle_t

#define cudaSuccess hipSuccess
#define cudaErrorPeerAccessAlreadyEnabled hipErrorPeerAccessAlreadyEnabled
#define cudaIpcMemLazyEnablePeerAccess hipIpcMemLazyEnablePeerAccess
#define cudaMemcpyHostToDevice hipMemcpyHostToDevice
#define cudaMemcpyDeviceToHost hipMemcpyDeviceToHost
#define cudaMemoryTypeDevice hipMemoryTypeDevice

#define cudaMalloc hipMalloc
#define cudaFree hipFree
#define cudaMallocHost hipHostMalloc
#define cudaFreeHost hipHostFree
#define cudaMemset hipMemset
#define cudaMemsetAsync hipMemsetAsync
#define cudaMemcpy hipMemcpy
#define cudaGetDevice hipGetDevice
#define cudaGetDeviceCount hipGetDeviceCount
#define cudaGetDeviceProperties hipGetDeviceProperties
#define cudaDeviceGetAttribute hipDeviceGetAttribute
#define cudaDevAttrClockRate hipDeviceAttributeWallClockRate
#define cudaDeviceCanAccessPeer hipDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess hipDeviceEnablePeerAccess
#define cudaGetErrorString hipGetErrorString
#define cudaGetLastError hipGetLastError
#define cudaIpcGetMemHandle hipIpcGetMemHandle
#define cudaIpcOpenMemHandle hipIpcOpenMemHandle
#define cudaIpcCloseMemHandle hipIpcCloseMemHandle
#define cudaHostRegister hipHostRegister
#define cudaHostUnregister hipHostUnregister
#define cudaHostGetDevicePointer hipHostGetDevicePointer
#define cudaHostRegisterPortable hipHostRegisterPortable
#define cudaHostRegisterMapped hipHostRegisterMapped
#define cudaHostRegisterIoMemory hipHostRegisterIoMemory
#define cudaStreamSynchronize hipStreamSynchronize
