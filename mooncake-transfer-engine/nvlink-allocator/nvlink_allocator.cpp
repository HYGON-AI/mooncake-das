#include "cuda_alike.h"
#include <sys/types.h>

#include <iostream>

// ref: http://github.com/NVIDIA/nccl/blob/v2.28.9-1/src/allocator.cc#L53-L68
static CUresult cuMemCreateTryFabric(CUmemGenericAllocationHandle *handle,
                                     size_t size, CUmemAllocationProp *prop,
                                     unsigned long long flags) {
    CUresult err = cuMemCreate(handle, size, prop, flags);
    if ((prop->requestedHandleTypes & CU_MEM_HANDLE_TYPE_FABRIC) &&
        (err == CUDA_ERROR_NOT_PERMITTED || err == CUDA_ERROR_NOT_SUPPORTED)) {
        prop->requestedHandleTypes = static_cast<CUmemAllocationHandleType>(
            prop->requestedHandleTypes & ~CU_MEM_HANDLE_TYPE_FABRIC);
        err = cuMemCreate(handle, size, prop, flags);
    }
    return err;
}

enum class MemoryBackendType { use_cudamalloc, use_cumemcreate, unknown };

extern "C" {

MemoryBackendType mc_probe_fabric_support(int device_id) {
    CUdevice dev;
    CUresult res = cuDeviceGet(&dev, device_id);
    if (res != CUDA_SUCCESS) {
        return MemoryBackendType::unknown;
    }

    // Check device attribute first
    int fabric_attr = 0;
    res = cuDeviceGetAttribute(
        &fabric_attr, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, dev);
    if (res != CUDA_SUCCESS || !fabric_attr) {
        return MemoryBackendType::use_cudamalloc;
    }

    CUmemAllocationProp prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = dev;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;  // require fabric

    CUmemGenericAllocationHandle handle;
    size_t size = 4096;

    res = cuMemCreate(&handle, size, &prop, 0);

    if (res == CUDA_SUCCESS) {
        cuMemRelease(handle);  // success → clean up
        return MemoryBackendType::use_cumemcreate;
    } else {
        return MemoryBackendType::use_cudamalloc;
    }
}

struct DeviceGuard {
    int old_device;
    DeviceGuard(int new_device) {
        cudaGetDevice(&old_device);
        cudaSetDevice(new_device);
    }
    ~DeviceGuard() {
        cudaSetDevice(old_device);
    }
};

constexpr int kAlignment = 2ULL * 1024 * 1024;  // 2MB

void *mc_nvlink_malloc(ssize_t size, int device, cudaStream_t stream) {
    if (size <= 0) {
        return nullptr;
    }
    DeviceGuard guard(device);

    void *ptr = nullptr;
    size = (size + kAlignment - 1) & ~(kAlignment - 1);
    CUresult result = cudaMalloc(&ptr, size);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cudaMalloc failed: " << result << "\n";
        return nullptr;
    }
    return ptr;
}

void mc_nvlink_free(void *ptr, ssize_t ssize, int device, cudaStream_t stream) {
    if (ptr) {
        DeviceGuard guard(device);
        CUresult result = cudaFree(ptr);
        if (result != CUDA_SUCCESS) {
            std::cerr << "cudaFree failed: " << result << "\n";
        }
    }
}
}
