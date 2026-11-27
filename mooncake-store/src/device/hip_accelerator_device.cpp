#include "device/accelerator_registry.h"
#include "ext/ahe_loader.h"
#include "pinned_host_buffer.h"

#include "cuda_alike.h"

#include <cstdint>
#include <mutex>
#include <shared_mutex>

#if defined(USE_HIP)

namespace mooncake {
namespace device {
namespace {

std::shared_mutex &HipRuntimeStateMutex() {
    // Pinned-buffer deleters may run during static teardown after this device
    // singleton starts destructing, so the lock must remain valid until exit.
    static auto *mutex = new std::shared_mutex();
    return *mutex;
}

hipError_t AllocateHipPinnedHost(void **addr, size_t size,
                                 unsigned int flags) {
    std::unique_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
    return hipHostMalloc(addr, size, flags);
}

hipError_t GetHipHostDevicePointer(void **device_addr, void *host_addr) {
    std::unique_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
    return hipHostGetDevicePointer(device_addr, host_addr, 0);
}

void FreeHipPinnedHost(void *addr) {
    std::unique_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
    (void)hipHostFree(addr);
}

void FreeHipPinnedHostBuffer(void *addr) { FreeHipPinnedHost(addr); }

// Mapped allocations made by the accelerator extension must be released by it,
// so their buffers carry a deleter that goes back through the provider.
void FreeAheMappedHost(void *addr) {
    ext::AheProvider::Instance().FreeMapped(addr);
}

// Batched host-to-device submission lives in the private accelerator
// extension. This backend keeps only the per-range DMA primitives and the
// default AcceleratorDevice::CopyFromHostBatchAsync fallback.
class HipAcceleratorDevice final : public ProbeCachedAcceleratorDevice {
   public:
    AcceleratorVendor Vendor() const override {
        return AcceleratorVendor::kHip;
    }

    bool ProbeAvailable() const override {
        int count = 0;
        return hipGetDeviceCount(&count) == hipSuccess && count > 0;
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        hipPointerAttribute_t attr{};
        if (hipPointerGetAttributes(&attr, ptr) == hipSuccess &&
            attr.type == hipMemoryTypeDevice) {
            return PointerInfo{.kind = MemoryKind::kDevice,
                               .device_id = attr.device};
        }
        hipGetLastError();
        return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
    }

    int32_t CurrentDeviceId() const override {
        int device_id = -1;
        return hipGetDevice(&device_id) == hipSuccess ? device_id : -1;
    }

    void SetContext(int32_t device_id) const override {
        if (device_id < 0) return;
        std::unique_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
        hipSetDevice(device_id);
    }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection direction) const override {
        hipMemcpyKind kind = hipMemcpyDefault;
        switch (direction) {
            case CopyDirection::kHostToDevice:
                kind = hipMemcpyHostToDevice;
                break;
            case CopyDirection::kDeviceToHost:
                kind = hipMemcpyDeviceToHost;
                break;
            case CopyDirection::kDeviceToDevice:
                kind = hipMemcpyDeviceToDevice;
                break;
            case CopyDirection::kHostToHost:
            case CopyDirection::kAuto:
                kind = hipMemcpyDefault;
                break;
        }
        return hipMemcpy(dst, src, size, kind) == hipSuccess;
    }

    bool CopyFromHostAsync(void* dst, const void* src, size_t size,
                           void* stream) const override {
        return hipMemcpyAsync(dst, src, size, hipMemcpyHostToDevice,
                              static_cast<hipStream_t>(stream)) == hipSuccess;
    }

    bool CreateStream(void** stream) const override {
        hipStream_t hip_stream = nullptr;
        if (hipStreamCreate(&hip_stream) != hipSuccess) {
            hipGetLastError();
            return false;
        }
        *stream = static_cast<void*>(hip_stream);
        return true;
    }

    bool SynchronizeStream(void* stream) const override {
        const auto hip_stream = static_cast<hipStream_t>(stream);
        std::shared_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
        const bool success = hipStreamSynchronize(hip_stream) == hipSuccess;
        if (!success) hipGetLastError();
        return success;
    }

    void DestroyStream(void* stream) const override {
        hipStreamDestroy(static_cast<hipStream_t>(stream));
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        void* addr = nullptr;
        if (AllocateHipPinnedHost(&addr, size, 0) != hipSuccess) {
            hipGetLastError();
            return PinnedHostBuffer();
        }
        return PinnedHostBuffer(addr, size, FreeHipPinnedHostBuffer);
    }

    PinnedHostBuffer AllocateMappedPinnedHost(size_t size) const override {
        // The extension owns the mapped-allocation policy (and therefore the
        // matching free). When it is absent the baseline below still produces
        // a mapped buffer with the runtime directly.
        auto &ahe = ext::AheProvider::Instance();
        if (ahe.available()) {
            int device_id = -1;
            {
                std::shared_lock<std::shared_mutex> lock(
                    HipRuntimeStateMutex());
                if (hipGetDevice(&device_id) != hipSuccess) {
                    hipGetLastError();
                    device_id = -1;
                }
            }
            if (device_id >= 0) {
                void *host = nullptr;
                void *device_addr = nullptr;
                if (ahe.AllocMapped(device_id, size, &host, &device_addr) &&
                    host != nullptr && device_addr != nullptr) {
                    return PinnedHostBuffer(host, size, FreeAheMappedHost,
                                            device_addr);
                }
            }
        }

        void *addr = nullptr;
        if (AllocateHipPinnedHost(&addr, size, hipHostMallocMapped) !=
            hipSuccess) {
            hipGetLastError();
            return PinnedHostBuffer();
        }

        void *device_addr = nullptr;
        if (GetHipHostDevicePointer(&device_addr, addr) != hipSuccess) {
            hipGetLastError();
            FreeHipPinnedHost(addr);
            return PinnedHostBuffer();
        }
        return PinnedHostBuffer(addr, size, FreeHipPinnedHostBuffer,
                                device_addr);
    }
};

const AcceleratorDevice& HipDeviceInstance() {
    static HipAcceleratorDevice device;
    return device;
}

const AcceleratorDeviceRegistrar registered_hip_device(HipDeviceInstance());

}  // namespace
}  // namespace device
}  // namespace mooncake

#endif
