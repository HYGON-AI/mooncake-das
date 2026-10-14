#include "device/accelerator_registry.h"
#include "pinned_host_buffer.h"

#include "cuda_alike.h"

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

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

#if defined(USE_HYGON)
    bool CopyFromHostBatchAsync(std::span<const HostCopyRange> ranges,
                                void* stream,
                                bool* used_batch_kernel = nullptr) const
        override {
        if (used_batch_kernel) *used_batch_kernel = false;
        constexpr size_t kKernelMinRanges = 32;
        if (ranges.size() < kKernelMinRanges) {
            return AcceleratorDevice::CopyFromHostBatchAsync(ranges, stream);
        }

        for (const auto &range : ranges) {
            // The scatter kernel reads the source through its device-visible
            // mapped-host alias. If any source is ordinary pinned memory, keep
            // the whole batch on the DMA path so submission order is preserved.
            if (!range.dst || !range.src || range.size == 0 ||
                !range.src_device) {
                return AcceleratorDevice::CopyFromHostBatchAsync(ranges, stream);
            }
        }

        hipFunction_t function = nullptr;
        if (!LoadCopyFunction(CurrentDeviceId(), &function)) {
            return AcceleratorDevice::CopyFromHostBatchAsync(ranges, stream);
        }

        struct DeviceRange {
            uint64_t dst_addr;
            uint64_t src_addr;
            uint64_t size;
        };
        if (ranges.size() > std::numeric_limits<size_t>::max() /
                                sizeof(DeviceRange) ||
            ranges.size() > std::numeric_limits<unsigned int>::max()) {
            return AcceleratorDevice::CopyFromHostBatchAsync(ranges, stream);
        }

        // Keep the descriptors in mapped pinned host memory. This avoids a
        // second H2D descriptor copy and remains valid until the stream has
        // consumed the kernel, at which point SynchronizeStream releases it.
        const size_t descriptor_bytes = ranges.size() * sizeof(DeviceRange);
        void *host_descriptors = nullptr;
        if (AllocateHipPinnedHost(&host_descriptors, descriptor_bytes,
                                  hipHostMallocMapped) != hipSuccess) {
            hipGetLastError();
            return AcceleratorDevice::CopyFromHostBatchAsync(ranges, stream);
        }
        auto *descriptors = static_cast<DeviceRange *>(host_descriptors);
        for (size_t i = 0; i < ranges.size(); ++i) {
            descriptors[i] = DeviceRange{
                reinterpret_cast<uint64_t>(ranges[i].dst),
                reinterpret_cast<uint64_t>(ranges[i].src_device),
                ranges[i].size};
        }

        void *device_descriptors = nullptr;
        if (GetHipHostDevicePointer(&device_descriptors, host_descriptors) !=
            hipSuccess) {
            hipGetLastError();
            FreeHipPinnedHost(host_descriptors);
            return AcceleratorDevice::CopyFromHostBatchAsync(ranges, stream);
        }

        uint64_t range_count = ranges.size();
        void *args[] = {&device_descriptors, &range_count};
        constexpr unsigned int kThreads = 256;
        const auto hip_stream = static_cast<hipStream_t>(stream);
        const hipError_t launch_result = hipModuleLaunchKernel(
            function, static_cast<unsigned int>(ranges.size()), 1, 1, kThreads,
            1, 1, 0, hip_stream, args, nullptr);
        if (launch_result != hipSuccess) {
            hipGetLastError();
            FreeHipPinnedHost(host_descriptors);
            return AcceleratorDevice::CopyFromHostBatchAsync(ranges, stream);
        }

        {
            std::lock_guard<std::mutex> lock(descriptor_mutex_);
            pending_descriptors_[static_cast<void *>(hip_stream)].push_back(
                host_descriptors);
        }
        if (used_batch_kernel) *used_batch_kernel = true;
        return true;
    }
#endif

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
        bool success = false;
        {
            std::shared_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
            success = hipStreamSynchronize(hip_stream) == hipSuccess;
        }
#if defined(USE_HYGON)
        // Synchronization establishes that the kernel no longer dereferences
        // the mapped descriptor array. Free it even when the stream reports a
        // device-side error; the stream has completed in either case.
        std::vector<void *> descriptors;
        {
            std::lock_guard<std::mutex> lock(descriptor_mutex_);
            auto it = pending_descriptors_.find(static_cast<void *>(hip_stream));
            if (it != pending_descriptors_.end()) {
                descriptors = std::move(it->second);
                pending_descriptors_.erase(it);
            }
        }
        for (void *descriptor : descriptors) {
            FreeHipPinnedHost(descriptor);
        }
#endif
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

#if defined(USE_HYGON)
    PinnedHostBuffer AllocateMappedPinnedHost(size_t size) const override {
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
#endif

    ~HipAcceleratorDevice() override {
#if defined(USE_HYGON)
        std::vector<void *> descriptors;
        {
            std::lock_guard<std::mutex> descriptor_lock(descriptor_mutex_);
            for (auto &[_, pending] : pending_descriptors_) {
                descriptors.insert(descriptors.end(), pending.begin(),
                                   pending.end());
            }
            pending_descriptors_.clear();
        }
        for (void *descriptor : descriptors) {
            FreeHipPinnedHost(descriptor);
        }

        std::lock_guard<std::mutex> lock(module_mutex_);
        for (const auto &[_, module] : copy_modules_) {
            hipModuleUnload(module);
        }
#endif
    }

   private:
#if defined(USE_HYGON)
    static std::string CopyKernelPath() {
        const char *directory = std::getenv("MC_COPY_KERNEL_PATH");
        if (directory && directory[0] != '\0') {
            return std::string(directory) + "/mc_copy_kernel.co";
        }
        return "/usr/local/lib/python3.10/dist-packages/mooncake/mc_copy_kernel.co";
    }

    bool LoadCopyFunction(int device_id, hipFunction_t *function) const {
        if (device_id < 0) return false;
        std::lock_guard<std::mutex> lock(module_mutex_);
        auto it = copy_functions_.find(device_id);
        if (it != copy_functions_.end()) {
            *function = it->second;
            return true;
        }

        const std::string path = CopyKernelPath();
        std::ifstream kernel_file(path);
        if (!kernel_file.good()) return false;

        hipModule_t module = nullptr;
        if (hipModuleLoad(&module, path.c_str()) != hipSuccess) {
            hipGetLastError();
            return false;
        }
        hipFunction_t loaded_function = nullptr;
        if (hipModuleGetFunction(&loaded_function, module,
                                 "MCStoreH2DScatterKernel") != hipSuccess) {
            hipGetLastError();
            hipModuleUnload(module);
            return false;
        }
        copy_modules_.emplace(device_id, module);
        copy_functions_.emplace(device_id, loaded_function);
        *function = loaded_function;
        return true;
    }

    mutable std::mutex module_mutex_;
    mutable std::mutex descriptor_mutex_;
    mutable std::unordered_map<int, hipModule_t> copy_modules_;
    mutable std::unordered_map<int, hipFunction_t> copy_functions_;
    mutable std::unordered_map<void *, std::vector<void *>>
        pending_descriptors_;
#endif
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
