/**
 * Private accelerator host-to-device extension.
 *
 * Owns the scatter kernel, the persistent per-device stream pool, the
 * synchronization workers and the mapped pinned allocator. The public tree
 * only knows the C ABI in ext/mooncake_private_ext_abi.h; when this library is
 * absent the client keeps its per-range DMA fallback.
 *
 * Everything HIP-related is serialized through one process-wide lock for the
 * same reason the public device used to: pinned-buffer deleters can run during
 * static teardown, after runtime state has already been torn down.
 */

#include "ext/mooncake_private_ext_abi.h"

#include <hip/hip_runtime.h>

#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <dlfcn.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

// Streams created per device when the caller does not ask for a count.
constexpr uint32_t kDefaultStreamCount = 4;
// Below this many ranges a plain DMA submission is cheaper than a kernel.
constexpr size_t kKernelMinRanges = 32;
// Blocks are launched with one warp-friendly block per range.
constexpr unsigned int kKernelThreads = 256;

std::shared_mutex& HipRuntimeStateMutex() {
    // Intentionally leaked: worker threads and pinned deleters may still touch
    // it while static destructors run.
    static auto* mutex = new std::shared_mutex();
    return *mutex;
}

struct DeviceRange {
    uint64_t dst_addr;
    uint64_t src_addr;
    uint64_t size;
};

struct StreamSlot {
    ~StreamSlot() {
        if (worker.joinable()) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                stopping = true;
            }
            task_cv.notify_one();
            worker.join();
        }
        if (stream != nullptr) {
            std::unique_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
            (void)hipStreamDestroy(stream);
            stream = nullptr;
        }
    }

    hipStream_t stream = nullptr;
    std::mutex mutex;
    std::condition_variable task_cv;
    std::condition_variable done_cv;
    std::thread worker;
    bool has_task = false;
    bool stopping = false;
    bool result = false;
};

struct DeviceHandle {
    int ordinal = -1;
    std::vector<std::unique_ptr<StreamSlot>> slots;
    std::mutex descriptors_mutex;
    // Mapped descriptor arrays still referenced by a queued kernel.
    std::vector<void*> pending_descriptors;
};

struct KernelModule {
    hipModule_t module = nullptr;
    hipFunction_t function = nullptr;
};

std::mutex& KernelMutex() {
    static auto* mutex = new std::mutex();
    return *mutex;
}

std::unordered_map<int, KernelModule>& KernelModules() {
    static auto* modules = new std::unordered_map<int, KernelModule>();
    return *modules;
}

// Per-device record of a failed kernel load. Caching it keeps a missing or
// incompatible kernel from being retried (and warning) on every batch; the
// caller simply stays on the per-range DMA path.
std::unordered_map<int, bool>& KernelLoadFailures() {
    static auto* failures = new std::unordered_map<int, bool>();
    return *failures;
}

std::string DirectoryOfSelf() {
    static int anchor = 0;
    Dl_info info{};
    if (dladdr(&anchor, &info) == 0 || info.dli_fname == nullptr) return {};
    const std::string path = info.dli_fname;
    const size_t slash = path.find_last_of('/');
    if (slash == std::string::npos) return ".";
    return path.substr(0, slash);
}

std::string KernelPath() {
    // Pre-change override: an explicit path wins when set. The old
    // implementation accepted either the kernel file or the directory that
    // holds it, so a directory is still expanded to the default file name.
    const char* override_path = std::getenv("MC_COPY_KERNEL_PATH");
    if (override_path != nullptr && override_path[0] != '\0') {
        struct stat info {};
        if (::stat(override_path, &info) == 0 && S_ISDIR(info.st_mode)) {
            return std::string(override_path) + "/mc_copy_kernel.co";
        }
        return override_path;
    }
    // Resolved relative to this shared object; deployment keeps the kernel in
    // the same directory as the provider.
    const std::string dir = DirectoryOfSelf();
    if (dir.empty()) return {};
    const std::string beside = dir + "/mc_copy_kernel.co";
    if (::access(beside.c_str(), R_OK) == 0) return beside;
    // Wheels keep the kernel one level up, inside the python package.
    const std::string packaged = dir + "/../mooncake/mc_copy_kernel.co";
    if (::access(packaged.c_str(), R_OK) == 0) return packaged;
    return {};
}

// Caller must hold the runtime lock and have selected `ordinal`.
bool LoadKernel(int ordinal, hipFunction_t* out) {
    std::lock_guard<std::mutex> lock(KernelMutex());
    auto& modules = KernelModules();
    auto it = modules.find(ordinal);
    if (it != modules.end()) {
        *out = it->second.function;
        return true;
    }
    auto& failures = KernelLoadFailures();
    if (failures.count(ordinal) != 0) return false;
    const std::string path = KernelPath();
    if (path.empty()) {
        failures[ordinal] = true;
        return false;
    }

    hipModule_t module = nullptr;
    if (hipModuleLoad(&module, path.c_str()) != hipSuccess) {
        (void)hipGetLastError();
        failures[ordinal] = true;
        return false;
    }
    hipFunction_t function = nullptr;
    if (hipModuleGetFunction(&function, module, "MCStoreH2DScatterKernel") !=
            hipSuccess ||
        function == nullptr) {
        (void)hipGetLastError();
        (void)hipModuleUnload(module);
        failures[ordinal] = true;
        return false;
    }
    KernelModule loaded{module, function};
    modules.emplace(ordinal, loaded);
    *out = function;
    return true;
}

void StartWorker(int ordinal, StreamSlot* slot) {
    slot->worker = std::thread([ordinal, slot]() {
        {
            std::unique_lock<std::shared_mutex> lock(HipRuntimeStateMutex());
            (void)hipSetDevice(ordinal);
        }
        while (true) {
            std::unique_lock<std::mutex> lock(slot->mutex);
            slot->task_cv.wait(lock, [slot] {
                return slot->has_task || slot->stopping;
            });
            if (slot->stopping && !slot->has_task) break;
            lock.unlock();

            bool ok = false;
            {
                std::shared_lock<std::shared_mutex> runtime(
                    HipRuntimeStateMutex());
                ok = hipStreamSynchronize(slot->stream) == hipSuccess;
                if (!ok) (void)hipGetLastError();
            }

            lock.lock();
            slot->result = ok;
            slot->has_task = false;
            lock.unlock();
            slot->done_cv.notify_all();
        }
    });
}

int SyncHandle(DeviceHandle* handle) {
    if (handle == nullptr) return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    if (handle->slots.empty()) return MOONCAKE_EXT_STATUS_UNAVAILABLE;

    for (auto& slot : handle->slots) {
        std::lock_guard<std::mutex> lock(slot->mutex);
        if (slot->stopping || slot->has_task) {
            return MOONCAKE_EXT_STATUS_UNAVAILABLE;
        }
    }
    for (auto& slot : handle->slots) {
        std::lock_guard<std::mutex> lock(slot->mutex);
        slot->has_task = true;
    }
    for (auto& slot : handle->slots) slot->task_cv.notify_one();

    bool ok = true;
    for (auto& slot : handle->slots) {
        std::unique_lock<std::mutex> lock(slot->mutex);
        slot->done_cv.wait(lock, [&slot] { return !slot->has_task; });
        ok = ok && slot->result;
    }

    // Every stream has drained, so no kernel still reads the descriptor
    // arrays queued since the previous synchronization.
    std::vector<void*> descriptors;
    {
        std::lock_guard<std::mutex> lock(handle->descriptors_mutex);
        descriptors.swap(handle->pending_descriptors);
    }
    if (!descriptors.empty()) {
        std::unique_lock<std::shared_mutex> runtime(HipRuntimeStateMutex());
        for (void* descriptor : descriptors) (void)hipHostFree(descriptor);
    }
    return ok ? MOONCAKE_EXT_STATUS_OK : MOONCAKE_EXT_STATUS_IO_ERROR;
}

int SyncEntry(void*, void* handle) {
    return SyncHandle(static_cast<DeviceHandle*>(handle));
}

void* CreateHandle(void*, int device_ordinal, uint32_t stream_count) {
    if (device_ordinal < 0) return nullptr;
    try {
    auto handle = std::make_unique<DeviceHandle>();
    handle->ordinal = device_ordinal;

    const uint32_t count =
        stream_count == 0 ? kDefaultStreamCount : stream_count;
    handle->slots.reserve(count);
    {
        std::unique_lock<std::shared_mutex> runtime(HipRuntimeStateMutex());
        int device_count = 0;
        if (hipGetDeviceCount(&device_count) != hipSuccess ||
            device_ordinal >= device_count) {
            (void)hipGetLastError();
            return nullptr;
        }
        if (hipSetDevice(device_ordinal) != hipSuccess) {
            (void)hipGetLastError();
            return nullptr;
        }
        for (uint32_t i = 0; i < count; ++i) {
            auto slot = std::make_unique<StreamSlot>();
            if (hipStreamCreate(&slot->stream) != hipSuccess) {
                (void)hipGetLastError();
                return nullptr;
            }
            handle->slots.push_back(std::move(slot));
        }
    }
    for (auto& slot : handle->slots) StartWorker(device_ordinal, slot.get());
    return handle.release();
    } catch (...) {
        // Never let a C++ exception cross the C ABI boundary.
        return nullptr;
    }
}

int SubmitEntry(void*, void* opaque, const mooncake_ahe_copy_range* ranges,
                size_t range_count, uint32_t stream_key) {
    auto* handle = static_cast<DeviceHandle*>(opaque);
    if (handle == nullptr || handle->slots.empty()) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    if (range_count == 0) return MOONCAKE_EXT_STATUS_OK;
    if (ranges == nullptr) return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;

    bool all_mapped = true;
    for (size_t i = 0; i < range_count; ++i) {
        if (ranges[i].src == nullptr || ranges[i].dst == 0 ||
            ranges[i].size == 0) {
            return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
        }
        if (ranges[i].src_device == nullptr) all_mapped = false;
    }

    StreamSlot& slot =
        *handle->slots[static_cast<size_t>(stream_key) % handle->slots.size()];

    std::unique_lock<std::shared_mutex> runtime(HipRuntimeStateMutex());
    if (hipSetDevice(handle->ordinal) != hipSuccess) {
        (void)hipGetLastError();
        return MOONCAKE_EXT_STATUS_IO_ERROR;
    }

    if (all_mapped && range_count >= kKernelMinRanges &&
        range_count <= UINT32_MAX) {
        hipFunction_t function = nullptr;
        if (LoadKernel(handle->ordinal, &function)) {
            void* host_descriptors = nullptr;
            if (hipHostMalloc(&host_descriptors,
                              range_count * sizeof(DeviceRange),
                              hipHostMallocMapped) == hipSuccess) {
                auto* descriptors =
                    static_cast<DeviceRange*>(host_descriptors);
                void* device_descriptors = nullptr;
                if (hipHostGetDevicePointer(&device_descriptors,
                                            host_descriptors,
                                            0) == hipSuccess) {
                    for (size_t i = 0; i < range_count; ++i) {
                        descriptors[i] = DeviceRange{
                            ranges[i].dst,
                            reinterpret_cast<uint64_t>(ranges[i].src_device),
                            ranges[i].size};
                    }
                    uint64_t count = range_count;
                    void* args[] = {&device_descriptors, &count};
                    const hipError_t launch = hipModuleLaunchKernel(
                        function, static_cast<unsigned int>(range_count), 1, 1,
                        kKernelThreads, 1, 1, 0, slot.stream, args, nullptr);
                    if (launch == hipSuccess) {
                        {
                            std::lock_guard<std::mutex> lock(
                                handle->descriptors_mutex);
                            handle->pending_descriptors.push_back(
                                host_descriptors);
                        }
                        return MOONCAKE_EXT_STATUS_OK;
                    }
                    (void)hipGetLastError();
                } else {
                    (void)hipGetLastError();
                }
                (void)hipHostFree(host_descriptors);
            } else {
                (void)hipGetLastError();
            }
        }
    }

    bool ok = true;
    for (size_t i = 0; i < range_count; ++i) {
        if (hipMemcpyAsync(reinterpret_cast<void*>(ranges[i].dst),
                           ranges[i].src, ranges[i].size,
                           hipMemcpyHostToDevice,
                           slot.stream) != hipSuccess) {
            (void)hipGetLastError();
            ok = false;
        }
    }
    return ok ? MOONCAKE_EXT_STATUS_OK : MOONCAKE_EXT_STATUS_IO_ERROR;
}

void DestroyEntry(void*, void* opaque) {
    auto* handle = static_cast<DeviceHandle*>(opaque);
    if (handle == nullptr) return;
    (void)SyncHandle(handle);
    delete handle;
}

int AllocMapped(void*, int device_ordinal, uint64_t size, void** host_ptr,
                void** device_ptr) {
    if (host_ptr == nullptr || device_ptr == nullptr || size == 0 ||
        device_ordinal < 0) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    *host_ptr = nullptr;
    *device_ptr = nullptr;

    std::unique_lock<std::shared_mutex> runtime(HipRuntimeStateMutex());
    if (hipSetDevice(device_ordinal) != hipSuccess) {
        (void)hipGetLastError();
        return MOONCAKE_EXT_STATUS_IO_ERROR;
    }
    void* host = nullptr;
    if (hipHostMalloc(&host, static_cast<size_t>(size),
                      hipHostMallocMapped) != hipSuccess) {
        (void)hipGetLastError();
        return MOONCAKE_EXT_STATUS_IO_ERROR;
    }
    void* device = nullptr;
    if (hipHostGetDevicePointer(&device, host, 0) != hipSuccess) {
        (void)hipGetLastError();
        (void)hipHostFree(host);
        return MOONCAKE_EXT_STATUS_IO_ERROR;
    }
    *host_ptr = host;
    *device_ptr = device;
    return MOONCAKE_EXT_STATUS_OK;
}

void FreeMapped(void*, void* host_ptr) {
    if (host_ptr == nullptr) return;
    std::unique_lock<std::shared_mutex> runtime(HipRuntimeStateMutex());
    (void)hipHostFree(host_ptr);
}

}  // namespace

extern "C" __attribute__((visibility("default"))) int
mooncake_ahe_extension_get(mooncake_ahe_ext* extension) {
    if (extension == nullptr ||
        extension->abi_version != MOONCAKE_AHE_ABI_VERSION ||
        extension->struct_size < sizeof(mooncake_ahe_ext)) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    extension->user_data = nullptr;
    extension->create = &CreateHandle;
    extension->submit = &SubmitEntry;
    extension->sync = &SyncEntry;
    extension->destroy = &DestroyEntry;
    extension->alloc_mapped = &AllocMapped;
    extension->free_mapped = &FreeMapped;
    return MOONCAKE_EXT_STATUS_OK;
}
