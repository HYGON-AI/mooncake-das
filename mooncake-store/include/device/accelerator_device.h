#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>

#include "pinned_host_buffer.h"

namespace mooncake {
namespace device {

enum class AcceleratorVendor {
    kNvidia,
    kMusa,
    kMaca,
    kHygon,
    kCorex,
    kHip,
    kAscend,
    kSunrise,
};

enum class MemoryKind {
    kHost,
    kDevice,
    kUnknown,
};

enum class CopyDirection {
    kHostToHost,
    kHostToDevice,
    kDeviceToHost,
    kDeviceToDevice,
    kAuto,
};

struct PointerInfo {
    MemoryKind kind = MemoryKind::kUnknown;
    int32_t device_id = -1;
};

struct HostCopyRange {
    void* dst = nullptr;
    const void* src = nullptr;
    size_t size = 0;
    // Optional device-visible alias for mapped host memory.
    const void* src_device = nullptr;
};

class AcceleratorDevice {
   public:
    virtual ~AcceleratorDevice() = default;

    virtual AcceleratorVendor Vendor() const = 0;
    virtual bool Available(bool ensure = false) const = 0;
    virtual PointerInfo QueryPointer(const void* ptr) const = 0;
    virtual int32_t CurrentDeviceId() const = 0;
    virtual void SetContext(int32_t device_id) const = 0;
    virtual bool Copy(void* dst, const void* src, size_t size,
                      CopyDirection direction) const = 0;
    // Asynchronous host-to-device copy. Backends without stream support use
    // the synchronous implementation as a correctness-preserving fallback.
    virtual bool CopyFromHostAsync(void* dst, const void* src, size_t size,
                                   void* stream) const {
        (void)stream;
        return Copy(dst, src, size, CopyDirection::kHostToDevice);
    }
    // Submit a set of independent host-to-device copies on one stream. The
    // default implementation deliberately keeps the old semantics; backends
    // with a native scatter-copy primitive or a suitable batch kernel can
    // override it to reduce host submission overhead.
    virtual bool CopyFromHostBatchAsync(std::span<const HostCopyRange> ranges,
                                        void* stream,
                                        bool* used_batch_kernel = nullptr) const {
        if (used_batch_kernel) *used_batch_kernel = false;
        bool success = true;
        for (const auto& range : ranges) {
            // Keep attempting the remaining ranges. A backend failure for one
            // range must not turn the logical batch into an early-aborted
            // submission with an ambiguous partial result.
            success = CopyFromHostAsync(range.dst, range.src, range.size,
                                        stream) && success;
        }
        return success;
    }
    virtual bool CreateStream(void** stream) const {
        (void)stream;
        return false;
    }
    virtual bool SynchronizeStream(void* stream) const {
        (void)stream;
        return true;
    }
    virtual void DestroyStream(void* stream) const { (void)stream; }
    virtual PinnedHostBuffer AllocatePinnedHost(size_t size) const = 0;

    // Allocate host memory with a device-visible mapping when supported. This
    // is separate from ordinary pinned memory because mapped host access is
    // only needed by the DFS scatter-kernel path.
    virtual PinnedHostBuffer AllocateMappedPinnedHost(size_t size) const {
        return AllocatePinnedHost(size);
    }
};

class ProbeCachedAcceleratorDevice : public AcceleratorDevice {
   public:
    bool Available(bool ensure = false) const override;

   protected:
    virtual bool ProbeAvailable() const = 0;

   private:
    mutable std::atomic<uint8_t> available_state_{0};
};

}  // namespace device
}  // namespace mooncake
