#pragma once

#include <cstddef>
#include <utility>

namespace mooncake {

using PinnedHostBufferDeleter = void (*)(void* addr);

struct PinnedHostBuffer {
    void* addr = nullptr;
    size_t size = 0;
    PinnedHostBufferDeleter deleter = nullptr;
    // Device-visible alias for mapped host allocations.
    void* device_addr = nullptr;

    PinnedHostBuffer() = default;
    PinnedHostBuffer(void* addr, size_t size, PinnedHostBufferDeleter deleter,
                     void* device_addr = nullptr)
        : addr(addr), size(size), deleter(deleter), device_addr(device_addr) {}

    PinnedHostBuffer(const PinnedHostBuffer&) = delete;
    PinnedHostBuffer& operator=(const PinnedHostBuffer&) = delete;

    PinnedHostBuffer(PinnedHostBuffer&& other) noexcept
        : addr(other.addr),
          size(other.size),
          deleter(other.deleter),
          device_addr(other.device_addr) {
        other.addr = nullptr;
        other.size = 0;
        other.deleter = nullptr;
        other.device_addr = nullptr;
    }
    PinnedHostBuffer& operator=(PinnedHostBuffer&& other) noexcept {
        if (this != &other) {
            reset();
            addr = other.addr;
            size = other.size;
            deleter = other.deleter;
            device_addr = other.device_addr;
            other.addr = nullptr;
            other.size = 0;
            other.deleter = nullptr;
            other.device_addr = nullptr;
        }
        return *this;
    }

    ~PinnedHostBuffer() { reset(); }

    void reset() {
        if (addr && deleter) deleter(addr);
        addr = nullptr;
        size = 0;
        deleter = nullptr;
        device_addr = nullptr;
    }
};

}  // namespace mooncake
