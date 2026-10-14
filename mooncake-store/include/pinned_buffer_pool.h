#pragma once

#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "device/accelerator_registry.h"
#include "pinned_host_buffer.h"

namespace mooncake {

/**
 * PinnedBufferPool: Thread-safe pool of reusable pinned host memory buffers.
 *
 * Platform-specific pinned host allocation is delegated to AcceleratorDevice.
 *
 * Acquire() falls back to pageable memory if pinned allocation fails, while
 * AcquirePinned() is suitable for asynchronous DMA and never falls back.
 *
 * Cached buffers are grouped by allocation size and bounded by total bytes.
 * This permits a large batch arena to be reused without allowing cached pinned
 * memory to grow without limit.
 */
class PinnedBufferPool {
   public:
    static constexpr size_t kDefaultMaxCachedBytes = 256ULL * 1024 * 1024;

    struct Buffer {
        PinnedHostBuffer pinned_host;
        std::unique_ptr<char[]> pageable_host;
        char* data = nullptr;
        size_t capacity = 0;
        bool mapped = false;
        void* device_data = nullptr;

        Buffer() = default;
        explicit Buffer(PinnedHostBuffer pinned_host)
            : pinned_host(std::move(pinned_host)),
              data(static_cast<char*>(this->pinned_host.addr)),
              capacity(this->pinned_host.size),
              mapped(this->pinned_host.device_addr != nullptr),
              device_data(this->pinned_host.device_addr) {}

        static Buffer Pageable(size_t size) {
            Buffer buf;
            buf.pageable_host = std::make_unique<char[]>(size);
            buf.data = buf.pageable_host.get();
            buf.capacity = size;
            return buf;
        }

        Buffer(const Buffer&) = delete;
        Buffer& operator=(const Buffer&) = delete;
        Buffer(Buffer&& other) noexcept
            : pinned_host(std::move(other.pinned_host)),
              pageable_host(std::move(other.pageable_host)),
              data(other.data),
              capacity(other.capacity),
              mapped(other.mapped),
              device_data(other.device_data) {
            other.data = nullptr;
            other.capacity = 0;
            other.mapped = false;
            other.device_data = nullptr;
        }
        Buffer& operator=(Buffer&& other) noexcept {
            if (this != &other) {
                pinned_host = std::move(other.pinned_host);
                pageable_host = std::move(other.pageable_host);
                data = other.data;
                capacity = other.capacity;
                mapped = other.mapped;
                device_data = other.device_data;
                other.data = nullptr;
                other.capacity = 0;
                other.mapped = false;
                other.device_data = nullptr;
            }
            return *this;
        }
    };

    explicit PinnedBufferPool(
        size_t max_cached_bytes = kDefaultMaxCachedBytes)
        : max_cached_bytes_(max_cached_bytes) {}

    ~PinnedBufferPool() { Clear(); }

    Buffer Acquire(size_t size) {
        const size_t capacity = SizeClass(size);
        if (capacity == 0) return {};
        {
            std::lock_guard<std::mutex> lk(mutex_);
            Buffer buf = TakeCached(capacity, false);
            if (buf.data) return buf;
        }
        return AllocWithPageableFallback(capacity);
    }

    // Acquire only pinned storage; unlike Acquire(), this never falls back to
    // pageable memory and is intended for asynchronous DMA sources.
    Buffer AcquirePinned(size_t size, bool *from_cache = nullptr,
                         bool mapped = false) {
        if (from_cache) *from_cache = false;
        const size_t capacity = SizeClass(size);
        if (capacity == 0) return {};
        {
            std::lock_guard<std::mutex> lk(mutex_);
            Buffer buf = TakeCached(capacity, true, mapped);
            if (buf.data) {
                if (from_cache) *from_cache = true;
                return buf;
            }
        }
        return AllocPinnedOnly(capacity, mapped);
    }

    // Never falls back to pageable memory. When mapped is true, allocation
    // succeeds only if the accelerator also provides a device-visible alias.
    static Buffer AllocatePinned(size_t size, bool mapped = false) {
        auto accelerators =
            device::GetAcceleratorRegistry().RuntimeAccelerators();
        for (auto* accelerator : accelerators.Devices()) {
            auto host = mapped ? accelerator->AllocateMappedPinnedHost(size)
                               : accelerator->AllocatePinnedHost(size);
            if (host.addr && (!mapped || host.device_addr)) {
                return Buffer(std::move(host));
            }
            host.reset();
        }
        return {};
    }

    void Release(Buffer buf) {
        if (!buf.data || buf.capacity == 0) return;

        {
            std::lock_guard<std::mutex> lk(mutex_);
            if (buf.capacity <= max_cached_bytes_ - cached_bytes_) {
                cached_bytes_ += buf.capacity;
                pool_[buf.capacity].push_back(std::move(buf));
                return;
            }
        }

        // Releasing pinned host memory may block in the accelerator runtime.
        // Do not hold the pool mutex while invoking the buffer deleter, so one
        // slow hipHostFree cannot block unrelated Acquire/Release operations.
        FreeBuffer(buf);
    }

    void Clear() {
        std::map<size_t, std::vector<Buffer>> buffers_to_free;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            buffers_to_free.swap(pool_);
            cached_bytes_ = 0;
        }

        // As in Release(), destroy pinned allocations without holding the pool
        // mutex because the platform deleter may perform a blocking operation.
        for (auto& [_, buffers] : buffers_to_free) {
            for (auto& buf : buffers) {
                FreeBuffer(buf);
            }
        }
    }

    size_t cached_bytes() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return cached_bytes_;
    }

   
    static constexpr size_t kSmallAllocationLimit = 1ULL * 1024 * 1024;
    static constexpr size_t kSmallAllocationMinimum = 4ULL * 1024;
    static constexpr size_t kLargeAllocationAlignment = 2ULL * 1024 * 1024;

    static size_t SizeClass(size_t size) {
        if (size == 0) return 0;
        if (size <= kSmallAllocationLimit) {
            size_t capacity = kSmallAllocationMinimum;
            while (capacity < size) capacity *= 2;
            return capacity;
        }
        if (size > std::numeric_limits<size_t>::max() -
                       (kLargeAllocationAlignment - 1)) {
            return 0;
        }
        return ((size + kLargeAllocationAlignment - 1) /
                kLargeAllocationAlignment) *
               kLargeAllocationAlignment;
    }

    Buffer TakeCached(size_t capacity, bool pinned_only, bool mapped = false) {
        for (auto it = pool_.lower_bound(capacity); it != pool_.end(); ++it) {
            auto& buffers = it->second;
            for (size_t i = 0; i < buffers.size(); ++i) {
                if (pinned_only && !buffers[i].pinned_host.addr) continue;
                if (mapped && (!buffers[i].mapped || !buffers[i].device_data))
                    continue;
                Buffer buf = std::move(buffers[i]);
                if (i != buffers.size() - 1) {
                    buffers[i] = std::move(buffers.back());
                }
                buffers.pop_back();
                cached_bytes_ -= buf.capacity;
                if (buffers.empty()) pool_.erase(it);
                return buf;
            }
        }
        return {};
    }

    static Buffer AllocPinnedOnly(size_t capacity, bool mapped = false) {
        const auto& registry = device::GetAcceleratorRegistry();
        auto runtime_accelerator = registry.RuntimeAccelerators();
        for (auto* accelerator : runtime_accelerator.Devices()) {
            auto host = mapped ? accelerator->AllocateMappedPinnedHost(capacity)
                               : accelerator->AllocatePinnedHost(capacity);
            if (host.addr) {
                Buffer buffer(std::move(host));
                buffer.mapped = mapped && buffer.device_data != nullptr;
                if (mapped && !buffer.mapped) {
                    FreeBuffer(buffer);
                    continue;
                }
                return buffer;
            }
        }
        return {};
    }

    static Buffer AllocWithPageableFallback(size_t capacity) {
        Buffer buf = AllocPinnedOnly(capacity);
        if (buf.data) return buf;
        return Buffer::Pageable(capacity);
    }

   private:
    static Buffer AllocNew(size_t size) {
        auto pinned = AllocatePinned(size);
        if (pinned.data) return pinned;
        return Buffer::Pageable(size);
    }

    static void FreeBuffer(Buffer& buf) {
        buf.pinned_host.reset();
        buf.pageable_host.reset();
        buf = {};
    }

    const size_t max_cached_bytes_;
    mutable std::mutex mutex_;
    size_t cached_bytes_ = 0;
    std::map<size_t, std::vector<Buffer>> pool_;
};

}  // namespace mooncake
