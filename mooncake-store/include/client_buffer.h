#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <string>
#include <vector>

#include "offset_allocator/offset_allocator.h"
#include "types.h"
#include "replica.h"
#include "dfs_buffer_usage.h"
#include "nof/dma_buffer_allocator.h"

namespace mooncake {

class BufferHandle;

/**
 * ClientBufferAllocator manages a contiguous memory buffer using an
 * offset-based allocation strategy. It provides thread-safe allocation and
 * automatic deallocation through RAII handles.
 *
 * This allocator is designed for client-side memory management where you need:
 * - Efficient sub-allocation within a larger buffer
 * - Automatic memory cleanup via RAII
 * - Thread-safe allocation operations
 * - Support for shared memory allocation
 */
class ClientBufferAllocator
    : public std::enable_shared_from_this<ClientBufferAllocator> {
   public:
    // Create for heap-allocated memory
    static std::shared_ptr<ClientBufferAllocator> create(
        size_t size, const std::string& protocol = "",
        bool use_hugepage = false,
        std::shared_ptr<DmaBufferAllocator> dma_allocator = nullptr);

    // Create for external memory. An optional owner keeps the backing
    // allocation alive until this allocator and every outstanding BufferHandle
    // are gone.
    static std::shared_ptr<ClientBufferAllocator> create(
        void* addr, size_t size, const std::string& protocol = "",
        void* device_addr = nullptr,
        std::shared_ptr<void> backing_owner = nullptr,
        std::shared_ptr<DfsBufferUsageTracker> usage_tracker = nullptr,
        DfsBufferUsageKind usage_kind = DfsBufferUsageKind::kRead);

    ~ClientBufferAllocator();

    // Disable copy constructor and copy assignment operator
    ClientBufferAllocator(const ClientBufferAllocator&) = delete;
    ClientBufferAllocator& operator=(const ClientBufferAllocator&) = delete;

    // Disable move constructor and move assignment operator
    ClientBufferAllocator(ClientBufferAllocator&&) = delete;
    ClientBufferAllocator& operator=(ClientBufferAllocator&&) = delete;

    [[nodiscard]] void* getBase() const { return buffer_; }

    [[nodiscard]] void* getDeviceBase() const { return device_buffer_; }

    [[nodiscard]] size_t size() const { return buffer_size_; }

    [[nodiscard]] size_t allocation_size_for_request(size_t size) const;
    [[nodiscard]] size_t allocation_size_for_aligned_request(
        size_t size, size_t alignment) const;

    [[nodiscard]] std::optional<BufferHandle> allocate(size_t size);

    // Allocate a host view with the requested address alignment. If this
    // allocator also has a device-visible base, the returned handle exposes
    // the device alias at the same sub-allocation offset.
    [[nodiscard]] std::optional<BufferHandle> allocate_aligned(
        size_t size, size_t alignment);
    [[nodiscard]] std::optional<BufferHandle> allocate_aligned(
        size_t size, size_t alignment, DfsBufferUsageKind kind);
    [[nodiscard]] std::optional<BufferHandle> allocate_aligned_wait(
        size_t size, size_t alignment, DfsBufferUsageKind kind,
        std::chrono::milliseconds timeout, bool* timed_out = nullptr,
        bool* request_exceeds_limit = nullptr);
    void SetUsageTracker(std::shared_ptr<DfsBufferUsageTracker> tracker,
                         DfsBufferUsageKind kind);

    [[nodiscard]] std::shared_ptr<DfsBufferUsageTracker> usage_tracker() const {
        return usage_tracker_;
    }

    [[nodiscard]] DfsBufferUsageKind usage_kind() const { return usage_kind_; }

    // Free space and a conservative lower bound of the largest free region
    // (taken from the largest non-empty bin). All zero when the allocator has
    // no backing buffer.
    void NotifyAllocationReleased();
    void ShutdownAllocationWaiters();

    [[nodiscard]] offset_allocator::OffsetAllocStorageReport storageReport()
        const;

   protected:
    // Constructors accessible to derived classes
    ClientBufferAllocator(void* addr, size_t size, const std::string& protocol,
                          void* device_addr = nullptr);

    void* buffer_;
    size_t buffer_size_;
    void* device_buffer_ = nullptr;
    bool use_hugepage_ = false;

   private:
    ClientBufferAllocator(size_t size, const std::string& protocol,
                          bool use_hugepage,
                          std::shared_ptr<DmaBufferAllocator> dma_allocator);

    // Declared before allocator_ so the backing outlives allocator teardown.
    std::shared_ptr<void> backing_owner_;
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator_;
    std::shared_ptr<DfsBufferUsageTracker> usage_tracker_;
    DfsBufferUsageKind usage_kind_ = DfsBufferUsageKind::kRead;
    mutable std::mutex allocation_wait_mutex_;
    std::condition_variable allocation_wait_cv_;
    uint64_t allocation_generation_ = 0;
    bool allocation_wait_shutdown_ = false;

    std::string protocol;
    bool is_external_memory_ = false;
    std::shared_ptr<DmaBufferAllocator> dma_allocator_;  // was use_spdk_dma_
};

/**
 * BufferHandle provides RAII management for allocated memory regions within
 * a ClientBufferAllocator. It automatically deallocates the memory when
 * destroyed and provides access to the allocated memory pointer and size.
 *
 * This class is move-only to ensure proper ownership semantics and prevent
 * accidental double-free errors.
 */
class BufferHandle {
   public:
    // Owning mode: backed by allocator
    BufferHandle(std::shared_ptr<ClientBufferAllocator> allocator,
                 offset_allocator::OffsetAllocationHandle handle,
                 std::shared_ptr<DfsBufferUsageTracker> usage_tracker = nullptr,
                 DfsBufferUsageKind usage_kind = DfsBufferUsageKind::kRead,
                 size_t reserved_size = 0);

    // View mode: non-owning, calls release_fn on destruction
    BufferHandle(void* ptr, size_t size, std::function<void()> release_fn,
                 void* device_ptr = nullptr, size_t allocation_size = 0);

    ~BufferHandle();

    BufferHandle(BufferHandle&& other) noexcept;
    BufferHandle& operator=(BufferHandle&& other) noexcept;

    // Disable copy constructor and copy assignment operator
    BufferHandle(const BufferHandle&) = delete;
    BufferHandle& operator=(const BufferHandle&) = delete;

    [[nodiscard]] void* ptr() const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t allocation_size() const;
    // Device-visible alias for a mapped host view, when one is available.
    [[nodiscard]] void* device_ptr() const;

   private:
    friend class ClientBufferAllocator;

    // Owning mode members
    std::shared_ptr<ClientBufferAllocator> allocator_;
    offset_allocator::OffsetAllocationHandle handle_;
    std::unique_ptr<DfsBufferAllocationUsage> usage_;

    // View mode members
    void* view_ptr_ = nullptr;
    size_t view_size_ = 0;
    size_t view_allocation_size_ = 0;
    std::function<void()> release_fn_;
    void* view_device_ptr_ = nullptr;
};

// Utility functions for buffer and slice management
/**
 * @brief Split a BufferHandle into slices of maximum size kMaxSliceSize
 * @param handle The buffer handle to split
 * @return Vector of slices covering the entire buffer
 */
std::vector<Slice> split_into_slices(BufferHandle& handle);

/**
 * @brief Split a buffer into slices of maximum kMaxSliceSize
 * @param buffer The buffer buffer to split
 * @param length The length of the buffer to split
 * @return Vector of slices covering the entire buffer
 */
std::vector<Slice> split_into_slices(void* buffer, size_t length);

/**
 * @brief Calculate the total size of a replica descriptor
 * @param replica The replica descriptor to calculate size for
 * @return Total size in bytes
 */
uint64_t calculate_total_size(const Replica::Descriptor& replica);

/**
 * @brief Allocate slices from a buffer handle based on replica descriptor
 * @param slices Output vector to store the allocated slices
 * @param replica The replica descriptor defining the slice structure
 * @param buffer_ptr The buffer pointer to allocate slices from
 * @return 0 on success, non-zero on error
 */
int allocateSlices(std::vector<Slice>& slices,
                   const Replica::Descriptor& replica, void* buffer_ptr);

}  // namespace mooncake
