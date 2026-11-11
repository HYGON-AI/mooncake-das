#include "client_buffer.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <vector>
#include <sys/mman.h>  // For shm_open, mmap, munmap
#include <sys/stat.h>  // For S_IRUSR, S_IWUSR
#include <fcntl.h>     // For O_CREAT, O_RDWR
#include <unistd.h>    // For ftruncate, close, shm_unlink

#include "common/client_buffer_allocation.h"

#if defined(USE_SUNRISE)
#include "sunrise_allocator.h"
#endif

namespace mooncake {

std::shared_ptr<ClientBufferAllocator> ClientBufferAllocator::create(
    size_t size, const std::string& protocol, bool use_hugepage,
    std::shared_ptr<DmaBufferAllocator> dma_allocator) {
    return std::shared_ptr<ClientBufferAllocator>(new ClientBufferAllocator(
        size, protocol, use_hugepage, std::move(dma_allocator)));
}

std::shared_ptr<ClientBufferAllocator> ClientBufferAllocator::create(
    void* addr, size_t size, const std::string& protocol, void* device_addr,
    std::shared_ptr<void> backing_owner,
    std::shared_ptr<DfsBufferUsageTracker> usage_tracker,
    DfsBufferUsageKind usage_kind) {
    auto allocator = std::shared_ptr<ClientBufferAllocator>(
        new ClientBufferAllocator(addr, size, protocol, device_addr));
    allocator->backing_owner_ = std::move(backing_owner);
    allocator->SetUsageTracker(std::move(usage_tracker), usage_kind);
    return allocator;
}

ClientBufferAllocator::ClientBufferAllocator(
    size_t size, const std::string& protocol, bool use_hugepage,
    std::shared_ptr<DmaBufferAllocator> dma_allocator)
    : buffer_size_(size),
      use_hugepage_(use_hugepage),
      protocol(protocol),
      dma_allocator_(std::move(dma_allocator)) {
    if (size == 0) {
        buffer_ = nullptr;
        allocator_ = nullptr;
        return;
    }
    // Align to 64 bytes(cache line size) for better cache performance
    constexpr size_t alignment = 64;
    if (use_hugepage_) {
        buffer_ = allocate_buffer_mmap_memory(size, alignment);
    } else {
#if defined(USE_SUNRISE)
        if (protocol == "sunrise_link") {
            buffer_ = sunrise_allocate_memory(size, alignment, false);
        } else
#endif
        {
            buffer_ = allocate_buffer_allocator_memory(
                size, protocol, alignment, dma_allocator_.get());
        }
    }
    if (!buffer_) {
        throw std::bad_alloc();
    }

    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::ClientBufferAllocator(void* addr, size_t size,
                                             const std::string& protocol,
                                             void* device_addr)
    : buffer_(addr),
      buffer_size_(size),
      device_buffer_(device_addr),
      protocol(protocol),
      is_external_memory_(true) {
    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::~ClientBufferAllocator() {
    // Free the aligned allocated memory or unmap shared memory
    if (!is_external_memory_ && buffer_) {
        if (use_hugepage_) {
            free_buffer_mmap_memory(buffer_, buffer_size_);
        } else {
            free_memory(protocol, buffer_, dma_allocator_.get());
        }
    }
}

offset_allocator::OffsetAllocStorageReport
ClientBufferAllocator::storageReport() const {
    if (allocator_ == nullptr) {
        return {0, 0};
    }
    return allocator_->storageReport();
}

void ClientBufferAllocator::SetUsageTracker(
    std::shared_ptr<DfsBufferUsageTracker> tracker,
    DfsBufferUsageKind kind) {
    usage_tracker_ = std::move(tracker);
    usage_kind_ = kind;
}

void ClientBufferAllocator::NotifyAllocationReleased() {
    {
        std::lock_guard<std::mutex> lock(allocation_wait_mutex_);
        ++allocation_generation_;
    }
    allocation_wait_cv_.notify_all();
}

void ClientBufferAllocator::ShutdownAllocationWaiters() {
    {
        std::lock_guard<std::mutex> lock(allocation_wait_mutex_);
        allocation_wait_shutdown_ = true;
        ++allocation_generation_;
    }
    allocation_wait_cv_.notify_all();
}

size_t ClientBufferAllocator::allocation_size_for_request(size_t size) const {
    if (!allocator_) return 0;
    return allocator_->normalizedAllocationSize(size);
}

size_t ClientBufferAllocator::allocation_size_for_aligned_request(
    size_t size, size_t alignment) const {
    if (size == 0 || alignment == 0 ||
        size > std::numeric_limits<size_t>::max() - (alignment - 1)) {
        return 0;
    }
    return allocation_size_for_request(size + alignment - 1);
}

std::optional<BufferHandle> ClientBufferAllocator::allocate(size_t size) {
    if (allocator_ == nullptr) {
        return std::nullopt;
    }
    const auto estimated_size = allocation_size_for_request(size);
    if (estimated_size == 0) {
        return std::nullopt;
    }
    if (usage_tracker_ &&
        usage_tracker_->TryReserve(usage_kind_, estimated_size) !=
            DfsBufferAdmissionFailure::kNone) {
        return std::nullopt;
    }
    auto handle = allocator_->allocate(size);
    if (!handle) {
        if (usage_tracker_) {
            usage_tracker_->CancelReservation(usage_kind_, estimated_size);
        }
        return std::nullopt;
    }
    DCHECK_EQ(estimated_size, handle->allocationSize());

    return std::make_optional<BufferHandle>(
        shared_from_this(), std::move(*handle), usage_tracker_, usage_kind_,
        estimated_size);
}

std::optional<BufferHandle> ClientBufferAllocator::allocate_aligned_wait(
    size_t size, size_t alignment, DfsBufferUsageKind kind,
    std::chrono::milliseconds timeout, bool* timed_out,
    bool* request_exceeds_limit_out) {
    if (timed_out) *timed_out = false;
    if (request_exceeds_limit_out) *request_exceeds_limit_out = false;
    const bool finite_timeout = timeout != std::chrono::milliseconds::max();
    const auto deadline = finite_timeout
                              ? std::chrono::steady_clock::now() + timeout
                              : std::chrono::steady_clock::time_point::max();
    for (;;) {
        std::unique_lock<std::mutex> lock(allocation_wait_mutex_);
        if (allocation_wait_shutdown_) return std::nullopt;
        const auto generation = allocation_generation_;
        lock.unlock();
        std::optional<BufferHandle> allocation;
        // Only the attempt that uses the caller's real size can prove the
        // request itself is too large. The padded attempt asks for
        // size + alignment - 1, so exceeding a hard limit there does not mean
        // the real request could never fit once space is freed.
        bool request_exceeds_limit = false;
        const auto try_allocate = [&](size_t request_size, bool require_aligned,
                                      bool* exceeds_limit)
            -> std::optional<BufferHandle> {
            if (request_size == 0 || !allocator_) return std::nullopt;
            const auto estimated_size =
                allocator_->normalizedAllocationSize(request_size);
            if (estimated_size == 0) {
                *exceeds_limit = true;
                return std::nullopt;
            }
            if (usage_tracker_) {
                const auto admission = usage_tracker_->TryReserve(
                    kind, estimated_size);
                if (admission != DfsBufferAdmissionFailure::kNone) {
                    if (admission ==
                        DfsBufferAdmissionFailure::kRequestExceedsLimit) {
                        *exceeds_limit = true;
                    }
                    return std::nullopt;
                }
            }
            auto handle = allocator_->allocate(request_size);
            if (!handle) {
                if (usage_tracker_) {
                    usage_tracker_->CancelReservation(kind, estimated_size);
                }
                return std::nullopt;
            }
            if (require_aligned &&
                reinterpret_cast<uintptr_t>(handle->ptr()) % alignment != 0) {
                handle.reset();
                if (usage_tracker_) {
                    usage_tracker_->CancelReservation(kind, estimated_size);
                }
                return std::nullopt;
            }
            DCHECK_EQ(estimated_size, handle->allocationSize());
            return BufferHandle(shared_from_this(), std::move(*handle),
                                usage_tracker_, kind, estimated_size);
        };
        if (size != 0 && alignment != 0) {
            allocation = try_allocate(size, true, &request_exceeds_limit);
        }
        if (!allocation && size != 0 && alignment != 0 &&
            size <= std::numeric_limits<size_t>::max() - (alignment - 1)) {
            // A padded miss only means the aligned address could not be
            // satisfied. Its outcome must not be reported as the caller's
            // request exceeding the limit, so the limit flag it writes is
            // deliberately discarded.
            bool padded_limit_ignored = false;
            auto padded_allocation =
                try_allocate(size + alignment - 1, false,
                             &padded_limit_ignored);
            if (padded_allocation) {
                auto owner = std::make_shared<BufferHandle>(
                    std::move(*padded_allocation));
                const auto owner_allocation_size = owner->allocation_size();
                auto* host_base = static_cast<char*>(owner->ptr());
                const size_t remainder =
                    reinterpret_cast<uintptr_t>(host_base) % alignment;
                const size_t offset =
                    remainder == 0 ? 0 : alignment - remainder;
                void* device_ptr = owner->device_ptr();
                if (device_ptr) {
                    device_ptr = static_cast<char*>(device_ptr) + offset;
                }
                auto allocator = shared_from_this();
                allocation.emplace(
                    host_base + offset, size,
                    [owner = std::move(owner), allocator]() mutable {
                        owner.reset();
                        allocator->NotifyAllocationReleased();
                    },
                    device_ptr, owner_allocation_size);
            }
        }
        if (allocation) {
            return allocation;
        }
        if (request_exceeds_limit) {
            if (request_exceeds_limit_out) *request_exceeds_limit_out = true;
            return std::nullopt;
        }
        lock.lock();
        if (allocation_wait_shutdown_) return std::nullopt;
        // Check the wait budget before deciding whether to retry or keep
        // waiting. Doing it first means a busy generation counter can no
        // longer let a bounded wait outlive its deadline, and the retry path
        // below does not need its own copy of these checks.
        if (timeout == std::chrono::milliseconds::zero()) {
            if (timed_out) *timed_out = true;
            break;
        }
        if (finite_timeout && std::chrono::steady_clock::now() >= deadline) {
            if (timed_out) *timed_out = true;
            break;
        }
        if (allocation_generation_ != generation) continue;
        const auto ready = [this, generation] {
            return allocation_wait_shutdown_ ||
                   allocation_generation_ != generation;
        };
        if (timeout == std::chrono::milliseconds::max()) {
            allocation_wait_cv_.wait(lock, ready);
        } else if (allocation_wait_cv_.wait_until(lock, deadline, ready) ==
                   false) {
            if (timed_out) *timed_out = true;
            break;
        }
    }
    return std::nullopt;
}

std::optional<BufferHandle> ClientBufferAllocator::allocate_aligned(
    size_t size, size_t alignment) {
    return allocate_aligned(size, alignment, usage_kind_);
}

std::optional<BufferHandle> ClientBufferAllocator::allocate_aligned(
    size_t size, size_t alignment, DfsBufferUsageKind kind) {
    if (size == 0 || alignment == 0) {
        return std::nullopt;
    }

    const auto allocate_for_kind = [this, kind](size_t allocation_size)
        -> std::optional<BufferHandle> {
        if (allocator_ == nullptr) return std::nullopt;
        const auto estimated_size = allocator_->normalizedAllocationSize(
            allocation_size);
        if (estimated_size == 0) return std::nullopt;
        if (usage_tracker_) {
            const auto admission = usage_tracker_->TryReserve(
                kind, estimated_size);
            if (admission != DfsBufferAdmissionFailure::kNone) {
                return std::nullopt;
            }
        }
        auto handle = allocator_->allocate(allocation_size);
        if (!handle) {
            if (usage_tracker_) {
                usage_tracker_->CancelReservation(kind, estimated_size);
            }
            return std::nullopt;
        }
        DCHECK_EQ(estimated_size, handle->allocationSize());
        return BufferHandle(shared_from_this(), std::move(*handle),
                            usage_tracker_, kind, estimated_size);
    };

    auto allocation = allocate_for_kind(size);
    if (!allocation) return std::nullopt;

    const auto address = reinterpret_cast<uintptr_t>(allocation->ptr());
    if (address % alignment == 0) {
        return allocation;
    }

    allocation.reset();
    if (size > std::numeric_limits<size_t>::max() - (alignment - 1)) {
        return std::nullopt;
    }
    allocation = allocate_for_kind(size + alignment - 1);
    if (!allocation) return std::nullopt;

    auto owner = std::make_shared<BufferHandle>(std::move(allocation.value()));
    const auto owner_allocation_size = owner->allocation_size();
    auto* host_base = static_cast<char*>(owner->ptr());
    const size_t remainder = reinterpret_cast<uintptr_t>(host_base) % alignment;
    const size_t offset = remainder == 0 ? 0 : alignment - remainder;
    void* device_ptr = owner->device_ptr();
    if (device_ptr) {
        device_ptr = static_cast<char*>(device_ptr) + offset;
    }
    auto allocator = shared_from_this();
    return BufferHandle(
        host_base + offset, size,
        [owner = std::move(owner), allocator]() mutable {
            owner.reset();
            allocator->NotifyAllocationReleased();
        },
        device_ptr, owner_allocation_size);
}

BufferHandle::BufferHandle(
    std::shared_ptr<ClientBufferAllocator> allocator,
    mooncake::offset_allocator::OffsetAllocationHandle handle,
    std::shared_ptr<DfsBufferUsageTracker> usage_tracker,
    DfsBufferUsageKind usage_kind, size_t reserved_size)
    : allocator_(std::move(allocator)),
      handle_(std::move(handle)),
      usage_(usage_tracker
                 ? std::make_unique<DfsBufferAllocationUsage>(
                       std::move(usage_tracker), usage_kind,
                       handle_.allocationSize(), reserved_size)
                 : nullptr) {}

BufferHandle::BufferHandle(BufferHandle&& other) noexcept
    : allocator_(std::move(other.allocator_)),
      handle_(std::move(other.handle_)),
      usage_(std::move(other.usage_)),
      view_ptr_(other.view_ptr_),
      view_size_(other.view_size_),
      view_allocation_size_(other.view_allocation_size_),
      release_fn_(std::move(other.release_fn_)),
      view_device_ptr_(other.view_device_ptr_) {
    other.view_ptr_ = nullptr;
    other.view_size_ = 0;
    other.view_allocation_size_ = 0;
    other.view_device_ptr_ = nullptr;
}

BufferHandle& BufferHandle::operator=(BufferHandle&& other) noexcept {
    if (this == &other) return *this;
    auto old_allocator = allocator_;
    if (release_fn_) {
        release_fn_();
        release_fn_ = nullptr;
    }
    usage_.reset();
    handle_ = std::move(other.handle_);
    usage_ = std::move(other.usage_);
    if (old_allocator) old_allocator->NotifyAllocationReleased();
    allocator_ = std::move(other.allocator_);
    view_ptr_ = other.view_ptr_;
    view_size_ = other.view_size_;
    view_allocation_size_ = other.view_allocation_size_;
    release_fn_ = std::move(other.release_fn_);
    view_device_ptr_ = other.view_device_ptr_;
    other.view_ptr_ = nullptr;
    other.view_size_ = 0;
    other.view_allocation_size_ = 0;
    other.view_device_ptr_ = nullptr;
    return *this;
}

BufferHandle::BufferHandle(void* ptr, size_t size,
                           std::function<void()> release_fn,
                           void* device_ptr, size_t allocation_size)
    : view_ptr_(ptr),
      view_size_(size),
      view_allocation_size_(allocation_size == 0 ? size : allocation_size),
      release_fn_(std::move(release_fn)),
      view_device_ptr_(device_ptr) {}

BufferHandle::~BufferHandle() {
    if (release_fn_) {
        release_fn_();
        release_fn_ = nullptr;
    }
    if (allocator_) {
        usage_.reset();
        handle_ = offset_allocator::OffsetAllocationHandle();
        allocator_->NotifyAllocationReleased();
    }
    // Otherwise OffsetAllocationHandle destructor handles deallocation
}

void* BufferHandle::ptr() const {
    return view_ptr_ ? view_ptr_ : handle_.ptr();
}

size_t BufferHandle::size() const {
    return view_ptr_ ? view_size_ : handle_.size();
}

size_t BufferHandle::allocation_size() const {
    return view_ptr_ ? view_allocation_size_ : handle_.allocationSize();
}

void* BufferHandle::device_ptr() const {
    if (view_ptr_) return view_device_ptr_;
    if (!allocator_ || !allocator_->getDeviceBase() || !handle_.ptr()) {
        return nullptr;
    }
    const auto offset = static_cast<char*>(handle_.ptr()) -
                        static_cast<char*>(allocator_->getBase());
    return static_cast<char*>(allocator_->getDeviceBase()) + offset;
}

// Utility functions for buffer and slice management
std::string DescribeUnifiedBufferState(
    const std::shared_ptr<ClientBufferAllocator>& allocator) {
    if (!allocator) return {};
    const auto report = allocator->storageReport();
    std::ostringstream out;
    // free/contig together separate fragmentation (contig too small) from a
    // genuine shortage (free itself too small) or an admission refusal (both
    // large enough, yet the request was rejected).
    out << "free=" << report.totalFreeSpace
        << ", contig=" << report.largestFreeRegion;
    if (const auto tracker = allocator->usage_tracker()) {
        const auto usage = tracker->Snapshot();
        out << ", rd=" << usage.read_bytes
            << ", pf=" << usage.prefetch_bytes
            << ", wr=" << usage.write_bytes;
        const auto limits = tracker->AdmissionLimits();
        out << ", rd_prot=" << limits.read_min_protection_bytes
            << ", wr_prot=" << limits.write_min_protection_bytes
            << ", pf_cap=" << limits.prefetch_limit_bytes;
    }
    return out.str();
}

std::vector<Slice> split_into_slices(BufferHandle& handle) {
    auto base = static_cast<uint8_t*>(handle.ptr());
    auto length = handle.size();
    return split_into_slices(base, length);
}

std::vector<Slice> split_into_slices(void* buffer, size_t length) {
    std::vector<Slice> slices;
    auto base = static_cast<uint8_t*>(buffer);
    size_t offset = 0;

    while (offset < length) {
        size_t chunk_size = std::min(length - offset, kMaxSliceSize);
        slices.push_back({base + offset, chunk_size});
        offset += chunk_size;
    }
    return slices;
}

uint64_t calculate_total_size(const Replica::Descriptor& replica) {
    uint64_t total_length = 0;
    if (replica.is_disk_replica()) {
        auto& disk_descriptor = replica.get_disk_descriptor();
        total_length = disk_descriptor.object_size;
    } else if (replica.is_local_disk_replica()) {
        total_length = replica.get_local_disk_descriptor().object_size;
    } else if (replica.is_dfs_replica()) {
        total_length = replica.get_dfs_descriptor().object_size;
    } else if (replica.is_nof_replica()) {
        total_length = replica.get_nof_descriptor().buffer_descriptor.size_;
    } else {
        total_length = replica.get_memory_descriptor().buffer_descriptor.size_;
    }
    return total_length;
}

int allocateSlices(std::vector<Slice>& slices,
                   const Replica::Descriptor& replica, void* buffer_ptr) {
    if (replica.is_disk_replica()) {
        // For disk-based replica, split into slices based on file size
        uint64_t offset = 0;
        uint64_t total_length = replica.get_disk_descriptor().object_size;
        while (offset < total_length) {
            auto chunk_size = std::min(total_length - offset, kMaxSliceSize);
            void* chunk_ptr = static_cast<char*>(buffer_ptr) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }
    } else if (replica.is_local_disk_replica()) {
        slices.emplace_back(
            Slice{buffer_ptr, replica.get_local_disk_descriptor().object_size});
    } else if (replica.is_dfs_replica()) {
        // Use aligned_size (value + padding) so the slice length is a multiple
        // of the DFS block alignment, letting O_DIRECT go through the zero-bounce
        // preadv path. The trailing padding is read but not scattered.
        const auto& desc = replica.get_dfs_descriptor();
        const uint64_t slice_size =
            desc.aligned_size > 0 ? desc.aligned_size : desc.object_size;
        slices.emplace_back(Slice{buffer_ptr, slice_size});
    } else if (replica.is_nof_replica()) {
        auto& handle = replica.get_nof_descriptor().buffer_descriptor;
        void* chunk_ptr = buffer_ptr;
        slices.emplace_back(Slice{chunk_ptr, handle.size_});
    } else {
        // For memory-based replica, split into slices based on buffer
        // descriptors
        auto& handle = replica.get_memory_descriptor().buffer_descriptor;
        void* chunk_ptr = buffer_ptr;
        slices.emplace_back(Slice{chunk_ptr, handle.size_});
    }
    return 0;
}

}  // namespace mooncake
