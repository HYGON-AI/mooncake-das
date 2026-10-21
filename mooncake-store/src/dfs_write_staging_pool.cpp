#include "dfs_write_staging_pool.h"

#include <stdexcept>
#include <utility>

namespace mooncake {

DfsWriteStagingPool::DfsWriteStagingPool(size_t arena_size,
                                         size_t arena_count)
    : arena_size_(arena_size) {
    if (arena_size == 0 || arena_count == 0) {
        throw std::invalid_argument("DFS write staging arena is disabled");
    }

    slots_.reserve(arena_count);
    free_slots_.reserve(arena_count);
    for (size_t i = 0; i < arena_count; ++i) {
        auto backing = PinnedBufferPool::AllocatePinned(arena_size);
        if (!backing.data) {
            backing = PinnedBufferPool::Buffer::Pageable(arena_size);
        }
        if (!backing.data) {
            throw std::runtime_error(
                "Failed to allocate DFS write staging arena");
        }
        auto slot = std::make_shared<Slot>();
        slot->backing = std::move(backing);
        slots_.push_back(slot);
        free_slots_.push_back(std::move(slot));
    }
}

DfsWriteStagingPool::~DfsWriteStagingPool() { Shutdown(); }

std::shared_ptr<DfsWriteStagingPool::Slot> DfsWriteStagingPool::Acquire(
    std::chrono::milliseconds timeout, bool* timed_out) {
    if (timed_out) *timed_out = false;
    std::unique_lock<std::mutex> lock(mutex_);
    if (!cv_.wait_for(
            lock, timeout,
            [this] { return shutting_down_ || !free_slots_.empty(); })) {
        if (timed_out) *timed_out = true;
        return nullptr;
    }
    if (shutting_down_) return nullptr;
    auto slot = std::move(free_slots_.back());
    free_slots_.pop_back();
    return slot;
}

void DfsWriteStagingPool::Release(std::shared_ptr<Slot> slot) {
    if (!slot) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutting_down_) return;
    free_slots_.push_back(std::move(slot));
    cv_.notify_one();
}

void DfsWriteStagingPool::Shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutting_down_) return;
    shutting_down_ = true;
    cv_.notify_all();
}

size_t DfsWriteStagingPool::capacity() const {
    return arena_size_ * slots_.size();
}

size_t DfsWriteStagingPool::available() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return free_slots_.size();
}

}  // namespace mooncake
