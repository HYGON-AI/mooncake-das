#pragma once

#include <condition_variable>
#include <cstddef>
#include <chrono>
#include <memory>
#include <mutex>
#include <vector>

#include "pinned_buffer_pool.h"

namespace mooncake {

class DfsWriteStagingPool {
   public:
    // Owns a fixed number of arenas. A slot remains unavailable until the
    // asynchronous DFS task releases its context.
    struct Slot {
        PinnedBufferPool::Buffer backing;
    };

    DfsWriteStagingPool(size_t arena_size, size_t arena_count);
    ~DfsWriteStagingPool();

    DfsWriteStagingPool(const DfsWriteStagingPool&) = delete;
    DfsWriteStagingPool& operator=(const DfsWriteStagingPool&) = delete;

    std::shared_ptr<Slot> Acquire(
        std::chrono::milliseconds timeout = std::chrono::seconds(10),
        bool* timed_out = nullptr);
    void Release(std::shared_ptr<Slot> slot);
    void Shutdown();

    size_t arena_size() const { return arena_size_; }
    size_t capacity() const;
    size_t available() const;

   private:
    const size_t arena_size_;
    std::vector<std::shared_ptr<Slot>> slots_;
    std::vector<std::shared_ptr<Slot>> free_slots_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool shutting_down_{false};
};

}  // namespace mooncake
