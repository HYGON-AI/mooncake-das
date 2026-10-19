#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>

namespace mooncake {

// Use one instance per DFS failure log site, leaving unrelated logs untouched.
class DfsLogLimiter {
   public:
    using Clock = std::chrono::steady_clock;

    std::optional<uint64_t> ShouldLog(Clock::time_point now = Clock::now()) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (now < next_log_) {
            ++suppressed_;
            return std::nullopt;
        }
        next_log_ = now + std::chrono::seconds(30);
        const auto suppressed = suppressed_;
        suppressed_ = 0;
        return suppressed;
    }

   private:
    std::mutex mutex_;
    Clock::time_point next_log_ = Clock::time_point::min();
    uint64_t suppressed_ = 0;
};

}  // namespace mooncake
