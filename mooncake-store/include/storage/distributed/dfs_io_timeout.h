#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>

namespace mooncake {

inline constexpr uint64_t kDfsTimeoutOneGiB = 1ULL << 30;
inline constexpr uint64_t kDfsTimeoutFourGiB = 4ULL << 30;

inline uint64_t SaturatingScaleTimeoutMs(uint64_t base_timeout_ms,
                                         uint64_t multiplier) {
    if (base_timeout_ms == 0) return 0;
    if (base_timeout_ms >
        std::numeric_limits<uint64_t>::max() / multiplier) {
        return std::numeric_limits<uint64_t>::max();
    }
    return base_timeout_ms * multiplier;
}

inline uint64_t DfsBatchGetTimeoutMs(uint64_t base_timeout_ms,
                                     uint64_t total_bytes) {
    uint64_t multiplier = 16;
    if (total_bytes <= kDfsTimeoutOneGiB) {
        multiplier = 2;
    } else if (total_bytes <= kDfsTimeoutFourGiB) {
        multiplier = 8;
    }
    return SaturatingScaleTimeoutMs(base_timeout_ms, multiplier);
}

inline uint64_t DfsBatchPutTimeoutMs(uint64_t base_timeout_ms,
                                     uint64_t total_bytes) {
    uint64_t multiplier = 32;
    if (total_bytes <= kDfsTimeoutOneGiB) {
        multiplier = 4;
    } else if (total_bytes <= kDfsTimeoutFourGiB) {
        multiplier = 16;
    }
    return SaturatingScaleTimeoutMs(base_timeout_ms, multiplier);
}

inline void SaturatingAddBytes(uint64_t bytes, uint64_t* total) {
    if (bytes > std::numeric_limits<uint64_t>::max() - *total) {
        *total = std::numeric_limits<uint64_t>::max();
    } else {
        *total += bytes;
    }
}

inline std::chrono::steady_clock::time_point DfsIoDeadlineFromNow(
    uint64_t timeout_ms) {
    const auto now = std::chrono::steady_clock::now();
    const auto available = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::time_point::max() - now);
    const auto max_count = static_cast<uint64_t>(available.count());
    const auto clamped = std::min(timeout_ms, max_count > 0 ? max_count - 1 : 0);
    return now + std::chrono::milliseconds(clamped);
}

}  // namespace mooncake
