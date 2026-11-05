#pragma once

#include <array>
#include <cstdint>

namespace mooncake {

inline constexpr std::array<double, 17> kL4ReadLatencySecondsBuckets = {
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
    0.1,    0.25,    0.5,    1,     2.5,    5,     10,   20};

struct ServiceL4Metrics {
    // Read indices: prefetch success/failure, on-demand success/failure.
    std::array<uint64_t, 4> read_operations{};
    std::array<uint64_t, 4> read_bytes{};
    std::array<uint64_t, 4> read_duration_us{};
    // Exclusive buckets, including the final +Inf bucket.
    std::array<std::array<uint64_t, kL4ReadLatencySecondsBuckets.size() + 1>, 4>
        read_duration_buckets{};
    // Restore indices: prefetched, on-demand.
    std::array<uint64_t, 2> restore_ranges{};
    std::array<uint64_t, 2> restore_bytes{};

    bool operator==(const ServiceL4Metrics&) const = default;
};
}  // namespace mooncake
