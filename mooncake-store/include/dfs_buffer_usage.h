#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <limits>

#include <glog/logging.h>

namespace mooncake {

inline constexpr uint64_t kHighWatermarkNumerator = 8;
inline constexpr uint64_t kHighWatermarkDenominator = 10;

enum class DfsBufferUsageKind : uint8_t { kRead, kPrefetch, kWrite };

enum class DfsBufferAdmissionFailure : uint8_t {
    kNone,
    kCapacity,
    kReadReserve,
    kPrefetchLimit,
    kRequestExceedsLimit,
    kOverflow,
};

struct DfsBufferUsageSnapshot {
    uint64_t read_bytes = 0;
    uint64_t prefetch_bytes = 0;
    uint64_t write_bytes = 0;
    uint64_t total_bytes = 0;
    uint64_t peak_total_bytes = 0;
};

struct DfsBufferAdmissionLimits {
    uint64_t read_min_protection_bytes = 0;
    uint64_t write_min_protection_bytes = 0;
    uint64_t prefetch_limit_bytes = 0;
};

class DfsBufferUsageTracker {
   public:
    explicit DfsBufferUsageTracker(uint64_t capacity_bytes = 0)
        : capacity_bytes_(capacity_bytes) {}

    // Sets the minimum read protection used by non-read admission. The
    // effective read protection is max(this value, current read usage
    // including outstanding reservations). The write protection is carved
    // out of the prefetch budget so DFS write staging always has room for
    // one chunk. Writes wait for space (bounded by
    // MC_STORE_DFS_WRITE_ARENA_WAIT_MS) instead of failing immediately, so a
    // fixed reservation at configuration time is sufficient and admission
    // does not account for it per request.
    DfsBufferAdmissionLimits ConfigureAdmission(
        uint64_t read_min_protection_bytes, uint64_t write_min_protection_bytes,
        uint64_t prefetch_limit_bytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        read_min_protection_bytes_ =
            std::min(read_min_protection_bytes, capacity_bytes_);
        const auto read_free_capacity =
            capacity_bytes_ - read_min_protection_bytes_;
        const auto write_protection =
            std::min(write_min_protection_bytes, read_free_capacity);
        const auto prefetch_capacity = read_free_capacity - write_protection;
        prefetch_limit_bytes_ =
            std::min(prefetch_limit_bytes, prefetch_capacity);
        admission_configured_ = true;
        return DfsBufferAdmissionLimits{read_min_protection_bytes_,
                                        write_protection,
                                        prefetch_limit_bytes_};
    }

    DfsBufferAdmissionFailure TryReserve(DfsBufferUsageKind kind,
                                          uint64_t bytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto index = static_cast<size_t>(kind);
        if (bytes > std::numeric_limits<uint64_t>::max() - reserved_total_) {
            return DfsBufferAdmissionFailure::kOverflow;
        }
        const auto reserved_after = reserved_total_ + bytes;
        if (capacity_bytes_ > 0 && kind != DfsBufferUsageKind::kRead &&
            bytes > capacity_bytes_ - read_min_protection_bytes_) {
            return DfsBufferAdmissionFailure::kRequestExceedsLimit;
        }
        if (total_bytes_ >
            std::numeric_limits<uint64_t>::max() - reserved_after) {
            return DfsBufferAdmissionFailure::kOverflow;
        }
        const auto total_after = total_bytes_ + reserved_after;
        if (capacity_bytes_ > 0 && total_after > capacity_bytes_) {
            return DfsBufferAdmissionFailure::kCapacity;
        }
        const auto read_index =
            static_cast<size_t>(DfsBufferUsageKind::kRead);
        if (bytes_[read_index] >
            std::numeric_limits<uint64_t>::max() - reserved_[read_index]) {
            return DfsBufferAdmissionFailure::kOverflow;
        }
        const auto current_read_bytes =
            bytes_[read_index] + reserved_[read_index];
        const auto effective_read_protection = std::min(
            capacity_bytes_,
            std::max(read_min_protection_bytes_, current_read_bytes));
        const auto non_read_after = total_after - current_read_bytes;

        switch (kind) {
            case DfsBufferUsageKind::kRead:
                break;
            case DfsBufferUsageKind::kPrefetch: {
                if (capacity_bytes_ > 0 &&
                    non_read_after > capacity_bytes_ -
                                         effective_read_protection) {
                    return DfsBufferAdmissionFailure::kReadReserve;
                }
                const auto prefetch_bytes =
                    bytes_[static_cast<size_t>(DfsBufferUsageKind::kPrefetch)];
                if (prefetch_bytes >
                        std::numeric_limits<uint64_t>::max() -
                            reserved_[index]) {
                    return DfsBufferAdmissionFailure::kOverflow;
                }
                const auto prefetch_reserved =
                    prefetch_bytes + reserved_[index];
                if (bytes >
                    std::numeric_limits<uint64_t>::max() -
                        prefetch_reserved) {
                    return DfsBufferAdmissionFailure::kOverflow;
                }
                if (admission_configured_ &&
                    prefetch_reserved + bytes > prefetch_limit_bytes_) {
                    return DfsBufferAdmissionFailure::kPrefetchLimit;
                }
                break;
            }
            case DfsBufferUsageKind::kWrite:
                if (capacity_bytes_ > 0 &&
                    non_read_after > capacity_bytes_ -
                                         effective_read_protection) {
                    return DfsBufferAdmissionFailure::kReadReserve;
                }
                break;
        }
        if (bytes > std::numeric_limits<uint64_t>::max() - reserved_[index]) {
            return DfsBufferAdmissionFailure::kOverflow;
        }
        reserved_[index] += bytes;
        reserved_total_ = reserved_after;
        return DfsBufferAdmissionFailure::kNone;
    }

    void CommitReservation(DfsBufferUsageKind kind, uint64_t reserved,
                           uint64_t actual) {
        bool warn = false;
        uint64_t total = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto index = static_cast<size_t>(kind);
            DCHECK_GE(reserved_[index], reserved);
            DCHECK_GE(reserved_total_, reserved);
            reserved_[index] = reserved_[index] >= reserved
                                   ? reserved_[index] - reserved
                                   : 0;
            reserved_total_ = reserved_total_ >= reserved
                                  ? reserved_total_ - reserved
                                  : 0;
            bytes_[index] += actual;
            total_bytes_ += actual;
            peak_total_bytes_ = std::max(peak_total_bytes_, total_bytes_);
            if (capacity_bytes_ > 0) {
                const bool above_threshold =
                    static_cast<unsigned __int128>(total_bytes_) *
                        kHighWatermarkDenominator >=
                    static_cast<unsigned __int128>(capacity_bytes_) *
                        kHighWatermarkNumerator;
                warn = above_threshold && !high_watermark_warned_;
                high_watermark_warned_ = above_threshold;
                total = total_bytes_;
            }
        }
        if (warn) {
            LOG(WARNING) << "DFS_UNIFIED_BUFFER_HIGH_USAGE total_bytes="
                         << total << " capacity_bytes=" << capacity_bytes_
                         << " utilization="
                         << (static_cast<double>(total) * 100.0 /
                             static_cast<double>(capacity_bytes_))
                         << "%";
        }
    }

    void CancelReservation(DfsBufferUsageKind kind, uint64_t bytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto index = static_cast<size_t>(kind);
        DCHECK_GE(reserved_[index], bytes);
        DCHECK_GE(reserved_total_, bytes);
        reserved_[index] = reserved_[index] >= bytes
                               ? reserved_[index] - bytes
                               : 0;
        reserved_total_ = reserved_total_ >= bytes ? reserved_total_ - bytes : 0;
    }

    void Add(DfsBufferUsageKind kind, uint64_t bytes) {
        bool warn = false;
        uint64_t total = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& value = bytes_[static_cast<size_t>(kind)];
            value += bytes;
            total_bytes_ += bytes;
            peak_total_bytes_ = std::max(peak_total_bytes_, total_bytes_);
            if (capacity_bytes_ > 0) {
                const bool above_threshold =
                    static_cast<unsigned __int128>(total_bytes_) *
                        kHighWatermarkDenominator >=
                    static_cast<unsigned __int128>(capacity_bytes_) *
                        kHighWatermarkNumerator;
                warn = above_threshold && !high_watermark_warned_;
                high_watermark_warned_ = above_threshold;
                total = total_bytes_;
            }
        }
        if (warn) {
            LOG(WARNING) << "DFS_UNIFIED_BUFFER_HIGH_USAGE total_bytes="
                         << total << " capacity_bytes=" << capacity_bytes_
                         << " utilization="
                         << (static_cast<double>(total) * 100.0 /
                             static_cast<double>(capacity_bytes_))
                         << "%";
        }
    }

    void Remove(DfsBufferUsageKind kind, uint64_t bytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& value = bytes_[static_cast<size_t>(kind)];
        DCHECK_GE(value, bytes)
            << "DFS buffer usage underflow for kind="
            << static_cast<int>(kind) << ", current=" << value
            << ", remove=" << bytes;
        DCHECK_GE(total_bytes_, bytes)
            << "DFS total buffer usage underflow: current=" << total_bytes_
            << ", remove=" << bytes;
        value = value >= bytes ? value - bytes : 0;
        total_bytes_ = total_bytes_ >= bytes ? total_bytes_ - bytes : 0;
        if (capacity_bytes_ > 0 &&
            static_cast<unsigned __int128>(total_bytes_) *
                    kHighWatermarkDenominator <
                static_cast<unsigned __int128>(capacity_bytes_) *
                    kHighWatermarkNumerator) {
            high_watermark_warned_ = false;
        }
    }

    [[nodiscard]] DfsBufferUsageSnapshot Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return {bytes_[0], bytes_[1], bytes_[2], total_bytes_,
                peak_total_bytes_};
    }

    // Effective prefetch budget after read and write protections are carved
    // out. Returns 0 when admission has not been configured or when the
    // protections consume the whole buffer, in which case every prefetch
    // request is refused with kPrefetchLimit.
    [[nodiscard]] uint64_t PrefetchLimitBytes() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return prefetch_limit_bytes_;
    }

   private:
    mutable std::mutex mutex_;
    std::array<uint64_t, 3> bytes_{};
    std::array<uint64_t, 3> reserved_{};
    uint64_t reserved_total_ = 0;
    uint64_t read_min_protection_bytes_ = 0;
    uint64_t prefetch_limit_bytes_ = 0;
    uint64_t total_bytes_ = 0;
    uint64_t peak_total_bytes_ = 0;
    uint64_t capacity_bytes_ = 0;
    bool admission_configured_ = false;
    bool high_watermark_warned_ = false;
};

class DfsBufferAllocationUsage {
   public:
    DfsBufferAllocationUsage(std::shared_ptr<DfsBufferUsageTracker> tracker,
                             DfsBufferUsageKind kind, uint64_t bytes,
                             uint64_t reserved_bytes = 0)
        : tracker_(std::move(tracker)), kind_(kind), bytes_(bytes) {
        if (tracker_) {
            if (reserved_bytes > 0) {
                tracker_->CommitReservation(kind_, reserved_bytes, bytes_);
            } else {
                tracker_->Add(kind_, bytes_);
            }
        }
    }

    ~DfsBufferAllocationUsage() {
        if (tracker_) tracker_->Remove(kind_, bytes_);
    }

    DfsBufferAllocationUsage(const DfsBufferAllocationUsage&) = delete;
    DfsBufferAllocationUsage& operator=(const DfsBufferAllocationUsage&) = delete;

   private:
    std::shared_ptr<DfsBufferUsageTracker> tracker_;
    DfsBufferUsageKind kind_;
    uint64_t bytes_;
};

}  // namespace mooncake
