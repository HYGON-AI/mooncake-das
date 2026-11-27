#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ext/mooncake_private_ext_abi.h"

namespace mooncake::ext {

/**
 * Process-wide handle to the DFS data-plane extension.
 *
 * The provider is optional in every sense: when it is missing, disabled, or
 * rejected by the ABI check, `available()` is false and every helper below
 * returns false so the caller keeps its baseline behavior.
 */
class DfeProvider {
   public:
    static DfeProvider& Instance();

    bool available() const { return ext_ != nullptr; }

    // Why the provider is unavailable; stable strings suitable for one startup
    // log line.
    const std::string& state() const { return state_; }
    const std::string& detail() const { return detail_; }

    /**
     * Batched read/write. Returns false when the provider is unavailable or
     * rejected the batch shape, in which case `results` must be ignored.
     */
    bool IoBatch(const mooncake_ext_io_request* requests,
                 mooncake_ext_io_result* results, size_t count) const;

    /**
     * Ask the provider where one batch should start inside a bucket.
     * Returns false when no provider is available.
     */
    bool AlignBatchOffset(uint64_t append_offset, uint64_t capacity,
                          uint64_t object_size, uint64_t* out_offset,
                          uint64_t* out_alignment, bool* fits) const;

    /** Returns MOONCAKE_EXT_STATUS_* or UNAVAILABLE without a provider. */
    int ConfigureWriteFd(int fd, uint64_t max_write_bytes) const;

    /** Returns MOONCAKE_EXT_STATUS_* or UNAVAILABLE without a provider. */
    int RefreshFileMtime(int fd) const;

    /**
     * Derive the unified DFS buffer admission budgets. Returns false when no
     * provider is available, in which case the caller applies its own
     * baseline policy.
     */
    bool BufferPolicy(const mooncake_ext_buffer_policy_query& query,
                      mooncake_ext_buffer_policy* result) const;

   private:
    DfeProvider();
    DfeProvider(const DfeProvider&) = delete;
    DfeProvider& operator=(const DfeProvider&) = delete;

    const mooncake_dfe_ext* ext_ = nullptr;
    std::string state_ = "unloaded";
    std::string detail_;
};

}  // namespace mooncake::ext
