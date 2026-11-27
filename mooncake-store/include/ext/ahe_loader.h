#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "ext/mooncake_private_ext_abi.h"

namespace mooncake::ext {

/**
 * Process-wide handle to the accelerator host-to-device extension.
 *
 * Only the client process loads it, and only when the build enables HIP.
 * Everything the provider returns is opaque to the public side: submit
 * handles and mapped pointers must be passed back to this provider.
 */
class AheProvider {
   public:
    static AheProvider& Instance();

    bool available() const { return ext_ != nullptr; }
    const std::string& state() const { return state_; }
    const std::string& detail() const { return detail_; }

    void* Create(int device_ordinal, uint32_t stream_count);
    bool Submit(void* handle, const mooncake_ahe_copy_range* ranges,
                size_t range_count, uint32_t stream_key);
    bool Sync(void* handle);
    void Destroy(void* handle);
    bool AllocMapped(int device_ordinal, uint64_t size, void** host_ptr,
                     void** device_ptr);
    void FreeMapped(void* host_ptr);

   private:
    AheProvider();
    AheProvider(const AheProvider&) = delete;
    AheProvider& operator=(const AheProvider&) = delete;

    const mooncake_ahe_ext* ext_ = nullptr;
    std::string state_ = "unloaded";
    std::string detail_;
};

}  // namespace mooncake::ext
