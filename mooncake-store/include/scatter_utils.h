#pragma once

#include "types.h"
#include "device/accelerator_registry.h"

namespace mooncake {

inline tl::expected<void, ErrorCode> scatter_host_to_maybe_device(
    void *dst, const void *src, size_t size, const std::string &context) {
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    if (!runtime_accelerator.CopyFromHost(dst, src, size)) {
        LOG(ERROR) << "H2D copy failed: " << context;
        return tl::unexpected(ErrorCode::TRANSFER_FAIL);
    }
    return {};
}

inline tl::expected<void, ErrorCode> gather_maybe_device_to_host(
    void *dst, const void *src, size_t size, const std::string &context) {
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    if (!runtime_accelerator.CopyToHost(dst, src, size)) {
        LOG(ERROR) << "D2H copy failed: " << context;
        return tl::unexpected(ErrorCode::TRANSFER_FAIL);
    }
    return {};
}

}  // namespace mooncake
