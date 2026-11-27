#include "ext/dfe_loader.h"

#include <glog/logging.h>

#include <mutex>

#include "ext/shared_library.h"

namespace mooncake::ext {
namespace {

constexpr char kDfeFileName[] = "libmooncake_dfe.so.1";
constexpr char kDfeSymbol[] = "mooncake_dfe_extension_get";

struct DfeState {
    SharedLibrary library;
    mooncake_dfe_ext ext{};
    bool loaded = false;
    std::string state = "unloaded";
    std::string detail;
};

// Intentionally leaked: the provider may register atexit handlers or own
// threads, so its state must outlive every static destructor in the process.
DfeState& State() {
    static DfeState* state = new DfeState();
    return *state;
}

const char* StatusName(int status) {
    switch (status) {
        case MOONCAKE_EXT_STATUS_OK:
            return "ok";
        case MOONCAKE_EXT_STATUS_UNAVAILABLE:
            return "unavailable";
        case MOONCAKE_EXT_STATUS_INVALID_ARGUMENT:
            return "invalid_argument";
        case MOONCAKE_EXT_STATUS_IO_ERROR:
            return "io_error";
        case MOONCAKE_EXT_STATUS_NOT_SUPPORTED:
            return "not_supported";
        case MOONCAKE_EXT_STATUS_NO_SPACE:
            return "no_space";
        case MOONCAKE_EXT_STATUS_DISABLED:
            return "disabled";
        default:
            return "unknown";
    }
}

void Load() {
    DfeState& state = State();
    if (!ExtensionsEnabled()) {
        state.state = "disabled";
        state.detail = "MOONCAKE_ENABLE_EXT=0";
        LOG(INFO) << "dfe_provider=disabled reason=not_enabled";
        return;
    }
    const std::string path = ResolveProviderPath(kDfeFileName, nullptr);
    if (path.empty()) {
        state.state = "absent";
        state.detail = kDfeFileName;
        LOG(INFO) << "dfe_provider=absent file=" << kDfeFileName;
        return;
    }
    if (!state.library.Open(path)) {
        state.state = "load_failed";
        state.detail = state.library.error();
        LOG(WARNING) << "dfe_provider=disabled reason=load_failed path=" << path
                     << " detail=" << state.detail;
        return;
    }

    using GetProviderFn = int (*)(mooncake_dfe_ext*);
    auto get_provider = reinterpret_cast<GetProviderFn>(
        state.library.Symbol(kDfeSymbol));
    if (get_provider == nullptr) {
        state.state = "symbol_not_found";
        state.detail = kDfeSymbol;
        LOG(WARNING) << "dfe_provider=disabled reason=symbol_not_found path="
                     << path;
        return;
    }

    state.ext.abi_version = MOONCAKE_DFE_ABI_VERSION;
    state.ext.struct_size = sizeof(state.ext);
    const int rc = get_provider(&state.ext);
    if (rc != MOONCAKE_EXT_STATUS_OK || state.ext.io_batch == nullptr) {
        state.state = "abi_mismatch";
        state.detail = StatusName(rc);
        LOG(WARNING) << "dfe_provider=disabled reason=abi_mismatch path=" << path
                     << " status=" << state.detail;
        return;
    }

    state.loaded = true;
    state.state = "loaded";
    state.detail = path;
    LOG(INFO) << "dfe_provider=loaded path=" << path;
}

}  // namespace

DfeProvider& DfeProvider::Instance() {
    static DfeProvider* instance = new DfeProvider();
    return *instance;
}

DfeProvider::DfeProvider() {
    static std::once_flag once;
    std::call_once(once, Load);
    const DfeState& state = State();
    if (state.loaded && state.ext.io_batch != nullptr) {
        ext_ = &state.ext;
    }
    state_ = state.state;
    detail_ = state.detail;
}

bool DfeProvider::IoBatch(const mooncake_ext_io_request* requests,
                          mooncake_ext_io_result* results, size_t count) const {
    if (ext_ == nullptr || ext_->io_batch == nullptr) return false;
    if (count != 0 && (requests == nullptr || results == nullptr)) return false;
    if (count == 0) return true;
    return ext_->io_batch(ext_->user_data, requests, results, count) ==
           MOONCAKE_EXT_STATUS_OK;
}

bool DfeProvider::AlignBatchOffset(uint64_t append_offset, uint64_t capacity,
                                   uint64_t object_size, uint64_t* out_offset,
                                   uint64_t* out_alignment, bool* fits) const {
    if (ext_ == nullptr || ext_->align_batch_offset == nullptr ||
        out_offset == nullptr || out_alignment == nullptr || fits == nullptr) {
        return false;
    }
    uint32_t raw_fits = 0;
    if (ext_->align_batch_offset(ext_->user_data, append_offset, capacity,
                                 object_size, out_offset, out_alignment,
                                 &raw_fits) != MOONCAKE_EXT_STATUS_OK) {
        return false;
    }
    *fits = raw_fits != 0;
    return true;
}

int DfeProvider::ConfigureWriteFd(int fd, uint64_t max_write_bytes) const {
    if (ext_ == nullptr || ext_->configure_write_fd == nullptr) {
        return MOONCAKE_EXT_STATUS_UNAVAILABLE;
    }
    return ext_->configure_write_fd(ext_->user_data, fd, max_write_bytes);
}

int DfeProvider::RefreshFileMtime(int fd) const {
    if (ext_ == nullptr || ext_->refresh_file_mtime == nullptr) {
        return MOONCAKE_EXT_STATUS_UNAVAILABLE;
    }
    return ext_->refresh_file_mtime(ext_->user_data, fd);
}

bool DfeProvider::BufferPolicy(const mooncake_ext_buffer_policy_query& query,
                               mooncake_ext_buffer_policy* result) const {
    if (ext_ == nullptr || ext_->buffer_policy == nullptr ||
        result == nullptr) {
        return false;
    }
    return ext_->buffer_policy(ext_->user_data, &query, result) ==
           MOONCAKE_EXT_STATUS_OK;
}

}  // namespace mooncake::ext
