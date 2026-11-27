#include "ext/ahe_loader.h"

#include <glog/logging.h>

#include <mutex>

#include "ext/shared_library.h"

namespace mooncake::ext {
namespace {

constexpr char kAheFileName[] = "libmooncake_ahe.so.1";
constexpr char kAheSymbol[] = "mooncake_ahe_extension_get";

struct AheState {
    SharedLibrary library;
    mooncake_ahe_ext ext{};
    bool loaded = false;
    std::string state = "unloaded";
    std::string detail;
};

// Intentionally leaked for the same reason as the DFE state: the provider owns
// HIP runtime state that must not be torn down by static destructors.
AheState& State() {
    static AheState* state = new AheState();
    return *state;
}

void Load() {
    AheState& state = State();
    if (!ExtensionsEnabled()) {
        state.state = "disabled";
        state.detail = "MOONCAKE_ENABLE_EXT=0";
        LOG(INFO) << "ahe_provider=disabled reason=not_enabled";
        return;
    }
    const std::string path = ResolveProviderPath(kAheFileName, nullptr);
    if (path.empty()) {
        state.state = "absent";
        state.detail = kAheFileName;
        LOG(INFO) << "ahe_provider=absent file=" << kAheFileName;
        return;
    }
    if (!state.library.Open(path)) {
        state.state = "load_failed";
        state.detail = state.library.error();
        LOG(WARNING) << "ahe_provider=disabled reason=load_failed path=" << path
                     << " detail=" << state.detail;
        return;
    }

    using GetProviderFn = int (*)(mooncake_ahe_ext*);
    auto get_provider = reinterpret_cast<GetProviderFn>(
        state.library.Symbol(kAheSymbol));
    if (get_provider == nullptr) {
        state.state = "symbol_not_found";
        state.detail = kAheSymbol;
        LOG(WARNING) << "ahe_provider=disabled reason=symbol_not_found path="
                     << path;
        return;
    }

    state.ext.abi_version = MOONCAKE_AHE_ABI_VERSION;
    state.ext.struct_size = sizeof(state.ext);
    const int rc = get_provider(&state.ext);
    if (rc != MOONCAKE_EXT_STATUS_OK || state.ext.create == nullptr ||
        state.ext.submit == nullptr || state.ext.sync == nullptr ||
        state.ext.destroy == nullptr) {
        state.state = "abi_mismatch";
        state.detail = std::to_string(rc);
        LOG(WARNING) << "ahe_provider=disabled reason=abi_mismatch path=" << path
                     << " status=" << state.detail;
        return;
    }

    state.loaded = true;
    state.state = "loaded";
    state.detail = path;
    LOG(INFO) << "ahe_provider=loaded path=" << path;
}

}  // namespace

AheProvider& AheProvider::Instance() {
    static AheProvider* instance = new AheProvider();
    return *instance;
}

AheProvider::AheProvider() {
    static std::once_flag once;
    std::call_once(once, Load);
    const AheState& state = State();
    if (state.loaded && state.ext.create != nullptr) {
        ext_ = &state.ext;
    }
    state_ = state.state;
    detail_ = state.detail;
}

void* AheProvider::Create(int device_ordinal, uint32_t stream_count) {
    if (ext_ == nullptr || ext_->create == nullptr) return nullptr;
    return ext_->create(ext_->user_data, device_ordinal, stream_count);
}

bool AheProvider::Submit(void* handle, const mooncake_ahe_copy_range* ranges,
                         size_t range_count, uint32_t stream_key) {
    if (ext_ == nullptr || ext_->submit == nullptr || handle == nullptr) {
        return false;
    }
    return ext_->submit(ext_->user_data, handle, ranges, range_count,
                        stream_key) == MOONCAKE_EXT_STATUS_OK;
}

bool AheProvider::Sync(void* handle) {
    if (ext_ == nullptr || ext_->sync == nullptr || handle == nullptr) {
        return false;
    }
    return ext_->sync(ext_->user_data, handle) == MOONCAKE_EXT_STATUS_OK;
}

void AheProvider::Destroy(void* handle) {
    if (ext_ == nullptr || ext_->destroy == nullptr || handle == nullptr) {
        return;
    }
    ext_->destroy(ext_->user_data, handle);
}

bool AheProvider::AllocMapped(int device_ordinal, uint64_t size,
                              void** host_ptr, void** device_ptr) {
    if (ext_ == nullptr || ext_->alloc_mapped == nullptr ||
        host_ptr == nullptr || device_ptr == nullptr) {
        return false;
    }
    return ext_->alloc_mapped(ext_->user_data, device_ordinal, size, host_ptr,
                              device_ptr) == MOONCAKE_EXT_STATUS_OK;
}

void AheProvider::FreeMapped(void* host_ptr) {
    if (ext_ == nullptr || ext_->free_mapped == nullptr || host_ptr == nullptr) {
        return;
    }
    ext_->free_mapped(ext_->user_data, host_ptr);
}

}  // namespace mooncake::ext
