#include "ext/shared_library.h"

#include <cstdlib>
#include <cstring>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#include <unistd.h>
#endif

#include "bool_parser.h"

namespace mooncake::ext {
namespace {

constexpr char kDefaultProviderDir[] = "/opt/mooncake/plugins";

bool ExistingFile(const std::string& path) {
#ifdef _WIN32
    return GetFileAttributesA(path.c_str()) != INVALID_FILE_ATTRIBUTES;
#else
    return ::access(path.c_str(), R_OK) == 0;
#endif
}

std::string Join(const std::string& dir, const char* file_name) {
    if (dir.empty()) return {};
    if (dir.back() == '/') return dir + file_name;
    return dir + "/" + file_name;
}

}  // namespace

SharedLibrary::~SharedLibrary() {
    // Deliberately no close: see the class comment.
    handle_ = nullptr;
}

bool SharedLibrary::Open(const std::string& path) {
    if (path.empty()) {
        error_ = "empty_path";
        return false;
    }
#ifdef _WIN32
    // The private providers are ELF shared objects; there is no Windows build.
    error_ = "unsupported_platform";
    return false;
#else
    void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL
#ifdef RTLD_NODELETE
                                              | RTLD_NODELETE
#endif
    );
    if (handle == nullptr) {
        const char* detail = dlerror();
        error_ = detail != nullptr ? detail : "dlopen_failed";
        return false;
    }
    handle_ = handle;
    path_ = path;
    error_.clear();
    return true;
#endif
}

void* SharedLibrary::Symbol(const char* name) const {
    if (handle_ == nullptr || name == nullptr) return nullptr;
#ifdef _WIN32
    return nullptr;
#else
    return dlsym(handle_, name);
#endif
}

std::string ResolveProviderPath(const char* file_name,
                                const char* default_dir) {
    const char* dir = std::getenv("MOONCAKE_EXT_DIR");
    if (dir != nullptr && dir[0] != '\0') {
        const std::string candidate = Join(dir, file_name);
        if (ExistingFile(candidate)) return candidate;
    }
    const char* base =
        default_dir != nullptr ? default_dir : kDefaultProviderDir;
    const std::string candidate = Join(base, file_name);
    if (ExistingFile(candidate)) return candidate;
    return {};
}

bool ExtensionsEnabled() {
    const char* value = std::getenv("MOONCAKE_ENABLE_EXT");
    if (value == nullptr || value[0] == '\0') return true;
    // Same rules as every other boolean in the tree: 0/false/no/off/disable
    // turn extensions off regardless of case or surrounding whitespace.
    return TryParseBool(value).value_or(true);
}

}  // namespace mooncake::ext
