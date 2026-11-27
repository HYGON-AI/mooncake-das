#pragma once

#include <string>

namespace mooncake::ext {

/**
 * Minimal `dlopen` wrapper used by the extension loaders.
 *
 * The handle is never closed: a provider may own threads and static state, and
 * unloading it while any of that is alive would crash the process. Callers
 * therefore treat a successful load as permanent for the process lifetime.
 */
class SharedLibrary {
   public:
    SharedLibrary() = default;
    ~SharedLibrary();
    SharedLibrary(const SharedLibrary&) = delete;
    SharedLibrary& operator=(const SharedLibrary&) = delete;

    // `path` is used verbatim. Returns false and stores a short reason on
    // failure.
    bool Open(const std::string& path);

    void* Symbol(const char* name) const;

    bool is_open() const { return handle_ != nullptr; }
    const std::string& path() const { return path_; }
    const std::string& error() const { return error_; }

   private:
    void* handle_ = nullptr;
    std::string path_;
    std::string error_;
};

/**
 * Resolve a provider library from, in order:
 *   1. `<MOONCAKE_EXT_DIR>/<file_name>`
 *   2. `<default_dir>/<file_name>`
 *
 * Only those two directories are consulted; there is no per-provider path
 * override. Returns an empty string when every candidate is unusable.
 */
std::string ResolveProviderPath(const char* file_name, const char* default_dir);

/** True unless `MOONCAKE_ENABLE_EXT` is explicitly disabled. */
bool ExtensionsEnabled();

}  // namespace mooncake::ext
