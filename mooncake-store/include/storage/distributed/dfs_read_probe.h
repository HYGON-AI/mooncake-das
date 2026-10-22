#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace mooncake {

inline constexpr std::array<std::string_view, 2>
    kDfsReadProbeDirectoryNames = {".health1", ".health2"};
inline constexpr char kDfsReadProbeFileName[] = ".mooncake_dfs_read_probe";
inline constexpr size_t kDfsReadProbeSize = 1U << 20;
inline constexpr size_t kDfsReadProbeBlockSize = 4096;
inline constexpr size_t kDfsReadProbeBlockCount =
    kDfsReadProbeSize / kDfsReadProbeBlockSize;

// Stable across processes, standard-library versions and restarts. The root
// identity makes a probe from another cluster fail validation, while the probe
// and block indexes give both files distinct deterministic payloads.
inline uint64_t DfsReadProbeSeed(std::string_view identity, size_t probe_index,
                                 size_t block_index) {
    uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char byte : identity) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    hash ^= static_cast<uint64_t>(probe_index);
    hash *= 1099511628211ULL;
    hash ^= static_cast<uint64_t>(block_index);
    hash *= 1099511628211ULL;
    return hash == 0 ? 0x9e3779b97f4a7c15ULL : hash;
}

inline void FillDfsReadProbeBlock(std::string_view identity,
                                  size_t probe_index, size_t block_index,
                                  char* destination) {
    uint64_t state = DfsReadProbeSeed(identity, probe_index, block_index);
    for (size_t offset = 0; offset < kDfsReadProbeBlockSize; ++offset) {
        // xorshift64*; deterministic generation, not cryptographic randomness.
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        destination[offset] = static_cast<char>(
            (state * 2685821657736338717ULL) >> 56);
    }
}

inline std::string BuildDfsReadProbeContents(std::string_view identity,
                                             size_t probe_index) {
    std::string contents(kDfsReadProbeSize, '\0');
    for (size_t block = 0; block < kDfsReadProbeBlockCount; ++block) {
        FillDfsReadProbeBlock(identity, probe_index, block,
                              contents.data() + block * kDfsReadProbeBlockSize);
    }
    return contents;
}

}  // namespace mooncake
