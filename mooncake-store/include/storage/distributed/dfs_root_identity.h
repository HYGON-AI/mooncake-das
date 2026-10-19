#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "types.h"

namespace mooncake {

inline constexpr char kDfsRootIdentityFileName[] =
    ".mooncake_dfs_root_identity";
inline constexpr std::string_view kDfsRootIdentityMagic =
    "mooncake-dfs-root-identity-v1";
inline constexpr size_t kMaxDfsRootIdentitySize = 256;

inline std::string SerializeDfsRootIdentity(const std::string& cluster_id,
                                            const UUID& token) {
    if (!IsValidClusterIdComponent(cluster_id) || token == UUID{}) {
        return {};
    }
    return std::string(kDfsRootIdentityMagic) + "\n" + cluster_id + "\n" +
           UuidToString(token) + "\n";
}

inline bool ParseDfsRootIdentity(std::string_view contents,
                                 const std::string& expected_cluster_id,
                                 std::string& identity) {
    identity.clear();
    if (contents.empty() || contents.size() > kMaxDfsRootIdentitySize ||
        !IsValidClusterIdComponent(expected_cluster_id)) {
        return false;
    }

    const std::string magic_prefix = std::string(kDfsRootIdentityMagic) + "\n";
    if (!contents.starts_with(magic_prefix)) return false;
    contents.remove_prefix(magic_prefix.size());

    const size_t cluster_end = contents.find('\n');
    if (cluster_end == std::string_view::npos ||
        contents.substr(0, cluster_end) != expected_cluster_id) {
        return false;
    }
    contents.remove_prefix(cluster_end + 1);

    if (contents.empty() || contents.back() != '\n') return false;
    contents.remove_suffix(1);

    UUID token{};
    if (!StringToUuid(std::string(contents), token) || token == UUID{}) {
        return false;
    }
    identity = SerializeDfsRootIdentity(expected_cluster_id, token);
    return !identity.empty() && identity ==
                                      std::string(magic_prefix) +
                                          expected_cluster_id + "\n" +
                                          std::string(contents) + "\n";
}

}  // namespace mooncake
