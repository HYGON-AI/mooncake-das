#pragma once

#include "real_client.h"

namespace mooncake::l4 {

uint64_t NextDfsReadTraceId();

std::string DirectSourceForReplica(const Replica::Descriptor &replica);

bool CompatibleSessionCacheRefresh(
    const QueryResult &old_session, const QueryResult &fresh_session);

bool SameGetSessionSnapshot(const QueryResult &lhs, const QueryResult &rhs);

bool AlignUp(size_t value, size_t alignment, size_t *aligned);

void DetachPrefetchedSessionBuffer(
    std::unordered_map<std::string, PrefetchedSessionBuffer> &cache,
    const std::string &key,
    std::vector<std::shared_ptr<BufferHandle>> &detached_buffers);

QueryResult FilterQueryResult(
    const QueryResult &qr, const Replica::Descriptor &replica,
    bool include_object_checksum = true);

const Replica::Descriptor *SelectSessionReplica(
    const std::vector<Replica::Descriptor> &replicas,
    const std::unordered_set<std::string> &local_endpoints, bool allow_dfs);

}  // namespace mooncake::l4
