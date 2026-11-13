#pragma once

#include "storage/distributed/immutable_bucket_allocator.h"

namespace mooncake::test {
class ImmutableBucketAllocatorTestPeer {
   public:
    static void ExpireDeletions(ImmutableBucketAllocator& allocator) {
        std::lock_guard<std::mutex> lock(allocator.mutex_);
        for (auto& [id, bucket] : allocator.buckets_) {
            if (bucket->evicting && bucket->delete_after) {
                bucket->delete_after = std::chrono::system_clock::time_point{};
            }
        }
    }
};
}  // namespace mooncake::test
