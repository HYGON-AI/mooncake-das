#pragma once

#include "storage/distributed/immutable_bucket_allocator.h"

namespace mooncake::test {
class ImmutableBucketAllocatorTestPeer {
   public:
    static auto& DeletionMutex(ImmutableBucketAllocator& allocator) {
        return allocator.metadata_io_mutex_;
    }

    static void SetAdapter(ImmutableBucketAllocator& allocator,
                           std::unique_ptr<FileSystemAdapter> adapter) {
        allocator.fs_adapter_ = std::move(adapter);
    }

    static auto RetryState(ImmutableBucketAllocator& allocator, int64_t id) {
        std::lock_guard<std::mutex> lock(allocator.mutex_);
        const auto& state = *allocator.buckets_.at(id);
        return std::pair{state.delete_failures, state.delete_retry_after};
    }

    static void PostponeRetry(ImmutableBucketAllocator& allocator, int64_t id) {
        std::lock_guard<std::mutex> lock(allocator.mutex_);
        allocator.buckets_.at(id)->delete_retry_after =
            std::chrono::steady_clock::now() + std::chrono::hours(1);
    }

    static ImmutableBucketAllocator::EvictedBucket DeletionToken(
        ImmutableBucketAllocator& allocator, int64_t id) {
        std::lock_guard<std::mutex> lock(allocator.mutex_);
        ImmutableBucketAllocator::EvictedBucket token;
        token.bucket_id_ = id;
        token.identity_ = allocator.buckets_.at(id);
        return token;
    }

    static void ExpireDeletions(ImmutableBucketAllocator& allocator) {
        std::lock_guard<std::mutex> lock(allocator.mutex_);
        for (auto& [id, bucket] : allocator.buckets_) {
            if (bucket->evicting && bucket->delete_after) {
                bucket->delete_after = std::chrono::system_clock::time_point{};
                bucket->delete_retry_after =
                    std::chrono::steady_clock::time_point{};
            }
        }
    }
};
}  // namespace mooncake::test
