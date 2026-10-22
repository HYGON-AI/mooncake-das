#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "dfs_write_staging_pool.h"

namespace mooncake::test {

TEST(DfsWriteStagingPoolTest, TimesOutWhenAllSlotsAreBusy) {
    DfsWriteStagingPool pool(1, 1);
    auto first = pool.Acquire(std::chrono::milliseconds(1));
    ASSERT_NE(first, nullptr);

    bool timed_out = false;
    auto second = pool.Acquire(std::chrono::milliseconds(5), &timed_out);

    EXPECT_EQ(second, nullptr);
    EXPECT_TRUE(timed_out);
}

TEST(DfsWriteStagingPoolTest, ShutdownWakesWaitingAcquire) {
    DfsWriteStagingPool pool(1, 1);
    auto first = pool.Acquire(std::chrono::milliseconds(1));
    ASSERT_NE(first, nullptr);

    std::atomic<bool> finished{false};
    std::thread waiter([&] {
        bool timed_out = false;
        auto second = pool.Acquire(std::chrono::seconds(10), &timed_out);
        EXPECT_EQ(second, nullptr);
        EXPECT_FALSE(timed_out);
        finished.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    pool.Shutdown();
    waiter.join();

    EXPECT_TRUE(finished.load(std::memory_order_acquire));
}

TEST(DfsWriteStagingPoolTest, ReturnedSlotCanBeReusedByNextChunk) {
    DfsWriteStagingPool pool(1, 1);
    auto first = pool.Acquire(std::chrono::milliseconds(1));
    ASSERT_NE(first, nullptr);
    char* first_address = first->backing.data;
    pool.Release(std::move(first));

    auto second = pool.Acquire(std::chrono::milliseconds(1));
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second->backing.data, first_address);
}

}  // namespace mooncake::test
