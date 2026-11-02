// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Unit tests for the fork-state rollback used on MR unregister
// (Mooncake#3639). No RDMA device is needed: the interesting behaviour is a
// property of the mapping, which is observable through /proc/self/smaps
// ("VmFlags: ... dc" marks VM_DONTCOPY).

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cuda_alike.h"
#include "transport/rdma_transport/fork_state.h"

namespace mooncake {
namespace {

size_t pageSize() { return static_cast<size_t>(sysconf(_SC_PAGESIZE)); }

// Whether the VMA covering `addr` carries the given smaps flag token (e.g.
// "dc" for VM_DONTCOPY).
bool vmaHasFlag(const void *addr, const std::string &wanted) {
    std::ifstream smaps("/proc/self/smaps");
    if (!smaps) return false;
    const uintptr_t target = reinterpret_cast<uintptr_t>(addr);
    bool in_range = false;
    std::string line;
    while (std::getline(smaps, line)) {
        unsigned long lo = 0;
        unsigned long hi = 0;
        if (std::sscanf(line.c_str(), "%lx-%lx", &lo, &hi) == 2) {
            in_range = target >= lo && target < hi;
            continue;
        }
        if (!in_range || line.compare(0, 8, "VmFlags:") != 0) continue;
        std::istringstream flags(line.substr(8));
        std::string token;
        while (flags >> token) {
            if (token == wanted) return true;
        }
        return false;
    }
    return false;
}

class RdmaForkStateTest : public ::testing::Test {
   protected:
    void SetUp() override { page_ = pageSize(); }

    // A dirty anonymous mapping of `pages` pages, released by TearDown().
    void *MapPages(size_t pages) {
        size_t length = pages * page_;
        void *addr = mmap(nullptr, length, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (addr == MAP_FAILED) return nullptr;
        std::memset(addr, 0x5a, length);
        mappings_.push_back({addr, length});
        return addr;
    }

    void TearDown() override {
        for (auto &mapping : mappings_) munmap(mapping.first, mapping.second);
        mappings_.clear();
    }

    size_t page_ = 0;
    std::vector<std::pair<void *, size_t>> mappings_;
};

// ---------------------------------------------------------------------------
// Range rounding
// ---------------------------------------------------------------------------

TEST_F(RdmaForkStateTest, AlignForkRangeKeepsAlignedRange) {
    auto base = reinterpret_cast<uintptr_t>(MapPages(4));
    ASSERT_NE(base, 0u);

    uintptr_t start = 0;
    uintptr_t end = 0;
    alignForkRange(reinterpret_cast<void *>(base), 3 * page_, start, end);

    EXPECT_EQ(start, base);
    EXPECT_EQ(end, base + 3 * page_);
}

TEST_F(RdmaForkStateTest, AlignForkRangeWidensInteriorPointer) {
    auto base = reinterpret_cast<uintptr_t>(MapPages(4));
    ASSERT_NE(base, 0u);

    uintptr_t start = 0;
    uintptr_t end = 0;
    // A one byte registration at page 2 covers (at least) that whole page.
    alignForkRange(reinterpret_cast<void *>(base + 2 * page_ + 1), 1, start,
                   end);

    EXPECT_EQ(start, base + 2 * page_);
    EXPECT_EQ(end, base + 3 * page_);
}

TEST_F(RdmaForkStateTest, AlignForkRangeWidensUnAlignedTail) {
    auto base = reinterpret_cast<uintptr_t>(MapPages(4));
    ASSERT_NE(base, 0u);

    uintptr_t start = 0;
    uintptr_t end = 0;
    // 1.5 pages from an aligned start must cover two pages.
    alignForkRange(reinterpret_cast<void *>(base), page_ + page_ / 2, start,
                   end);

    EXPECT_EQ(start, base);
    EXPECT_EQ(end, base + 2 * page_);
}

TEST_F(RdmaForkStateTest, AlignForkRangeRejectsEmptyInput) {
    uintptr_t start = 123;
    uintptr_t end = 456;

    alignForkRange(nullptr, page_, start, end);
    EXPECT_EQ(start, 0u);
    EXPECT_EQ(end, 0u);

    auto base = reinterpret_cast<uintptr_t>(MapPages(1));
    ASSERT_NE(base, 0u);
    alignForkRange(reinterpret_cast<void *>(base), 0, start, end);
    EXPECT_EQ(start, 0u);
    EXPECT_EQ(end, 0u);
}

// ---------------------------------------------------------------------------
// Eligibility: only a plain ibv_reg_mr() registration of a host mapping can
// carry rdma-core's MADV_DONTFORK state.
// ---------------------------------------------------------------------------

TEST_F(RdmaForkStateTest, ForkStateShouldBeRestoredTruthTable) {
    // registered_by_ibv_reg_mr, export_reports_host_vma, host_backed
    EXPECT_TRUE(forkStateShouldBeRestored(true, true, true));

    // dma-buf import: libibverbs never ran its DONTFORK bookkeeping.
    EXPECT_FALSE(forkStateShouldBeRestored(false, true, true));
    // the export path already classified the buffer as device memory.
    EXPECT_FALSE(forkStateShouldBeRestored(true, false, true));
    // the runtime probe says the range is not a host mapping (device iova).
    EXPECT_FALSE(forkStateShouldBeRestored(true, true, false));
    EXPECT_FALSE(forkStateShouldBeRestored(false, false, false));
    EXPECT_FALSE(forkStateShouldBeRestored(true, false, false));
    EXPECT_FALSE(forkStateShouldBeRestored(false, true, false));
    EXPECT_FALSE(forkStateShouldBeRestored(false, false, true));
}

// ---------------------------------------------------------------------------
// Runtime memory classification
// ---------------------------------------------------------------------------

TEST_F(RdmaForkStateTest, HostMappingIsHostBacked) {
    void *mapped = MapPages(2);
    ASSERT_NE(mapped, nullptr);
    EXPECT_TRUE(isHostBackedRange(mapped));

    void *heap = std::malloc(4 * page_);
    ASSERT_NE(heap, nullptr);
    EXPECT_TRUE(isHostBackedRange(heap));
    std::free(heap);
}

TEST_F(RdmaForkStateTest, DeviceMemoryIsNotHostBacked) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA) || defined(USE_SUNRISE)
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0)
        GTEST_SKIP() << "no accelerator present";
    void *device_buffer = nullptr;
    ASSERT_EQ(cudaMalloc(&device_buffer, 1UL << 20), cudaSuccess);
    // A device iova has no host VMA behind it, so unregistering it must not
    // issue madvise(MADV_DOFORK) -- that is the noisy case from #3639.
    EXPECT_FALSE(isHostBackedRange(device_buffer));
    cudaFree(device_buffer);
#else
    GTEST_SKIP() << "accelerator support is not compiled in";
#endif
}

// ---------------------------------------------------------------------------
// The rollback itself: MADV_DOFORK must clear VM_DONTCOPY, including when the
// caller registered an interior pointer (the range is widened first).
// ---------------------------------------------------------------------------

TEST_F(RdmaForkStateTest, RestoreForkStateClearsDontFork) {
    void *mapped = MapPages(4);
    ASSERT_NE(mapped, nullptr);
    ASSERT_EQ(madvise(mapped, page_, MADV_DONTFORK), 0);
    ASSERT_TRUE(vmaHasFlag(mapped, "dc")) << "MADV_DONTFORK did not take effect";

    restoreForkState(mapped, page_);

    EXPECT_FALSE(vmaHasFlag(mapped, "dc"));
}

TEST_F(RdmaForkStateTest, RestoreForkStateClearsDontForkForInteriorPointer) {
    void *mapped = MapPages(4);
    ASSERT_NE(mapped, nullptr);
    auto *page = static_cast<char *>(mapped) + page_;
    ASSERT_EQ(madvise(page, page_, MADV_DONTFORK), 0);
    ASSERT_TRUE(vmaHasFlag(page, "dc"));

    // Registered at an interior, unaligned address inside that page: the call
    // has to widen the range, otherwise madvise() fails with EINVAL and the VMA
    // stays split.
    restoreForkState(page + 128, page_ / 2);

    EXPECT_FALSE(vmaHasFlag(page, "dc"));
}

TEST_F(RdmaForkStateTest, RestoreForkStateIgnoresEmptyRange) {
    void *mapped = MapPages(1);
    ASSERT_NE(mapped, nullptr);
    ASSERT_EQ(madvise(mapped, page_, MADV_DONTFORK), 0);

    restoreForkState(nullptr, page_);
    restoreForkState(mapped, 0);

    // Nothing was restored yet.
    EXPECT_TRUE(vmaHasFlag(mapped, "dc"));
    restoreForkState(mapped, page_);
    EXPECT_FALSE(vmaHasFlag(mapped, "dc"));
}

}  // namespace
}  // namespace mooncake

