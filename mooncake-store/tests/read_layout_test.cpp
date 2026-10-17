// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "read_layout.h"
#include "dummy_client.h"

#include <gtest/gtest.h>
#include <climits>
#include <limits>
#include <stdexcept>

namespace mooncake {
namespace {
struct LayoutBackend : DummyClient {
    int calls = 0;
    bool short_read = false, short_results = false, extra_results = false;
    int failed_key_index = -1;
    std::vector<std::string> seen;
    std::vector<int> batch_get_into_multi_buffer_ranges(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& buffers,
        const std::vector<std::vector<size_t>>& sizes,
        const std::vector<std::vector<size_t>>& offsets) override {
        ++calls;
        seen = keys;
        std::vector<int> result;
        for (size_t i = 0; i < keys.size(); ++i) {
            int total = 0;
            for (size_t j = 0; j < sizes[i].size(); ++j) {
                auto* p = static_cast<unsigned char*>(buffers[i][j]);
                for (size_t k = 0; k < sizes[i][j]; ++k)
                    p[k] = keys[i][0] + offsets[i][j] + k;
                total += sizes[i][j];
            }
            result.push_back(
                i == static_cast<size_t>(failed_key_index)
                    ? static_cast<int>(toInt(ErrorCode::INVALID_PARAMS))
                : short_read ? total - 1
                             : total);
        }
        if (short_results) result.pop_back();
        if (extra_results) result.push_back(0);
        return result;
    }
};

TEST(ReadLayout, ReorderedDuplicateKeysAndComponents) {
    unsigned char dst[64] = {};
    const auto base = reinterpret_cast<size_t>(dst);
    auto layout = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"a", "a"}, std::vector<size_t>{7, 2},
        ReadKeyMapping::PER_ROW,
        std::vector<ReadLayoutComponent>{{base, 8, 4, 0}, {base + 4, 8, 4, 4}});
    LayoutBackend backend;
    EXPECT_EQ(ReadIntoLayouts(backend, {layout}),
              (std::vector<std::vector<int>>{{8, 8}}));
    EXPECT_EQ(backend.seen, (std::vector<std::string>{"a", "a"}));
    for (size_t row : {7, 2})
        for (size_t j = 0; j < 8; ++j) EXPECT_EQ(dst[row * 8 + j], 'a' + j);
    EXPECT_EQ(dst[0], 0);
}

TEST(ReadLayout, SeparateKeysAndMultipleLayouts) {
    unsigned char dst[32] = {};
    const auto base = reinterpret_cast<size_t>(dst);
    auto layout = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"a", "b", "c", "d"}, std::vector<size_t>{1, 0},
        ReadKeyMapping::PER_ROW_COMPONENT,
        std::vector<ReadLayoutComponent>{{base, 8, 4, 0}, {base + 4, 8, 4, 1}});
    auto other = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"a"}, std::vector<size_t>{3},
        ReadKeyMapping::PER_ROW,
        std::vector<ReadLayoutComponent>{{base, 8, 8, 0}});
    LayoutBackend backend;
    EXPECT_EQ(ReadIntoLayouts(backend, {layout, other}),
              (std::vector<std::vector<int>>{{4, 4, 4, 4}, {8}}));
    EXPECT_EQ(backend.calls, 1);
    EXPECT_EQ(dst[8], 'a');
    EXPECT_EQ(dst[12], 'b' + 1);
    EXPECT_EQ(dst[0], 'c');
    EXPECT_EQ(dst[4], 'd' + 1);
    EXPECT_EQ(dst[24], 'a');
}

TEST(ReadLayout, EmptyAndZeroByteEntries) {
    LayoutBackend backend;
    auto empty = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"unused"}, std::vector<size_t>{},
        ReadKeyMapping::PER_ROW, std::vector<ReadLayoutComponent>{});
    EXPECT_EQ(ReadIntoLayouts(backend, {}), (std::vector<std::vector<int>>{}));
    EXPECT_EQ(ReadIntoLayouts(backend, {empty}),
              (std::vector<std::vector<int>>{{}}));
    EXPECT_EQ(backend.calls, 0);
    unsigned char dst = 0;
    auto zero = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"a"}, std::vector<size_t>{0},
        ReadKeyMapping::PER_ROW,
        std::vector<ReadLayoutComponent>{
            {reinterpret_cast<size_t>(&dst), 0, 0, 0}});
    auto no_rows = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{}, std::vector<size_t>{},
        ReadKeyMapping::PER_ROW, zero->components);
    EXPECT_EQ(ReadIntoLayouts(backend, {empty, zero, no_rows, empty}),
              (std::vector<std::vector<int>>{{}, {0}, {}, {}}));
    EXPECT_EQ(backend.calls, 1);  // Preserve backend session validation.
    EXPECT_EQ(dst, 0);
}

TEST(ReadLayout, InvalidArgumentsBeforeAnyRead) {
    LayoutBackend backend;
    EXPECT_THROW(ReadIntoLayouts(backend, {nullptr}), std::invalid_argument);
    for (auto mapping :
         {ReadKeyMapping::PER_ROW, ReadKeyMapping::PER_ROW_COMPONENT}) {
        EXPECT_THROW(
            (ReadLayoutDescriptor({"a"}, {0}, mapping, {{0, 0, 0, 0}})),
            std::invalid_argument);
    }
    EXPECT_THROW((ReadLayoutDescriptor({"a"}, {0}, ReadKeyMapping::PER_ROW,
                                       {{0, 8, 8, 0}})),
                 std::invalid_argument);
    EXPECT_THROW((ReadLayoutDescriptor({"a"}, {0, 1}, ReadKeyMapping::PER_ROW,
                                       {{64, 8, 8, 0}})),
                 std::invalid_argument);
    const auto max = std::numeric_limits<size_t>::max();
    EXPECT_THROW((ReadLayoutComponent(64, 8, 8, max)), std::overflow_error);
    EXPECT_THROW((ReadLayoutDescriptor({"a"}, {max}, ReadKeyMapping::PER_ROW,
                                       {{64, 8, 8, 0}})),
                 std::overflow_error);
    EXPECT_THROW((ReadLayoutDescriptor({"a"}, {0}, ReadKeyMapping::PER_ROW,
                                       {{64, 0, size_t(INT_MAX) + 1, 0}})),
                 std::overflow_error);
    EXPECT_EQ(backend.calls, 0);
}

TEST(ReadLayout, ShortReadsAndFailuresAreReturned) {
    unsigned char dst[16];
    auto layout = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"a"}, std::vector<size_t>{0},
        ReadKeyMapping::PER_ROW,
        std::vector<ReadLayoutComponent>{
            {reinterpret_cast<size_t>(dst), 8, 8, 0}});
    LayoutBackend backend;
    backend.short_read = true;
    EXPECT_EQ(ReadIntoLayouts(backend, {layout}),
              (std::vector<std::vector<int>>{{7}}));
    backend.short_read = false;
    backend.failed_key_index = 0;
    auto other = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"b"}, std::vector<size_t>{1},
        ReadKeyMapping::PER_ROW_COMPONENT, layout->components);
    EXPECT_EQ(ReadIntoLayouts(backend, {layout, other}),
              (std::vector<std::vector<int>>{
                  {static_cast<int>(toInt(ErrorCode::INVALID_PARAMS))}, {8}}));
    EXPECT_EQ(backend.seen, (std::vector<std::string>{"a", "b"}));
}

TEST(ReadLayout, ResultCountMismatchRaises) {
    unsigned char dst[8];
    auto layout = std::make_shared<ReadLayoutDescriptor>(
        std::vector<std::string>{"a"}, std::vector<size_t>{0},
        ReadKeyMapping::PER_ROW,
        std::vector<ReadLayoutComponent>{
            {reinterpret_cast<size_t>(dst), 8, 8, 0}});
    LayoutBackend backend;
    backend.short_results = true;
    EXPECT_THROW(ReadIntoLayouts(backend, {layout}), std::runtime_error);
    backend.short_results = false;
    backend.extra_results = true;
    EXPECT_THROW(ReadIntoLayouts(backend, {layout}), std::runtime_error);
}
}  // namespace
}  // namespace mooncake
