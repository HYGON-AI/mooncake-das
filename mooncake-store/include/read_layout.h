// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace mooncake {
class PyClient;

enum class ReadKeyMapping { PER_ROW, PER_ROW_COMPONENT };

struct ReadLayoutComponent {
    const size_t destination_base;
    const size_t destination_stride;
    const size_t byte_count;
    const size_t source_offset;

    ReadLayoutComponent(size_t base, size_t stride, size_t bytes,
                        size_t offset);
};

// Immutable snapshots: validated arithmetic remains valid throughout a read.
// Selected destination addresses must be nonzero, even for zero-byte entries.
// Addresses do not own memory. The caller owns registration, session lifetime,
// nonoverlapping destinations, and synchronization with consumers/client close.
class ReadLayoutDescriptor {
   public:
    const std::vector<std::string> keys;
    const std::vector<size_t> rows;
    const ReadKeyMapping key_mapping;
    const std::vector<ReadLayoutComponent> components;

    ReadLayoutDescriptor(std::vector<std::string> keys,
                         std::vector<size_t> rows, ReadKeyMapping mapping,
                         std::vector<ReadLayoutComponent> components);
};

// Synchronous adapter to the existing range path. Returns per-layout, per-key
// byte counts or negative error codes, preserving empty/skipped layouts.
// Failure may partially modify destinations; completion follows the backend.
std::vector<std::vector<int>> ReadIntoLayouts(
    PyClient& client,
    const std::vector<std::shared_ptr<ReadLayoutDescriptor>>& layouts);
}  // namespace mooncake
