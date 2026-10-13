// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "read_layout.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "pyclient.h"

namespace mooncake {
namespace {
size_t CheckedAdd(size_t a, size_t b) {
    if (b > std::numeric_limits<size_t>::max() - a)
        throw std::overflow_error("read layout address/size overflow");
    return a + b;
}

size_t CheckedAddress(const ReadLayoutComponent& c, size_t row) {
    if (c.destination_stride &&
        row > std::numeric_limits<size_t>::max() / c.destination_stride)
        throw std::overflow_error("read layout row/stride overflow");
    const size_t address =
        CheckedAdd(c.destination_base, row * c.destination_stride);
    CheckedAdd(address, c.byte_count);
    if (address == 0)
        throw std::invalid_argument("read layout has a null destination");
    return address;
}
}  // namespace

ReadLayoutComponent::ReadLayoutComponent(size_t base, size_t stride,
                                         size_t bytes, size_t offset)
    : destination_base(base),
      destination_stride(stride),
      byte_count(bytes),
      source_offset(offset) {
    CheckedAdd(base, bytes);
    CheckedAdd(offset, bytes);
}

ReadLayoutDescriptor::ReadLayoutDescriptor(
    std::vector<std::string> input_keys, std::vector<size_t> input_rows,
    ReadKeyMapping mapping, std::vector<ReadLayoutComponent> input_components)
    : keys(std::move(input_keys)),
      rows(std::move(input_rows)),
      key_mapping(mapping),
      components(std::move(input_components)) {
    if (mapping != ReadKeyMapping::PER_ROW &&
        mapping != ReadKeyMapping::PER_ROW_COMPONENT)
        throw std::invalid_argument("invalid read key mapping");
    if (components.empty()) return;
    const size_t width =
        mapping == ReadKeyMapping::PER_ROW ? 1 : components.size();
    if (rows.size() > std::numeric_limits<size_t>::max() / width)
        throw std::overflow_error("read layout key count overflow");
    if (keys.size() != rows.size() * width)
        throw std::invalid_argument("read layout key/row/component mismatch");

    size_t row_bytes = 0;
    const auto bounds = std::minmax_element(rows.begin(), rows.end());
    for (const auto& component : components) {
        // The existing PyClient range API returns an int for each key.
        const size_t bytes = mapping == ReadKeyMapping::PER_ROW
                                 ? CheckedAdd(row_bytes, component.byte_count)
                                 : component.byte_count;
        if (bytes > static_cast<size_t>(std::numeric_limits<int>::max()))
            throw std::overflow_error(
                "read layout per-key size exceeds INT_MAX");
        if (mapping == ReadKeyMapping::PER_ROW) row_bytes = bytes;
        if (!rows.empty()) {
            CheckedAddress(component, *bounds.first);
            CheckedAddress(component, *bounds.second);
        }
    }
}

std::vector<std::vector<int>> ReadIntoLayouts(
    PyClient& client,
    const std::vector<std::shared_ptr<ReadLayoutDescriptor>>& layouts) {
    size_t count = 0;
    for (const auto& layout : layouts) {
        if (!layout) throw std::invalid_argument("null read layout");
        if (!layout->components.empty())
            count = CheckedAdd(count, layout->keys.size());
    }
    std::vector<std::vector<int>> layout_results(layouts.size());
    if (!count) return layout_results;

    std::vector<std::string> keys;
    std::vector<std::vector<void*>> addresses;
    std::vector<std::vector<size_t>> sizes, offsets;
    keys.reserve(count);
    addresses.reserve(count);
    sizes.reserve(count);
    offsets.reserve(count);
    for (const auto& layout : layouts) {
        if (layout->components.empty()) continue;
        keys.insert(keys.end(), layout->keys.begin(), layout->keys.end());
        const bool per_row = layout->key_mapping == ReadKeyMapping::PER_ROW;
        for (size_t row : layout->rows) {
            if (per_row) {
                addresses.emplace_back();
                sizes.emplace_back();
                offsets.emplace_back();
                addresses.back().reserve(layout->components.size());
                sizes.back().reserve(layout->components.size());
                offsets.back().reserve(layout->components.size());
            }
            for (const auto& component : layout->components) {
                // Bounds were checked for the immutable row extrema at
                // creation.
                auto* address =
                    reinterpret_cast<void*>(component.destination_base +
                                            row * component.destination_stride);
                if (per_row) {
                    addresses.back().push_back(address);
                    sizes.back().push_back(component.byte_count);
                    offsets.back().push_back(component.source_offset);
                } else {
                    addresses.push_back({address});
                    sizes.push_back({component.byte_count});
                    offsets.push_back({component.source_offset});
                }
            }
        }
    }
    // Preserve duplicate keys and explicit zero-byte entries. The backend must
    // still check sessions, leases, and source bounds for these entries.
    const auto results = client.batch_get_into_multi_buffer_ranges(
        keys, addresses, sizes, offsets);
    if (results.size() != count)
        throw std::runtime_error(
            "read layout result count mismatch: expected=" +
            std::to_string(count) +
            " actual=" + std::to_string(results.size()));
    size_t position = 0;
    for (size_t i = 0; i < layouts.size(); ++i) {
        const auto& layout = *layouts[i];
        if (layout.components.empty()) continue;
        const size_t end = position + layout.keys.size();
        layout_results[i].assign(results.begin() + position,
                                 results.begin() + end);
        position = end;
    }
    return layout_results;
}
}  // namespace mooncake
