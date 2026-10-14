#include "crc_checksum.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <string_view>
#include <vector>

namespace mooncake {

TEST(CrcChecksumTest, MatchesCrc64EcmaKnownVector) {
    constexpr std::string_view value = "123456789";
    EXPECT_EQ(ComputeCrcChecksum(value.data(), value.size()),
              0x6C40DF5F0B497347ULL);
}

TEST(CrcChecksumTest, StreamingMatchesContiguousForArbitraryLengths) {
    const std::array<uint8_t, 17> value = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
                                           0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
                                           0x0C, 0x0D, 0x0E, 0x0F, 0x10};

    CrcChecksum streaming;
    streaming.Update(value.data(), 3);
    streaming.Update(value.data() + 3, 7);
    streaming.Update(value.data() + 10, value.size() - 10);

    EXPECT_EQ(streaming.Finalize(),
              ComputeCrcChecksum(value.data(), value.size()));
    EXPECT_EQ(ComputeCrcChecksum(nullptr, 0), 0);
}

TEST(CrcChecksumTest, StagedSlicesMatchChunkedUnalignedInput) {
    std::vector<uint8_t> source(4099);
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<uint8_t>(i * 37);
    }
    const auto* data = source.data() + 1;
    constexpr size_t lengths[] = {3, 0, 1025, 3070};
    CrcChecksum original;
    CrcChecksum staged;
    size_t offset = 0;
    for (const auto length : lengths) {
        const std::vector<uint8_t> host(data + offset, data + offset + length);
        staged.Update(host.data(), host.size());
        for (size_t chunk = 0; chunk < length; chunk += 17) {
            original.Update(data + offset + chunk,
                            std::min(size_t{17}, length - chunk));
        }
        offset += length;
    }
    EXPECT_EQ(original.Finalize(), staged.Finalize());
    EXPECT_EQ(staged.Finalize(), ComputeCrcChecksum(data, offset));
}

}  // namespace mooncake
