#include "ocs/uds_protocol.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace {

ocs::uds::Frame sampleFrame() {
    return {
        .version = ocs::uds::kProtocolVersion,
        .message_type = ocs::uds::MessageType::kApplyConnections,
        .flags = 0x01020304,
        .request_id = 0x0102030405060708,
        .device_generation = 42,
        .payload = R"({"commands":[{"id":"conn-001"}]})",
    };
}

TEST(UdsCodecTest, RoundTripsFrameAndUsesBigEndianHeader) {
    const auto expected = sampleFrame();

    const auto encoded = ocs::uds::encode(expected);

    ASSERT_TRUE(encoded.ok());
    ASSERT_GE(encoded.bytes.size(), ocs::uds::kHeaderSize);
    EXPECT_EQ(encoded.bytes.at(0), std::byte{0x4F});
    EXPECT_EQ(encoded.bytes.at(1), std::byte{0x43});
    EXPECT_EQ(encoded.bytes.at(2), std::byte{0x53});
    EXPECT_EQ(encoded.bytes.at(3), std::byte{0x31});
    EXPECT_EQ(encoded.bytes.at(12), std::byte{0x01});
    EXPECT_EQ(encoded.bytes.at(19), std::byte{0x08});

    const auto decoded = ocs::uds::decode(encoded.bytes);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded.frame.value(), expected);
}

TEST(UdsCodecTest, RejectsTruncatedHeader) {
    const std::vector<std::byte> bytes(ocs::uds::kHeaderSize - 1);

    const auto decoded = ocs::uds::decode(bytes);

    EXPECT_EQ(decoded.error.code, ocs::ErrorCode::kProtocolMalformed);
    EXPECT_FALSE(decoded.frame.has_value());
}

TEST(UdsCodecTest, RejectsWrongMagic) {
    auto encoded = ocs::uds::encode(sampleFrame());
    ASSERT_TRUE(encoded.ok());
    encoded.bytes.at(0) = std::byte{0};

    EXPECT_EQ(ocs::uds::decode(encoded.bytes).error.code, ocs::ErrorCode::kProtocolMalformed);
}

TEST(UdsCodecTest, RejectsUnsupportedVersion) {
    auto encoded = ocs::uds::encode(sampleFrame());
    ASSERT_TRUE(encoded.ok());
    encoded.bytes.at(4) = std::byte{0};
    encoded.bytes.at(5) = std::byte{2};

    EXPECT_EQ(ocs::uds::decode(encoded.bytes).error.code, ocs::ErrorCode::kProtocolVersion);
}

TEST(UdsCodecTest, RejectsUnknownMessageType) {
    auto encoded = ocs::uds::encode(sampleFrame());
    ASSERT_TRUE(encoded.ok());
    encoded.bytes.at(6) = std::byte{0x7F};
    encoded.bytes.at(7) = std::byte{0xFF};

    EXPECT_EQ(ocs::uds::decode(encoded.bytes).error.code, ocs::ErrorCode::kProtocolMalformed);
}

TEST(UdsCodecTest, RejectsPayloadLengthMismatch) {
    auto encoded = ocs::uds::encode(sampleFrame());
    ASSERT_TRUE(encoded.ok());
    encoded.bytes.at(23) = std::byte{0};

    EXPECT_EQ(ocs::uds::decode(encoded.bytes).error.code, ocs::ErrorCode::kProtocolMalformed);
}

TEST(UdsCodecTest, RejectsOversizedPayloadOnEncode) {
    auto frame = sampleFrame();
    frame.payload.assign(ocs::uds::kMaxPayloadSize + 1, 'x');

    const auto encoded = ocs::uds::encode(frame);

    EXPECT_EQ(encoded.error.code, ocs::ErrorCode::kPayloadTooLarge);
    EXPECT_TRUE(encoded.bytes.empty());
}

TEST(UdsCodecTest, RejectsOversizedDeclaredPayloadBeforeReadingBody) {
    auto encoded = ocs::uds::encode(sampleFrame());
    ASSERT_TRUE(encoded.ok());
    const auto oversized = static_cast<std::uint32_t>(ocs::uds::kMaxPayloadSize + 1);
    encoded.bytes.at(20) = static_cast<std::byte>((oversized >> 24U) & 0xFFU);
    encoded.bytes.at(21) = static_cast<std::byte>((oversized >> 16U) & 0xFFU);
    encoded.bytes.at(22) = static_cast<std::byte>((oversized >> 8U) & 0xFFU);
    encoded.bytes.at(23) = static_cast<std::byte>(oversized & 0xFFU);

    EXPECT_EQ(ocs::uds::decode(encoded.bytes).error.code, ocs::ErrorCode::kPayloadTooLarge);
}

}  // namespace
