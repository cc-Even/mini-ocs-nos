#include "ocs/uds_protocol.hpp"

#include <limits>
#include <string_view>
#include <type_traits>
#include <utility>

namespace ocs::uds {
namespace {

template <typename Integer>
void appendBigEndian(std::vector<std::byte>& output, Integer value) {
    static_assert(std::is_unsigned_v<Integer>);
    for (std::size_t offset = sizeof(Integer); offset > 0; --offset) {
        const auto shift = static_cast<unsigned int>((offset - 1) * 8);
        output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
    }
}

template <typename Integer>
Integer readBigEndian(const std::vector<std::byte>& input, std::size_t& offset) {
    static_assert(std::is_unsigned_v<Integer>);
    Integer value{};
    for (std::size_t index = 0; index < sizeof(Integer); ++index) {
        value = static_cast<Integer>(
            (value << 8U) | static_cast<Integer>(std::to_integer<unsigned char>(input[offset++])));
    }
    return value;
}

Error protocolError(ErrorCode code, std::string message) {
    return Error{code, std::move(message)};
}

bool isKnownMessageType(std::uint16_t value) {
    return value >= static_cast<std::uint16_t>(MessageType::kHello) &&
           value <= static_cast<std::uint16_t>(MessageType::kErrorReply);
}

}  // namespace

EncodeResult encode(const Frame& frame) {
    if (frame.version != kProtocolVersion) {
        return {
            protocolError(ErrorCode::kProtocolVersion, "unsupported UDS protocol version"),
            {},
        };
    }
    if (frame.payload.size() > kMaxPayloadSize ||
        frame.payload.size() > std::numeric_limits<std::uint32_t>::max()) {
        return {protocolError(ErrorCode::kPayloadTooLarge, "UDS payload exceeds limit"), {}};
    }

    std::vector<std::byte> output;
    output.reserve(kHeaderSize + frame.payload.size());
    appendBigEndian(output, kMagic);
    appendBigEndian(output, frame.version);
    appendBigEndian(output, static_cast<std::uint16_t>(frame.message_type));
    appendBigEndian(output, frame.flags);
    appendBigEndian(output, frame.request_id);
    appendBigEndian(output, static_cast<std::uint32_t>(frame.payload.size()));
    appendBigEndian(output, frame.device_generation);
    for (const char value : frame.payload) {
        output.push_back(static_cast<std::byte>(static_cast<unsigned char>(value)));
    }
    return {Error::success(), std::move(output)};
}

DecodeResult decode(const std::vector<std::byte>& bytes) {
    if (bytes.size() < kHeaderSize) {
        return {protocolError(ErrorCode::kProtocolMalformed, "UDS frame header is truncated"), {}};
    }

    std::size_t offset = 0;
    const auto magic = readBigEndian<std::uint32_t>(bytes, offset);
    const auto version = readBigEndian<std::uint16_t>(bytes, offset);
    const auto message_type = readBigEndian<std::uint16_t>(bytes, offset);
    const auto flags = readBigEndian<std::uint32_t>(bytes, offset);
    const auto request_id = readBigEndian<std::uint64_t>(bytes, offset);
    const auto payload_length = readBigEndian<std::uint32_t>(bytes, offset);
    const auto device_generation = readBigEndian<std::uint64_t>(bytes, offset);

    if (magic != kMagic) {
        return {protocolError(ErrorCode::kProtocolMalformed, "UDS frame magic is invalid"), {}};
    }
    if (version != kProtocolVersion) {
        return {protocolError(ErrorCode::kProtocolVersion, "unsupported UDS protocol version"), {}};
    }
    if (!isKnownMessageType(message_type)) {
        return {protocolError(ErrorCode::kProtocolMalformed, "UDS message type is invalid"), {}};
    }
    if (payload_length > kMaxPayloadSize) {
        return {protocolError(ErrorCode::kPayloadTooLarge, "UDS payload exceeds limit"), {}};
    }
    if (bytes.size() != kHeaderSize + payload_length) {
        return {
            protocolError(ErrorCode::kProtocolMalformed, "UDS payload length does not match frame"),
            {},
        };
    }

    std::string payload;
    payload.reserve(payload_length);
    for (; offset < bytes.size(); ++offset) {
        payload.push_back(static_cast<char>(std::to_integer<unsigned char>(bytes[offset])));
    }

    return {
        Error::success(),
        Frame{
            .version = version,
            .message_type = static_cast<MessageType>(message_type),
            .flags = flags,
            .request_id = request_id,
            .device_generation = device_generation,
            .payload = std::move(payload),
        },
    };
}

}  // namespace ocs::uds
