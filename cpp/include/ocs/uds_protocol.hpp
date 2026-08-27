#pragma once

#include "ocs/errors.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace ocs::uds {

inline constexpr std::uint32_t kMagic = 0x4F435331;  // "OCS1"
inline constexpr std::uint16_t kProtocolVersion = 1;
inline constexpr std::size_t kHeaderSize = 32;
inline constexpr std::size_t kMaxPayloadSize = 1024 * 1024;

enum class MessageType : std::uint16_t {
    kHello = 1,
    kHelloReply = 2,
    kGetDeviceInfo = 3,
    kDeviceInfoReply = 4,
    kApplyConnections = 5,
    kApplyResult = 6,
    kGetConnections = 7,
    kConnectionsReply = 8,
    kGetPortState = 9,
    kPortStateReply = 10,
    kReset = 11,
    kResetResult = 12,
    kInjectFault = 13,
    kFaultResult = 14,
    kDeviceEvent = 15,
    kErrorReply = 16,
};

struct Frame {
    std::uint16_t version{kProtocolVersion};
    MessageType message_type{MessageType::kHello};
    std::uint32_t flags{};
    std::uint64_t request_id{};
    std::uint64_t device_generation{};
    std::string payload;

    bool operator==(const Frame&) const = default;
};

struct EncodeResult {
    Error error;
    std::vector<std::byte> bytes;

    [[nodiscard]] bool ok() const noexcept { return error.ok(); }
};

struct DecodeResult {
    Error error;
    std::optional<Frame> frame;

    [[nodiscard]] bool ok() const noexcept { return error.ok() && frame.has_value(); }
};

[[nodiscard]] EncodeResult encode(const Frame& frame);
[[nodiscard]] DecodeResult decode(const std::vector<std::byte>& bytes);

}  // namespace ocs::uds
