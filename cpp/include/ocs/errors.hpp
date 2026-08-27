#pragma once

#include <string>
#include <string_view>

namespace ocs {

enum class ErrorCode {
    kOk,
    kInvalidArgument,
    kInvalidPort,
    kConnectionNotFound,
    kConnectionExists,
    kInputConflict,
    kOutputConflict,
    kPortDisabled,
    kPortDown,
    kDeviceNotReady,
    kApplyTimeout,
    kApplyFailed,
    kVersionStale,
    kProtocolMalformed,
    kProtocolVersion,
    kPayloadTooLarge,
    kUnsupported,
    kInternalError,
};

[[nodiscard]] std::string_view toString(ErrorCode code) noexcept;
[[nodiscard]] ErrorCode errorCodeFromString(std::string_view value) noexcept;

struct Error {
    ErrorCode code{ErrorCode::kOk};
    std::string message;

    [[nodiscard]] bool ok() const noexcept { return code == ErrorCode::kOk; }

    static Error success() { return {}; }
};

}  // namespace ocs
