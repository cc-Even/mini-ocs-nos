#include "ocs/errors.hpp"

namespace ocs {

std::string_view toString(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::kOk:
            return "OCS_OK";
        case ErrorCode::kInvalidArgument:
            return "OCS_INVALID_ARGUMENT";
        case ErrorCode::kInvalidPort:
            return "OCS_INVALID_PORT";
        case ErrorCode::kConnectionNotFound:
            return "OCS_CONNECTION_NOT_FOUND";
        case ErrorCode::kConnectionExists:
            return "OCS_CONNECTION_EXISTS";
        case ErrorCode::kInputConflict:
            return "OCS_INPUT_CONFLICT";
        case ErrorCode::kOutputConflict:
            return "OCS_OUTPUT_CONFLICT";
        case ErrorCode::kPortDisabled:
            return "OCS_PORT_DISABLED";
        case ErrorCode::kPortDown:
            return "OCS_PORT_DOWN";
        case ErrorCode::kDeviceNotReady:
            return "OCS_DEVICE_NOT_READY";
        case ErrorCode::kApplyTimeout:
            return "OCS_APPLY_TIMEOUT";
        case ErrorCode::kApplyFailed:
            return "OCS_APPLY_FAILED";
        case ErrorCode::kVersionStale:
            return "OCS_VERSION_STALE";
        case ErrorCode::kProtocolMalformed:
            return "OCS_PROTOCOL_MALFORMED";
        case ErrorCode::kProtocolVersion:
            return "OCS_PROTOCOL_VERSION";
        case ErrorCode::kPayloadTooLarge:
            return "OCS_PAYLOAD_TOO_LARGE";
        case ErrorCode::kUnsupported:
            return "OCS_UNSUPPORTED";
        case ErrorCode::kInternalError:
            return "OCS_INTERNAL_ERROR";
    }
    return "OCS_INTERNAL_ERROR";
}

ErrorCode errorCodeFromString(std::string_view value) noexcept {
    for (const auto code : {
             ErrorCode::kOk,
             ErrorCode::kInvalidArgument,
             ErrorCode::kInvalidPort,
             ErrorCode::kConnectionNotFound,
             ErrorCode::kConnectionExists,
             ErrorCode::kInputConflict,
             ErrorCode::kOutputConflict,
             ErrorCode::kPortDisabled,
             ErrorCode::kPortDown,
             ErrorCode::kDeviceNotReady,
             ErrorCode::kApplyTimeout,
             ErrorCode::kApplyFailed,
             ErrorCode::kVersionStale,
             ErrorCode::kProtocolMalformed,
             ErrorCode::kProtocolVersion,
             ErrorCode::kPayloadTooLarge,
             ErrorCode::kUnsupported,
             ErrorCode::kInternalError,
         }) {
        if (toString(code) == value) {
            return code;
        }
    }
    return ErrorCode::kInternalError;
}

}  // namespace ocs
