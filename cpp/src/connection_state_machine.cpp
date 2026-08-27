#include "ocs/connection_state_machine.hpp"

#include <stdexcept>
#include <string>

namespace ocs {

std::string_view toString(ConnectionApplyStatus status) noexcept {
    switch (status) {
        case ConnectionApplyStatus::kAbsent:
            return "ABSENT";
        case ConnectionApplyStatus::kPendingCreate:
            return "PENDING_CREATE";
        case ConnectionApplyStatus::kApplying:
            return "APPLYING";
        case ConnectionApplyStatus::kActive:
            return "ACTIVE";
        case ConnectionApplyStatus::kPendingUpdate:
            return "PENDING_UPDATE";
        case ConnectionApplyStatus::kPendingDelete:
            return "PENDING_DELETE";
        case ConnectionApplyStatus::kRemoving:
            return "REMOVING";
        case ConnectionApplyStatus::kFailed:
            return "FAILED";
        case ConnectionApplyStatus::kRetryWait:
            return "RETRY_WAIT";
        case ConnectionApplyStatus::kDrifted:
            return "DRIFTED";
        case ConnectionApplyStatus::kReconciling:
            return "RECONCILING";
    }
    return "ABSENT";
}

ConnectionApplyStatus connectionApplyStatusFromString(std::string_view value) {
    if (value == "ABSENT") {
        return ConnectionApplyStatus::kAbsent;
    }
    if (value == "PENDING_CREATE") {
        return ConnectionApplyStatus::kPendingCreate;
    }
    if (value == "APPLYING") {
        return ConnectionApplyStatus::kApplying;
    }
    if (value == "ACTIVE") {
        return ConnectionApplyStatus::kActive;
    }
    if (value == "PENDING_UPDATE") {
        return ConnectionApplyStatus::kPendingUpdate;
    }
    if (value == "PENDING_DELETE") {
        return ConnectionApplyStatus::kPendingDelete;
    }
    if (value == "REMOVING") {
        return ConnectionApplyStatus::kRemoving;
    }
    if (value == "FAILED") {
        return ConnectionApplyStatus::kFailed;
    }
    if (value == "RETRY_WAIT") {
        return ConnectionApplyStatus::kRetryWait;
    }
    if (value == "DRIFTED") {
        return ConnectionApplyStatus::kDrifted;
    }
    if (value == "RECONCILING") {
        return ConnectionApplyStatus::kReconciling;
    }
    throw std::invalid_argument("unknown connection apply status: " + std::string(value));
}

bool canTransition(ConnectionApplyStatus from, ConnectionApplyStatus to) noexcept {
    switch (from) {
        case ConnectionApplyStatus::kAbsent:
            return to == ConnectionApplyStatus::kPendingCreate;
        case ConnectionApplyStatus::kPendingCreate:
        case ConnectionApplyStatus::kPendingUpdate:
            return to == ConnectionApplyStatus::kApplying ||
                   to == ConnectionApplyStatus::kFailed;
        case ConnectionApplyStatus::kApplying:
            return to == ConnectionApplyStatus::kActive ||
                   to == ConnectionApplyStatus::kFailed;
        case ConnectionApplyStatus::kActive:
            return to == ConnectionApplyStatus::kPendingUpdate ||
                   to == ConnectionApplyStatus::kPendingDelete ||
                   to == ConnectionApplyStatus::kDrifted;
        case ConnectionApplyStatus::kPendingDelete:
            return to == ConnectionApplyStatus::kRemoving ||
                   to == ConnectionApplyStatus::kFailed;
        case ConnectionApplyStatus::kRemoving:
            return to == ConnectionApplyStatus::kAbsent ||
                   to == ConnectionApplyStatus::kFailed;
        case ConnectionApplyStatus::kFailed:
            return to == ConnectionApplyStatus::kRetryWait;
        case ConnectionApplyStatus::kRetryWait:
            return to == ConnectionApplyStatus::kApplying ||
                   to == ConnectionApplyStatus::kRemoving;
        case ConnectionApplyStatus::kDrifted:
            return to == ConnectionApplyStatus::kReconciling;
        case ConnectionApplyStatus::kReconciling:
            return to == ConnectionApplyStatus::kActive ||
                   to == ConnectionApplyStatus::kFailed;
    }
    return false;
}

void requireTransition(ConnectionApplyStatus from, ConnectionApplyStatus to) {
    if (!canTransition(from, to)) {
        throw std::logic_error(
            "invalid connection state transition: " + std::string(toString(from)) + " -> " +
            std::string(toString(to)));
    }
}

}  // namespace ocs
