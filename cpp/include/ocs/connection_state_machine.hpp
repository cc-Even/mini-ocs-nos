#pragma once

#include <string_view>

namespace ocs {

enum class ConnectionApplyStatus {
    kAbsent,
    kPendingCreate,
    kApplying,
    kActive,
    kPendingUpdate,
    kPendingDelete,
    kRemoving,
    kFailed,
    kRetryWait,
    kDrifted,
    kReconciling,
};

[[nodiscard]] std::string_view toString(ConnectionApplyStatus status) noexcept;
[[nodiscard]] ConnectionApplyStatus connectionApplyStatusFromString(std::string_view value);
[[nodiscard]] bool canTransition(
    ConnectionApplyStatus from,
    ConnectionApplyStatus to) noexcept;
void requireTransition(ConnectionApplyStatus from, ConnectionApplyStatus to);

}  // namespace ocs
