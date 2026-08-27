#pragma once

#include "ocs/device_command.hpp"

#include <string>
#include <vector>

namespace ocs {

enum class ConnectionDriftKind {
    kMissing,
    kMismatched,
    kUnexpected,
};

struct ConnectionDrift {
    std::string id;
    ConnectionDriftKind kind{ConnectionDriftKind::kMissing};
    ConnectionCommand desired;
    AppliedConnection actual;
};

struct ReconciliationPlan {
    DeviceCommandBatch full_snapshot;
    std::vector<ConnectionDrift> drifts;

    [[nodiscard]] bool converged() const noexcept { return drifts.empty(); }
};

[[nodiscard]] ReconciliationPlan buildReconciliationPlan(
    std::vector<ConnectionCommand> desired,
    std::vector<AppliedConnection> actual);

}  // namespace ocs
