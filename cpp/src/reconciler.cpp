#include "ocs/reconciler.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <utility>

namespace ocs {

ReconciliationPlan buildReconciliationPlan(
    std::vector<ConnectionCommand> desired,
    std::vector<AppliedConnection> actual) {
    std::ranges::sort(desired, {}, &ConnectionCommand::id);
    std::ranges::sort(actual, {}, &AppliedConnection::id);
    if (std::ranges::adjacent_find(desired, {}, &ConnectionCommand::id) != desired.end() ||
        std::ranges::adjacent_find(actual, {}, &AppliedConnection::id) != actual.end()) {
        throw std::invalid_argument("reconciliation snapshots contain duplicate connection IDs");
    }

    ReconciliationPlan plan;
    plan.full_snapshot.options.atomic = true;
    plan.full_snapshot.options.timeout = std::chrono::milliseconds(1000);
    plan.full_snapshot.commands = desired;

    std::map<std::string, AppliedConnection> actual_by_id;
    for (const auto& connection : actual) {
        actual_by_id.emplace(connection.id, connection);
    }
    for (const auto& command : desired) {
        const auto found = actual_by_id.find(command.id);
        if (found == actual_by_id.end()) {
            plan.drifts.push_back({
                .id = command.id,
                .kind = ConnectionDriftKind::kMissing,
                .desired = command,
                .actual = {},
            });
            continue;
        }
        if (found->second.input_port != command.input_port ||
            found->second.output_port != command.output_port ||
            found->second.applied_version != command.desired_version) {
            plan.drifts.push_back({
                .id = command.id,
                .kind = ConnectionDriftKind::kMismatched,
                .desired = command,
                .actual = found->second,
            });
        }
        actual_by_id.erase(found);
    }
    for (const auto& [id, connection] : actual_by_id) {
        ConnectionCommand removal{
            .operation = ConnectionOperation::kRemove,
            .id = id,
            .desired_version = std::max<std::uint64_t>(connection.applied_version, 1),
        };
        plan.full_snapshot.commands.push_back(removal);
        plan.drifts.push_back({
            .id = id,
            .kind = ConnectionDriftKind::kUnexpected,
            .desired = removal,
            .actual = connection,
        });
    }
    std::ranges::sort(plan.full_snapshot.commands, {}, &ConnectionCommand::id);
    std::ranges::sort(plan.drifts, {}, &ConnectionDrift::id);
    return plan;
}

}  // namespace ocs
