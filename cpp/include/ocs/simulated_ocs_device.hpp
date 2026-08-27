#pragma once

#include "ocs/device_types.hpp"

#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <vector>

namespace ocs {

class SimulatedOcsDevice {
public:
    explicit SimulatedOcsDevice(DeviceInfo info);

    [[nodiscard]] DeviceInfo getDeviceInfo() const;
    [[nodiscard]] DeviceHealth getHealth() const;
    ApplyResult applyConnections(
        const std::vector<ConnectionCommand>& commands,
        const ApplyOptions& options);
    [[nodiscard]] std::vector<AppliedConnection> getConnections() const;
    [[nodiscard]] PortState getPortState(PortDirection direction, PortId id) const;
    ResetResult reset(ResetMode mode);
    FaultResult injectFault(const FaultSpec& fault);
    FaultResult clearFault(const FaultSelector& selector);
    Error setPortAdminState(PortDirection direction, PortId id, bool enabled);

private:
    [[nodiscard]] Error validatePort(PortDirection direction, PortId id) const;
    [[nodiscard]] Error validatePortAvailable(PortDirection direction, PortId id) const;

    mutable std::mutex mutex_;
    DeviceInfo info_;
    DeviceHealth health_{DeviceOperStatus::kReady, Error::success()};
    std::map<std::string, AppliedConnection> connections_;
    std::map<std::string, std::uint64_t> last_versions_;
    std::vector<std::optional<PortId>> input_to_output_;
    std::vector<std::optional<PortId>> output_to_input_;
    std::vector<PortState> input_ports_;
    std::vector<PortState> output_ports_;
    std::map<std::string, ApplyResult> apply_results_;
    std::deque<std::string> apply_result_order_;
    bool fail_next_apply_{false};
};

}  // namespace ocs
