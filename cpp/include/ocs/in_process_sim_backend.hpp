#pragma once

#include "ocs/device_api.hpp"
#include "ocs/simulated_ocs_device.hpp"

#include <memory>

namespace ocs {

class InProcessSimBackend final : public OcsDeviceApi {
public:
    explicit InProcessSimBackend(DeviceInfo info);
    explicit InProcessSimBackend(std::shared_ptr<SimulatedOcsDevice> device);

    [[nodiscard]] DeviceInfo getDeviceInfo() const override;
    [[nodiscard]] DeviceHealth getHealth() const override;
    ApplyResult applyConnections(
        const std::vector<ConnectionCommand>& commands,
        const ApplyOptions& options) override;
    [[nodiscard]] std::vector<AppliedConnection> getConnections() const override;
    [[nodiscard]] PortState getInputPortState(PortId id) const override;
    [[nodiscard]] PortState getOutputPortState(PortId id) const override;
    ResetResult reset(ResetMode mode) override;
    FaultResult injectFault(const FaultSpec& fault) override;
    FaultResult clearFault(const FaultSelector& selector) override;

private:
    std::shared_ptr<SimulatedOcsDevice> device_;
};

}  // namespace ocs
