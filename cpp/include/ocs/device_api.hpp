#pragma once

#include "ocs/device_types.hpp"

#include <vector>

namespace ocs {

class OcsDeviceApi {
public:
    virtual ~OcsDeviceApi() = default;

    [[nodiscard]] virtual DeviceInfo getDeviceInfo() const = 0;
    [[nodiscard]] virtual DeviceHealth getHealth() const = 0;
    virtual ApplyResult applyConnections(
        const std::vector<ConnectionCommand>& commands,
        const ApplyOptions& options) = 0;
    [[nodiscard]] virtual std::vector<AppliedConnection> getConnections() const = 0;
    [[nodiscard]] virtual PortState getInputPortState(PortId id) const = 0;
    [[nodiscard]] virtual PortState getOutputPortState(PortId id) const = 0;
    virtual ResetResult reset(ResetMode mode) = 0;
    virtual FaultResult injectFault(const FaultSpec& fault) = 0;
    virtual FaultResult clearFault(const FaultSelector& selector) = 0;
};

}  // namespace ocs
