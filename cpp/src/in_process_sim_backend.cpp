#include "ocs/in_process_sim_backend.hpp"

#include <stdexcept>
#include <utility>

namespace ocs {

InProcessSimBackend::InProcessSimBackend(DeviceInfo info)
    : device_(std::make_shared<SimulatedOcsDevice>(std::move(info))) {}

InProcessSimBackend::InProcessSimBackend(std::shared_ptr<SimulatedOcsDevice> device)
    : device_(std::move(device)) {
    if (!device_) {
        throw std::invalid_argument("simulated device must not be null");
    }
}

DeviceInfo InProcessSimBackend::getDeviceInfo() const {
    return device_->getDeviceInfo();
}

DeviceHealth InProcessSimBackend::getHealth() const {
    return device_->getHealth();
}

ApplyResult InProcessSimBackend::applyConnections(
    const std::vector<ConnectionCommand>& commands,
    const ApplyOptions& options) {
    return device_->applyConnections(commands, options);
}

std::vector<AppliedConnection> InProcessSimBackend::getConnections() const {
    return device_->getConnections();
}

PortState InProcessSimBackend::getInputPortState(PortId id) const {
    return device_->getPortState(PortDirection::kInput, id);
}

PortState InProcessSimBackend::getOutputPortState(PortId id) const {
    return device_->getPortState(PortDirection::kOutput, id);
}

ResetResult InProcessSimBackend::reset(ResetMode mode) {
    return device_->reset(mode);
}

FaultResult InProcessSimBackend::injectFault(const FaultSpec& fault) {
    return device_->injectFault(fault);
}

FaultResult InProcessSimBackend::clearFault(const FaultSelector& selector) {
    return device_->clearFault(selector);
}

}  // namespace ocs
