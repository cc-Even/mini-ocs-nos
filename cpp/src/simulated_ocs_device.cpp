#include "ocs/simulated_ocs_device.hpp"

#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <utility>

namespace ocs {
namespace {

Error makeError(ErrorCode code, std::string message) {
    return Error{code, std::move(message)};
}

std::vector<PortState> makePorts(PortDirection direction, PortId count) {
    std::vector<PortState> ports;
    ports.reserve(count);
    for (PortId id = 1; id <= count; ++id) {
        ports.push_back(PortState{id, direction, true, PortOperStatus::kUp, -3.0});
    }
    return ports;
}

}  // namespace

SimulatedOcsDevice::SimulatedOcsDevice(DeviceInfo info)
    : info_(std::move(info)),
      input_to_output_(info_.input_port_count),
      output_to_input_(info_.output_port_count),
      input_ports_(makePorts(PortDirection::kInput, info_.input_port_count)),
      output_ports_(makePorts(PortDirection::kOutput, info_.output_port_count)) {
    if (info_.input_port_count == 0 || info_.output_port_count == 0) {
        throw std::invalid_argument("device port counts must be positive");
    }
}

DeviceInfo SimulatedOcsDevice::getDeviceInfo() const {
    std::scoped_lock lock(mutex_);
    return info_;
}

DeviceHealth SimulatedOcsDevice::getHealth() const {
    std::scoped_lock lock(mutex_);
    return health_;
}

ApplyResult SimulatedOcsDevice::applyConnections(
    const std::vector<ConnectionCommand>& commands,
    const ApplyOptions& options) {
    std::scoped_lock lock(mutex_);
    if (!options.operation_id.empty()) {
        if (const auto cached = apply_results_.find(options.operation_id);
            cached != apply_results_.end()) {
            return cached->second;
        }
    }
    const auto finish = [this, &options](ApplyResult result) {
        if (!options.operation_id.empty()) {
            apply_results_.insert_or_assign(options.operation_id, result);
        }
        return result;
    };
    if (!options.atomic) {
        return finish({makeError(ErrorCode::kUnsupported, "non-atomic apply is not supported"), {}});
    }

    auto candidate_connections = connections_;

    for (const auto& command : commands) {
        if (command.desired_version == 0) {
            continue;
        }
        const auto last = last_versions_.find(command.id);
        if (last != last_versions_.end() && last->second > command.desired_version) {
            return finish(
                {makeError(ErrorCode::kVersionStale, "connection version is stale"), {}});
        }
    }

    for (const auto& command : commands) {
        if (command.id.empty()) {
            return finish(
                {makeError(ErrorCode::kInvalidArgument, "connection id must not be empty"), {}});
        }

        if (command.operation == ConnectionOperation::kRemove) {
            const auto existing = candidate_connections.find(command.id);
            if (existing != candidate_connections.end()) {
                candidate_connections.erase(existing);
            }
            continue;
        }

        if (const auto error = validatePortAvailable(PortDirection::kInput, command.input_port);
            !error.ok()) {
            return finish({error, {}});
        }
        if (const auto error = validatePortAvailable(PortDirection::kOutput, command.output_port);
            !error.ok()) {
            return finish({error, {}});
        }

        candidate_connections.insert_or_assign(
            command.id,
            AppliedConnection{
                command.id,
                command.input_port,
                command.output_port,
                command.desired_version,
            });
    }

    std::vector<std::optional<PortId>> candidate_input_to_output(info_.input_port_count);
    std::vector<std::optional<PortId>> candidate_output_to_input(info_.output_port_count);
    for (const auto& [id, connection] : candidate_connections) {
        static_cast<void>(id);
        if (candidate_input_to_output.at(connection.input_port - 1).has_value()) {
            return finish(
                {makeError(ErrorCode::kInputConflict, "input port is already connected"), {}});
        }
        if (candidate_output_to_input.at(connection.output_port - 1).has_value()) {
            return finish(
                {makeError(ErrorCode::kOutputConflict, "output port is already connected"), {}});
        }
        candidate_input_to_output.at(connection.input_port - 1) = connection.output_port;
        candidate_output_to_input.at(connection.output_port - 1) = connection.input_port;
    }

    if (fail_next_apply_) {
        fail_next_apply_ = false;
        return finish({makeError(ErrorCode::kApplyFailed, "injected next-apply failure"), {}});
    }

    connections_.swap(candidate_connections);
    input_to_output_.swap(candidate_input_to_output);
    output_to_input_.swap(candidate_output_to_input);
    for (const auto& command : commands) {
        if (command.desired_version != 0) {
            last_versions_.insert_or_assign(command.id, command.desired_version);
        }
    }

    std::vector<AppliedConnection> changed;
    changed.reserve(commands.size());
    for (const auto& command : commands) {
        if (const auto connection = connections_.find(command.id); connection != connections_.end()) {
            changed.push_back(connection->second);
        }
    }
    return finish({Error::success(), std::move(changed)});
}

std::vector<AppliedConnection> SimulatedOcsDevice::getConnections() const {
    std::scoped_lock lock(mutex_);
    std::vector<AppliedConnection> result;
    result.reserve(connections_.size());
    std::ranges::transform(
        connections_, std::back_inserter(result), [](const auto& item) { return item.second; });
    return result;
}

PortState SimulatedOcsDevice::getPortState(PortDirection direction, PortId id) const {
    std::scoped_lock lock(mutex_);
    const auto error = validatePort(direction, id);
    if (!error.ok()) {
        return PortState{id, direction, false, PortOperStatus::kUnknown, 0.0};
    }
    const auto& ports = direction == PortDirection::kInput ? input_ports_ : output_ports_;
    return ports.at(id - 1);
}

ResetResult SimulatedOcsDevice::reset(ResetMode mode) {
    std::scoped_lock lock(mutex_);
    connections_.clear();
    last_versions_.clear();
    std::ranges::fill(input_to_output_, std::nullopt);
    std::ranges::fill(output_to_input_, std::nullopt);
    input_ports_ = makePorts(PortDirection::kInput, info_.input_port_count);
    output_ports_ = makePorts(PortDirection::kOutput, info_.output_port_count);
    fail_next_apply_ = false;
    apply_results_.clear();
    if (mode == ResetMode::kHard) {
        ++info_.generation;
    }
    health_ = DeviceHealth{DeviceOperStatus::kReady, Error::success()};
    return {Error::success(), info_.generation};
}

FaultResult SimulatedOcsDevice::injectFault(const FaultSpec& fault) {
    std::scoped_lock lock(mutex_);
    if (fault.type == FaultType::kNextApplyError) {
        fail_next_apply_ = true;
        return {Error::success()};
    }

    const auto direction = fault.type == FaultType::kInputPortDown ? PortDirection::kInput
                                                                   : PortDirection::kOutput;
    if (const auto error = validatePort(direction, fault.port_id); !error.ok()) {
        return {error};
    }
    auto& ports = direction == PortDirection::kInput ? input_ports_ : output_ports_;
    ports.at(fault.port_id - 1).oper_status = PortOperStatus::kDown;
    return {Error::success()};
}

FaultResult SimulatedOcsDevice::clearFault(const FaultSelector& selector) {
    std::scoped_lock lock(mutex_);
    if (selector.type == FaultType::kNextApplyError) {
        fail_next_apply_ = false;
        return {Error::success()};
    }

    const auto direction = selector.type == FaultType::kInputPortDown ? PortDirection::kInput
                                                                      : PortDirection::kOutput;
    if (const auto error = validatePort(direction, selector.port_id); !error.ok()) {
        return {error};
    }
    auto& ports = direction == PortDirection::kInput ? input_ports_ : output_ports_;
    ports.at(selector.port_id - 1).oper_status = PortOperStatus::kUp;
    return {Error::success()};
}

Error SimulatedOcsDevice::setPortAdminState(PortDirection direction, PortId id, bool enabled) {
    std::scoped_lock lock(mutex_);
    if (const auto error = validatePort(direction, id); !error.ok()) {
        return error;
    }
    auto& ports = direction == PortDirection::kInput ? input_ports_ : output_ports_;
    ports.at(id - 1).admin_enabled = enabled;
    return Error::success();
}

Error SimulatedOcsDevice::validatePort(PortDirection direction, PortId id) const {
    const auto port_count =
        direction == PortDirection::kInput ? info_.input_port_count : info_.output_port_count;
    if (id == 0 || id > port_count) {
        return makeError(ErrorCode::kInvalidPort, "port is outside the configured matrix");
    }
    return Error::success();
}

Error SimulatedOcsDevice::validatePortAvailable(PortDirection direction, PortId id) const {
    if (const auto error = validatePort(direction, id); !error.ok()) {
        return error;
    }
    const auto& ports = direction == PortDirection::kInput ? input_ports_ : output_ports_;
    const auto& port = ports.at(id - 1);
    if (!port.admin_enabled) {
        return makeError(ErrorCode::kPortDisabled, "port is administratively disabled");
    }
    if (port.oper_status != PortOperStatus::kUp) {
        return makeError(ErrorCode::kPortDown, "port is not operationally up");
    }
    return Error::success();
}

}  // namespace ocs
