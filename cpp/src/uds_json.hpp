#pragma once

#include "ocs/device_types.hpp"

#include <string>
#include <utility>
#include <vector>

namespace ocs::uds::json {

struct ApplyRequest {
    std::vector<ConnectionCommand> commands;
    ApplyOptions options;
};

struct PortRequest {
    PortDirection direction{PortDirection::kInput};
    PortId port_id{};
};

struct FaultRequest {
    bool clear{};
    FaultSpec fault;
};

[[nodiscard]] std::string encodeError(const Error& error);
[[nodiscard]] Error decodeError(const std::string& payload);
[[nodiscard]] std::string encodeDeviceInfo(const DeviceInfo& info);
[[nodiscard]] DeviceInfo decodeDeviceInfo(const std::string& payload);
[[nodiscard]] std::string encodeApplyRequest(
    const std::vector<ConnectionCommand>& commands,
    const ApplyOptions& options);
[[nodiscard]] ApplyRequest decodeApplyRequest(const std::string& payload);
[[nodiscard]] std::string encodeApplyResult(const ApplyResult& result);
[[nodiscard]] ApplyResult decodeApplyResult(const std::string& payload);
[[nodiscard]] std::string encodeConnections(const std::vector<AppliedConnection>& connections);
[[nodiscard]] std::vector<AppliedConnection> decodeConnections(const std::string& payload);
[[nodiscard]] std::string encodePortRequest(PortDirection direction, PortId port_id);
[[nodiscard]] PortRequest decodePortRequest(const std::string& payload);
[[nodiscard]] std::string encodePortState(const PortState& state);
[[nodiscard]] PortState decodePortState(const std::string& payload);
[[nodiscard]] std::string encodeResetRequest(ResetMode mode);
[[nodiscard]] ResetMode decodeResetRequest(const std::string& payload);
[[nodiscard]] std::string encodeResetResult(const ResetResult& result);
[[nodiscard]] ResetResult decodeResetResult(const std::string& payload);
[[nodiscard]] std::string encodeFaultRequest(const FaultSpec& fault, bool clear);
[[nodiscard]] FaultRequest decodeFaultRequest(const std::string& payload);
[[nodiscard]] std::string encodeFaultResult(const FaultResult& result);
[[nodiscard]] FaultResult decodeFaultResult(const std::string& payload);

}  // namespace ocs::uds::json
