#pragma once

#include "ocs/connection.hpp"
#include "ocs/errors.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace ocs {

enum class DeviceOperStatus {
    kInit,
    kReady,
    kDegraded,
    kFailed,
    kResetting,
};

enum class PortDirection {
    kInput,
    kOutput,
};

enum class PortOperStatus {
    kUp,
    kDown,
    kLowPower,
    kFault,
    kUnknown,
};

struct DeviceInfo {
    std::string name;
    PortId input_port_count{};
    PortId output_port_count{};
    std::string model;
    std::string serial_number;
    std::string firmware_version;
    std::uint64_t generation{};
};

struct DeviceHealth {
    DeviceOperStatus status{DeviceOperStatus::kInit};
    Error last_error;
};

struct PortState {
    PortId id{};
    PortDirection direction{PortDirection::kInput};
    bool admin_enabled{true};
    PortOperStatus oper_status{PortOperStatus::kUnknown};
    double optical_power_dbm{-3.0};
};

struct ApplyOptions {
    bool atomic{true};
    std::chrono::milliseconds timeout{1000};
    std::string operation_id;
};

struct ApplyResult {
    Error error;
    std::vector<AppliedConnection> connections;

    [[nodiscard]] bool ok() const noexcept { return error.ok(); }
};

enum class ResetMode {
    kSoft,
    kHard,
};

struct ResetResult {
    Error error;
    std::uint64_t generation{};
};

enum class FaultType {
    kNextApplyError,
    kNextApplyTimeout,
    kInputPortDown,
    kOutputPortDown,
};

struct FaultSpec {
    FaultType type{FaultType::kNextApplyError};
    PortId port_id{};
};

struct FaultSelector {
    FaultType type{FaultType::kNextApplyError};
    PortId port_id{};
};

struct FaultResult {
    Error error;
};

}  // namespace ocs
