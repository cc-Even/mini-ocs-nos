#include "uds_json.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <string_view>

namespace ocs::uds::json {
namespace {

using Json = nlohmann::json;

std::string_view directionName(PortDirection direction) {
    return direction == PortDirection::kInput ? "INPUT" : "OUTPUT";
}

PortDirection parseDirection(std::string_view value) {
    return value == "OUTPUT" ? PortDirection::kOutput : PortDirection::kInput;
}

std::string_view operationName(ConnectionOperation operation) {
    return operation == ConnectionOperation::kRemove ? "REMOVE" : "UPSERT";
}

ConnectionOperation parseOperation(std::string_view value) {
    return value == "REMOVE" ? ConnectionOperation::kRemove : ConnectionOperation::kUpsert;
}

std::string_view portStatusName(PortOperStatus status) {
    switch (status) {
        case PortOperStatus::kUp:
            return "UP";
        case PortOperStatus::kDown:
            return "DOWN";
        case PortOperStatus::kLowPower:
            return "LOW_POWER";
        case PortOperStatus::kFault:
            return "FAULT";
        case PortOperStatus::kUnknown:
            return "UNKNOWN";
    }
    return "UNKNOWN";
}

PortOperStatus parsePortStatus(std::string_view value) {
    if (value == "UP") {
        return PortOperStatus::kUp;
    }
    if (value == "DOWN") {
        return PortOperStatus::kDown;
    }
    if (value == "LOW_POWER") {
        return PortOperStatus::kLowPower;
    }
    if (value == "FAULT") {
        return PortOperStatus::kFault;
    }
    return PortOperStatus::kUnknown;
}

std::string_view faultName(FaultType type) {
    switch (type) {
        case FaultType::kNextApplyError:
            return "NEXT_APPLY_ERROR";
        case FaultType::kInputPortDown:
            return "INPUT_PORT_DOWN";
        case FaultType::kOutputPortDown:
            return "OUTPUT_PORT_DOWN";
    }
    return "NEXT_APPLY_ERROR";
}

FaultType parseFault(std::string_view value) {
    if (value == "INPUT_PORT_DOWN") {
        return FaultType::kInputPortDown;
    }
    if (value == "OUTPUT_PORT_DOWN") {
        return FaultType::kOutputPortDown;
    }
    return FaultType::kNextApplyError;
}

Json errorJson(const Error& error) {
    return {{"code", toString(error.code)}, {"message", error.message}};
}

Error parseError(const Json& value) {
    return {
        errorCodeFromString(value.value("code", "OCS_INTERNAL_ERROR")),
        value.value("message", ""),
    };
}

Json connectionJson(const AppliedConnection& connection) {
    return {
        {"id", connection.id},
        {"input_port", connection.input_port},
        {"output_port", connection.output_port},
        {"applied_version", connection.applied_version},
    };
}

AppliedConnection parseConnection(const Json& value) {
    return {
        .id = value.at("id").get<std::string>(),
        .input_port = value.at("input_port").get<PortId>(),
        .output_port = value.at("output_port").get<PortId>(),
        .applied_version = value.at("applied_version").get<std::uint64_t>(),
    };
}

}  // namespace

std::string encodeError(const Error& error) {
    return errorJson(error).dump();
}

Error decodeError(const std::string& payload) {
    return parseError(Json::parse(payload));
}

std::string encodeDeviceInfo(const DeviceInfo& info) {
    return Json{
        {"name", info.name},
        {"input_port_count", info.input_port_count},
        {"output_port_count", info.output_port_count},
        {"model", info.model},
        {"serial_number", info.serial_number},
        {"firmware_version", info.firmware_version},
        {"generation", info.generation},
    }.dump();
}

DeviceInfo decodeDeviceInfo(const std::string& payload) {
    const auto value = Json::parse(payload);
    return {
        .name = value.at("name").get<std::string>(),
        .input_port_count = value.at("input_port_count").get<PortId>(),
        .output_port_count = value.at("output_port_count").get<PortId>(),
        .model = value.at("model").get<std::string>(),
        .serial_number = value.at("serial_number").get<std::string>(),
        .firmware_version = value.at("firmware_version").get<std::string>(),
        .generation = value.at("generation").get<std::uint64_t>(),
    };
}

std::string encodeApplyRequest(
    const std::vector<ConnectionCommand>& commands,
    const ApplyOptions& options) {
    Json command_values = Json::array();
    for (const auto& command : commands) {
        command_values.push_back({
            {"operation", operationName(command.operation)},
            {"id", command.id},
            {"input_port", command.input_port},
            {"output_port", command.output_port},
            {"desired_version", command.desired_version},
        });
    }
    return Json{
        {"commands", std::move(command_values)},
        {"atomic", options.atomic},
        {"timeout_ms", options.timeout.count()},
        {"operation_id", options.operation_id},
    }.dump();
}

ApplyRequest decodeApplyRequest(const std::string& payload) {
    const auto value = Json::parse(payload);
    ApplyRequest result;
    result.options.atomic = value.at("atomic").get<bool>();
    result.options.timeout = std::chrono::milliseconds(value.at("timeout_ms").get<std::int64_t>());
    result.options.operation_id = value.value("operation_id", "");
    for (const auto& command : value.at("commands")) {
        result.commands.push_back({
            .operation = parseOperation(command.at("operation").get<std::string>()),
            .id = command.at("id").get<std::string>(),
            .input_port = command.at("input_port").get<PortId>(),
            .output_port = command.at("output_port").get<PortId>(),
            .desired_version = command.at("desired_version").get<std::uint64_t>(),
        });
    }
    return result;
}

std::string encodeApplyResult(const ApplyResult& result) {
    Json connections = Json::array();
    for (const auto& connection : result.connections) {
        connections.push_back(connectionJson(connection));
    }
    return Json{{"error", errorJson(result.error)}, {"connections", std::move(connections)}}.dump();
}

ApplyResult decodeApplyResult(const std::string& payload) {
    const auto value = Json::parse(payload);
    ApplyResult result{.error = parseError(value.at("error")), .connections = {}};
    for (const auto& connection : value.at("connections")) {
        result.connections.push_back(parseConnection(connection));
    }
    return result;
}

std::string encodeConnections(const std::vector<AppliedConnection>& connections) {
    Json values = Json::array();
    for (const auto& connection : connections) {
        values.push_back(connectionJson(connection));
    }
    return values.dump();
}

std::vector<AppliedConnection> decodeConnections(const std::string& payload) {
    std::vector<AppliedConnection> result;
    for (const auto& value : Json::parse(payload)) {
        result.push_back(parseConnection(value));
    }
    return result;
}

std::string encodePortRequest(PortDirection direction, PortId port_id) {
    return Json{{"direction", directionName(direction)}, {"port_id", port_id}}.dump();
}

PortRequest decodePortRequest(const std::string& payload) {
    const auto value = Json::parse(payload);
    return {
        .direction = parseDirection(value.at("direction").get<std::string>()),
        .port_id = value.at("port_id").get<PortId>(),
    };
}

std::string encodePortState(const PortState& state) {
    return Json{
        {"id", state.id},
        {"direction", directionName(state.direction)},
        {"admin_enabled", state.admin_enabled},
        {"oper_status", portStatusName(state.oper_status)},
        {"optical_power_dbm", state.optical_power_dbm},
    }.dump();
}

PortState decodePortState(const std::string& payload) {
    const auto value = Json::parse(payload);
    return {
        .id = value.at("id").get<PortId>(),
        .direction = parseDirection(value.at("direction").get<std::string>()),
        .admin_enabled = value.at("admin_enabled").get<bool>(),
        .oper_status = parsePortStatus(value.at("oper_status").get<std::string>()),
        .optical_power_dbm = value.at("optical_power_dbm").get<double>(),
    };
}

std::string encodeResetRequest(ResetMode mode) {
    return Json{{"mode", mode == ResetMode::kHard ? "HARD" : "SOFT"}}.dump();
}

ResetMode decodeResetRequest(const std::string& payload) {
    return Json::parse(payload).at("mode").get<std::string>() == "HARD" ? ResetMode::kHard
                                                                         : ResetMode::kSoft;
}

std::string encodeResetResult(const ResetResult& result) {
    return Json{{"error", errorJson(result.error)}, {"generation", result.generation}}.dump();
}

ResetResult decodeResetResult(const std::string& payload) {
    const auto value = Json::parse(payload);
    return {parseError(value.at("error")), value.at("generation").get<std::uint64_t>()};
}

std::string encodeFaultRequest(const FaultSpec& fault, bool clear) {
    return Json{
        {"action", clear ? "CLEAR" : "INJECT"},
        {"type", faultName(fault.type)},
        {"port_id", fault.port_id},
    }.dump();
}

FaultRequest decodeFaultRequest(const std::string& payload) {
    const auto value = Json::parse(payload);
    return {
        .clear = value.at("action").get<std::string>() == "CLEAR",
        .fault = {
            .type = parseFault(value.at("type").get<std::string>()),
            .port_id = value.at("port_id").get<PortId>(),
        },
    };
}

std::string encodeFaultResult(const FaultResult& result) {
    return Json{{"error", errorJson(result.error)}}.dump();
}

FaultResult decodeFaultResult(const std::string& payload) {
    return {parseError(Json::parse(payload).at("error"))};
}

}  // namespace ocs::uds::json
