#include "ocs/device_command.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace ocs {
namespace {

using Json = nlohmann::json;

std::string_view operationName(ConnectionOperation operation) {
    return operation == ConnectionOperation::kRemove ? "REMOVE" : "UPSERT";
}

ConnectionOperation parseOperation(std::string_view operation) {
    if (operation == "UPSERT") {
        return ConnectionOperation::kUpsert;
    }
    if (operation == "REMOVE") {
        return ConnectionOperation::kRemove;
    }
    throw std::invalid_argument("unsupported device command operation");
}

PortId parsePort(const Json& command, const char* field) {
    const auto& value = command.at(field);
    std::uint64_t parsed{};
    if (value.is_number_unsigned()) {
        parsed = value.get<std::uint64_t>();
    } else if (value.is_number_integer()) {
        const auto signed_value = value.get<std::int64_t>();
        if (signed_value <= 0) {
            throw std::invalid_argument(std::string(field) + " must be positive");
        }
        parsed = static_cast<std::uint64_t>(signed_value);
    } else {
        throw std::invalid_argument(std::string(field) + " must be an integer");
    }
    if (parsed == 0 || parsed > std::numeric_limits<PortId>::max()) {
        throw std::invalid_argument(std::string(field) + " is outside the supported port range");
    }
    return static_cast<PortId>(parsed);
}

}  // namespace

std::string encodeDeviceCommand(const DeviceCommandBatch& batch) {
    Json commands = Json::array();
    for (const auto& command : batch.commands) {
        commands.push_back({
            {"operation", operationName(command.operation)},
            {"id", command.id},
            {"input_port", command.input_port},
            {"output_port", command.output_port},
            {"desired_version", command.desired_version},
        });
    }
    return Json{
        {"commands", std::move(commands)},
        {"atomic", batch.options.atomic},
        {"timeout_ms", batch.options.timeout.count()},
        {"operation_id", batch.options.operation_id},
    }.dump();
}

DeviceCommandBatch decodeDeviceCommand(const std::string& payload) {
    const auto value = Json::parse(payload);
    DeviceCommandBatch batch;
    batch.options.atomic = value.at("atomic").get<bool>();
    batch.options.timeout = std::chrono::milliseconds(value.at("timeout_ms").get<long long>());
    batch.options.operation_id = value.value("operation_id", "");
    if (batch.options.timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("device command timeout must be positive");
    }
    for (const auto& command : value.at("commands")) {
        const auto operation = parseOperation(command.at("operation").get<std::string>());
        ConnectionCommand decoded{
            .operation = operation,
            .id = command.at("id").get<std::string>(),
            .desired_version = command.at("desired_version").get<std::uint64_t>(),
        };
        if (operation == ConnectionOperation::kUpsert) {
            decoded.input_port = parsePort(command, "input_port");
            decoded.output_port = parsePort(command, "output_port");
        }
        batch.commands.push_back(std::move(decoded));
    }
    if (batch.commands.empty()) {
        throw std::invalid_argument("device command batch must not be empty");
    }
    return batch;
}

}  // namespace ocs
