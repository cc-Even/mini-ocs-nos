#include "ocs/device_command.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <stdexcept>
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
    }.dump();
}

DeviceCommandBatch decodeDeviceCommand(const std::string& payload) {
    const auto value = Json::parse(payload);
    DeviceCommandBatch batch;
    batch.options.atomic = value.at("atomic").get<bool>();
    batch.options.timeout = std::chrono::milliseconds(value.at("timeout_ms").get<long long>());
    if (batch.options.timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("device command timeout must be positive");
    }
    for (const auto& command : value.at("commands")) {
        batch.commands.push_back({
            .operation = parseOperation(command.at("operation").get<std::string>()),
            .id = command.at("id").get<std::string>(),
            .input_port = command.at("input_port").get<PortId>(),
            .output_port = command.at("output_port").get<PortId>(),
            .desired_version = command.at("desired_version").get<std::uint64_t>(),
        });
    }
    if (batch.commands.empty()) {
        throw std::invalid_argument("device command batch must not be empty");
    }
    return batch;
}

}  // namespace ocs
