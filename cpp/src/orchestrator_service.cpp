#include "ocs/orchestrator_service.hpp"

#include "ocs/connection_state_machine.hpp"
#include "ocs/device_command.hpp"
#include "ocs/redis_keys.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ocs {
namespace {

inline constexpr std::string_view kConsumerGroup = "ocs-orch";

std::uint64_t timestampNowNs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

ConnectionApplyStatus currentStatus(const std::map<std::string, std::string>& application) {
    const auto status = application.find("apply_status");
    return status == application.end() ? ConnectionApplyStatus::kAbsent
                                       : connectionApplyStatusFromString(status->second);
}

std::uint64_t currentVersion(const std::map<std::string, std::string>& application) {
    const auto version = application.find("desired_version");
    return version == application.end() ? std::uint64_t{} : std::stoull(version->second);
}

PortId decodePort(const nlohmann::json& payload, const char* field) {
    const auto& value = payload.at(field);
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

ConnectionCommand decodeConfigEvent(const redis::EventEnvelope& event) {
    if (event.resource_type != "connection") {
        throw std::invalid_argument("orch only supports connection configuration events");
    }
    if (event.desired_version == 0) {
        throw std::invalid_argument("connection desired version must be positive");
    }

    ConnectionCommand command{
        .id = event.resource_id,
        .desired_version = event.desired_version,
    };
    if (event.operation == "REMOVE") {
        command.operation = ConnectionOperation::kRemove;
        return command;
    }
    if (event.operation != "UPSERT") {
        throw std::invalid_argument("unsupported connection configuration operation");
    }

    const auto payload = nlohmann::json::parse(event.payload);
    command.input_port = decodePort(payload, "input_port");
    command.output_port = decodePort(payload, "output_port");
    return command;
}

DeviceCommandBatch decodeConfigBatchEvent(const redis::EventEnvelope& event) {
    if (event.resource_type == "connection") {
        return {.commands = {decodeConfigEvent(event)}, .options = {}};
    }
    if (event.resource_type != "connection-batch" || event.operation != "APPLY_BATCH") {
        throw std::invalid_argument("orch only supports connection configuration events");
    }
    if (event.desired_version == 0) {
        throw std::invalid_argument("connection batch desired version must be positive");
    }
    auto batch = decodeDeviceCommand(event.payload);
    for (const auto& command : batch.commands) {
        if (command.desired_version != event.desired_version) {
            throw std::invalid_argument("connection batch has inconsistent desired versions");
        }
    }
    return batch;
}

PortId snapshotPort(
    const std::map<std::string, std::string>& snapshot,
    const std::string& field) {
    const auto value = snapshot.find(field);
    if (value == snapshot.end()) {
        throw std::invalid_argument("connection snapshot is missing field: " + field);
    }
    const auto parsed = std::stoull(value->second);
    if (parsed == 0 || parsed > std::numeric_limits<PortId>::max()) {
        throw std::invalid_argument("connection snapshot has invalid port: " + field);
    }
    return static_cast<PortId>(parsed);
}

ConnectionCommand loadDesiredSnapshot(
    redis::RedisRepository& config_db,
    const redis::EventEnvelope& event) {
    const auto snapshot =
        config_db.getHash(redis::connectionConfigKey(event.device, event.resource_id));
    if (snapshot.empty()) {
        if (event.operation == "REMOVE") {
            return {
                .operation = ConnectionOperation::kRemove,
                .id = event.resource_id,
                .desired_version = event.desired_version,
            };
        }
        throw std::runtime_error("version gap cannot reload missing desired snapshot");
    }
    const auto version = snapshot.find("desired_version");
    if (version == snapshot.end()) {
        throw std::invalid_argument("connection snapshot is missing desired_version");
    }
    const auto desired_version = std::stoull(version->second);
    if (desired_version < event.desired_version) {
        throw std::invalid_argument("connection snapshot is older than configuration event");
    }
    return {
        .operation = ConnectionOperation::kUpsert,
        .id = event.resource_id,
        .input_port = snapshotPort(snapshot, "input_port"),
        .output_port = snapshotPort(snapshot, "output_port"),
        .desired_version = desired_version,
    };
}

ConnectionApplyStatus startApplyTransition(
    ConnectionApplyStatus current,
    ConnectionOperation operation) {
    const auto executing = operation == ConnectionOperation::kRemove
                               ? ConnectionApplyStatus::kRemoving
                               : ConnectionApplyStatus::kApplying;
    if (current == ConnectionApplyStatus::kApplying ||
        current == ConnectionApplyStatus::kRemoving) {
        return executing;
    }
    if (current == ConnectionApplyStatus::kFailed) {
        requireTransition(current, ConnectionApplyStatus::kRetryWait);
        requireTransition(ConnectionApplyStatus::kRetryWait, executing);
        return executing;
    }

    const auto pending = operation == ConnectionOperation::kRemove
                             ? ConnectionApplyStatus::kPendingDelete
                             : (current == ConnectionApplyStatus::kAbsent
                                    ? ConnectionApplyStatus::kPendingCreate
                                    : ConnectionApplyStatus::kPendingUpdate);
    requireTransition(current, pending);
    requireTransition(pending, executing);
    return executing;
}

std::string operationName(ConnectionOperation operation) {
    return operation == ConnectionOperation::kRemove ? "REMOVE" : "UPSERT";
}

}  // namespace

OrchestratorService::OrchestratorService(redis::RedisEndpoint endpoint)
    : config_db_(endpoint, redis::LogicalDb::kConfig),
      appl_db_(endpoint, redis::LogicalDb::kAppl),
      device_db_(std::move(endpoint), redis::LogicalDb::kDevice) {}

void OrchestratorService::initialize() {
    config_db_.createConsumerGroup(std::string(redis::kConfigEvents), std::string(kConsumerGroup));
    device_db_.createConsumerGroup(std::string(redis::kDeviceResults), std::string(kConsumerGroup));
}

bool OrchestratorService::processConfigOne(const std::string& consumer_name) {
    const auto messages = config_db_.readGroup(
        std::string(redis::kConfigEvents), std::string(kConsumerGroup), consumer_name, 1);
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    auto batch = decodeConfigBatchEvent(message.event);
    std::vector<ConnectionCommand> commands;
    commands.reserve(batch.commands.size());
    for (auto command : batch.commands) {
        const auto app_key = redis::connectionAppKey(message.event.device, command.id);
        const auto existing_application = appl_db_.getHash(app_key);
        const auto previous_version = currentVersion(existing_application);
        if (command.desired_version <= previous_version) {
            continue;
        }
        if (command.operation == ConnectionOperation::kRemove && existing_application.empty()) {
            continue;
        }
        if (message.event.resource_type == "connection" &&
            command.desired_version - previous_version > 1) {
            command = loadDesiredSnapshot(config_db_, message.event);
        }
        commands.push_back(std::move(command));
    }
    if (commands.empty()) {
        const auto acknowledged = config_db_.acknowledge(
            std::string(redis::kConfigEvents), std::string(kConsumerGroup), message.id);
        if (acknowledged != 1) {
            throw std::runtime_error("orch failed to acknowledge stale configuration event");
        }
        return true;
    }

    const auto command_id = message.event.event_id + ":command";
    for (const auto& command : commands) {
        const auto app_key = redis::connectionAppKey(message.event.device, command.id);
        const auto device_key = redis::connectionDeviceKey(message.event.device, command.id);
        const auto existing_application = appl_db_.getHash(app_key);
        const auto status = startApplyTransition(
            currentStatus(existing_application), command.operation);
        const std::map<std::string, std::string> application{
            {"device", message.event.device},
            {"id", command.id},
            {"input_port", std::to_string(command.input_port)},
            {"output_port", std::to_string(command.output_port)},
            {"desired_version", std::to_string(command.desired_version)},
            {"apply_status", std::string(toString(status))},
            {"operation", operationName(command.operation)},
            {"request_id", message.event.request_id},
            {"event_id", message.event.event_id},
            {"command_id", command_id},
            {"last_error_code", ""},
            {"last_error_message", ""},
        };
        appl_db_.putHash(app_key, application);
        device_db_.putHash(device_key, application);
    }

    batch.commands = std::move(commands);
    const redis::EventEnvelope command_event{
        .event_schema_version = 1,
        .event_id = command_id,
        .request_id = message.event.request_id,
        .timestamp_ns = timestampNowNs(),
        .device = message.event.device,
        .resource_type = batch.commands.size() == 1 ? "connection" : "connection-batch",
        .resource_id = batch.commands.size() == 1 ? batch.commands.front().id
                                                  : message.event.request_id,
        .operation = batch.commands.size() == 1
                         ? operationName(batch.commands.front().operation)
                         : "APPLY_BATCH",
        .desired_version = message.event.desired_version,
        .payload = encodeDeviceCommand(batch),
    };
    static_cast<void>(
        device_db_.appendEvent(std::string(redis::kDeviceCommands), command_event));

    const auto acknowledged = config_db_.acknowledge(
        std::string(redis::kConfigEvents), std::string(kConsumerGroup), message.id);
    if (acknowledged != 1) {
        throw std::runtime_error("orch failed to acknowledge processed configuration event");
    }
    return true;
}

bool OrchestratorService::processResultOne(const std::string& consumer_name) {
    const auto messages = device_db_.readGroup(
        std::string(redis::kDeviceResults), std::string(kConsumerGroup), consumer_name, 1);
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    const auto result = nlohmann::json::parse(message.event.payload);
    const auto success = result.at("success").get<bool>();
    const auto app_key = redis::connectionAppKey(message.event.device, message.event.resource_id);
    const auto device_key =
        redis::connectionDeviceKey(message.event.device, message.event.resource_id);
    auto application = appl_db_.getHash(app_key);
    if (application.empty()) {
        throw std::runtime_error("device result has no matching application state");
    }

    const auto application_version = currentVersion(application);
    if (message.event.desired_version <= application_version &&
        (message.event.desired_version < application_version ||
         currentStatus(application) == ConnectionApplyStatus::kActive ||
         currentStatus(application) == ConnectionApplyStatus::kAbsent ||
         currentStatus(application) == ConnectionApplyStatus::kFailed)) {
        const auto acknowledged = device_db_.acknowledge(
            std::string(redis::kDeviceResults), std::string(kConsumerGroup), message.id);
        if (acknowledged != 1) {
            throw std::runtime_error("orch failed to acknowledge stale device result");
        }
        return true;
    }
    if (message.event.desired_version > application_version) {
        throw std::runtime_error("device result is newer than application state");
    }

    const auto current = currentStatus(application);
    ConnectionApplyStatus next = ConnectionApplyStatus::kFailed;
    if (success) {
        next = current == ConnectionApplyStatus::kRemoving ? ConnectionApplyStatus::kAbsent
                                                           : ConnectionApplyStatus::kActive;
    }
    requireTransition(current, next);

    if (next == ConnectionApplyStatus::kAbsent) {
        application["apply_status"] = std::string(toString(next));
        application["last_error_code"] = "";
        application["last_error_message"] = "";
        appl_db_.putHash(app_key, application);
        static_cast<void>(device_db_.deleteKey(device_key));
    } else {
        application["apply_status"] = std::string(toString(next));
        application["last_error_code"] = result.at("error_code").get<std::string>();
        application["last_error_message"] = result.at("error_message").get<std::string>();
        appl_db_.putHash(app_key, application);
        device_db_.putHash(device_key, application);
    }

    const auto acknowledged = device_db_.acknowledge(
        std::string(redis::kDeviceResults), std::string(kConsumerGroup), message.id);
    if (acknowledged != 1) {
        throw std::runtime_error("orch failed to acknowledge processed device result");
    }
    return true;
}

}  // namespace ocs
