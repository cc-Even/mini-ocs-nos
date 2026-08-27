#include "ocs/syncd_service.hpp"

#include "ocs/connection_state_machine.hpp"
#include "ocs/device_command.hpp"
#include "ocs/redis_keys.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ocs {
namespace {

inline constexpr std::string_view kConsumerGroup = "ocs-syncd";
inline constexpr std::string_view kFaultConsumerGroup = "ocs-syncd-faults";

std::uint64_t timestampNowNs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::string resultPayload(const ApplyResult& result, std::string_view command_id) {
    return nlohmann::json{
        {"success", result.ok()},
        {"error_code", toString(result.error.code)},
        {"error_message", result.error.message},
        {"command_id", command_id},
    }.dump();
}

std::string durableResultPayload(const ApplyResult& result) {
    nlohmann::json connections = nlohmann::json::array();
    for (const auto& connection : result.connections) {
        connections.push_back({
            {"id", connection.id},
            {"input_port", connection.input_port},
            {"output_port", connection.output_port},
            {"applied_version", connection.applied_version},
        });
    }
    return nlohmann::json{
        {"success", result.ok()},
        {"error_code", toString(result.error.code)},
        {"error_message", result.error.message},
        {"connections", std::move(connections)},
    }.dump();
}

ApplyResult decodeDurableResult(const std::string& payload) {
    const auto value = nlohmann::json::parse(payload);
    ApplyResult result;
    result.error = {
        errorCodeFromString(value.at("error_code").get<std::string>()),
        value.at("error_message").get<std::string>(),
    };
    for (const auto& connection : value.at("connections")) {
        result.connections.push_back({
            .id = connection.at("id").get<std::string>(),
            .input_port = connection.at("input_port").get<PortId>(),
            .output_port = connection.at("output_port").get<PortId>(),
            .applied_version = connection.at("applied_version").get<std::uint64_t>(),
        });
    }
    return result;
}

DeviceCommandBatch recoverPreparedBatch(
    redis::RedisRepository& device_db,
    const redis::EventEnvelope& command_event) {
    constexpr std::string_view suffix = ":command";
    if (!command_event.event_id.ends_with(suffix)) {
        return {};
    }
    const auto config_event_id = command_event.event_id.substr(
        0, command_event.event_id.size() - suffix.size());
    const auto prepared = device_db.getHash(redis::orchConfigBatchKey(config_event_id));
    const auto payload = prepared.find("payload");
    if (payload == prepared.end()) {
        return {};
    }
    return decodeDeviceCommand(payload->second);
}

std::string statePayload(const std::map<std::string, std::string>& fields) {
    return nlohmann::json(fields).dump();
}

PortId snapshotPort(
    const std::map<std::string, std::string>& snapshot,
    const std::string& field) {
    const auto value = snapshot.find(field);
    if (value == snapshot.end()) {
        throw std::invalid_argument("device connection snapshot is missing " + field);
    }
    const auto parsed = std::stoull(value->second);
    if (parsed == 0 || parsed > std::numeric_limits<PortId>::max()) {
        throw std::invalid_argument("device connection snapshot has invalid " + field);
    }
    return static_cast<PortId>(parsed);
}

bool isGenerationRecovery(std::string_view command_id) {
    return command_id.starts_with("hwsim-recovery:");
}

bool isReconciliation(std::string_view command_id) {
    return command_id.starts_with("reconcile:");
}

std::string_view portDirectionName(PortDirection direction) {
    return direction == PortDirection::kInput ? "input" : "output";
}

std::string_view portResourceType(PortDirection direction) {
    return direction == PortDirection::kInput ? "input-port" : "output-port";
}

std::string_view portOperStatusName(PortOperStatus status) {
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

bool portUnavailable(const PortState& port) {
    return !port.admin_enabled || port.oper_status != PortOperStatus::kUp;
}

FaultType managedFaultType(std::string_view value) {
    if (value == "NEXT_APPLY_ERROR") {
        return FaultType::kNextApplyError;
    }
    if (value == "NEXT_APPLY_TIMEOUT") {
        return FaultType::kNextApplyTimeout;
    }
    if (value == "INPUT_PORT_DOWN") {
        return FaultType::kInputPortDown;
    }
    if (value == "OUTPUT_PORT_DOWN") {
        return FaultType::kOutputPortDown;
    }
    if (value == "ALL") {
        return FaultType::kAll;
    }
    throw std::invalid_argument("unsupported managed fault type");
}

}  // namespace

SyncdService::SyncdService(
    redis::RedisEndpoint endpoint,
    std::unique_ptr<OcsDeviceApi> device,
    std::chrono::milliseconds pending_min_idle)
    : device_db_(endpoint, redis::LogicalDb::kDevice),
      state_db_(endpoint, redis::LogicalDb::kState),
      counters_db_(endpoint, redis::LogicalDb::kCounters),
      config_db_(endpoint, redis::LogicalDb::kConfig),
      alarm_db_(std::move(endpoint), redis::LogicalDb::kAlarm),
      device_(std::move(device)),
      pending_min_idle_(pending_min_idle) {
    if (!device_) {
        throw std::invalid_argument("syncd device backend must not be null");
    }
    if (pending_min_idle_.count() < 0) {
        throw std::invalid_argument("syncd pending minimum idle time must not be negative");
    }
}

void SyncdService::initialize() {
    device_db_.createConsumerGroup(std::string(redis::kDeviceCommands), std::string(kConsumerGroup));
    device_db_.createConsumerGroup(
        std::string(redis::kFaultCommands), std::string(kFaultConsumerGroup));
    const auto info = device_->getDeviceInfo();
    const std::map<std::string, std::string> inventory{
        {"name", info.name},
        {"input_port_count", std::to_string(info.input_port_count)},
        {"output_port_count", std::to_string(info.output_port_count)},
        {"admin_status", "ENABLED"},
        {"model", info.model},
        {"serial_number", info.serial_number},
        {"firmware_version", info.firmware_version},
    };
    const auto key = redis::deviceConfigKey(info.name);
    static_cast<void>(config_db_.putHashIfAbsent(key, inventory));
    const auto configured = config_db_.getHash(key);
    if (configured.find("name") == configured.end() || configured.at("name") != info.name ||
        configured.find("input_port_count") == configured.end() ||
        configured.at("input_port_count") != std::to_string(info.input_port_count) ||
        configured.find("output_port_count") == configured.end() ||
        configured.at("output_port_count") != std::to_string(info.output_port_count)) {
        throw std::runtime_error("configured device inventory does not match backend identity");
    }
    device_name_ = info.name;
    device_generation_ = info.generation;
    counters_db_.ensureHashFields(
        redis::deviceCountersKey(info.name),
        {
            {"active_alarms", "0"},
            {"active_connections", "0"},
            {"config_rejected_total", "0"},
            {"config_requests_total", "0"},
            {"device_apply_failure_total", "0"},
            {"device_apply_success_total", "0"},
            {"device_apply_timeout_total", "0"},
            {"device_apply_total", "0"},
            {"drift_detected_total", "0"},
            {"last_apply_latency_ms", "0"},
            {"max_apply_latency_ms", "0"},
            {"port_down_total", "0"},
            {"reconciliation_success_total", "0"},
            {"reconciliation_total", "0"},
    });
    const auto actual = device_->getConnections();
    static_cast<void>(pollPortStates(info, actual));
    publishHwsimHeartbeat(true);
    const auto existing_state = state_db_.getHash(redis::deviceStateKey(info.name));
    if (existing_state.empty()) {
        state_db_.putHash(
            redis::deviceStateKey(info.name),
            {
                {"name", info.name},
                {"oper_status", "READY"},
                {"device_generation", std::to_string(info.generation)},
                {"actual_connection_count", std::to_string(actual.size())},
                {"last_error_code", ""},
                {"last_error_message", ""},
            });
    } else {
        const auto generation = existing_state.find("device_generation");
        if (generation == existing_state.end() ||
            std::stoull(generation->second) != info.generation) {
            scheduleGenerationRecovery(info, actual);
            publishDeviceState(info, actual.size(), "READY", Error::success());
        } else if (existing_state.find("oper_status") == existing_state.end() ||
                   existing_state.at("oper_status") != "READY") {
            publishDeviceState(info, actual.size(), "READY", Error::success());
        }
    }
}

bool SyncdService::processFaultOne(const std::string& consumer_name) {
    auto messages = device_db_.claimPending(
        std::string(redis::kFaultCommands),
        std::string(kFaultConsumerGroup),
        consumer_name,
        pending_min_idle_,
        1);
    if (messages.empty()) {
        messages = device_db_.readGroupIfNoPending(
            std::string(redis::kFaultCommands),
            std::string(kFaultConsumerGroup),
            consumer_name);
    }
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    const auto result_key = redis::faultResultKey(message.event.event_id);
    auto result_fields = device_db_.getHash(result_key);
    if (result_fields.empty()) {
        FaultResult result;
        try {
            if (message.event.resource_type != "fault" ||
                (message.event.operation != "INJECT" && message.event.operation != "CLEAR")) {
                throw std::invalid_argument("invalid fault command envelope");
            }
            const auto payload = nlohmann::json::parse(message.event.payload);
            const auto operation = payload.at("operation").get<std::string>();
            if (operation != message.event.operation) {
                throw std::invalid_argument("fault payload operation does not match envelope");
            }
            const auto type = managedFaultType(payload.at("fault_type").get<std::string>());
            const auto raw_port = payload.at("port_id").get<std::uint64_t>();
            if (raw_port > std::numeric_limits<PortId>::max()) {
                throw std::invalid_argument("fault port is outside the supported range");
            }
            if ((type == FaultType::kInputPortDown || type == FaultType::kOutputPortDown) &&
                raw_port == 0) {
                throw std::invalid_argument("port fault requires a positive port");
            }
            if (type == FaultType::kAll && operation != "CLEAR") {
                throw std::invalid_argument("ALL is only valid for clear");
            }
            const auto info = device_->getDeviceInfo();
            if (info.name != message.event.device) {
                result.error = {
                    ErrorCode::kDeviceNotReady,
                    "fault device does not match the connected backend",
                };
            } else {
                const FaultSpec fault{
                    .type = type,
                    .port_id = static_cast<PortId>(raw_port),
                };
                result = operation == "INJECT"
                             ? device_->injectFault(fault)
                             : device_->clearFault({.type = fault.type, .port_id = fault.port_id});
            }
        } catch (const nlohmann::json::exception& error) {
            result.error = {ErrorCode::kProtocolMalformed, error.what()};
        } catch (const std::invalid_argument& error) {
            result.error = {ErrorCode::kInvalidArgument, error.what()};
        } catch (const std::exception& error) {
            result.error = {ErrorCode::kDeviceNotReady, error.what()};
        }
        static_cast<void>(device_db_.putHashIfAbsent(
            result_key,
            {
                {"command_id", message.event.event_id},
                {"device", message.event.device},
                {"success", result.error.ok() ? "true" : "false"},
                {"error_code", std::string(toString(result.error.code))},
                {"error_message", result.error.message},
                {"completed_at_ns", std::to_string(timestampNowNs())},
            }));
    }

    const auto acknowledged = device_db_.acknowledge(
        std::string(redis::kFaultCommands),
        std::string(kFaultConsumerGroup),
        message.id);
    if (acknowledged != 1) {
        throw std::runtime_error("syncd failed to acknowledge processed fault command");
    }
    return true;
}

bool SyncdService::pollDevice() {
    try {
        const auto info = device_->getDeviceInfo();
        if (!device_name_.empty() && info.name != device_name_) {
            throw std::runtime_error("connected device identity changed after handshake");
        }
        const auto actual = device_->getConnections();
        const auto effective_actual = pollPortStates(info, actual);
        const auto state = state_db_.getHash(redis::deviceStateKey(info.name));
        const auto generation = state.find("device_generation");
        const bool generation_changed = generation == state.end() ||
                                        std::stoull(generation->second) != info.generation;
        const auto status = state.find("oper_status");
        const auto actual_count = state.find("actual_connection_count");
        const bool actual_count_changed =
            actual_count == state.end() || std::stoull(actual_count->second) != actual.size();
        device_name_ = info.name;
        device_generation_ = info.generation;
        if (generation_changed) {
            scheduleGenerationRecovery(info, actual);
        } else {
            const auto generation_recovery = device_db_.getHash(
                redis::syncdGenerationRecoveryKey(info.name, info.generation));
            const bool recovery_pending =
                generation_recovery.contains("recovery_required") &&
                generation_recovery.at("recovery_required") == "true" &&
                generation_recovery.contains("command_id") &&
                device_db_
                    .getHash(redis::processedDeviceCommandKey(
                        generation_recovery.at("command_id")))
                    .empty();
            if (!recovery_pending) {
                reconcileDevice(info, effective_actual);
            }
        }
        if (generation_changed || actual_count_changed || status == state.end() ||
            status->second != "READY") {
            publishDeviceState(info, actual.size(), "READY", Error::success());
        }
        publishHwsimHeartbeat(true);
        return true;
    } catch (const std::exception& error) {
        if (device_name_.empty()) {
            return false;
        }
        const auto state = state_db_.getHash(redis::deviceStateKey(device_name_));
        const auto status = state.find("oper_status");
        if (status == state.end() || status->second != "FAILED") {
            const DeviceInfo unavailable{
                .name = device_name_,
                .input_port_count = 0,
                .output_port_count = 0,
                .model = "",
                .serial_number = "",
                .firmware_version = "",
                .generation = device_generation_,
            };
            publishDeviceState(
                unavailable,
                state.contains("actual_connection_count")
                    ? std::stoull(state.at("actual_connection_count"))
                    : 0,
                "FAILED",
                {ErrorCode::kDeviceNotReady, error.what()});
        }
        publishHwsimHeartbeat(false);
        return false;
    }
}

bool SyncdService::pollDeviceLiveness() {
    try {
        const auto info = device_->getDeviceInfo();
        if (!device_name_.empty() && info.name != device_name_) {
            throw std::runtime_error("connected device identity changed after handshake");
        }
        publishHwsimHeartbeat(true);
        return true;
    } catch (const std::exception&) {
        publishHwsimHeartbeat(false);
        return false;
    }
}

void SyncdService::publishHwsimHeartbeat(bool online) {
    state_db_.putHash(
        redis::serviceStateKey("ocs-hwsim"),
        {
            {"service", "ocs-hwsim"},
            {"status", online ? "ONLINE" : "OFFLINE"},
            {"last_seen_ns", std::to_string(timestampNowNs())},
        });
}

void SyncdService::publishDeviceState(
    const DeviceInfo& info,
    std::size_t actual_connection_count,
    std::string_view status,
    const Error& error) {
    const std::map<std::string, std::string> fields{
        {"name", info.name},
        {"oper_status", std::string(status)},
        {"device_generation", std::to_string(info.generation)},
        {"actual_connection_count", std::to_string(actual_connection_count)},
        {"last_error_code", error.ok() ? "" : std::string(toString(error.code))},
        {"last_error_message", error.message},
    };
    const auto timestamp = timestampNowNs();
    const redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = "syncd-device:" + info.name + ":" + std::to_string(info.generation) + ":" +
                    std::string(status) + ":" + std::to_string(timestamp),
        .request_id = "syncd-device-health:" + info.name,
        .timestamp_ns = timestamp,
        .device = info.name,
        .resource_type = "device",
        .resource_id = info.name,
        .operation = "UPSERT",
        .desired_version = 0,
        .payload = statePayload(fields),
    };
    state_db_.replaceHashAndAppendEvent(
        redis::deviceStateKey(info.name), fields, std::string(redis::kStateEvents), event);
}

void SyncdService::scheduleGenerationRecovery(
    const DeviceInfo& info,
    const std::vector<AppliedConnection>& actual) {
    const auto recovery_key = redis::syncdGenerationRecoveryKey(info.name, info.generation);
    if (!device_db_.getHash(recovery_key).empty()) {
        return;
    }

    std::unordered_map<std::string, AppliedConnection> actual_by_id;
    for (const auto& connection : actual) {
        actual_by_id.emplace(connection.id, connection);
    }

    DeviceCommandBatch batch;
    batch.options.atomic = true;
    batch.options.timeout = std::chrono::milliseconds(1000);
    long long active_delta = 0;
    const auto keys = device_db_.scanKeys(redis::connectionDevicePattern(info.name));
    for (const auto& key : keys) {
        const auto snapshot = device_db_.getHash(key);
        if (snapshot.empty() || !snapshot.contains("id") ||
            !snapshot.contains("desired_version")) {
            continue;
        }
        ConnectionCommand command{
            .id = snapshot.at("id"),
            .desired_version = std::stoull(snapshot.at("desired_version")),
        };
        const auto operation = snapshot.find("operation");
        if (operation != snapshot.end() && operation->second == "REMOVE") {
            command.operation = ConnectionOperation::kRemove;
        } else {
            command.input_port = snapshotPort(snapshot, "input_port");
            command.output_port = snapshotPort(snapshot, "output_port");
        }

        const auto found = actual_by_id.find(command.id);
        const bool matches = command.operation == ConnectionOperation::kRemove
                                 ? found == actual_by_id.end()
                                 : found != actual_by_id.end() &&
                                       found->second.input_port == command.input_port &&
                                       found->second.output_port == command.output_port &&
                                       found->second.applied_version == command.desired_version;

        const auto state_key = redis::connectionStateKey(info.name, command.id);
        const auto previous = state_db_.getHash(state_key);
        const bool was_active = previous.contains("actual_present")
                                    ? previous.at("actual_present") == "true"
                                    : previous.contains("applied_version") &&
                                          previous.at("applied_version") != "0";
        std::map<std::string, std::string> refreshed;
        std::string state_operation = "UPSERT";
        const bool is_active = found != actual_by_id.end();
        if (command.operation == ConnectionOperation::kRemove && !is_active) {
            state_operation = "REMOVE";
        } else if (is_active) {
            refreshed = {
                {"device", info.name},
                {"id", command.id},
                {"input_port", std::to_string(found->second.input_port)},
                {"output_port", std::to_string(found->second.output_port)},
                {"desired_version", std::to_string(command.desired_version)},
                {"applied_version", std::to_string(found->second.applied_version)},
                {"actual_present", "true"},
                {"apply_status", matches ? "ACTIVE" : "FAILED"},
                {"last_error_code", matches ? "" : std::string(toString(ErrorCode::kApplyFailed))},
                {"last_error_message", matches ? "" : "actual state differs after device restart"},
            };
        } else {
            refreshed = {
                {"device", info.name},
                {"id", command.id},
                {"input_port", std::to_string(command.input_port)},
                {"output_port", std::to_string(command.output_port)},
                {"desired_version", std::to_string(command.desired_version)},
                {"applied_version", "0"},
                {"actual_present", "false"},
                {"apply_status", "FAILED"},
                {"last_error_code", std::string(toString(ErrorCode::kDeviceNotReady))},
                {"last_error_message", "actual state was lost when the device generation changed"},
            };
        }
        const auto event_id = "hwsim-recovery:" + info.name + ":" +
                              std::to_string(info.generation) + ":actual:" + command.id;
        const redis::EventEnvelope state_event{
            .event_schema_version = 1,
            .event_id = event_id,
            .request_id = "hwsim-recovery:" + info.name + ":" +
                          std::to_string(info.generation),
            .timestamp_ns = timestampNowNs(),
            .device = info.name,
            .resource_type = "connection",
            .resource_id = command.id,
            .operation = state_operation,
            .desired_version = command.desired_version,
            .payload = statePayload(refreshed),
        };
        const auto publication_key = redis::syncdGenerationStatePublicationKey(
            info.name, info.generation, command.id);
        const auto command_delta = was_active == is_active ? 0 : (is_active ? 1 : -1);
        const auto published = state_db_.replaceHashAndAppendEventOnce(
            publication_key,
            {
                {"device_generation", std::to_string(info.generation)},
                {"connection_id", command.id},
                {"active_delta", std::to_string(command_delta)},
            },
            state_key,
            refreshed,
            std::string(redis::kStateEvents),
            state_event);
        active_delta += published
                            ? command_delta
                            : std::stoll(state_db_.getHash(publication_key).at("active_delta"));
        if (!matches) {
            batch.commands.push_back(command);
        }
        actual_by_id.erase(command.id);
    }
    for (const auto& [id, connection] : actual_by_id) {
        const auto state_key = redis::connectionStateKey(info.name, id);
        const auto previous = state_db_.getHash(state_key);
        const bool was_active = previous.contains("actual_present") &&
                                previous.at("actual_present") == "true";
        const std::map<std::string, std::string> refreshed{
            {"device", info.name},
            {"id", id},
            {"input_port", std::to_string(connection.input_port)},
            {"output_port", std::to_string(connection.output_port)},
            {"desired_version", std::to_string(connection.applied_version)},
            {"applied_version", std::to_string(connection.applied_version)},
            {"actual_present", "true"},
            {"apply_status", "FAILED"},
            {"last_error_code", std::string(toString(ErrorCode::kApplyFailed))},
            {"last_error_message", "unexpected actual state after device restart"},
        };
        const auto event_id = "hwsim-recovery:" + info.name + ":" +
                              std::to_string(info.generation) + ":actual:" + id;
        const redis::EventEnvelope state_event{
            .event_schema_version = 1,
            .event_id = event_id,
            .request_id = "hwsim-recovery:" + info.name + ":" +
                          std::to_string(info.generation),
            .timestamp_ns = timestampNowNs(),
            .device = info.name,
            .resource_type = "connection",
            .resource_id = id,
            .operation = "UPSERT",
            .desired_version = connection.applied_version,
            .payload = statePayload(refreshed),
        };
        const auto publication_key = redis::syncdGenerationStatePublicationKey(
            info.name, info.generation, id);
        const auto published = state_db_.replaceHashAndAppendEventOnce(
            publication_key,
            {
                {"device_generation", std::to_string(info.generation)},
                {"connection_id", id},
                {"active_delta", was_active ? "0" : "1"},
            },
            state_key,
            refreshed,
            std::string(redis::kStateEvents),
            state_event);
        active_delta += published ? (was_active ? 0 : 1)
                                  : std::stoll(state_db_.getHash(publication_key).at("active_delta"));
        batch.commands.push_back({
            .operation = ConnectionOperation::kRemove,
            .id = id,
            .desired_version = std::max<std::uint64_t>(connection.applied_version, 1),
        });
    }
    static_cast<void>(counters_db_.incrementHashFieldsOnce(
        redis::syncdGenerationCountersPublicationKey(info.name, info.generation),
        {{"device_generation", std::to_string(info.generation)}},
        redis::deviceCountersKey(info.name),
        {{"active_connections", active_delta}}));
    std::ranges::sort(batch.commands, {}, &ConnectionCommand::id);

    if (batch.commands.empty()) {
        static_cast<void>(device_db_.putHashIfAbsent(
            recovery_key,
            {
                {"device", info.name},
                {"device_generation", std::to_string(info.generation)},
                {"recovery_required", "false"},
            }));
        return;
    }

    const auto command_id = "hwsim-recovery:" + info.name + ":" +
                            std::to_string(info.generation) + ":command";
    batch.options.operation_id = command_id;
    const auto payload = encodeDeviceCommand(batch);
    const auto prepared_id = command_id.substr(0, command_id.size() - std::string(":command").size());
    device_db_.putHash(
        redis::orchConfigBatchKey(prepared_id),
        {
            {"payload", payload},
            {"device_generation", std::to_string(info.generation)},
        });
    std::uint64_t desired_version = 1;
    for (const auto& command : batch.commands) {
        desired_version = std::max(desired_version, command.desired_version);
    }
    const redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = command_id,
        .request_id = "hwsim-recovery:" + info.name + ":" + std::to_string(info.generation),
        .timestamp_ns = timestampNowNs(),
        .device = info.name,
        .resource_type = batch.commands.size() == 1 ? "connection" : "connection-batch",
        .resource_id = batch.commands.size() == 1 ? batch.commands.front().id : info.name,
        .operation = "GENERATION_RECOVERY",
        .desired_version = desired_version,
        .payload = payload,
    };
    static_cast<void>(device_db_.appendEventOnce(
        recovery_key,
        {
            {"device", info.name},
            {"device_generation", std::to_string(info.generation)},
            {"command_id", command_id},
            {"recovery_required", "true"},
        },
        std::string(redis::kDeviceCommands),
        event));
}

std::vector<ConnectionCommand> SyncdService::loadDesiredConnections(std::string_view device) {
    std::vector<ConnectionCommand> desired;
    const auto keys = config_db_.scanKeys(redis::connectionConfigPattern(device));
    desired.reserve(keys.size());
    for (const auto& key : keys) {
        const auto snapshot = config_db_.getHash(key);
        if (snapshot.empty()) {
            continue;
        }
        const auto separator = key.rfind('|');
        const auto id = snapshot.contains("id")
                            ? snapshot.at("id")
                            : key.substr(separator == std::string::npos ? 0 : separator + 1);
        if (id.empty() || !snapshot.contains("desired_version")) {
            throw std::invalid_argument("desired connection snapshot is missing identity or version");
        }
        desired.push_back({
            .id = id,
            .input_port = snapshotPort(snapshot, "input_port"),
            .output_port = snapshotPort(snapshot, "output_port"),
            .desired_version = std::stoull(snapshot.at("desired_version")),
        });
    }
    return desired;
}

bool SyncdService::orchestrationSettled(
    std::string_view device,
    const std::vector<ConnectionCommand>& desired) {
    std::unordered_map<std::string, ConnectionCommand> desired_by_id;
    for (const auto& command : desired) {
        desired_by_id.emplace(command.id, command);
    }
    const auto keys = device_db_.scanKeys(redis::connectionDevicePattern(device));
    for (const auto& key : keys) {
        const auto snapshot = device_db_.getHash(key);
        if (snapshot.empty() || !snapshot.contains("id") ||
            !snapshot.contains("desired_version") || !snapshot.contains("apply_status")) {
            return false;
        }
        const auto desired_item = desired_by_id.find(snapshot.at("id"));
        if (desired_item == desired_by_id.end()) {
            const auto operation = snapshot.find("operation");
            if (operation == snapshot.end() || operation->second != "REMOVE" ||
                snapshot.at("apply_status") != "FAILED") {
                return false;
            }
            continue;
        }
        const auto& command = desired_item->second;
        const auto status = snapshot.at("apply_status");
        if ((status != "ACTIVE" && status != "FAILED") ||
            std::stoull(snapshot.at("desired_version")) != command.desired_version ||
            snapshotPort(snapshot, "input_port") != command.input_port ||
            snapshotPort(snapshot, "output_port") != command.output_port) {
            return false;
        }
        desired_by_id.erase(desired_item);
    }
    return desired_by_id.empty();
}

std::vector<AppliedConnection> SyncdService::pollPortStates(
    const DeviceInfo& info,
    const std::vector<AppliedConnection>& actual) {
    std::unordered_set<PortId> unavailable_inputs;
    std::unordered_set<PortId> unavailable_outputs;
    const auto poll_direction = [&](PortDirection direction, PortId count) {
        for (PortId id = 1; id <= count; ++id) {
            const auto port = direction == PortDirection::kInput
                                  ? device_->getInputPortState(id)
                                  : device_->getOutputPortState(id);
            const auto affected = static_cast<std::size_t>(std::ranges::count_if(
                actual,
                [&port](const AppliedConnection& connection) {
                    return port.direction == PortDirection::kInput
                               ? connection.input_port == port.id
                               : connection.output_port == port.id;
                }));
            publishPortState(info, port, affected);
            if (portUnavailable(port)) {
                (direction == PortDirection::kInput ? unavailable_inputs
                                                    : unavailable_outputs)
                    .insert(id);
            }
        }
    };
    poll_direction(PortDirection::kInput, info.input_port_count);
    poll_direction(PortDirection::kOutput, info.output_port_count);

    std::vector<AppliedConnection> effective;
    effective.reserve(actual.size());
    std::ranges::copy_if(actual, std::back_inserter(effective), [&](const auto& connection) {
        return !unavailable_inputs.contains(connection.input_port) &&
               !unavailable_outputs.contains(connection.output_port);
    });
    return effective;
}

void SyncdService::publishPortState(
    const DeviceInfo& info,
    const PortState& port,
    std::size_t affected_connections) {
    const auto direction = std::string(portDirectionName(port.direction));
    const auto port_id = std::to_string(port.id);
    const auto state_key = port.direction == PortDirection::kInput
                               ? redis::inputPortStateKey(info.name, port_id)
                               : redis::outputPortStateKey(info.name, port_id);
    const auto previous = state_db_.getHash(state_key);
    std::map<std::string, std::string> fields{
        {"device", info.name},
        {"id", port_id},
        {"direction", direction},
        {"admin_enabled", port.admin_enabled ? "true" : "false"},
        {"oper_status", std::string(portOperStatusName(port.oper_status))},
        {"optical_power_dbm", std::to_string(port.optical_power_dbm)},
    };
    const bool changed = previous.empty() ||
                         std::ranges::any_of(fields, [&previous](const auto& field) {
                             const auto found = previous.find(field.first);
                             return found == previous.end() || found->second != field.second;
                         });
    if (changed) {
        const auto timestamp = timestampNowNs();
        fields["last_change_ns"] = std::to_string(timestamp);
        if (previous.empty()) {
            state_db_.putHash(state_key, fields);
        } else {
            const redis::EventEnvelope event{
                .event_schema_version = 1,
                .event_id = "syncd-port:" + info.name + ":" + direction + ":" + port_id + ":" +
                            std::to_string(timestamp),
                .request_id = "syncd-port-health:" + info.name,
                .timestamp_ns = timestamp,
                .device = info.name,
                .resource_type = std::string(portResourceType(port.direction)),
                .resource_id = port_id,
                .operation = "UPSERT",
                .desired_version = 0,
                .payload = statePayload(fields),
            };
            state_db_.replaceHashAndAppendEvent(
                state_key, fields, std::string(redis::kStateEvents), event);
        }
    }

    const auto fault_key = redis::syncdPortFaultKey(info.name, direction, port_id);
    auto fault = device_db_.getHash(fault_key);
    if (portUnavailable(port)) {
        if (fault.empty() || !fault.contains("status") || fault.at("status") == "RECOVERED") {
            const auto activation_id = "port-fault:" + info.name + ":" + direction + ":" +
                                       port_id + ":" + std::to_string(timestampNowNs());
            fault = {
                {"device", info.name},
                {"direction", direction},
                {"port_id", port_id},
                {"activation_id", activation_id},
                {"status", "ACTIVE"},
                {"raised_at_ns", std::to_string(timestampNowNs())},
            };
            device_db_.putHash(fault_key, fault);
        }
        publishPortAlarm(info, port, fault.at("activation_id"), affected_connections);
        return;
    }
    if (fault.empty() || !fault.contains("status") || fault.at("status") == "RECOVERED") {
        return;
    }

    clearPortAlarm(info, port, fault.at("activation_id"));
    fault["cleared_at_ns"] = std::to_string(timestampNowNs());
    if (fault.at("status") == "ACTIVE") {
        fault["status"] = "CLEARED";
    } else if (fault.at("status") == "RECOVERY_PUBLISHED" &&
               fault.contains("command_id")) {
        const auto processed = device_db_.getHash(
            redis::processedDeviceCommandKey(fault.at("command_id")));
        if (!processed.empty()) {
            fault["status"] = processed.contains("applied") &&
                                      processed.at("applied") == "true"
                                  ? "RECOVERED"
                                  : "CLEARED";
        } else {
            fault["cleared"] = "true";
        }
    }
    device_db_.putHash(fault_key, fault);
}

void SyncdService::publishPortAlarm(
    const DeviceInfo& info,
    const PortState& port,
    std::string_view activation_id,
    std::size_t affected_connections) {
    const auto direction = std::string(portDirectionName(port.direction));
    const auto port_id = std::to_string(port.id);
    const auto alarm_id = redis::portDownAlarmId(direction, port_id);
    const auto alarm_key = redis::activeAlarmKey(info.name, alarm_id);
    const auto marker_key = redis::syncdPortAlarmPublicationKey(activation_id);
    if (alarm_db_.getHash(marker_key).empty()) {
        const auto now = timestampNowNs();
        const std::map<std::string, std::string> fields{
            {"id", alarm_id},
            {"active", "true"},
            {"severity", "MAJOR"},
            {"resource_type", std::string(portResourceType(port.direction))},
            {"resource_id", port_id},
            {"direction", direction},
            {"port_id", port_id},
            {"affected_connection_count", std::to_string(affected_connections)},
            {"error_code", std::string(toString(port.admin_enabled ? ErrorCode::kPortDown
                                                                    : ErrorCode::kPortDisabled))},
            {"error_message", "port is not operationally available"},
            {"activation_id", std::string(activation_id)},
            {"first_raised_ns", std::to_string(now)},
            {"last_change_ns", std::to_string(now)},
        };
        const redis::EventEnvelope event{
            .event_schema_version = 1,
            .event_id = std::string(activation_id) + ":alarm",
            .request_id = std::string(activation_id),
            .timestamp_ns = now,
            .device = info.name,
            .resource_type = "alarm",
            .resource_id = alarm_id,
            .operation = "UPSERT",
            .desired_version = 0,
            .payload = statePayload(fields),
        };
        static_cast<void>(alarm_db_.replaceHashAndAppendEventOnce(
            marker_key,
            {{"activation_id", std::string(activation_id)}},
            alarm_key,
            fields,
            std::string(redis::kAlarmEvents),
            event));
    }
    static_cast<void>(counters_db_.incrementHashFieldsOnce(
        redis::syncdPortAlarmCounterPublicationKey(activation_id, false),
        {{"activation_id", std::string(activation_id)}},
        redis::deviceCountersKey(info.name),
        {{"active_alarms", 1}, {"port_down_total", 1}}));
}

void SyncdService::clearPortAlarm(
    const DeviceInfo& info,
    const PortState& port,
    std::string_view activation_id) {
    const auto direction = std::string(portDirectionName(port.direction));
    const auto port_id = std::to_string(port.id);
    const auto alarm_id = redis::portDownAlarmId(direction, port_id);
    const auto alarm_key = redis::activeAlarmKey(info.name, alarm_id);
    const auto marker_key = redis::syncdPortAlarmClearPublicationKey(activation_id);
    auto marker = alarm_db_.getHash(marker_key);
    if (marker.empty()) {
        auto active = alarm_db_.getHash(alarm_key);
        if (active.empty()) {
            return;
        }
        const auto now = timestampNowNs();
        active["active"] = "false";
        active["last_change_ns"] = std::to_string(now);
        const redis::EventEnvelope event{
            .event_schema_version = 1,
            .event_id = std::string(activation_id) + ":alarm-clear",
            .request_id = std::string(activation_id),
            .timestamp_ns = now,
            .device = info.name,
            .resource_type = "alarm",
            .resource_id = alarm_id,
            .operation = "REMOVE",
            .desired_version = 0,
            .payload = statePayload(active),
        };
        static_cast<void>(alarm_db_.replaceHashAndAppendEventOnce(
            marker_key,
            {{"activation_id", std::string(activation_id)}},
            alarm_key,
            {},
            std::string(redis::kAlarmEvents),
            event));
        marker = alarm_db_.getHash(marker_key);
    }
    if (!marker.empty()) {
        static_cast<void>(counters_db_.incrementHashFieldsOnce(
            redis::syncdPortAlarmCounterPublicationKey(activation_id, true),
            {{"activation_id", std::string(activation_id)}},
            redis::deviceCountersKey(info.name),
            {{"active_alarms", -1}}));
    }
}

std::vector<std::pair<std::string, std::map<std::string, std::string>>>
SyncdService::portFaults(std::string_view device) {
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> faults;
    for (const auto& key : device_db_.scanKeys(redis::syncdPortFaultPattern(device))) {
        auto fields = device_db_.getHash(key);
        if (!fields.empty()) {
            faults.emplace_back(key, std::move(fields));
        }
    }
    return faults;
}

void SyncdService::associatePortFaults(
    const DeviceInfo& info,
    std::string_view command_id) {
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> updates;
    for (auto& [key, fault] : portFaults(info.name)) {
        if (!fault.contains("status") ||
            (fault.at("status") != "ACTIVE" && fault.at("status") != "CLEARED")) {
            continue;
        }
        fault["status"] = "RECOVERY_PUBLISHED";
        fault["command_id"] = std::string(command_id);
        fault["recovery_published_at_ns"] = std::to_string(timestampNowNs());
        updates.emplace_back(key, std::move(fault));
    }
    if (!updates.empty()) {
        device_db_.putHashesAtomically(updates);
    }
}

void SyncdService::reconcileDevice(
    const DeviceInfo& info,
    const std::vector<AppliedConnection>& actual) {
    const auto desired = loadDesiredConnections(info.name);
    if (!orchestrationSettled(info.name, desired)) {
        return;
    }
    auto plan = buildReconciliationPlan(desired, actual);
    bool confirmed_state_recovery_required = false;
    if (plan.converged()) {
        std::unordered_map<std::string, AppliedConnection> actual_by_id;
        std::unordered_set<std::string> desired_ids;
        for (const auto& connection : actual) {
            actual_by_id.emplace(connection.id, connection);
        }
        for (const auto& command : desired) {
            desired_ids.insert(command.id);
            const auto found = actual_by_id.find(command.id);
            if (found == actual_by_id.end()) {
                continue;
            }
            const auto device_state = device_db_.getHash(
                redis::connectionDeviceKey(info.name, command.id));
            const auto confirmed_state = state_db_.getHash(
                redis::connectionStateKey(info.name, command.id));
            const auto failed = [](const auto& snapshot) {
                return snapshot.contains("apply_status") &&
                       snapshot.at("apply_status") == "FAILED";
            };
            if (!failed(device_state) && !failed(confirmed_state)) {
                continue;
            }
            plan.drifts.push_back({
                .id = command.id,
                .kind = ConnectionDriftKind::kMismatched,
                .desired = command,
                .actual = found->second,
            });
            confirmed_state_recovery_required = true;
        }
        for (const auto& key : device_db_.scanKeys(
                 redis::connectionDevicePattern(info.name))) {
            const auto snapshot = device_db_.getHash(key);
            if (!snapshot.contains("id") || desired_ids.contains(snapshot.at("id")) ||
                !snapshot.contains("operation") || snapshot.at("operation") != "REMOVE" ||
                !snapshot.contains("apply_status") || snapshot.at("apply_status") != "FAILED" ||
                !snapshot.contains("desired_version") ||
                actual_by_id.contains(snapshot.at("id"))) {
                continue;
            }
            const ConnectionCommand removal{
                .operation = ConnectionOperation::kRemove,
                .id = snapshot.at("id"),
                .desired_version = std::stoull(snapshot.at("desired_version")),
            };
            plan.full_snapshot.commands.push_back(removal);
            plan.drifts.push_back({
                .id = removal.id,
                .kind = ConnectionDriftKind::kMissing,
                .desired = removal,
                .actual = {},
            });
            confirmed_state_recovery_required = true;
        }
        std::ranges::sort(plan.full_snapshot.commands, {}, &ConnectionCommand::id);
        std::ranges::sort(plan.drifts, {}, &ConnectionDrift::id);
    }
    auto port_faults = portFaults(info.name);
    const bool port_recovery_required = std::ranges::any_of(
        port_faults,
        [](const auto& item) {
            return item.second.contains("status") && item.second.at("status") == "CLEARED";
        });
    if (plan.converged() && port_recovery_required) {
        std::unordered_map<std::string, ConnectionCommand> desired_by_id;
        std::unordered_map<std::string, AppliedConnection> actual_by_id;
        for (const auto& command : desired) {
            desired_by_id.emplace(command.id, command);
        }
        for (const auto& connection : actual) {
            actual_by_id.emplace(connection.id, connection);
        }
        for (const auto& command : desired) {
            const auto snapshot = state_db_.getHash(
                redis::connectionStateKey(info.name, command.id));
            if (!snapshot.contains("apply_status") || snapshot.at("apply_status") != "FAILED") {
                continue;
            }
            const auto desired_item = desired_by_id.find(command.id);
            const auto actual_item = actual_by_id.find(command.id);
            if (desired_item == desired_by_id.end() || actual_item == actual_by_id.end()) {
                continue;
            }
            plan.drifts.push_back({
                .id = desired_item->first,
                .kind = ConnectionDriftKind::kMismatched,
                .desired = desired_item->second,
                .actual = actual_item->second,
            });
        }
        std::ranges::sort(plan.drifts, {}, &ConnectionDrift::id);
        if (plan.converged()) {
            std::vector<std::pair<std::string, std::map<std::string, std::string>>> recovered;
            for (auto& [key, fault] : port_faults) {
                if (fault.contains("status") && fault.at("status") == "CLEARED") {
                    fault["status"] = "RECOVERED";
                    fault["recovered_at_ns"] = std::to_string(timestampNowNs());
                    recovered.emplace_back(key, std::move(fault));
                }
            }
            if (!recovered.empty()) {
                device_db_.putHashesAtomically(recovered);
            }
        }
    }
    auto control = device_db_.getHash(redis::syncdReconciliationKey(info.name));
    if (plan.converged()) {
        if (!control.empty() && control.find("status") != control.end() &&
            control.at("status") != "CONVERGED") {
            const auto command_id = control.at("command_id");
            const auto desired_version = control.contains("desired_version")
                                             ? std::stoull(control.at("desired_version"))
                                             : 0;
            clearDriftAlarm(info, command_id, desired_version);
            control["status"] = "CONVERGED";
            control["converged_at_ns"] = std::to_string(timestampNowNs());
            device_db_.putHash(redis::syncdReconciliationKey(info.name), control);
        }
        return;
    }

    auto batch = plan.full_snapshot;
    const auto signature = encodeDeviceCommand(batch);
    std::string command_id;
    const bool reopen_failed_port_recovery = port_recovery_required &&
                                             control.contains("status") &&
                                             control.at("status") == "FAILED";
    const bool reopen_failed_state_recovery =
        confirmed_state_recovery_required && control.contains("status") &&
        control.at("status") == "FAILED" &&
        (!control.contains("reason") || control.at("reason") != "CONFIRMED_STATE_RECOVERY");
    if (!reopen_failed_port_recovery && !reopen_failed_state_recovery && !control.empty() &&
        control.contains("signature") &&
        control.at("signature") == signature && control.contains("status") &&
        control.at("status") != "CONVERGED") {
        if (control.at("status") != "PUBLISHING") {
            return;
        }
        command_id = control.at("command_id");
    } else {
        command_id = "reconcile:" + info.name + ":" + std::to_string(timestampNowNs()) +
                     ":command";
        std::uint64_t desired_version = 1;
        for (const auto& command : batch.commands) {
            desired_version = std::max(desired_version, command.desired_version);
        }
        control = {
            {"device", info.name},
            {"device_generation", std::to_string(info.generation)},
            {"command_id", command_id},
            {"desired_version", std::to_string(desired_version)},
            {"signature", signature},
            {"reason", port_recovery_required
                           ? "PORT_RECOVERY"
                           : (confirmed_state_recovery_required
                                  ? "CONFIRMED_STATE_RECOVERY"
                                  : "DESIRED_ACTUAL_DRIFT")},
            {"status", "PUBLISHING"},
            {"started_at_ns", std::to_string(timestampNowNs())},
        };
        device_db_.putHash(redis::syncdReconciliationKey(info.name), control);
    }

    associatePortFaults(info, command_id);
    batch.options.operation_id = command_id;
    publishReconciliationState(info, plan, command_id);
    const auto desired_version = std::stoull(control.at("desired_version"));
    publishDriftAlarm(info, command_id, desired_version, plan.drifts.size());
    const auto payload = encodeDeviceCommand(batch);
    constexpr std::string_view suffix = ":command";
    const auto prepared_id = command_id.substr(0, command_id.size() - suffix.size());
    device_db_.putHash(
        redis::orchConfigBatchKey(prepared_id),
        {
            {"payload", payload},
            {"device_generation", std::to_string(info.generation)},
        });
    const redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = command_id,
        .request_id = prepared_id,
        .timestamp_ns = timestampNowNs(),
        .device = info.name,
        .resource_type = batch.commands.size() == 1 ? "connection" : "connection-batch",
        .resource_id = batch.commands.size() == 1 ? batch.commands.front().id : info.name,
        .operation = "RECONCILE",
        .desired_version = desired_version,
        .payload = payload,
    };
    static_cast<void>(device_db_.appendEventOnce(
        redis::syncdReconciliationCommandPublicationKey(command_id),
        {
            {"command_id", command_id},
            {"device_generation", std::to_string(info.generation)},
            {"desired_version", std::to_string(desired_version)},
        },
        std::string(redis::kDeviceCommands),
        event));
    control["status"] = "PUBLISHED";
    control["published_at_ns"] = std::to_string(timestampNowNs());
    device_db_.putHash(redis::syncdReconciliationKey(info.name), control);
}

void SyncdService::publishReconciliationState(
    const DeviceInfo& info,
    const ReconciliationPlan& plan,
    std::string_view command_id) {
    long long active_delta = 0;
    for (const auto& drift : plan.drifts) {
        const auto state_key = redis::connectionStateKey(info.name, drift.id);
        const auto previous = state_db_.getHash(state_key);
        if (previous.contains("apply_status") &&
            (previous.at("apply_status") == "FAILED" ||
             previous.at("apply_status") == "RETRY_WAIT")) {
            continue;
        }
        const bool was_active = previous.contains("actual_present")
                                    ? previous.at("actual_present") == "true"
                                    : previous.contains("applied_version") &&
                                          previous.at("applied_version") != "0";
        const bool is_active = drift.kind != ConnectionDriftKind::kMissing;
        std::map<std::string, std::string> drifted{
            {"device", info.name},
            {"id", drift.id},
            {"input_port", std::to_string(is_active ? drift.actual.input_port
                                                     : drift.desired.input_port)},
            {"output_port", std::to_string(is_active ? drift.actual.output_port
                                                      : drift.desired.output_port)},
            {"desired_version", std::to_string(drift.desired.desired_version)},
            {"applied_version", std::to_string(is_active ? drift.actual.applied_version : 0)},
            {"actual_present", is_active ? "true" : "false"},
            {"apply_status", "DRIFTED"},
            {"last_error_code", std::string(toString(ErrorCode::kApplyFailed))},
            {"last_error_message", "desired and actual connection state differ"},
        };
        if (!previous.empty() && previous.contains("apply_status") &&
            previous.at("apply_status") == "ACTIVE") {
            requireTransition(ConnectionApplyStatus::kActive, ConnectionApplyStatus::kDrifted);
        }
        const auto desired_version = drift.desired.desired_version;
        const redis::EventEnvelope drift_event{
            .event_schema_version = 1,
            .event_id = std::string(command_id) + ":drifted:" + drift.id,
            .request_id = std::string(command_id),
            .timestamp_ns = timestampNowNs(),
            .device = info.name,
            .resource_type = "connection",
            .resource_id = drift.id,
            .operation = "UPSERT",
            .desired_version = desired_version,
            .payload = statePayload(drifted),
        };
        const auto drift_marker = redis::syncdReconciliationStatePublicationKey(
            command_id, drift.id, "DRIFTED");
        const auto delta = was_active == is_active ? 0 : (is_active ? 1 : -1);
        const auto published = state_db_.replaceHashAndAppendEventOnce(
            drift_marker,
            {
                {"command_id", std::string(command_id)},
                {"connection_id", drift.id},
                {"active_delta", std::to_string(delta)},
            },
            state_key,
            drifted,
            std::string(redis::kStateEvents),
            drift_event);
        active_delta += published
                            ? delta
                            : std::stoll(state_db_.getHash(drift_marker).at("active_delta"));

        requireTransition(ConnectionApplyStatus::kDrifted, ConnectionApplyStatus::kReconciling);
        auto reconciling = drifted;
        reconciling["apply_status"] = "RECONCILING";
        const redis::EventEnvelope reconciling_event{
            .event_schema_version = 1,
            .event_id = std::string(command_id) + ":reconciling:" + drift.id,
            .request_id = std::string(command_id),
            .timestamp_ns = timestampNowNs(),
            .device = info.name,
            .resource_type = "connection",
            .resource_id = drift.id,
            .operation = "UPSERT",
            .desired_version = desired_version,
            .payload = statePayload(reconciling),
        };
        static_cast<void>(state_db_.replaceHashAndAppendEventOnce(
            redis::syncdReconciliationStatePublicationKey(
                command_id, drift.id, "RECONCILING"),
            {{"command_id", std::string(command_id)}, {"connection_id", drift.id}},
            state_key,
            reconciling,
            std::string(redis::kStateEvents),
            reconciling_event));
    }
    static_cast<void>(counters_db_.incrementHashFieldsOnce(
        redis::syncdReconciliationCountersPublicationKey(command_id),
        {{"command_id", std::string(command_id)}},
        redis::deviceCountersKey(info.name),
        {
            {"active_connections", active_delta},
            {"drift_detected_total", 1},
            {"reconciliation_total", 1},
        }));
}

void SyncdService::publishDriftAlarm(
    const DeviceInfo& info,
    std::string_view command_id,
    std::uint64_t desired_version,
    std::size_t drift_count) {
    const auto alarm_id = redis::desiredActualDriftAlarmId();
    const auto alarm_key = redis::activeAlarmKey(info.name, alarm_id);
    const auto marker_key = redis::syncdDriftAlarmPublicationKey(command_id);
    auto marker = alarm_db_.getHash(marker_key);
    if (marker.empty()) {
        const auto active = alarm_db_.getHash(alarm_key);
        const auto now = timestampNowNs();
        const auto activation_id = active.contains("activation_id")
                                       ? active.at("activation_id")
                                       : std::string(command_id);
        const std::map<std::string, std::string> fields{
            {"id", alarm_id},
            {"active", "true"},
            {"severity", "MAJOR"},
            {"resource_type", "device"},
            {"resource_id", info.name},
            {"desired_version", std::to_string(desired_version)},
            {"error_code", std::string(toString(ErrorCode::kApplyFailed))},
            {"error_message", "desired and actual connection matrices differ"},
            {"drift_count", std::to_string(drift_count)},
            {"activation_id", activation_id},
            {"first_raised_ns", active.contains("first_raised_ns")
                                    ? active.at("first_raised_ns")
                                    : std::to_string(now)},
            {"last_change_ns", std::to_string(now)},
        };
        marker = {
            {"command_id", std::string(command_id)},
            {"activation_id", activation_id},
            {"activated", active.empty() ? "true" : "false"},
        };
        const redis::EventEnvelope event{
            .event_schema_version = 1,
            .event_id = std::string(command_id) + ":alarm",
            .request_id = std::string(command_id),
            .timestamp_ns = now,
            .device = info.name,
            .resource_type = "alarm",
            .resource_id = alarm_id,
            .operation = "UPSERT",
            .desired_version = desired_version,
            .payload = statePayload(fields),
        };
        static_cast<void>(alarm_db_.replaceHashAndAppendEventOnce(
            marker_key,
            marker,
            alarm_key,
            fields,
            std::string(redis::kAlarmEvents),
            event));
        marker = alarm_db_.getHash(marker_key);
    }
    if (marker.at("activated") == "true") {
        static_cast<void>(counters_db_.incrementHashFieldsOnce(
            redis::syncdDriftAlarmCounterPublicationKey(
                info.name, marker.at("activation_id"), false),
            {{"activation_id", marker.at("activation_id")}},
            redis::deviceCountersKey(info.name),
            {{"active_alarms", 1}}));
    }
}

void SyncdService::clearDriftAlarm(
    const DeviceInfo& info,
    std::string_view command_id,
    std::uint64_t desired_version) {
    const auto alarm_id = redis::desiredActualDriftAlarmId();
    const auto alarm_key = redis::activeAlarmKey(info.name, alarm_id);
    const auto marker_key = redis::syncdDriftAlarmClearPublicationKey(command_id);
    auto marker = alarm_db_.getHash(marker_key);
    auto active = alarm_db_.getHash(alarm_key);
    if (marker.empty() && active.empty()) {
        return;
    }
    if (marker.empty()) {
        const auto now = timestampNowNs();
        active["active"] = "false";
        active["last_change_ns"] = std::to_string(now);
        marker = {
            {"command_id", std::string(command_id)},
            {"activation_id", active.at("activation_id")},
            {"cleared", "true"},
        };
        const redis::EventEnvelope event{
            .event_schema_version = 1,
            .event_id = std::string(command_id) + ":alarm-clear",
            .request_id = std::string(command_id),
            .timestamp_ns = now,
            .device = info.name,
            .resource_type = "alarm",
            .resource_id = alarm_id,
            .operation = "REMOVE",
            .desired_version = desired_version,
            .payload = statePayload(active),
        };
        static_cast<void>(alarm_db_.replaceHashAndAppendEventOnce(
            marker_key,
            marker,
            alarm_key,
            {},
            std::string(redis::kAlarmEvents),
            event));
        marker = alarm_db_.getHash(marker_key);
    }
    static_cast<void>(counters_db_.incrementHashFieldsOnce(
        redis::syncdDriftAlarmCounterPublicationKey(
            info.name, marker.at("activation_id"), true),
        {{"activation_id", marker.at("activation_id")}},
        redis::deviceCountersKey(info.name),
        {{"active_alarms", -1}}));
}

ApplyResult SyncdService::confirmAppliedResult(
    const DeviceCommandBatch& batch,
    ApplyResult result) {
    if (result.ok() || batch.commands.empty()) {
        return result;
    }
    try {
        const auto actual = device_->getConnections();
        std::unordered_map<std::string, AppliedConnection> actual_by_id;
        for (const auto& connection : actual) {
            actual_by_id.emplace(connection.id, connection);
        }
        std::vector<AppliedConnection> confirmed;
        confirmed.reserve(batch.commands.size());
        for (const auto& command : batch.commands) {
            const auto found = actual_by_id.find(command.id);
            if (command.operation == ConnectionOperation::kRemove) {
                if (found != actual_by_id.end()) {
                    return result;
                }
                continue;
            }
            if (found == actual_by_id.end() || found->second.input_port != command.input_port ||
                found->second.output_port != command.output_port ||
                found->second.applied_version != command.desired_version) {
                return result;
            }
            const auto input = device_->getInputPortState(command.input_port);
            const auto output = device_->getOutputPortState(command.output_port);
            if (!input.admin_enabled || input.oper_status != PortOperStatus::kUp ||
                !output.admin_enabled || output.oper_status != PortOperStatus::kUp) {
                return result;
            }
            confirmed.push_back(found->second);
        }
        result.error = Error::success();
        result.connections = std::move(confirmed);
    } catch (const std::exception&) {
        // Preserve the bounded apply error until a later device poll can confirm the outcome.
    }
    return result;
}

bool SyncdService::processOne(
    const std::string& consumer_name,
    const std::function<void()>& before_ack,
    const std::function<void(std::string_view)>& after_phase) {
    auto messages = device_db_.claimPending(
        std::string(redis::kDeviceCommands),
        std::string(kConsumerGroup),
        consumer_name,
        pending_min_idle_,
        1);
    if (messages.empty()) {
        messages = device_db_.readGroupIfNoPending(
            std::string(redis::kDeviceCommands), std::string(kConsumerGroup), consumer_name);
    }
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    if (isAlreadyProcessed(message.event)) {
        acknowledge(message.id);
        return true;
    }
    DeviceCommandBatch batch;
    try {
        batch = decodeDeviceCommand(message.event.payload);
        batch.options.operation_id = message.event.event_id;
    } catch (const std::exception& error) {
        ApplyResult malformed;
        malformed.error = {ErrorCode::kProtocolMalformed, error.what()};
        try {
            batch = recoverPreparedBatch(device_db_, message.event);
        } catch (const std::exception&) {
            batch = {};
        }
        device_db_.putHash(
            redis::deviceApplyResultKey(message.event.event_id),
            {{"payload", durableResultPayload(malformed)}, {"latency_ms", "0"}});
    }

    const auto result_key = redis::deviceApplyResultKey(message.event.event_id);
    auto stored_result = device_db_.getHash(result_key);
    if (stored_result.empty()) {
        const auto attempt_key = redis::deviceApplyAttemptKey(message.event.event_id);
        const bool recovering_attempt = !device_db_.getHash(attempt_key).empty();
        if (!recovering_attempt && isStaleVersion(message.event, batch)) {
            markProcessed(message.event, batch, false);
            acknowledge(message.id);
            return true;
        }
        static_cast<void>(device_db_.putHashIfAbsent(
            attempt_key,
            {
                {"command_id", message.event.event_id},
                {"device", message.event.device},
                {"started_at_ns", std::to_string(timestampNowNs())},
                {"payload", encodeDeviceCommand(batch)},
            }));
        ApplyResult result;
        bool apply_attempted = false;
        const auto apply_started = std::chrono::steady_clock::now();
        try {
            const auto device_info = device_->getDeviceInfo();
            device_name_ = device_info.name;
            device_generation_ = device_info.generation;
            if (device_info.name != message.event.device) {
                result.error = {
                    ErrorCode::kDeviceNotReady,
                    "command device does not match the connected device backend",
                };
            } else {
                try {
                    apply_attempted = true;
                    result = device_->applyConnections(batch.commands, batch.options);
                } catch (const std::exception& error) {
                    result.error = {ErrorCode::kProtocolMalformed, error.what()};
                }
            }
        } catch (const std::exception& error) {
            result.error = {ErrorCode::kDeviceNotReady, error.what()};
        }
        if (apply_attempted && !result.ok()) {
            result = confirmAppliedResult(batch, std::move(result));
        }
        if (after_phase) {
            after_phase("device-apply");
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - apply_started);
        const auto latency_ms = (elapsed.count() + 999) / 1000;
        device_db_.putHash(
            result_key,
            {
                {"payload", durableResultPayload(result)},
                {"latency_ms", std::to_string(latency_ms)},
            });
        stored_result = device_db_.getHash(result_key);
    }
    const auto result_payload = stored_result.find("payload");
    if (result_payload == stored_result.end()) {
        throw std::runtime_error("durable device apply result is missing payload");
    }
    const auto result = decodeDurableResult(result_payload->second);
    if (after_phase) {
        after_phase("apply-result");
    }

    const auto active_delta = publishState(message.event, batch, result);
    if (after_phase) {
        after_phase("state");
    }
    std::map<std::string, long long> counter_increments{
        {"device_apply_total", 1},
        {result.ok() ? "device_apply_success_total" : "device_apply_failure_total", 1},
        {"active_connections", active_delta},
    };
    if (result.error.code == ErrorCode::kApplyTimeout) {
        counter_increments["device_apply_timeout_total"] = 1;
    }
    const auto latency = stored_result.contains("latency_ms")
                             ? std::stoll(stored_result.at("latency_ms"))
                             : 0;
    static_cast<void>(counters_db_.recordApplyCountersOnce(
        redis::syncdCountersPublicationKey(message.event.event_id),
        {{"command_id", message.event.event_id}},
        redis::deviceCountersKey(message.event.device),
        counter_increments,
        latency));
    if (after_phase) {
        after_phase("counters");
    }
    publishResult(message.event, batch, result);
    if (after_phase) {
        after_phase("result");
    }
    markProcessed(message.event, batch, result.ok());
    if (after_phase) {
        after_phase("processed");
    }
    if (before_ack) {
        before_ack();
    }
    acknowledge(message.id);
    return true;
}

bool SyncdService::isAlreadyProcessed(const redis::EventEnvelope& command_event) {
    return !device_db_.getHash(redis::processedDeviceCommandKey(command_event.event_id)).empty();
}

bool SyncdService::isStaleVersion(
    const redis::EventEnvelope& command_event,
    const DeviceCommandBatch& batch) {
    if (isGenerationRecovery(command_event.event_id) ||
        isReconciliation(command_event.event_id)) {
        return false;
    }
    return std::ranges::all_of(batch.commands, [this, &command_event](const auto& command) {
        const auto version = device_db_.getHash(
            redis::syncdConnectionVersionKey(command_event.device, command.id));
        const auto last = version.find("last_successful_version");
        const auto generation = version.find("device_generation");
        return last != version.end() && generation != version.end() &&
               std::stoull(generation->second) == device_generation_ &&
               command.desired_version <= std::stoull(last->second);
    });
}

void SyncdService::markProcessed(
    const redis::EventEnvelope& command_event,
    const DeviceCommandBatch& batch,
    bool applied) {
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> hashes;
    hashes.push_back({
        redis::processedDeviceCommandKey(command_event.event_id),
        {
            {"command_id", command_event.event_id},
            {"device", command_event.device},
            {"resource_id", command_event.resource_id},
            {"desired_version", std::to_string(command_event.desired_version)},
            {"processed_at_ns", std::to_string(timestampNowNs())},
            {"applied", applied ? "true" : "false"},
        }});
    if (applied) {
        for (const auto& command : batch.commands) {
            const auto version_key =
                redis::syncdConnectionVersionKey(command_event.device, command.id);
            const auto existing = device_db_.getHash(version_key);
            const auto last = existing.find("last_successful_version");
            const auto generation = existing.find("device_generation");
            if (last != existing.end() &&
                generation != existing.end() &&
                std::stoull(generation->second) == device_generation_ &&
                std::stoull(last->second) >= command.desired_version) {
                continue;
            }
            hashes.push_back({
                version_key,
                {
                    {"last_successful_version", std::to_string(command.desired_version)},
                    {"last_command_id", command_event.event_id},
                    {"device_generation", std::to_string(device_generation_)},
                    {"operation", command.operation == ConnectionOperation::kRemove
                                      ? "REMOVE"
                                      : "UPSERT"},
                }});
        }
    }
    if (isReconciliation(command_event.event_id)) {
        auto control = device_db_.getHash(redis::syncdReconciliationKey(command_event.device));
        if (!control.empty() && control.contains("command_id") &&
            control.at("command_id") == command_event.event_id) {
            control["status"] = applied ? "APPLIED" : "FAILED";
            control["completed_at_ns"] = std::to_string(timestampNowNs());
            hashes.push_back({redis::syncdReconciliationKey(command_event.device), control});
        }
        if (applied) {
            static_cast<void>(counters_db_.incrementHashFieldsOnce(
                redis::syncdReconciliationSuccessCountersPublicationKey(command_event.event_id),
                {{"command_id", command_event.event_id}},
                redis::deviceCountersKey(command_event.device),
                {{"reconciliation_success_total", 1}}));
            for (auto& [key, fault] : portFaults(command_event.device)) {
                if (!fault.contains("command_id") ||
                    fault.at("command_id") != command_event.event_id) {
                    continue;
                }
                fault["status"] = "RECOVERED";
                fault["recovered_at_ns"] = std::to_string(timestampNowNs());
                hashes.emplace_back(key, std::move(fault));
            }
        }
    }
    device_db_.putHashesAtomically(hashes);
}

void SyncdService::acknowledge(const std::string& message_id) {
    const auto acknowledged = device_db_.acknowledge(
        std::string(redis::kDeviceCommands), std::string(kConsumerGroup), message_id);
    if (acknowledged != 1) {
        throw std::runtime_error("syncd failed to acknowledge processed device command");
    }
}

long long SyncdService::publishState(
    const redis::EventEnvelope& command_event,
    const DeviceCommandBatch& batch,
    const ApplyResult& result) {
    long long active_delta = 0;
    for (const auto& command : batch.commands) {
        const auto publication_key =
            redis::syncdStatePublicationKey(command_event.event_id, command.id);
        const auto published = state_db_.getHash(publication_key);
        if (!published.empty()) {
            active_delta += std::stoll(published.at("active_delta"));
            continue;
        }
        const auto state_key = redis::connectionStateKey(command_event.device, command.id);
        const auto previous_state = state_db_.getHash(state_key);
        const auto previous_desired = previous_state.find("desired_version");
        if (previous_desired != previous_state.end() &&
            std::stoull(previous_desired->second) > command.desired_version) {
            static_cast<void>(state_db_.putHashIfAbsent(
                publication_key,
                {
                    {"command_id", command_event.event_id},
                    {"connection_id", command.id},
                    {"active_delta", "0"},
                    {"superseded", "true"},
                }));
            active_delta += std::stoll(
                state_db_.getHash(publication_key).at("active_delta"));
            continue;
        }
        const auto actual_present = previous_state.find("actual_present");
        const auto applied_version = previous_state.find("applied_version");
        const bool was_active = actual_present != previous_state.end()
                                    ? actual_present->second == "true"
                                    : applied_version != previous_state.end() &&
                                          applied_version->second != "0";
        const bool preserve_confirmed_port_state =
            result.error.code == ErrorCode::kPortDown &&
            command.operation == ConnectionOperation::kUpsert && was_active &&
            previous_state.contains("apply_status") &&
            previous_state.at("apply_status") == "ACTIVE" &&
            previous_state.contains("input_port") &&
            snapshotPort(previous_state, "input_port") == command.input_port &&
            previous_state.contains("output_port") &&
            snapshotPort(previous_state, "output_port") == command.output_port &&
            previous_state.contains("desired_version") &&
            std::stoull(previous_state.at("desired_version")) == command.desired_version &&
            previous_state.contains("applied_version") &&
            std::stoull(previous_state.at("applied_version")) == command.desired_version;
        if (preserve_confirmed_port_state) {
            static_cast<void>(state_db_.putHashIfAbsent(
                publication_key,
                {
                    {"command_id", command_event.event_id},
                    {"connection_id", command.id},
                    {"active_delta", "0"},
                    {"confirmed_unchanged", "true"},
                }));
            active_delta += std::stoll(
                state_db_.getHash(publication_key).at("active_delta"));
            continue;
        }
        std::map<std::string, std::string> state;
        std::string operation = "UPSERT";
        if (result.ok() && command.operation == ConnectionOperation::kRemove) {
            operation = "REMOVE";
        } else if (!result.ok()) {
            state = state_db_.getHash(state_key);
            if (state.empty()) {
                state = {
                    {"device", command_event.device},
                    {"id", command.id},
                    {"input_port", std::to_string(command.input_port)},
                    {"output_port", std::to_string(command.output_port)},
                    {"applied_version", "0"},
                    {"actual_present", "false"},
                };
            }
            state["desired_version"] = std::to_string(command.desired_version);
            state["apply_status"] = "FAILED";
            state["last_error_code"] = std::string(toString(result.error.code));
            state["last_error_message"] = result.error.message;
        } else {
            const auto applied =
                std::ranges::find_if(result.connections, [&command](const auto& item) {
                    return item.id == command.id;
                });
            if (applied == result.connections.end()) {
                throw std::runtime_error("successful device apply omitted connection result");
            }
            state = {
                {"device", command_event.device},
                {"id", command.id},
                {"input_port", std::to_string(command.input_port)},
                {"output_port", std::to_string(command.output_port)},
                {"desired_version", std::to_string(command.desired_version)},
                {"applied_version", std::to_string(applied->applied_version)},
                {"actual_present", "true"},
                {"apply_status", "ACTIVE"},
                {"last_error_code", std::string(toString(result.error.code))},
                {"last_error_message", result.error.message},
            };
        }

        const redis::EventEnvelope state_event{
            .event_schema_version = 1,
            .event_id = command_event.event_id + ":state:" + command.id,
            .request_id = command_event.request_id,
            .timestamp_ns = timestampNowNs(),
            .device = command_event.device,
            .resource_type = "connection",
            .resource_id = command.id,
            .operation = operation,
            .desired_version = command.desired_version,
            .payload = statePayload(state),
        };
        const bool is_active = result.ok()
                                   ? command.operation != ConnectionOperation::kRemove
                                   : was_active;
        const long long command_delta = was_active == is_active ? 0 : (is_active ? 1 : -1);
        const auto published_now = state_db_.replaceHashAndAppendEventOnce(
            publication_key,
            {
                {"command_id", command_event.event_id},
                {"connection_id", command.id},
                {"active_delta", std::to_string(command_delta)},
            },
            state_key,
            state,
            std::string(redis::kStateEvents),
            state_event);
        active_delta += published_now
                            ? command_delta
                            : std::stoll(state_db_.getHash(publication_key).at("active_delta"));
    }
    return active_delta;
}

void SyncdService::publishResult(
    const redis::EventEnvelope& command_event,
    const DeviceCommandBatch& batch,
    const ApplyResult& result) {
    if (batch.commands.empty()) {
        const redis::EventEnvelope result_event{
            .event_schema_version = 1,
            .event_id = command_event.event_id + ":result",
            .request_id = command_event.request_id,
            .timestamp_ns = timestampNowNs(),
            .device = command_event.device,
            .resource_type = command_event.resource_type,
            .resource_id = command_event.resource_id,
            .operation = "APPLY_RESULT",
            .desired_version = command_event.desired_version,
            .payload = resultPayload(result, command_event.event_id),
        };
        static_cast<void>(device_db_.appendEventOnce(
            redis::syncdResultPublicationKey(
                command_event.event_id, command_event.resource_id),
            {
                {"command_id", command_event.event_id},
                {"connection_id", command_event.resource_id},
            },
            std::string(redis::kDeviceResults),
            result_event));
        return;
    }
    for (const auto& command : batch.commands) {
        ApplyResult command_result = result;
        const auto state_publication = state_db_.getHash(
            redis::syncdStatePublicationKey(command_event.event_id, command.id));
        if (state_publication.contains("confirmed_unchanged") &&
            state_publication.at("confirmed_unchanged") == "true") {
            command_result.error = Error::success();
            command_result.connections = {{
                .id = command.id,
                .input_port = command.input_port,
                .output_port = command.output_port,
                .applied_version = command.desired_version,
            }};
        }
        redis::EventEnvelope result_event{
            .event_schema_version = 1,
            .event_id = command_event.event_id + ":result:" + command.id,
            .request_id = command_event.request_id,
            .timestamp_ns = timestampNowNs(),
            .device = command_event.device,
            .resource_type = "connection",
            .resource_id = command.id,
            .operation = "APPLY_RESULT",
            .desired_version = command.desired_version,
            .payload = resultPayload(command_result, command_event.event_id),
        };
        static_cast<void>(device_db_.appendEventOnce(
            redis::syncdResultPublicationKey(command_event.event_id, command.id),
            {{"command_id", command_event.event_id}, {"connection_id", command.id}},
            std::string(redis::kDeviceResults),
            result_event));
    }
}

}  // namespace ocs
