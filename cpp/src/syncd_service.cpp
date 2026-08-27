#include "ocs/syncd_service.hpp"

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
#include <utility>
#include <vector>

namespace ocs {
namespace {

inline constexpr std::string_view kConsumerGroup = "ocs-syncd";

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

}  // namespace

SyncdService::SyncdService(
    redis::RedisEndpoint endpoint,
    std::unique_ptr<OcsDeviceApi> device,
    std::chrono::milliseconds pending_min_idle)
    : device_db_(endpoint, redis::LogicalDb::kDevice),
      state_db_(endpoint, redis::LogicalDb::kState),
      counters_db_(endpoint, redis::LogicalDb::kCounters),
      config_db_(std::move(endpoint), redis::LogicalDb::kConfig),
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
    const auto actual = device_->getConnections();
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

bool SyncdService::pollDevice() {
    try {
        const auto info = device_->getDeviceInfo();
        if (!device_name_.empty() && info.name != device_name_) {
            throw std::runtime_error("connected device identity changed after handshake");
        }
        const auto actual = device_->getConnections();
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
        }
        if (generation_changed || actual_count_changed || status == state.end() ||
            status->second != "READY") {
            publishDeviceState(info, actual.size(), "READY", Error::success());
        }
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
        return false;
    }
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
            {{"payload", durableResultPayload(malformed)}});
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
                    result = device_->applyConnections(batch.commands, batch.options);
                } catch (const std::exception& error) {
                    result.error = {ErrorCode::kProtocolMalformed, error.what()};
                }
            }
        } catch (const std::exception& error) {
            result.error = {ErrorCode::kDeviceNotReady, error.what()};
        }
        if (after_phase) {
            after_phase("device-apply");
        }
        device_db_.putHash(result_key, {{"payload", durableResultPayload(result)}});
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
    static_cast<void>(counters_db_.incrementHashFieldsOnce(
        redis::syncdCountersPublicationKey(message.event.event_id),
        {{"command_id", message.event.event_id}},
        redis::deviceCountersKey(message.event.device),
        counter_increments));
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
    if (isGenerationRecovery(command_event.event_id)) {
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
            .payload = resultPayload(result, command_event.event_id),
        };
        static_cast<void>(device_db_.appendEventOnce(
            redis::syncdResultPublicationKey(command_event.event_id, command.id),
            {{"command_id", command_event.event_id}, {"connection_id", command.id}},
            std::string(redis::kDeviceResults),
            result_event));
    }
}

}  // namespace ocs
