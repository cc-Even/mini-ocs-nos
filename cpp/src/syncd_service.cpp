#include "ocs/syncd_service.hpp"

#include "ocs/device_command.hpp"
#include "ocs/redis_keys.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>

namespace ocs {
namespace {

inline constexpr std::string_view kConsumerGroup = "ocs-syncd";

std::uint64_t timestampNowNs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::string resultPayload(const ApplyResult& result) {
    return nlohmann::json{
        {"success", result.ok()},
        {"error_code", toString(result.error.code)},
        {"error_message", result.error.message},
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
            }));
        ApplyResult result;
        try {
            const auto device_info = device_->getDeviceInfo();
            if (device_info.name != message.event.device) {
                result.error = {
                    ErrorCode::kDeviceNotReady,
                    "command device does not match the connected device backend",
                };
            } else {
                result = device_->applyConnections(batch.commands, batch.options);
            }
        } catch (const std::exception& error) {
            result.error = {ErrorCode::kProtocolMalformed, error.what()};
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
    const std::map<std::string, long long> counter_increments{
        {"device_apply_total", 1},
        {result.ok() ? "device_apply_success_total" : "device_apply_failure_total", 1},
        {"active_connections", active_delta},
    };
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
    return std::ranges::all_of(batch.commands, [this, &command_event](const auto& command) {
        const auto version = device_db_.getHash(
            redis::syncdConnectionVersionKey(command_event.device, command.id));
        const auto last = version.find("last_successful_version");
        return last != version.end() && command.desired_version <= std::stoull(last->second);
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
            if (last != existing.end() &&
                std::stoull(last->second) >= command.desired_version) {
                continue;
            }
            hashes.push_back({
                version_key,
                {
                    {"last_successful_version", std::to_string(command.desired_version)},
                    {"last_command_id", command_event.event_id},
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
            .payload = resultPayload(result),
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
            .payload = resultPayload(result),
        };
        static_cast<void>(device_db_.appendEventOnce(
            redis::syncdResultPublicationKey(command_event.event_id, command.id),
            {{"command_id", command_event.event_id}, {"connection_id", command.id}},
            std::string(redis::kDeviceResults),
            result_event));
    }
}

}  // namespace ocs
