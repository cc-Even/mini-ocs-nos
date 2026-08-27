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

std::string statePayload(const std::map<std::string, std::string>& fields) {
    return nlohmann::json(fields).dump();
}

}  // namespace

SyncdService::SyncdService(redis::RedisEndpoint endpoint, std::unique_ptr<OcsDeviceApi> device)
    : device_db_(endpoint, redis::LogicalDb::kDevice),
      state_db_(endpoint, redis::LogicalDb::kState),
      counters_db_(std::move(endpoint), redis::LogicalDb::kCounters),
      device_(std::move(device)) {
    if (!device_) {
        throw std::invalid_argument("syncd device backend must not be null");
    }
}

void SyncdService::initialize() {
    device_db_.createConsumerGroup(std::string(redis::kDeviceCommands), std::string(kConsumerGroup));
}

bool SyncdService::processOne(const std::string& consumer_name) {
    const auto messages = device_db_.readGroup(
        std::string(redis::kDeviceCommands), std::string(kConsumerGroup), consumer_name, 1);
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    if (isAlreadyProcessed(message.event)) {
        acknowledge(message.id);
        return true;
    }
    if (isStaleVersion(message.event)) {
        markProcessed(message.event, false);
        acknowledge(message.id);
        return true;
    }

    ApplyResult result;
    DeviceCommandBatch batch;
    try {
        batch = decodeDeviceCommand(message.event.payload);
        result = device_->applyConnections(batch.commands, batch.options);
    } catch (const std::exception& error) {
        result.error = {ErrorCode::kProtocolMalformed, error.what()};
    }

    publishState(message.event, batch, result);
    static_cast<void>(counters_db_.incrementHashField(
        redis::deviceCountersKey(message.event.device), "device_apply_total"));
    static_cast<void>(counters_db_.incrementHashField(
        redis::deviceCountersKey(message.event.device),
        result.ok() ? "device_apply_success_total" : "device_apply_failure_total"));
    publishResult(message.event, result);
    markProcessed(message.event, result.ok());
    acknowledge(message.id);
    return true;
}

bool SyncdService::isAlreadyProcessed(const redis::EventEnvelope& command_event) {
    return !device_db_.getHash(redis::processedDeviceCommandKey(command_event.event_id)).empty();
}

bool SyncdService::isStaleVersion(const redis::EventEnvelope& command_event) {
    const auto version = device_db_.getHash(
        redis::syncdConnectionVersionKey(command_event.device, command_event.resource_id));
    const auto last = version.find("last_successful_version");
    return last != version.end() && command_event.desired_version <= std::stoull(last->second);
}

void SyncdService::markProcessed(
    const redis::EventEnvelope& command_event,
    bool applied) {
    device_db_.putHash(
        redis::processedDeviceCommandKey(command_event.event_id),
        {
            {"command_id", command_event.event_id},
            {"device", command_event.device},
            {"resource_id", command_event.resource_id},
            {"desired_version", std::to_string(command_event.desired_version)},
            {"processed_at_ns", std::to_string(timestampNowNs())},
            {"applied", applied ? "true" : "false"},
        });
    if (applied) {
        device_db_.putHash(
            redis::syncdConnectionVersionKey(
                command_event.device, command_event.resource_id),
            {
                {"last_successful_version", std::to_string(command_event.desired_version)},
                {"last_command_id", command_event.event_id},
                {"operation", command_event.operation},
            });
    }
}

void SyncdService::acknowledge(const std::string& message_id) {
    const auto acknowledged = device_db_.acknowledge(
        std::string(redis::kDeviceCommands), std::string(kConsumerGroup), message_id);
    if (acknowledged != 1) {
        throw std::runtime_error("syncd failed to acknowledge processed device command");
    }
}

void SyncdService::publishState(
    const redis::EventEnvelope& command_event,
    const DeviceCommandBatch& batch,
    const ApplyResult& result) {
    for (const auto& command : batch.commands) {
        const auto state_key = redis::connectionStateKey(command_event.device, command.id);
        const auto previous_state = state_db_.getHash(state_key);
        const auto previous_status = previous_state.find("apply_status");
        const bool was_active = previous_status != previous_state.end() &&
                                previous_status->second == "ACTIVE";
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
        state_db_.replaceHashAndAppendEvent(
            state_key,
            state,
            std::string(redis::kStateEvents),
            state_event);
        if (result.ok()) {
            const bool is_active = command.operation != ConnectionOperation::kRemove;
            if (was_active != is_active) {
                static_cast<void>(counters_db_.incrementHashField(
                    redis::deviceCountersKey(command_event.device),
                    "active_connections",
                    is_active ? 1 : -1));
            }
        }
    }
}

void SyncdService::publishResult(
    const redis::EventEnvelope& command_event,
    const ApplyResult& result) {
    redis::EventEnvelope result_event{
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
    static_cast<void>(
        device_db_.appendEvent(std::string(redis::kDeviceResults), result_event));
}

}  // namespace ocs
