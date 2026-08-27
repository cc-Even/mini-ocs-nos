#include "ocs/orchestrator_service.hpp"

#include "ocs/connection_state_machine.hpp"
#include "ocs/device_command.hpp"
#include "ocs/redis_keys.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
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
inline constexpr std::string_view kRetryConsumerGroup = "ocs-orch-retry";

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
    if (current == ConnectionApplyStatus::kRetryWait) {
        requireTransition(current, executing);
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

std::string resultCommandId(
    const redis::EventEnvelope& event,
    const nlohmann::json& result) {
    const auto command_id = result.value("command_id", std::string{});
    if (!command_id.empty()) {
        return command_id;
    }
    const auto suffix = event.event_id.find(":result");
    if (suffix == std::string::npos) {
        throw std::invalid_argument("device result is missing command_id");
    }
    return event.event_id.substr(0, suffix);
}

bool isGenerationRecovery(std::string_view command_id) {
    return command_id.starts_with("hwsim-recovery:");
}

bool isReconciliation(std::string_view command_id) {
    return command_id.starts_with("reconcile:");
}

std::uint64_t retryAtNs(
    std::uint64_t now_ns,
    std::size_t retry_attempt,
    const ApplyRetryPolicy& policy) {
    auto delay = policy.base_backoff;
    for (std::size_t exponent = 1; delay.count() != 0 && exponent < retry_attempt;
         ++exponent) {
        if (delay >= policy.max_backoff / 2) {
            delay = policy.max_backoff;
            break;
        }
        delay *= 2;
    }
    delay = std::min(delay, policy.max_backoff);
    return now_ns + static_cast<std::uint64_t>(delay.count()) * 1'000'000ULL;
}

std::string retryPayload(
    const DeviceCommandBatch& batch,
    std::string_view failed_command_id,
    std::size_t retry_attempt,
    std::uint64_t not_before_ns) {
    return nlohmann::json{
        {"batch", nlohmann::json::parse(encodeDeviceCommand(batch))},
        {"failed_command_id", failed_command_id},
        {"retry_attempt", retry_attempt},
        {"not_before_ns", not_before_ns},
    }.dump();
}

DeviceCommandBatch retryBatch(const nlohmann::json& payload) {
    return decodeDeviceCommand(payload.at("batch").dump());
}

std::string alarmPayload(const std::map<std::string, std::string>& fields) {
    return nlohmann::json(fields).dump();
}

}  // namespace

OrchestratorService::OrchestratorService(
    redis::RedisEndpoint endpoint,
    std::chrono::milliseconds pending_min_idle,
    ApplyRetryPolicy retry_policy)
    : config_db_(endpoint, redis::LogicalDb::kConfig),
      appl_db_(endpoint, redis::LogicalDb::kAppl),
      device_db_(endpoint, redis::LogicalDb::kDevice),
      alarm_db_(endpoint, redis::LogicalDb::kAlarm),
      counters_db_(endpoint, redis::LogicalDb::kCounters),
      state_db_(std::move(endpoint), redis::LogicalDb::kState),
      pending_min_idle_(pending_min_idle),
      retry_policy_(retry_policy) {
    if (pending_min_idle_.count() < 0 || pending_min_idle_ > std::chrono::hours(1)) {
        throw std::invalid_argument("orch pending minimum idle time is outside 0..3600000ms");
    }
    if (retry_policy_.max_retries > 100 || retry_policy_.base_backoff.count() < 0 ||
        retry_policy_.max_backoff < retry_policy_.base_backoff ||
        retry_policy_.max_backoff > std::chrono::hours(1)) {
        throw std::invalid_argument("orch apply retry policy is outside supported bounds");
    }
}

void OrchestratorService::initialize() {
    config_db_.createConsumerGroup(std::string(redis::kConfigEvents), std::string(kConsumerGroup));
    device_db_.createConsumerGroup(std::string(redis::kDeviceResults), std::string(kConsumerGroup));
    device_db_.createConsumerGroup(
        std::string(redis::kDeviceRetries), std::string(kRetryConsumerGroup));
    publishHeartbeat();
}

void OrchestratorService::publishHeartbeat() {
    state_db_.putHash(
        redis::serviceStateKey("ocs-orch"),
        {
            {"service", "ocs-orch"},
            {"status", "ONLINE"},
            {"last_seen_ns", std::to_string(timestampNowNs())},
        });
}

bool OrchestratorService::processConfigOne(
    const std::string& consumer_name,
    const std::function<void(std::string_view)>& after_phase) {
    auto messages = config_db_.claimPending(
        std::string(redis::kConfigEvents),
        std::string(kConsumerGroup),
        consumer_name,
        pending_min_idle_,
        1);
    if (messages.empty()) {
        messages = config_db_.readGroupIfNoPending(
            std::string(redis::kConfigEvents), std::string(kConsumerGroup), consumer_name);
    }
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    const auto prepared_key = redis::orchConfigBatchKey(message.event.event_id);
    const auto application_publication_key =
        redis::orchApplicationPublicationKey(message.event.event_id);
    const auto device_state_publication_key =
        redis::orchDeviceStatePublicationKey(message.event.event_id);
    const auto publication_key =
        redis::orchDeviceCommandPublicationKey(message.event.event_id);
    if (!device_db_.getHash(publication_key).empty()) {
        const auto acknowledged = config_db_.acknowledge(
            std::string(redis::kConfigEvents), std::string(kConsumerGroup), message.id);
        static_cast<void>(acknowledged);
        return true;
    }

    auto prepared = device_db_.getHash(prepared_key);
    DeviceCommandBatch batch;
    if (!prepared.empty()) {
        batch = decodeDeviceCommand(prepared.at("payload"));
    } else {
        batch = decodeConfigBatchEvent(message.event);
        std::vector<ConnectionCommand> commands;
        commands.reserve(batch.commands.size());
        bool stale_batch = false;
        for (auto command : batch.commands) {
            const auto app_key = redis::connectionAppKey(message.event.device, command.id);
            const auto existing_application = appl_db_.getHash(app_key);
            const auto previous_version = currentVersion(existing_application);
            if (command.desired_version <= previous_version) {
                stale_batch = true;
                break;
            }
            if (command.operation == ConnectionOperation::kRemove &&
                existing_application.empty()) {
                continue;
            }
            if (message.event.resource_type == "connection" &&
                command.desired_version - previous_version > 1) {
                command = loadDesiredSnapshot(config_db_, message.event);
            }
            commands.push_back(std::move(command));
        }
        if (stale_batch || commands.empty()) {
            const auto acknowledged = config_db_.acknowledge(
                std::string(redis::kConfigEvents), std::string(kConsumerGroup), message.id);
            static_cast<void>(acknowledged);
            return true;
        }
        batch.commands = std::move(commands);
        static_cast<void>(device_db_.putHashIfAbsent(
            prepared_key,
            {
                {"event_id", message.event.event_id},
                {"request_id", message.event.request_id},
                {"device", message.event.device},
                {"payload", encodeDeviceCommand(batch)},
            }));
        prepared = device_db_.getHash(prepared_key);
        batch = decodeDeviceCommand(prepared.at("payload"));
    }
    if (after_phase) {
        after_phase("prepared");
    }

    const auto command_id = message.event.event_id + ":command";
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> applications;
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> device_states;
    for (const auto& command : batch.commands) {
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
            {"retry_attempt", "0"},
            {"next_retry_at_ns", ""},
            {"failed_command_id", ""},
            {"last_error_code", ""},
            {"last_error_message", ""},
        };
        applications.push_back({app_key, application});
        device_states.push_back({device_key, application});
    }
    static_cast<void>(appl_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        application_publication_key,
        {
            {"event_id", message.event.event_id},
            {"command_id", command_id},
            {"desired_version", std::to_string(message.event.desired_version)},
        },
        applications));
    if (after_phase) {
        after_phase("application");
    }
    static_cast<void>(device_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        device_state_publication_key,
        {
            {"event_id", message.event.event_id},
            {"command_id", command_id},
            {"desired_version", std::to_string(message.event.desired_version)},
        },
        device_states));
    if (after_phase) {
        after_phase("device-state");
    }
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
    static_cast<void>(device_db_.appendEventOnce(
        publication_key,
        {{"event_id", message.event.event_id}, {"command_id", command_id}},
        std::string(redis::kDeviceCommands),
        command_event));
    if (after_phase) {
        after_phase("command");
    }

    const auto acknowledged = config_db_.acknowledge(
        std::string(redis::kConfigEvents), std::string(kConsumerGroup), message.id);
    static_cast<void>(acknowledged);
    return true;
}

bool OrchestratorService::processResultOne(
    const std::string& consumer_name,
    const std::function<void(std::string_view)>& after_phase) {
    auto messages = device_db_.claimPending(
        std::string(redis::kDeviceResults),
        std::string(kConsumerGroup),
        consumer_name,
        pending_min_idle_,
        1);
    if (messages.empty()) {
        messages = device_db_.readGroupIfNoPending(
            std::string(redis::kDeviceResults), std::string(kConsumerGroup), consumer_name);
    }
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    const auto result = nlohmann::json::parse(message.event.payload);
    const auto success = result.at("success").get<bool>();
    const auto error_code = result.at("error_code").get<std::string>();
    if (!success && error_code == toString(ErrorCode::kApplyTimeout)) {
        handleTimeoutResult(
            message,
            resultCommandId(message.event, result),
            result.at("error_message").get<std::string>(),
            after_phase);
        static_cast<void>(device_db_.acknowledge(
            std::string(redis::kDeviceResults), std::string(kConsumerGroup), message.id));
        return true;
    }
    const auto app_key = redis::connectionAppKey(message.event.device, message.event.resource_id);
    const auto device_key =
        redis::connectionDeviceKey(message.event.device, message.event.resource_id);
    const auto application_publication_key =
        redis::orchResultApplicationPublicationKey(message.event.event_id);
    const auto device_publication_key =
        redis::orchResultDevicePublicationKey(message.event.event_id);
    if (!appl_db_.getHash(application_publication_key).empty() &&
        !device_db_.getHash(device_publication_key).empty()) {
        if (success) {
            clearTimeoutAlarm(message.event);
        }
        static_cast<void>(device_db_.acknowledge(
            std::string(redis::kDeviceResults), std::string(kConsumerGroup), message.id));
        return true;
    }
    auto application = appl_db_.getHash(app_key);
    if (application.empty()) {
        static_cast<void>(device_db_.acknowledge(
            std::string(redis::kDeviceResults), std::string(kConsumerGroup), message.id));
        return true;
    }

    const auto application_version = currentVersion(application);
    const auto current = currentStatus(application);
    const auto result_command_id = result.value("command_id", std::string{});
    const bool recovery_result = isGenerationRecovery(result_command_id) ||
                                 isGenerationRecovery(message.event.event_id) ||
                                 isReconciliation(result_command_id) ||
                                 isReconciliation(message.event.event_id);
    const bool recovering_failed_application =
        recovery_result && success &&
        (current == ConnectionApplyStatus::kFailed ||
         current == ConnectionApplyStatus::kRetryWait);
    if (message.event.desired_version <= application_version &&
        (message.event.desired_version < application_version ||
         current == ConnectionApplyStatus::kActive ||
         current == ConnectionApplyStatus::kAbsent ||
         (current == ConnectionApplyStatus::kFailed && !recovering_failed_application))) {
        const auto acknowledged = device_db_.acknowledge(
            std::string(redis::kDeviceResults), std::string(kConsumerGroup), message.id);
        static_cast<void>(acknowledged);
        return true;
    }
    if (message.event.desired_version > application_version) {
        throw std::runtime_error("device result is newer than application state");
    }

    ConnectionApplyStatus next = ConnectionApplyStatus::kFailed;
    if (success) {
        const auto operation = application.find("operation");
        const bool removing = current == ConnectionApplyStatus::kRemoving ||
                              (recovery_result &&
                               operation != application.end() && operation->second == "REMOVE");
        next = removing ? ConnectionApplyStatus::kAbsent : ConnectionApplyStatus::kActive;
    }
    if (recovering_failed_application) {
        if (current == ConnectionApplyStatus::kFailed) {
            requireTransition(current, ConnectionApplyStatus::kRetryWait);
        }
        const auto executing = next == ConnectionApplyStatus::kAbsent
                                   ? ConnectionApplyStatus::kRemoving
                                   : ConnectionApplyStatus::kApplying;
        requireTransition(ConnectionApplyStatus::kRetryWait, executing);
        requireTransition(executing, next);
    } else {
        requireTransition(current, next);
    }

    application["apply_status"] = std::string(toString(next));
    application["last_error_code"] =
        next == ConnectionApplyStatus::kAbsent
            ? ""
            : result.at("error_code").get<std::string>();
    application["last_error_message"] =
        next == ConnectionApplyStatus::kAbsent
            ? ""
            : result.at("error_message").get<std::string>();
    if (success) {
        application["retry_attempt"] = "0";
        application["next_retry_at_ns"] = "";
        application["failed_command_id"] = "";
    }
    static_cast<void>(device_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        device_publication_key,
        {
            {"result_event_id", message.event.event_id},
            {"desired_version", std::to_string(message.event.desired_version)},
        },
        {{device_key, next == ConnectionApplyStatus::kAbsent
                          ? std::map<std::string, std::string>{}
                          : application}}));
    if (after_phase) {
        after_phase("device-result");
    }
    static_cast<void>(appl_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        application_publication_key,
        {
            {"result_event_id", message.event.event_id},
            {"desired_version", std::to_string(message.event.desired_version)},
        },
        {{app_key, application}}));
    if (after_phase) {
        after_phase("application-result");
    }
    if (success) {
        clearTimeoutAlarm(message.event);
        if (after_phase) {
            after_phase("alarm-clear");
        }
    }

    const auto acknowledged = device_db_.acknowledge(
        std::string(redis::kDeviceResults), std::string(kConsumerGroup), message.id);
    static_cast<void>(acknowledged);
    return true;
}

void OrchestratorService::handleTimeoutResult(
    const redis::StreamMessage& message,
    const std::string& command_id,
    const std::string& error_message,
    const std::function<void(std::string_view)>& after_phase) {
    const auto attempt = device_db_.getHash(redis::deviceApplyAttemptKey(command_id));
    const auto payload = attempt.find("payload");
    if (payload == attempt.end()) {
        return;
    }
    DeviceCommandBatch batch;
    try {
        batch = decodeDeviceCommand(payload->second);
    } catch (const std::exception&) {
        return;
    }
    if (batch.commands.empty()) {
        return;
    }

    const auto application_marker_key =
        redis::orchTimeoutApplicationPublicationKey(command_id);
    const auto device_marker_key = redis::orchTimeoutDevicePublicationKey(command_id);
    auto metadata = appl_db_.getHash(application_marker_key);
    if (metadata.empty()) {
        metadata = device_db_.getHash(device_marker_key);
    }

    if (metadata.empty()) {
        const auto first = appl_db_.getHash(
            redis::connectionAppKey(message.event.device, batch.commands.front().id));
        if (first.empty() || currentVersion(first) != message.event.desired_version ||
            first.find("command_id") == first.end() || first.at("command_id") != command_id) {
            return;
        }
        const auto current = currentStatus(first);
        if (current != ConnectionApplyStatus::kApplying &&
            current != ConnectionApplyStatus::kRemoving) {
            return;
        }
        requireTransition(current, ConnectionApplyStatus::kFailed);
        const auto attempt_value = first.find("retry_attempt");
        const auto completed_retries = attempt_value == first.end()
                                           ? std::size_t{}
                                           : static_cast<std::size_t>(
                                                 std::stoull(attempt_value->second));
        const bool schedule_retry = completed_retries < retry_policy_.max_retries;
        const auto next_attempt = schedule_retry ? completed_retries + 1 : completed_retries;
        const auto next_status = schedule_retry ? ConnectionApplyStatus::kRetryWait
                                                : ConnectionApplyStatus::kFailed;
        if (schedule_retry) {
            requireTransition(ConnectionApplyStatus::kFailed, next_status);
        }
        const auto next_retry_at = schedule_retry
                                       ? retryAtNs(
                                             timestampNowNs(), next_attempt, retry_policy_)
                                       : std::uint64_t{};
        metadata = {
            {"command_id", command_id},
            {"desired_version", std::to_string(message.event.desired_version)},
            {"retry_attempt", std::to_string(next_attempt)},
            {"next_retry_at_ns", std::to_string(next_retry_at)},
            {"apply_status", std::string(toString(next_status))},
            {"retry_scheduled", schedule_retry ? "true" : "false"},
        };
    }

    std::vector<std::pair<std::string, std::map<std::string, std::string>>> applications;
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> device_states;
    for (const auto& command : batch.commands) {
        const auto app_key = redis::connectionAppKey(message.event.device, command.id);
        auto application = appl_db_.getHash(app_key);
        if (application.empty() || currentVersion(application) != command.desired_version ||
            application.find("command_id") == application.end() ||
            application.at("command_id") != command_id) {
            return;
        }
        application["apply_status"] = metadata.at("apply_status");
        application["retry_attempt"] = metadata.at("retry_attempt");
        application["next_retry_at_ns"] = metadata.at("next_retry_at_ns");
        application["failed_command_id"] = command_id;
        application["last_error_code"] = std::string(toString(ErrorCode::kApplyTimeout));
        application["last_error_message"] = error_message;
        applications.push_back({app_key, application});
        device_states.push_back({
            redis::connectionDeviceKey(message.event.device, command.id), application});
    }

    static_cast<void>(device_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        device_marker_key, metadata, device_states));
    if (after_phase) {
        after_phase("timeout-device");
    }
    static_cast<void>(appl_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        application_marker_key, metadata, applications));
    if (after_phase) {
        after_phase("timeout-application");
    }

    for (const auto& command : batch.commands) {
        auto alarm_event = message.event;
        alarm_event.resource_id = command.id;
        alarm_event.event_id = command_id + ":timeout-alarm:" + command.id;
        publishTimeoutAlarm(alarm_event, error_message);
    }
    if (after_phase) {
        after_phase("timeout-alarm");
    }

    if (metadata.at("retry_scheduled") == "true") {
        const auto retry_attempt = static_cast<std::size_t>(
            std::stoull(metadata.at("retry_attempt")));
        const auto not_before_ns = std::stoull(metadata.at("next_retry_at_ns"));
        const redis::EventEnvelope retry_event{
            .event_schema_version = 1,
            .event_id = command_id + ":retry:" + std::to_string(retry_attempt),
            .request_id = message.event.request_id,
            .timestamp_ns = timestampNowNs(),
            .device = message.event.device,
            .resource_type = batch.commands.size() == 1 ? "connection" : "connection-batch",
            .resource_id = batch.commands.size() == 1 ? batch.commands.front().id
                                                      : message.event.request_id,
            .operation = "RETRY",
            .desired_version = message.event.desired_version,
            .payload = retryPayload(batch, command_id, retry_attempt, not_before_ns),
        };
        static_cast<void>(device_db_.appendEventOnce(
            redis::orchRetryPublicationKey(command_id),
            {
                {"command_id", command_id},
                {"retry_event_id", retry_event.event_id},
                {"desired_version", std::to_string(message.event.desired_version)},
            },
            std::string(redis::kDeviceRetries),
            retry_event));
        if (after_phase) {
            after_phase("timeout-retry");
        }
    }
}

bool OrchestratorService::processRetryOne(
    const std::string& consumer_name,
    const std::function<void(std::string_view)>& after_phase) {
    auto messages = device_db_.claimPending(
        std::string(redis::kDeviceRetries),
        std::string(kRetryConsumerGroup),
        consumer_name,
        std::chrono::milliseconds(0),
        1);
    if (messages.empty()) {
        messages = device_db_.readGroupIfNoPending(
            std::string(redis::kDeviceRetries),
            std::string(kRetryConsumerGroup),
            consumer_name);
    }
    if (messages.empty()) {
        return false;
    }

    const auto& message = messages.front();
    const auto payload = nlohmann::json::parse(message.event.payload);
    if (timestampNowNs() < payload.at("not_before_ns").get<std::uint64_t>()) {
        return false;
    }
    const auto batch = retryBatch(payload);
    const auto failed_command_id = payload.at("failed_command_id").get<std::string>();
    const auto retry_attempt = payload.at("retry_attempt").get<std::size_t>();
    const auto command_id = message.event.event_id + ":command";
    const auto application_marker_key =
        redis::orchRetryApplicationPublicationKey(message.event.event_id);
    const auto device_marker_key = redis::orchRetryDevicePublicationKey(message.event.event_id);
    const auto command_marker_key = redis::orchRetryCommandPublicationKey(message.event.event_id);

    if (!device_db_.getHash(command_marker_key).empty()) {
        static_cast<void>(device_db_.acknowledge(
            std::string(redis::kDeviceRetries),
            std::string(kRetryConsumerGroup),
            message.id));
        return true;
    }

    const bool retry_started = !appl_db_.getHash(application_marker_key).empty() ||
                               !device_db_.getHash(device_marker_key).empty();
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> applications;
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> device_states;
    for (const auto& command : batch.commands) {
        const auto app_key = redis::connectionAppKey(message.event.device, command.id);
        auto application = appl_db_.getHash(app_key);
        const bool current_retry =
            !application.empty() && currentVersion(application) == command.desired_version &&
            application.find("command_id") != application.end() &&
            (application.at("command_id") == failed_command_id ||
             (retry_started && application.at("command_id") == command_id));
        if (!current_retry ||
            (!retry_started && currentStatus(application) != ConnectionApplyStatus::kRetryWait)) {
            static_cast<void>(device_db_.acknowledge(
                std::string(redis::kDeviceRetries),
                std::string(kRetryConsumerGroup),
                message.id));
            return true;
        }
        if (!retry_started) {
            const auto executing = command.operation == ConnectionOperation::kRemove
                                       ? ConnectionApplyStatus::kRemoving
                                       : ConnectionApplyStatus::kApplying;
            requireTransition(ConnectionApplyStatus::kRetryWait, executing);
            application["apply_status"] = std::string(toString(executing));
        }
        application["command_id"] = command_id;
        application["retry_attempt"] = std::to_string(retry_attempt);
        application["next_retry_at_ns"] = "";
        application["failed_command_id"] = failed_command_id;
        applications.push_back({app_key, application});
        device_states.push_back({
            redis::connectionDeviceKey(message.event.device, command.id), application});
    }

    const std::map<std::string, std::string> marker{
        {"retry_event_id", message.event.event_id},
        {"command_id", command_id},
        {"desired_version", std::to_string(message.event.desired_version)},
        {"retry_attempt", std::to_string(retry_attempt)},
    };
    static_cast<void>(appl_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        application_marker_key, marker, applications));
    if (after_phase) {
        after_phase("retry-application");
    }
    static_cast<void>(device_db_.putVersionedHashesAtomicallyIfMarkerAbsent(
        device_marker_key, marker, device_states));
    if (after_phase) {
        after_phase("retry-device");
    }
    static_cast<void>(device_db_.putHashIfAbsent(
        redis::orchConfigBatchKey(message.event.event_id),
        {
            {"event_id", message.event.event_id},
            {"request_id", message.event.request_id},
            {"device", message.event.device},
            {"payload", encodeDeviceCommand(batch)},
        }));
    const redis::EventEnvelope command_event{
        .event_schema_version = 1,
        .event_id = command_id,
        .request_id = message.event.request_id,
        .timestamp_ns = timestampNowNs(),
        .device = message.event.device,
        .resource_type = message.event.resource_type,
        .resource_id = message.event.resource_id,
        .operation = batch.commands.size() == 1
                         ? operationName(batch.commands.front().operation)
                         : "APPLY_BATCH",
        .desired_version = message.event.desired_version,
        .payload = encodeDeviceCommand(batch),
    };
    static_cast<void>(device_db_.appendEventOnce(
        command_marker_key,
        marker,
        std::string(redis::kDeviceCommands),
        command_event));
    if (after_phase) {
        after_phase("retry-command");
    }
    static_cast<void>(device_db_.acknowledge(
        std::string(redis::kDeviceRetries),
        std::string(kRetryConsumerGroup),
        message.id));
    return true;
}

void OrchestratorService::publishTimeoutAlarm(
    const redis::EventEnvelope& result_event,
    const std::string& error_message) {
    const auto alarm_id = redis::applyTimeoutAlarmId(result_event.resource_id);
    const auto key = redis::activeAlarmKey(result_event.device, alarm_id);
    const auto marker_key = redis::orchAlarmPublicationKey(result_event.event_id);
    auto marker = alarm_db_.getHash(marker_key);
    const auto active = alarm_db_.getHash(key);
    if (marker.empty()) {
        const auto now = timestampNowNs();
        const auto first_raised = active.find("first_raised_ns");
        const auto existing_activation = active.find("activation_id");
        const auto activation_id = existing_activation == active.end()
                                       ? result_event.event_id
                                       : existing_activation->second;
        std::map<std::string, std::string> fields{
            {"id", alarm_id},
            {"active", "true"},
            {"severity", "MAJOR"},
            {"resource_type", "connection"},
            {"resource_id", result_event.resource_id},
            {"desired_version", std::to_string(result_event.desired_version)},
            {"error_code", std::string(toString(ErrorCode::kApplyTimeout))},
            {"error_message", error_message},
            {"activation_id", activation_id},
            {"first_raised_ns", first_raised == active.end()
                                      ? std::to_string(now)
                                      : first_raised->second},
            {"last_change_ns", std::to_string(now)},
        };
        marker = {
            {"alarm_id", alarm_id},
            {"desired_version", std::to_string(result_event.desired_version)},
            {"activation_id", activation_id},
            {"activated", active.empty() ? "true" : "false"},
        };
        const redis::EventEnvelope alarm_event{
            .event_schema_version = 1,
            .event_id = result_event.event_id,
            .request_id = result_event.request_id,
            .timestamp_ns = now,
            .device = result_event.device,
            .resource_type = "alarm",
            .resource_id = alarm_id,
            .operation = "UPSERT",
            .desired_version = result_event.desired_version,
            .payload = alarmPayload(fields),
        };
        static_cast<void>(alarm_db_.replaceHashAndAppendEventOnce(
            marker_key,
            marker,
            key,
            fields,
            std::string(redis::kAlarmEvents),
            alarm_event));
        marker = alarm_db_.getHash(marker_key);
    }
    if (marker.at("activated") == "true") {
        static_cast<void>(counters_db_.incrementHashFieldsOnce(
            redis::orchAlarmCounterPublicationKey(
                result_event.device, alarm_id, marker.at("activation_id"), false),
            {
                {"alarm_id", alarm_id},
                {"desired_version", std::to_string(result_event.desired_version)},
            },
            redis::deviceCountersKey(result_event.device),
            {{"active_alarms", 1}}));
    }
}

void OrchestratorService::clearTimeoutAlarm(const redis::EventEnvelope& result_event) {
    const auto alarm_id = redis::applyTimeoutAlarmId(result_event.resource_id);
    const auto key = redis::activeAlarmKey(result_event.device, alarm_id);
    const auto marker_key = redis::orchAlarmClearPublicationKey(result_event.event_id);
    auto marker = alarm_db_.getHash(marker_key);
    auto active = alarm_db_.getHash(key);
    if (marker.empty() && active.empty()) {
        return;
    }
    if (marker.empty()) {
        const auto desired_version = std::stoull(active.at("desired_version"));
        const auto now = timestampNowNs();
        active["active"] = "false";
        active["last_change_ns"] = std::to_string(now);
        marker = {
            {"alarm_id", alarm_id},
            {"desired_version", std::to_string(desired_version)},
            {"activation_id", active.at("activation_id")},
            {"cleared", "true"},
        };
        const redis::EventEnvelope alarm_event{
            .event_schema_version = 1,
            .event_id = result_event.event_id + ":alarm-clear",
            .request_id = result_event.request_id,
            .timestamp_ns = now,
            .device = result_event.device,
            .resource_type = "alarm",
            .resource_id = alarm_id,
            .operation = "REMOVE",
            .desired_version = desired_version,
            .payload = alarmPayload(active),
        };
        static_cast<void>(alarm_db_.replaceHashAndAppendEventOnce(
            marker_key,
            marker,
            key,
            {},
            std::string(redis::kAlarmEvents),
            alarm_event));
        marker = alarm_db_.getHash(marker_key);
    }
    const auto desired_version = std::stoull(marker.at("desired_version"));
    static_cast<void>(counters_db_.incrementHashFieldsOnce(
        redis::orchAlarmCounterPublicationKey(
            result_event.device, alarm_id, marker.at("activation_id"), true),
        {
            {"alarm_id", alarm_id},
            {"desired_version", std::to_string(desired_version)},
        },
        redis::deviceCountersKey(result_event.device),
        {{"active_alarms", -1}}));
}

}  // namespace ocs
