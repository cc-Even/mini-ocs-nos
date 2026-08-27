#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace ocs::redis {

enum class LogicalDb : int {
    kAppl = 0,
    kDevice = 1,
    kCounters = 2,
    kConfig = 4,
    kState = 6,
    kAlarm = 8,
};

inline constexpr std::string_view kConfigEvents = "OCS_CONFIG_EVENTS";
inline constexpr std::string_view kDeviceCommands = "OCS_DEVICE_COMMANDS";
inline constexpr std::string_view kDeviceResults = "OCS_DEVICE_RESULTS";
inline constexpr std::string_view kDeviceRetries = "OCS_DEVICE_RETRIES";
inline constexpr std::string_view kStateEvents = "OCS_STATE_EVENTS";
inline constexpr std::string_view kAlarmEvents = "OCS_ALARM_EVENTS";
inline constexpr std::string_view kFaultCommands = "OCS_FAULT_COMMANDS";

[[nodiscard]] inline std::string deviceConfigKey(std::string_view device) {
    return "OCS_DEVICE|" + std::string(device);
}

[[nodiscard]] inline std::string inputPortConfigKey(std::string_view device, std::string_view port_id) {
    return "OCS_INPUT_PORT|" + std::string(device) + "|" + std::string(port_id);
}

[[nodiscard]] inline std::string outputPortConfigKey(std::string_view device, std::string_view port_id) {
    return "OCS_OUTPUT_PORT|" + std::string(device) + "|" + std::string(port_id);
}

[[nodiscard]] inline std::string connectionConfigKey(
    std::string_view device,
    std::string_view connection_id) {
    return "OCS_CONNECTION|" + std::string(device) + "|" + std::string(connection_id);
}

[[nodiscard]] inline std::string connectionAppKey(
    std::string_view device,
    std::string_view connection_id) {
    return "OCS_CONNECTION_APP|" + std::string(device) + "|" + std::string(connection_id);
}

[[nodiscard]] inline std::string connectionDeviceKey(
    std::string_view device,
    std::string_view connection_id) {
    return "OCS_CONNECTION_DEVICE|" + std::string(device) + "|" + std::string(connection_id);
}

[[nodiscard]] inline std::string syncdConnectionVersionKey(
    std::string_view device,
    std::string_view connection_id) {
    return "OCS_SYNCD_CONNECTION_VERSION|" + std::string(device) + "|" +
           std::string(connection_id);
}

[[nodiscard]] inline std::string connectionDevicePattern(std::string_view device) {
    return "OCS_CONNECTION_DEVICE|" + std::string(device) + "|*";
}

[[nodiscard]] inline std::string syncdGenerationRecoveryKey(
    std::string_view device,
    std::uint64_t generation) {
    return "OCS_SYNCD_GENERATION_RECOVERY|" + std::string(device) + "|" +
           std::to_string(generation);
}

[[nodiscard]] inline std::string syncdGenerationStatePublicationKey(
    std::string_view device,
    std::uint64_t generation,
    std::string_view connection_id) {
    return "OCS_SYNCD_GENERATION_STATE_PUBLISHED|" + std::string(device) + "|" +
           std::to_string(generation) + "|" + std::string(connection_id);
}

[[nodiscard]] inline std::string syncdGenerationCountersPublicationKey(
    std::string_view device,
    std::uint64_t generation) {
    return "OCS_SYNCD_GENERATION_COUNTERS_PUBLISHED|" + std::string(device) + "|" +
           std::to_string(generation);
}

[[nodiscard]] inline std::string processedDeviceCommandKey(std::string_view command_id) {
    return "OCS_PROCESSED_DEVICE_COMMAND|" + std::string(command_id);
}

[[nodiscard]] inline std::string orchConfigBatchKey(std::string_view event_id) {
    return "OCS_ORCH_CONFIG_BATCH|" + std::string(event_id);
}

[[nodiscard]] inline std::string orchApplicationPublicationKey(std::string_view event_id) {
    return "OCS_ORCH_APPLICATION_PUBLISHED|" + std::string(event_id);
}

[[nodiscard]] inline std::string orchDeviceStatePublicationKey(std::string_view event_id) {
    return "OCS_ORCH_DEVICE_STATE_PUBLISHED|" + std::string(event_id);
}

[[nodiscard]] inline std::string orchDeviceCommandPublicationKey(std::string_view event_id) {
    return "OCS_ORCH_DEVICE_COMMAND_PUBLISHED|" + std::string(event_id);
}

[[nodiscard]] inline std::string orchResultApplicationPublicationKey(
    std::string_view result_event_id) {
    return "OCS_ORCH_RESULT_APPLICATION_PUBLISHED|" + std::string(result_event_id);
}

[[nodiscard]] inline std::string orchResultDevicePublicationKey(
    std::string_view result_event_id) {
    return "OCS_ORCH_RESULT_DEVICE_PUBLISHED|" + std::string(result_event_id);
}

[[nodiscard]] inline std::string orchTimeoutApplicationPublicationKey(
    std::string_view command_id) {
    return "OCS_ORCH_TIMEOUT_APPLICATION_PUBLISHED|" + std::string(command_id);
}

[[nodiscard]] inline std::string orchTimeoutDevicePublicationKey(std::string_view command_id) {
    return "OCS_ORCH_TIMEOUT_DEVICE_PUBLISHED|" + std::string(command_id);
}

[[nodiscard]] inline std::string orchRetryPublicationKey(std::string_view command_id) {
    return "OCS_ORCH_RETRY_PUBLISHED|" + std::string(command_id);
}

[[nodiscard]] inline std::string orchRetryApplicationPublicationKey(
    std::string_view retry_event_id) {
    return "OCS_ORCH_RETRY_APPLICATION_PUBLISHED|" + std::string(retry_event_id);
}

[[nodiscard]] inline std::string orchRetryDevicePublicationKey(
    std::string_view retry_event_id) {
    return "OCS_ORCH_RETRY_DEVICE_PUBLISHED|" + std::string(retry_event_id);
}

[[nodiscard]] inline std::string orchRetryCommandPublicationKey(
    std::string_view retry_event_id) {
    return "OCS_ORCH_RETRY_COMMAND_PUBLISHED|" + std::string(retry_event_id);
}

[[nodiscard]] inline std::string deviceApplyAttemptKey(std::string_view command_id) {
    return "OCS_DEVICE_APPLY_ATTEMPT|" + std::string(command_id);
}

[[nodiscard]] inline std::string deviceApplyResultKey(std::string_view command_id) {
    return "OCS_DEVICE_APPLY_RESULT|" + std::string(command_id);
}

[[nodiscard]] inline std::string syncdStatePublicationKey(
    std::string_view command_id,
    std::string_view connection_id) {
    return "OCS_SYNCD_STATE_PUBLISHED|" + std::string(command_id) + "|" +
           std::string(connection_id);
}

[[nodiscard]] inline std::string syncdCountersPublicationKey(std::string_view command_id) {
    return "OCS_SYNCD_COUNTERS_PUBLISHED|" + std::string(command_id);
}

[[nodiscard]] inline std::string syncdResultPublicationKey(
    std::string_view command_id,
    std::string_view connection_id) {
    return "OCS_SYNCD_RESULT_PUBLISHED|" + std::string(command_id) + "|" +
           std::string(connection_id);
}

[[nodiscard]] inline std::string connectionStateKey(
    std::string_view device,
    std::string_view connection_id) {
    return "OCS_CONNECTION_STATE|" + std::string(device) + "|" + std::string(connection_id);
}

[[nodiscard]] inline std::string configRevisionKey(std::string_view device) {
    return "OCS_CONFIG_REVISION|" + std::string(device);
}

[[nodiscard]] inline std::string deviceStateKey(std::string_view device) {
    return "OCS_DEVICE_STATE|" + std::string(device);
}

[[nodiscard]] inline std::string inputPortStateKey(
    std::string_view device,
    std::string_view port_id) {
    return "OCS_INPUT_PORT_STATE|" + std::string(device) + "|" + std::string(port_id);
}

[[nodiscard]] inline std::string outputPortStateKey(
    std::string_view device,
    std::string_view port_id) {
    return "OCS_OUTPUT_PORT_STATE|" + std::string(device) + "|" + std::string(port_id);
}

[[nodiscard]] inline std::string deviceCountersKey(std::string_view device) {
    return "OCS_DEVICE_COUNTERS|" + std::string(device);
}

[[nodiscard]] inline std::string connectionCountersKey(
    std::string_view device,
    std::string_view connection_id) {
    return "OCS_CONNECTION_COUNTERS|" + std::string(device) + "|" +
           std::string(connection_id);
}

[[nodiscard]] inline std::string activeAlarmKey(
    std::string_view device,
    std::string_view alarm_id) {
    return "OCS_ACTIVE_ALARM|" + std::string(device) + "|" + std::string(alarm_id);
}

[[nodiscard]] inline std::string applyTimeoutAlarmId(std::string_view connection_id) {
    return "apply-timeout-" + std::string(connection_id);
}

[[nodiscard]] inline std::string orchAlarmPublicationKey(std::string_view result_event_id) {
    return "OCS_ORCH_ALARM_PUBLISHED|" + std::string(result_event_id);
}

[[nodiscard]] inline std::string orchAlarmClearPublicationKey(std::string_view result_event_id) {
    return "OCS_ORCH_ALARM_CLEAR_PUBLISHED|" + std::string(result_event_id);
}

[[nodiscard]] inline std::string orchAlarmCounterPublicationKey(
    std::string_view device,
    std::string_view alarm_id,
    std::string_view activation_id,
    bool clear) {
    return "OCS_ORCH_ALARM_COUNTER_" + std::string(clear ? "CLEARED|" : "RAISED|") +
           std::string(device) + "|" + std::string(alarm_id) + "|" +
           std::string(activation_id);
}

}  // namespace ocs::redis
