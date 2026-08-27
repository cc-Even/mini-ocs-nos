#pragma once

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

[[nodiscard]] inline std::string processedDeviceCommandKey(std::string_view command_id) {
    return "OCS_PROCESSED_DEVICE_COMMAND|" + std::string(command_id);
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

}  // namespace ocs::redis
