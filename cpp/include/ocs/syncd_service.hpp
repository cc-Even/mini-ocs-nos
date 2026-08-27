#pragma once

#include "ocs/device_command.hpp"
#include "ocs/device_api.hpp"
#include "ocs/redis_repository.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <cstdint>
#include <string>
#include <string_view>

namespace ocs {

class SyncdService {
public:
    SyncdService(
        redis::RedisEndpoint endpoint,
        std::unique_ptr<OcsDeviceApi> device,
        std::chrono::milliseconds pending_min_idle = std::chrono::seconds(5));

    void initialize();
    [[nodiscard]] bool pollDevice();
    [[nodiscard]] bool processOne(
        const std::string& consumer_name,
        const std::function<void()>& before_ack = {},
        const std::function<void(std::string_view)>& after_phase = {});

private:
    [[nodiscard]] bool isAlreadyProcessed(const redis::EventEnvelope& command_event);
    [[nodiscard]] bool isStaleVersion(
        const redis::EventEnvelope& command_event,
        const DeviceCommandBatch& batch);
    void markProcessed(
        const redis::EventEnvelope& command_event,
        const DeviceCommandBatch& batch,
        bool applied);
    void acknowledge(const std::string& message_id);
    [[nodiscard]] long long publishState(
        const redis::EventEnvelope& command_event,
        const DeviceCommandBatch& batch,
        const ApplyResult& result);
    void publishResult(
        const redis::EventEnvelope& command_event,
        const DeviceCommandBatch& batch,
        const ApplyResult& result);
    void publishDeviceState(
        const DeviceInfo& info,
        std::size_t actual_connection_count,
        std::string_view status,
        const Error& error);
    void scheduleGenerationRecovery(
        const DeviceInfo& info,
        const std::vector<AppliedConnection>& actual);

    redis::RedisRepository device_db_;
    redis::RedisRepository state_db_;
    redis::RedisRepository counters_db_;
    redis::RedisRepository config_db_;
    std::unique_ptr<OcsDeviceApi> device_;
    std::chrono::milliseconds pending_min_idle_;
    std::string device_name_;
    std::uint64_t device_generation_{};
};

}  // namespace ocs
