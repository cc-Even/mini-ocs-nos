#pragma once

#include "ocs/device_command.hpp"
#include "ocs/device_api.hpp"
#include "ocs/redis_repository.hpp"

#include <memory>
#include <string>

namespace ocs {

class SyncdService {
public:
    SyncdService(redis::RedisEndpoint endpoint, std::unique_ptr<OcsDeviceApi> device);

    void initialize();
    [[nodiscard]] bool processOne(const std::string& consumer_name);

private:
    [[nodiscard]] bool isAlreadyProcessed(const redis::EventEnvelope& command_event);
    [[nodiscard]] bool isStaleVersion(const redis::EventEnvelope& command_event);
    void markProcessed(const redis::EventEnvelope& command_event, bool applied);
    void acknowledge(const std::string& message_id);
    void publishState(
        const redis::EventEnvelope& command_event,
        const DeviceCommandBatch& batch,
        const ApplyResult& result);
    void publishResult(const redis::EventEnvelope& command_event, const ApplyResult& result);

    redis::RedisRepository device_db_;
    redis::RedisRepository state_db_;
    redis::RedisRepository counters_db_;
    std::unique_ptr<OcsDeviceApi> device_;
};

}  // namespace ocs
