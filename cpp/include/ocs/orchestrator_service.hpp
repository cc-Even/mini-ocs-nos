#pragma once

#include "ocs/redis_repository.hpp"

#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

namespace ocs {

struct ApplyRetryPolicy {
    std::size_t max_retries{3};
    std::chrono::milliseconds base_backoff{100};
    std::chrono::milliseconds max_backoff{5000};
};

class OrchestratorService {
public:
    explicit OrchestratorService(
        redis::RedisEndpoint endpoint,
        std::chrono::milliseconds pending_min_idle = std::chrono::seconds(5),
        ApplyRetryPolicy retry_policy = {});

    void initialize();
    [[nodiscard]] bool processConfigOne(
        const std::string& consumer_name,
        const std::function<void(std::string_view)>& after_phase = {});
    [[nodiscard]] bool processResultOne(
        const std::string& consumer_name,
        const std::function<void(std::string_view)>& after_phase = {});
    [[nodiscard]] bool processRetryOne(
        const std::string& consumer_name,
        const std::function<void(std::string_view)>& after_phase = {});

private:
    void handleTimeoutResult(
        const redis::StreamMessage& message,
        const std::string& command_id,
        const std::string& error_message,
        const std::function<void(std::string_view)>& after_phase);
    void publishTimeoutAlarm(
        const redis::EventEnvelope& result_event,
        const std::string& error_message);
    void clearTimeoutAlarm(const redis::EventEnvelope& result_event);

    redis::RedisRepository config_db_;
    redis::RedisRepository appl_db_;
    redis::RedisRepository device_db_;
    redis::RedisRepository alarm_db_;
    redis::RedisRepository counters_db_;
    std::chrono::milliseconds pending_min_idle_;
    ApplyRetryPolicy retry_policy_;
};

}  // namespace ocs
