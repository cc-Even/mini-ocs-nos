#pragma once

#include "ocs/redis_repository.hpp"

#include <chrono>
#include <functional>
#include <string>
#include <string_view>

namespace ocs {

class OrchestratorService {
public:
    explicit OrchestratorService(
        redis::RedisEndpoint endpoint,
        std::chrono::milliseconds pending_min_idle = std::chrono::milliseconds(0));

    void initialize();
    [[nodiscard]] bool processConfigOne(
        const std::string& consumer_name,
        const std::function<void(std::string_view)>& after_phase = {});
    [[nodiscard]] bool processResultOne(const std::string& consumer_name);

private:
    redis::RedisRepository config_db_;
    redis::RedisRepository appl_db_;
    redis::RedisRepository device_db_;
    std::chrono::milliseconds pending_min_idle_;
};

}  // namespace ocs
