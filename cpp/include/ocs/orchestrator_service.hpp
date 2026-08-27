#pragma once

#include "ocs/redis_repository.hpp"

#include <string>

namespace ocs {

class OrchestratorService {
public:
    explicit OrchestratorService(redis::RedisEndpoint endpoint);

    void initialize();
    [[nodiscard]] bool processConfigOne(const std::string& consumer_name);
    [[nodiscard]] bool processResultOne(const std::string& consumer_name);

private:
    redis::RedisRepository config_db_;
    redis::RedisRepository appl_db_;
    redis::RedisRepository device_db_;
};

}  // namespace ocs
