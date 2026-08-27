#include "ocs/orchestrator_service.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace {

std::atomic<bool> running{true};

void stopHandler(int signal) {
    static_cast<void>(signal);
    running.store(false);
}

long long environmentInteger(const char* name, long long fallback, long long maximum) {
    const char* configured = std::getenv(name);
    if (configured == nullptr) {
        return fallback;
    }
    const auto value = std::stoll(configured);
    if (value < 0 || value > maximum) {
        throw std::invalid_argument(std::string(name) + " is outside the supported range");
    }
    return value;
}

ocs::ApplyRetryPolicy retryPolicy() {
    return {
        .max_retries = static_cast<std::size_t>(
            environmentInteger("OCS_ORCH_APPLY_MAX_RETRIES", 3, 100)),
        .base_backoff = std::chrono::milliseconds(
            environmentInteger("OCS_ORCH_APPLY_RETRY_BASE_MS", 100, 3'600'000)),
        .max_backoff = std::chrono::milliseconds(
            environmentInteger("OCS_ORCH_APPLY_RETRY_MAX_MS", 5000, 3'600'000)),
    };
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "usage: ocs-orch REDIS_SOCKET\n";
        return 2;
    }
    std::signal(SIGINT, stopHandler);
    std::signal(SIGTERM, stopHandler);

    try {
        ocs::redis::RedisEndpoint endpoint;
        endpoint.unix_socket = argv[1];
        ocs::OrchestratorService service(
            std::move(endpoint), std::chrono::seconds(5), retryPolicy());
        service.initialize();
        while (running.load()) {
            const auto processed_config = service.processConfigOne("orch-main-config");
            const auto processed_result = service.processResultOne("orch-main-result");
            const auto processed_retry = service.processRetryOne("orch-main-retry");
            if (!processed_config && !processed_result && !processed_retry) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "ocs-orch failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
