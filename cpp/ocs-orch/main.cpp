#include "ocs/orchestrator_service.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <string>
#include <thread>
#include <utility>

namespace {

std::atomic<bool> running{true};

void stopHandler(int signal) {
    static_cast<void>(signal);
    running.store(false);
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
        ocs::OrchestratorService service(std::move(endpoint));
        service.initialize();
        while (running.load()) {
            const auto processed_config = service.processConfigOne("orch-main-config");
            const auto processed_result = service.processResultOne("orch-main-result");
            if (!processed_config && !processed_result) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "ocs-orch failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
