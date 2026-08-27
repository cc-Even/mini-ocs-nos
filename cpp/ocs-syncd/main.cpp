#include "ocs/syncd_service.hpp"
#include "ocs/uds_device_backend.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <memory>
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
    if (argc != 3) {
        std::cerr << "usage: ocs-syncd REDIS_SOCKET HWSIM_SOCKET\n";
        return 2;
    }
    std::signal(SIGINT, stopHandler);
    std::signal(SIGTERM, stopHandler);

    try {
        ocs::redis::RedisEndpoint endpoint;
        endpoint.unix_socket = argv[1];
        auto backend = std::make_unique<ocs::UdsDeviceBackend>(argv[2]);
        ocs::SyncdService service(std::move(endpoint), std::move(backend));
        service.initialize();
        while (running.load()) {
            if (!service.processOne("syncd-main")) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "ocs-syncd failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
