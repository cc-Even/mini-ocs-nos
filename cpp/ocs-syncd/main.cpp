#include "ocs/syncd_service.hpp"
#include "ocs/uds_device_backend.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
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

std::chrono::milliseconds pendingMinIdle() {
    const char* configured = std::getenv("OCS_SYNCD_PENDING_MIN_IDLE_MS");
    if (configured == nullptr) {
        return std::chrono::seconds(5);
    }
    const auto value = std::stoll(configured);
    if (value < 0 || value > 3'600'000) {
        throw std::invalid_argument("OCS_SYNCD_PENDING_MIN_IDLE_MS is outside 0..3600000");
    }
    return std::chrono::milliseconds(value);
}

std::chrono::milliseconds devicePollInterval() {
    const char* configured = std::getenv("OCS_SYNCD_DEVICE_POLL_MS");
    if (configured == nullptr) {
        return std::chrono::milliseconds(250);
    }
    const auto value = std::stoll(configured);
    if (value < 10 || value > 3'600'000) {
        throw std::invalid_argument("OCS_SYNCD_DEVICE_POLL_MS is outside 10..3600000");
    }
    return std::chrono::milliseconds(value);
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
        ocs::SyncdService service(
            std::move(endpoint), std::move(backend), pendingMinIdle());
        service.initialize();
        const auto poll_interval = devicePollInterval();
        auto next_poll = std::chrono::steady_clock::now() + poll_interval;
        bool crash_before_ack = std::getenv("OCS_SYNCD_CRASH_BEFORE_ACK_ONCE") != nullptr;
        while (running.load()) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_poll) {
                static_cast<void>(service.pollDevice());
                next_poll = now + poll_interval;
            }
            const auto before_ack = [&crash_before_ack] {
                if (crash_before_ack) {
                    crash_before_ack = false;
                    std::cerr << "ocs-syncd test crash before ACK\n";
                    std::_Exit(86);
                }
            };
            if (!service.processOne("syncd-main", before_ack)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "ocs-syncd failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
