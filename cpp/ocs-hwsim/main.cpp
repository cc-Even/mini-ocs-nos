#include "ocs/simulated_ocs_device.hpp"
#include "ocs/uds_server.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace {

std::atomic<bool> running{true};

void stopHandler(int signal) {
    static_cast<void>(signal);
    running.store(false);
}

ocs::DeviceInfo defaultDeviceInfo() {
    const auto generation = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    return {
        .name = "ocs0",
        .input_port_count = 16,
        .output_port_count = 16,
        .model = "SIM-16X16",
        .serial_number = "SIM-0001",
        .firmware_version = "sim-1.0.0",
        .generation = generation,
    };
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string socket_path = argc > 1 ? argv[1] : "/tmp/mini-ocs/ocs-hwsim.sock";
    std::signal(SIGINT, stopHandler);
    std::signal(SIGTERM, stopHandler);

    auto device = std::make_shared<ocs::SimulatedOcsDevice>(defaultDeviceInfo());
    ocs::UdsServer server(socket_path, device);
    if (const auto error = server.start(); !error.ok()) {
        std::cerr << "ocs-hwsim failed: " << ocs::toString(error.code) << ": " << error.message
                  << '\n';
        return 1;
    }

    std::cout << "ocs-hwsim ready socket=" << socket_path
              << " generation=" << device->getDeviceInfo().generation << '\n';
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    server.stop();
    return 0;
}
