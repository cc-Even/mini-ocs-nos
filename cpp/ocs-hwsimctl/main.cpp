#include "ocs/uds_device_backend.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

ocs::FaultType faultType(std::string_view value) {
    if (value == "INPUT_PORT_DOWN") {
        return ocs::FaultType::kInputPortDown;
    }
    if (value == "OUTPUT_PORT_DOWN") {
        return ocs::FaultType::kOutputPortDown;
    }
    throw std::invalid_argument("fault must be INPUT_PORT_DOWN or OUTPUT_PORT_DOWN");
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "usage: ocs-hwsimctl HWSIM_SOCKET inject|clear FAULT PORT_ID\n";
        return 2;
    }
    try {
        const auto type = faultType(argv[3]);
        const auto parsed = std::stoul(argv[4]);
        if (parsed == 0 || parsed > 65535) {
            throw std::invalid_argument("port ID is outside 1..65535");
        }
        const auto port_id = static_cast<ocs::PortId>(parsed);
        ocs::UdsDeviceBackend backend(argv[1]);
        const auto result = std::string_view(argv[2]) == "inject"
                                ? backend.injectFault({.type = type, .port_id = port_id})
                                : std::string_view(argv[2]) == "clear"
                                      ? backend.clearFault({.type = type, .port_id = port_id})
                                      : throw std::invalid_argument("operation must be inject or clear");
        if (!result.error.ok()) {
            std::cerr << ocs::toString(result.error.code) << ": " << result.error.message << '\n';
            return 1;
        }
        std::cout << "OK\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ocs-hwsimctl failed: " << error.what() << '\n';
        return 2;
    }
}
