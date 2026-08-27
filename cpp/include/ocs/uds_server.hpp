#pragma once

#include "ocs/simulated_ocs_device.hpp"
#include "ocs/uds_protocol.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace ocs {

class UdsServer {
public:
    UdsServer(std::string socket_path, std::shared_ptr<SimulatedOcsDevice> device);
    ~UdsServer();

    UdsServer(const UdsServer&) = delete;
    UdsServer& operator=(const UdsServer&) = delete;

    Error start();
    void stop();
    [[nodiscard]] bool running() const noexcept;

private:
    void acceptLoop(std::stop_token stop_token);
    void serveClient(int client_fd, std::stop_token stop_token);
    [[nodiscard]] uds::Frame dispatch(const uds::Frame& request);

    std::string socket_path_;
    std::shared_ptr<SimulatedOcsDevice> device_;
    std::atomic<int> listen_fd_{-1};
    std::atomic<int> client_fd_{-1};
    std::atomic<bool> owns_socket_{false};
    std::jthread worker_;
};

}  // namespace ocs
