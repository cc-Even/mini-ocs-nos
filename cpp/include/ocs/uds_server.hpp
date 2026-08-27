#pragma once

#include "ocs/simulated_ocs_device.hpp"
#include "ocs/uds_protocol.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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
    void dropNextApplyReplyForTest() noexcept;

private:
    struct ClientWorker {
        int fd{-1};
        std::shared_ptr<std::atomic<bool>> done;
        std::jthread thread;
    };

    void acceptLoop(std::stop_token stop_token);
    void serveClient(int client_fd, std::stop_token stop_token);
    [[nodiscard]] uds::Frame dispatch(const uds::Frame& request);

    std::string socket_path_;
    std::shared_ptr<SimulatedOcsDevice> device_;
    std::atomic<int> listen_fd_{-1};
    std::atomic<bool> owns_socket_{false};
    std::atomic<bool> drop_next_apply_reply_{false};
    std::jthread worker_;
    std::mutex clients_mutex_;
    std::vector<ClientWorker> client_workers_;
};

}  // namespace ocs
