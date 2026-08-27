#include "ocs/simulated_ocs_device.hpp"
#include "ocs/uds_device_backend.hpp"
#include "ocs/uds_protocol.hpp"
#include "ocs/uds_server.hpp"

#include <gtest/gtest.h>

#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <cstdlib>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

ocs::DeviceInfo defaultDeviceInfo() {
    return {
        .name = "ocs0",
        .input_port_count = 16,
        .output_port_count = 16,
        .model = "SIM-16X16",
        .serial_number = "SIM-0001",
        .firmware_version = "sim-1.0.0",
        .generation = 1,
    };
}

class UdsTransportTest : public testing::Test {
protected:
    void SetUp() override {
        char directory_template[] = "/tmp/mini-ocs-uds-test-XXXXXX";
        const char* created = ::mkdtemp(directory_template);
        ASSERT_NE(created, nullptr);
        runtime_directory_ = created;
        socket_path_ = runtime_directory_ + "/hwsim.sock";

        device_ = std::make_shared<ocs::SimulatedOcsDevice>(defaultDeviceInfo());
        server_ = std::make_unique<ocs::UdsServer>(socket_path_, device_);
        ASSERT_TRUE(server_->start().ok());
        backend_ = std::make_unique<ocs::UdsDeviceBackend>(socket_path_);
    }

    void TearDown() override {
        backend_.reset();
        if (server_) {
            server_->stop();
        }
        std::error_code error;
        std::filesystem::remove_all(runtime_directory_, error);
    }

    std::string runtime_directory_;
    std::string socket_path_;
    std::shared_ptr<ocs::SimulatedOcsDevice> device_;
    std::unique_ptr<ocs::UdsServer> server_;
    std::unique_ptr<ocs::UdsDeviceBackend> backend_;
};

TEST_F(UdsTransportTest, HandshakeAndDeviceInfoExposeGeneration) {
    const auto info = backend_->getDeviceInfo();

    EXPECT_EQ(info.name, "ocs0");
    EXPECT_EQ(info.input_port_count, 16);
    EXPECT_EQ(backend_->deviceGeneration(), 1);
    EXPECT_EQ(backend_->getHealth().status, ocs::DeviceOperStatus::kReady);
}

TEST_F(UdsTransportTest, AppliesQueriesAndRemovesConnection) {
    const ocs::ConnectionCommand create{
        .id = "conn-001",
        .input_port = 3,
        .output_port = 11,
        .desired_version = 7,
    };

    const auto applied = backend_->applyConnections({create}, {});

    ASSERT_TRUE(applied.ok());
    ASSERT_EQ(backend_->getConnections().size(), 1);
    EXPECT_EQ(backend_->getConnections().front().applied_version, 7);
    EXPECT_EQ(backend_->getInputPortState(3).oper_status, ocs::PortOperStatus::kUp);
    EXPECT_EQ(backend_->getOutputPortState(11).direction, ocs::PortDirection::kOutput);

    const ocs::ConnectionCommand remove{
        .operation = ocs::ConnectionOperation::kRemove,
        .id = "conn-001",
    };
    EXPECT_TRUE(backend_->applyConnections({remove}, {}).ok());
    EXPECT_TRUE(backend_->getConnections().empty());
}

TEST_F(UdsTransportTest, FaultAndResetCallsCrossSocketBoundary) {
    ASSERT_TRUE(
        backend_->injectFault({.type = ocs::FaultType::kNextApplyError}).error.ok());
    const auto failed = backend_->applyConnections(
        {{.id = "conn-001", .input_port = 1, .output_port = 9, .desired_version = 1}}, {});
    EXPECT_EQ(failed.error.code, ocs::ErrorCode::kApplyFailed);

    ASSERT_TRUE(
        backend_->injectFault({.type = ocs::FaultType::kInputPortDown, .port_id = 4}).error.ok());
    EXPECT_EQ(backend_->getInputPortState(4).oper_status, ocs::PortOperStatus::kDown);
    ASSERT_TRUE(
        backend_
            ->clearFault({.type = ocs::FaultType::kInputPortDown, .port_id = 4})
            .error.ok());
    EXPECT_EQ(backend_->getInputPortState(4).oper_status, ocs::PortOperStatus::kUp);

    const auto reset = backend_->reset(ocs::ResetMode::kHard);
    EXPECT_TRUE(reset.error.ok());
    EXPECT_EQ(reset.generation, 2);
    EXPECT_EQ(backend_->deviceGeneration(), 2);
}

TEST_F(UdsTransportTest, ReconnectsReadAfterServerRestartAndRefreshesGeneration) {
    ASSERT_EQ(backend_->getDeviceInfo().generation, 1);
    ASSERT_TRUE(
        backend_
            ->applyConnections(
                {{.id = "before-restart", .input_port = 1, .output_port = 9, .desired_version = 1}},
                {})
            .ok());

    server_->stop();
    const auto unconfirmed = backend_->applyConnections(
        {{.id = "during-restart", .input_port = 2, .output_port = 10, .desired_version = 1}}, {});
    EXPECT_FALSE(unconfirmed.ok());

    auto replacement_info = defaultDeviceInfo();
    replacement_info.generation = 9;
    device_ = std::make_shared<ocs::SimulatedOcsDevice>(replacement_info);
    server_ = std::make_unique<ocs::UdsServer>(socket_path_, device_);
    ASSERT_TRUE(server_->start().ok());

    const auto refreshed = backend_->getDeviceInfo();

    EXPECT_EQ(refreshed.generation, 9);
    EXPECT_EQ(backend_->deviceGeneration(), 9);
    EXPECT_TRUE(backend_->getConnections().empty());
}

TEST_F(UdsTransportTest, RejectsNonSocketAtRuntimePathWithoutDeletingIt) {
    backend_.reset();
    server_->stop();
    {
        std::ofstream ordinary_file(socket_path_);
        ordinary_file << "do not remove";
    }
    ocs::UdsServer replacement(socket_path_, device_);

    const auto result = replacement.start();

    EXPECT_EQ(result.code, ocs::ErrorCode::kInvalidArgument);
    EXPECT_TRUE(std::filesystem::is_regular_file(socket_path_));
}

TEST_F(UdsTransportTest, RemovesStaleSocketBeforeListening) {
    backend_.reset();
    server_->stop();
    const int stale_fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    ASSERT_GE(stale_fd, 0);
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, socket_path_.c_str(), socket_path_.size() + 1);
    ASSERT_EQ(::bind(stale_fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    ::close(stale_fd);

    ocs::UdsServer replacement(socket_path_, device_);

    EXPECT_TRUE(replacement.start().ok());
    replacement.stop();
}

enum class ScriptedPeerMode {
    kNoReply,
    kWrongRequestId,
};

class ScriptedPeer {
public:
    ScriptedPeer(std::string socket_path, ScriptedPeerMode mode)
        : socket_path_(std::move(socket_path)), mode_(mode) {}

    ~ScriptedPeer() { stop(); }

    bool start() {
        const int server_fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
        if (server_fd < 0) {
            return false;
        }
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::memcpy(address.sun_path, socket_path_.c_str(), socket_path_.size() + 1);
        if (::bind(server_fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
            ::listen(server_fd, 1) < 0) {
            ::close(server_fd);
            return false;
        }
        server_fd_.store(server_fd);
        worker_ = std::jthread([this](std::stop_token token) { run(token); });
        return true;
    }

    void stop() {
        worker_.request_stop();
        wait_condition_.notify_all();
        const int client_fd = client_fd_.exchange(-1);
        if (client_fd >= 0) {
            ::shutdown(client_fd, SHUT_RDWR);
            ::close(client_fd);
        }
        const int server_fd = server_fd_.exchange(-1);
        if (server_fd >= 0) {
            ::shutdown(server_fd, SHUT_RDWR);
            ::close(server_fd);
        }
        if (worker_.joinable()) {
            worker_.join();
        }
        std::error_code error;
        std::filesystem::remove(socket_path_, error);
    }

private:
    void run(std::stop_token stop_token) {
        const int client_fd = ::accept4(server_fd_.load(), nullptr, nullptr, SOCK_CLOEXEC);
        if (client_fd < 0) {
            return;
        }
        client_fd_.store(client_fd);
        std::vector<std::byte> bytes(ocs::uds::kHeaderSize + ocs::uds::kMaxPayloadSize);
        const auto received = ::recv(client_fd, bytes.data(), bytes.size(), 0);
        if (received <= 0) {
            return;
        }
        bytes.resize(static_cast<std::size_t>(received));
        const auto request = ocs::uds::decode(bytes);
        if (!request.ok()) {
            return;
        }

        if (mode_ == ScriptedPeerMode::kWrongRequestId) {
            const ocs::uds::Frame response{
                .message_type = ocs::uds::MessageType::kHelloReply,
                .request_id = request.frame->request_id + 1,
                .device_generation = 1,
                .payload = "{}",
            };
            const auto encoded = ocs::uds::encode(response);
            if (encoded.ok()) {
                static_cast<void>(
                    ::send(client_fd, encoded.bytes.data(), encoded.bytes.size(), MSG_NOSIGNAL));
            }
            return;
        }

        std::unique_lock lock(wait_mutex_);
        wait_condition_.wait(lock, stop_token, [] { return false; });
    }

    std::string socket_path_;
    ScriptedPeerMode mode_;
    std::atomic<int> server_fd_{-1};
    std::atomic<int> client_fd_{-1};
    std::mutex wait_mutex_;
    std::condition_variable_any wait_condition_;
    std::jthread worker_;
};

TEST(UdsFailureTest, EnforcesResponseDeadline) {
    char directory_template[] = "/tmp/mini-ocs-timeout-test-XXXXXX";
    const char* created = ::mkdtemp(directory_template);
    ASSERT_NE(created, nullptr);
    const std::string runtime_directory = created;
    const std::string socket_path = runtime_directory + "/peer.sock";
    ScriptedPeer peer(socket_path, ScriptedPeerMode::kNoReply);
    ASSERT_TRUE(peer.start());
    ocs::UdsDeviceBackend backend(socket_path, std::chrono::milliseconds(30));

    const auto start = std::chrono::steady_clock::now();
    const auto result = backend.applyConnections({}, {});
    const auto duration = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(result.error.code, ocs::ErrorCode::kApplyTimeout);
    EXPECT_LT(duration, std::chrono::milliseconds(500));
    peer.stop();
    std::filesystem::remove_all(runtime_directory);
}

TEST(UdsFailureTest, RejectsMismatchedResponseRequestId) {
    char directory_template[] = "/tmp/mini-ocs-correlation-test-XXXXXX";
    const char* created = ::mkdtemp(directory_template);
    ASSERT_NE(created, nullptr);
    const std::string runtime_directory = created;
    const std::string socket_path = runtime_directory + "/peer.sock";
    ScriptedPeer peer(socket_path, ScriptedPeerMode::kWrongRequestId);
    ASSERT_TRUE(peer.start());
    ocs::UdsDeviceBackend backend(socket_path);

    const auto result = backend.applyConnections({}, {});

    EXPECT_EQ(result.error.code, ocs::ErrorCode::kProtocolMalformed);
    peer.stop();
    std::filesystem::remove_all(runtime_directory);
}

class StandaloneHwsimTest : public testing::Test {
protected:
    void SetUp() override {
        char directory_template[] = "/tmp/mini-ocs-hwsim-test-XXXXXX";
        const char* created = ::mkdtemp(directory_template);
        ASSERT_NE(created, nullptr);
        runtime_directory_ = created;
        socket_path_ = runtime_directory_ + "/hwsim.sock";

        child_pid_ = ::fork();
        ASSERT_GE(child_pid_, 0);
        if (child_pid_ == 0) {
            ::execl(OCS_HWSIM_PATH, OCS_HWSIM_PATH, socket_path_.c_str(), nullptr);
            std::_Exit(127);
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!std::filesystem::exists(socket_path_) && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE(std::filesystem::exists(socket_path_));
    }

    void TearDown() override {
        if (child_pid_ > 0) {
            ::kill(child_pid_, SIGTERM);
            int status = 0;
            ASSERT_EQ(::waitpid(child_pid_, &status, 0), child_pid_);
            EXPECT_TRUE(WIFEXITED(status));
            EXPECT_EQ(WEXITSTATUS(status), 0);
        }
        std::error_code error;
        std::filesystem::remove_all(runtime_directory_, error);
    }

    std::string runtime_directory_;
    std::string socket_path_;
    pid_t child_pid_{-1};
};

TEST_F(StandaloneHwsimTest, ServesDeviceOperationsFromIndependentProcess) {
    ocs::UdsDeviceBackend backend(socket_path_);

    EXPECT_EQ(backend.getDeviceInfo().name, "ocs0");
    ASSERT_TRUE(
        backend
            .applyConnections(
                {{.id = "conn-001", .input_port = 1, .output_port = 9, .desired_version = 1}},
                {})
            .ok());
    ASSERT_EQ(backend.getConnections().size(), 1);
    EXPECT_EQ(backend.getConnections().front().id, "conn-001");
}

}  // namespace
