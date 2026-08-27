#include "ocs/uds_server.hpp"

#include "uds_json.hpp"

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

namespace ocs {
namespace {

Error makeError(ErrorCode code, std::string message) {
    return Error{code, std::move(message)};
}

uds::Frame errorReply(const uds::Frame& request, const Error& error, std::uint64_t generation) {
    return {
        .message_type = uds::MessageType::kErrorReply,
        .request_id = request.request_id,
        .device_generation = generation,
        .payload = uds::json::encodeError(error),
    };
}

Error removeStaleSocket(const std::string& socket_path) {
    struct stat status {};
    if (::lstat(socket_path.c_str(), &status) < 0) {
        return errno == ENOENT ? Error::success()
                              : makeError(ErrorCode::kInternalError, std::strerror(errno));
    }
    if (!S_ISSOCK(status.st_mode)) {
        return makeError(ErrorCode::kInvalidArgument, "UDS path exists and is not a socket");
    }

    const int probe_fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (probe_fd < 0) {
        return makeError(ErrorCode::kInternalError, std::strerror(errno));
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
    const int connect_result =
        ::connect(probe_fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    const int connect_error = errno;
    ::close(probe_fd);
    if (connect_result == 0) {
        return makeError(ErrorCode::kInvalidArgument, "UDS socket is already active");
    }
    if (connect_error != ECONNREFUSED && connect_error != ENOENT) {
        return makeError(ErrorCode::kInternalError, std::strerror(connect_error));
    }
    if (::unlink(socket_path.c_str()) < 0 && errno != ENOENT) {
        return makeError(ErrorCode::kInternalError, std::strerror(errno));
    }
    return Error::success();
}

}  // namespace

UdsServer::UdsServer(std::string socket_path, std::shared_ptr<SimulatedOcsDevice> device)
    : socket_path_(std::move(socket_path)), device_(std::move(device)) {
    if (!device_) {
        throw std::invalid_argument("simulated device must not be null");
    }
}

UdsServer::~UdsServer() {
    stop();
}

Error UdsServer::start() {
    if (running()) {
        return makeError(ErrorCode::kInvalidArgument, "UDS server is already running");
    }
    if (socket_path_.empty() || socket_path_.size() >= sizeof(sockaddr_un::sun_path)) {
        return makeError(ErrorCode::kInvalidArgument, "UDS socket path is invalid");
    }

    std::error_code filesystem_error;
    const auto parent = std::filesystem::path(socket_path_).parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent, filesystem_error);
        if (filesystem_error) {
            return makeError(ErrorCode::kInternalError, "failed to create UDS runtime directory");
        }
    }
    if (const auto stale_error = removeStaleSocket(socket_path_); !stale_error.ok()) {
        return stale_error;
    }

    const int server_fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (server_fd < 0) {
        return makeError(ErrorCode::kInternalError, std::strerror(errno));
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, socket_path_.c_str(), socket_path_.size() + 1);
    if (::bind(server_fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
        const std::string message = std::strerror(errno);
        ::close(server_fd);
        return makeError(ErrorCode::kInternalError, message);
    }
    if (::chmod(socket_path_.c_str(), 0660) < 0 || ::listen(server_fd, 8) < 0) {
        const std::string message = std::strerror(errno);
        ::close(server_fd);
        std::filesystem::remove(socket_path_, filesystem_error);
        return makeError(ErrorCode::kInternalError, message);
    }

    listen_fd_.store(server_fd);
    owns_socket_.store(true);
    worker_ = std::jthread([this](std::stop_token token) { acceptLoop(token); });
    return Error::success();
}

void UdsServer::stop() {
    worker_.request_stop();
    const int server_fd = listen_fd_.exchange(-1);
    if (server_fd >= 0) {
        ::shutdown(server_fd, SHUT_RDWR);
        ::close(server_fd);
    }
    const int client_fd = client_fd_.exchange(-1);
    if (client_fd >= 0) {
        ::shutdown(client_fd, SHUT_RDWR);
        ::close(client_fd);
    }
    if (worker_.joinable()) {
        worker_.join();
    }
    if (owns_socket_.exchange(false)) {
        struct stat status {};
        if (::lstat(socket_path_.c_str(), &status) == 0 && S_ISSOCK(status.st_mode)) {
            static_cast<void>(::unlink(socket_path_.c_str()));
        }
    }
}

bool UdsServer::running() const noexcept {
    return listen_fd_.load() >= 0;
}

void UdsServer::acceptLoop(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        const int server_fd = listen_fd_.load();
        if (server_fd < 0) {
            break;
        }
        const int client_fd = ::accept4(server_fd, nullptr, nullptr, SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        client_fd_.store(client_fd);
        serveClient(client_fd, stop_token);
        int expected = client_fd;
        if (client_fd_.compare_exchange_strong(expected, -1)) {
            ::close(client_fd);
        }
    }
}

void UdsServer::serveClient(int client_fd, std::stop_token stop_token) {
    std::vector<std::byte> buffer(uds::kHeaderSize + uds::kMaxPayloadSize);
    while (!stop_token.stop_requested()) {
        const auto received = ::recv(client_fd, buffer.data(), buffer.size(), MSG_TRUNC);
        if (received <= 0) {
            break;
        }
        if (static_cast<std::size_t>(received) > buffer.size()) {
            break;
        }
        const std::vector<std::byte> packet(buffer.begin(), buffer.begin() + received);
        const auto decoded = uds::decode(packet);
        if (!decoded.ok()) {
            break;
        }

        const auto response = dispatch(decoded.frame.value());
        const auto encoded = uds::encode(response);
        if (!encoded.ok()) {
            break;
        }
        const auto sent = ::send(client_fd, encoded.bytes.data(), encoded.bytes.size(), MSG_NOSIGNAL);
        if (sent != static_cast<ssize_t>(encoded.bytes.size())) {
            break;
        }
    }
}

uds::Frame UdsServer::dispatch(const uds::Frame& request) {
    const auto generation = device_->getDeviceInfo().generation;
    try {
        uds::Frame response{
            .request_id = request.request_id,
            .device_generation = generation,
            .payload = {},
        };
        switch (request.message_type) {
            case uds::MessageType::kHello:
                response.message_type = uds::MessageType::kHelloReply;
                response.payload = uds::json::encodeDeviceInfo(device_->getDeviceInfo());
                return response;
            case uds::MessageType::kGetDeviceInfo:
                response.message_type = uds::MessageType::kDeviceInfoReply;
                response.payload = uds::json::encodeDeviceInfo(device_->getDeviceInfo());
                return response;
            case uds::MessageType::kApplyConnections: {
                const auto apply = uds::json::decodeApplyRequest(request.payload);
                response.message_type = uds::MessageType::kApplyResult;
                response.payload =
                    uds::json::encodeApplyResult(device_->applyConnections(apply.commands, apply.options));
                return response;
            }
            case uds::MessageType::kGetConnections:
                response.message_type = uds::MessageType::kConnectionsReply;
                response.payload = uds::json::encodeConnections(device_->getConnections());
                return response;
            case uds::MessageType::kGetPortState: {
                const auto port = uds::json::decodePortRequest(request.payload);
                response.message_type = uds::MessageType::kPortStateReply;
                response.payload = uds::json::encodePortState(device_->getPortState(port.direction, port.port_id));
                return response;
            }
            case uds::MessageType::kReset: {
                response.message_type = uds::MessageType::kResetResult;
                const auto result = device_->reset(uds::json::decodeResetRequest(request.payload));
                response.device_generation = result.generation;
                response.payload = uds::json::encodeResetResult(result);
                return response;
            }
            case uds::MessageType::kInjectFault: {
                const auto fault = uds::json::decodeFaultRequest(request.payload);
                response.message_type = uds::MessageType::kFaultResult;
                const auto result = fault.clear
                    ? device_->clearFault({fault.fault.type, fault.fault.port_id})
                    : device_->injectFault(fault.fault);
                response.payload = uds::json::encodeFaultResult(result);
                return response;
            }
            default:
                return errorReply(
                    request,
                    makeError(ErrorCode::kUnsupported, "UDS request type is not supported"),
                    generation);
        }
    } catch (const std::exception& error) {
        return errorReply(
            request,
            makeError(ErrorCode::kProtocolMalformed, error.what()),
            generation);
    }
}

}  // namespace ocs
