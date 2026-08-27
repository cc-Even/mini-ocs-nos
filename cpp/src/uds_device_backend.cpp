#include "ocs/uds_device_backend.hpp"

#include "uds_json.hpp"

#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <vector>

namespace ocs {
namespace {

Error makeError(ErrorCode code, std::string message) {
    return Error{code, std::move(message)};
}

uds::DecodeResult failed(Error error) {
    return {std::move(error), {}};
}

}  // namespace

UdsDeviceBackend::UdsDeviceBackend(std::string socket_path, std::chrono::milliseconds timeout)
    : socket_path_(std::move(socket_path)), timeout_(timeout) {
    if (socket_path_.empty() || socket_path_.size() >= sizeof(sockaddr_un::sun_path)) {
        throw std::invalid_argument("UDS socket path is invalid");
    }
    if (timeout_ <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("UDS timeout must be positive");
    }
}

UdsDeviceBackend::~UdsDeviceBackend() {
    std::scoped_lock lock(mutex_);
    closeLocked();
}

DeviceInfo UdsDeviceBackend::getDeviceInfo() const {
    std::scoped_lock lock(mutex_);
    const auto response = exchangeLocked(
        uds::MessageType::kGetDeviceInfo, uds::MessageType::kDeviceInfoReply, "{}");
    if (!response.ok()) {
        throw std::runtime_error(response.error.message);
    }
    return uds::json::decodeDeviceInfo(response.frame->payload);
}

DeviceHealth UdsDeviceBackend::getHealth() const {
    try {
        static_cast<void>(getDeviceInfo());
        return {DeviceOperStatus::kReady, Error::success()};
    } catch (const std::exception& error) {
        return {DeviceOperStatus::kFailed, makeError(ErrorCode::kDeviceNotReady, error.what())};
    }
}

ApplyResult UdsDeviceBackend::applyConnections(
    const std::vector<ConnectionCommand>& commands,
    const ApplyOptions& options) {
    std::scoped_lock lock(mutex_);
    const auto response = exchangeLocked(
        uds::MessageType::kApplyConnections,
        uds::MessageType::kApplyResult,
        uds::json::encodeApplyRequest(commands, options));
    if (!response.ok()) {
        return {response.error, {}};
    }
    return uds::json::decodeApplyResult(response.frame->payload);
}

std::vector<AppliedConnection> UdsDeviceBackend::getConnections() const {
    std::scoped_lock lock(mutex_);
    const auto response = exchangeLocked(
        uds::MessageType::kGetConnections, uds::MessageType::kConnectionsReply, "{}");
    if (!response.ok()) {
        throw std::runtime_error(response.error.message);
    }
    return uds::json::decodeConnections(response.frame->payload);
}

PortState UdsDeviceBackend::getInputPortState(PortId id) const {
    std::scoped_lock lock(mutex_);
    const auto response = exchangeLocked(
        uds::MessageType::kGetPortState,
        uds::MessageType::kPortStateReply,
        uds::json::encodePortRequest(PortDirection::kInput, id));
    if (!response.ok()) {
        throw std::runtime_error(response.error.message);
    }
    return uds::json::decodePortState(response.frame->payload);
}

PortState UdsDeviceBackend::getOutputPortState(PortId id) const {
    std::scoped_lock lock(mutex_);
    const auto response = exchangeLocked(
        uds::MessageType::kGetPortState,
        uds::MessageType::kPortStateReply,
        uds::json::encodePortRequest(PortDirection::kOutput, id));
    if (!response.ok()) {
        throw std::runtime_error(response.error.message);
    }
    return uds::json::decodePortState(response.frame->payload);
}

ResetResult UdsDeviceBackend::reset(ResetMode mode) {
    std::scoped_lock lock(mutex_);
    const auto response = exchangeLocked(
        uds::MessageType::kReset,
        uds::MessageType::kResetResult,
        uds::json::encodeResetRequest(mode));
    if (!response.ok()) {
        return {response.error, device_generation_};
    }
    const auto result = uds::json::decodeResetResult(response.frame->payload);
    device_generation_ = result.generation;
    return result;
}

FaultResult UdsDeviceBackend::injectFault(const FaultSpec& fault) {
    std::scoped_lock lock(mutex_);
    const auto response = exchangeLocked(
        uds::MessageType::kInjectFault,
        uds::MessageType::kFaultResult,
        uds::json::encodeFaultRequest(fault, false));
    if (!response.ok()) {
        return {response.error};
    }
    return uds::json::decodeFaultResult(response.frame->payload);
}

FaultResult UdsDeviceBackend::clearFault(const FaultSelector& selector) {
    std::scoped_lock lock(mutex_);
    const FaultSpec fault{selector.type, selector.port_id};
    const auto response = exchangeLocked(
        uds::MessageType::kInjectFault,
        uds::MessageType::kFaultResult,
        uds::json::encodeFaultRequest(fault, true));
    if (!response.ok()) {
        return {response.error};
    }
    return uds::json::decodeFaultResult(response.frame->payload);
}

std::uint64_t UdsDeviceBackend::deviceGeneration() const {
    std::scoped_lock lock(mutex_);
    return device_generation_;
}

Error UdsDeviceBackend::ensureConnectedLocked() const {
    if (socket_fd_ >= 0) {
        return Error::success();
    }

    const int client_fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (client_fd < 0) {
        return makeError(ErrorCode::kDeviceNotReady, std::strerror(errno));
    }

    const auto timeout_seconds =
        std::chrono::duration_cast<std::chrono::seconds>(timeout_).count();
    const auto timeout_microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(timeout_).count() % 1'000'000;
    const timeval socket_timeout{
        .tv_sec = static_cast<time_t>(timeout_seconds),
        .tv_usec = static_cast<suseconds_t>(timeout_microseconds),
    };
    if (::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &socket_timeout, sizeof(socket_timeout)) < 0 ||
        ::setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &socket_timeout, sizeof(socket_timeout)) < 0) {
        const std::string message = std::strerror(errno);
        ::close(client_fd);
        return makeError(ErrorCode::kInternalError, message);
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, socket_path_.c_str(), socket_path_.size() + 1);
    if (::connect(client_fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
        const std::string message = std::strerror(errno);
        ::close(client_fd);
        return makeError(ErrorCode::kDeviceNotReady, message);
    }
    socket_fd_ = client_fd;

    const uds::Frame hello{
        .message_type = uds::MessageType::kHello,
        .request_id = next_request_id_++,
        .payload = R"({"client":"ocs-syncd"})",
    };
    const auto response = sendAndReceiveLocked(hello, uds::MessageType::kHelloReply);
    if (!response.ok()) {
        closeLocked();
        return response.error;
    }
    device_generation_ = response.frame->device_generation;
    return Error::success();
}

void UdsDeviceBackend::closeLocked() const noexcept {
    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }
}

uds::DecodeResult UdsDeviceBackend::exchangeLocked(
    uds::MessageType request_type,
    uds::MessageType response_type,
    std::string payload) const {
    if (const auto connection_error = ensureConnectedLocked(); !connection_error.ok()) {
        return failed(connection_error);
    }
    const uds::Frame request{
        .message_type = request_type,
        .request_id = next_request_id_++,
        .device_generation = device_generation_,
        .payload = std::move(payload),
    };
    auto response = sendAndReceiveLocked(request, response_type);
    if (!response.ok()) {
        closeLocked();
        const bool retry_safe = request_type == uds::MessageType::kGetDeviceInfo ||
                                request_type == uds::MessageType::kGetConnections ||
                                request_type == uds::MessageType::kGetPortState;
        if (retry_safe && ensureConnectedLocked().ok()) {
            const uds::Frame retry{
                .message_type = request_type,
                .request_id = next_request_id_++,
                .device_generation = device_generation_,
                .payload = request.payload,
            };
            response = sendAndReceiveLocked(retry, response_type);
            if (!response.ok()) {
                closeLocked();
            }
        }
    }
    if (response.ok()) {
        device_generation_ = response.frame->device_generation;
    }
    return response;
}

uds::DecodeResult UdsDeviceBackend::sendAndReceiveLocked(
    const uds::Frame& request,
    uds::MessageType response_type) const {
    const auto encoded = uds::encode(request);
    if (!encoded.ok()) {
        return failed(encoded.error);
    }
    const auto sent = ::send(socket_fd_, encoded.bytes.data(), encoded.bytes.size(), MSG_NOSIGNAL);
    if (sent != static_cast<ssize_t>(encoded.bytes.size())) {
        return failed(makeError(ErrorCode::kDeviceNotReady, std::strerror(errno)));
    }

    std::vector<std::byte> buffer(uds::kHeaderSize + uds::kMaxPayloadSize);
    const auto received = ::recv(socket_fd_, buffer.data(), buffer.size(), MSG_TRUNC);
    if (received <= 0) {
        const auto code = errno == EAGAIN || errno == EWOULDBLOCK ? ErrorCode::kApplyTimeout
                                                                 : ErrorCode::kDeviceNotReady;
        return failed(makeError(code, received == 0 ? "UDS peer disconnected" : std::strerror(errno)));
    }
    if (static_cast<std::size_t>(received) > buffer.size()) {
        return failed(makeError(ErrorCode::kPayloadTooLarge, "UDS response packet exceeds limit"));
    }
    buffer.resize(static_cast<std::size_t>(received));
    auto decoded = uds::decode(buffer);
    if (!decoded.ok()) {
        return decoded;
    }
    if (decoded.frame->request_id != request.request_id) {
        return failed(makeError(ErrorCode::kProtocolMalformed, "UDS response request ID mismatch"));
    }
    if (decoded.frame->message_type == uds::MessageType::kErrorReply) {
        return failed(uds::json::decodeError(decoded.frame->payload));
    }
    if (decoded.frame->message_type != response_type) {
        return failed(makeError(ErrorCode::kProtocolMalformed, "UDS response type mismatch"));
    }
    return decoded;
}

}  // namespace ocs
