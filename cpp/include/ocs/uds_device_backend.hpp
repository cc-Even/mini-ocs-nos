#pragma once

#include "ocs/device_api.hpp"
#include "ocs/uds_protocol.hpp"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>

namespace ocs {

class UdsDeviceBackend final : public OcsDeviceApi {
public:
    explicit UdsDeviceBackend(
        std::string socket_path,
        std::chrono::milliseconds timeout = std::chrono::milliseconds{1000});
    ~UdsDeviceBackend() override;

    UdsDeviceBackend(const UdsDeviceBackend&) = delete;
    UdsDeviceBackend& operator=(const UdsDeviceBackend&) = delete;

    [[nodiscard]] DeviceInfo getDeviceInfo() const override;
    [[nodiscard]] DeviceHealth getHealth() const override;
    ApplyResult applyConnections(
        const std::vector<ConnectionCommand>& commands,
        const ApplyOptions& options) override;
    [[nodiscard]] std::vector<AppliedConnection> getConnections() const override;
    [[nodiscard]] PortState getInputPortState(PortId id) const override;
    [[nodiscard]] PortState getOutputPortState(PortId id) const override;
    ResetResult reset(ResetMode mode) override;
    FaultResult injectFault(const FaultSpec& fault) override;
    FaultResult clearFault(const FaultSelector& selector) override;

    [[nodiscard]] std::uint64_t deviceGeneration() const;

private:
    Error ensureConnectedLocked() const;
    void closeLocked() const noexcept;
    [[nodiscard]] uds::DecodeResult exchangeLocked(
        uds::MessageType request_type,
        uds::MessageType response_type,
        std::string payload) const;
    [[nodiscard]] uds::DecodeResult sendAndReceiveLocked(
        const uds::Frame& request,
        uds::MessageType response_type) const;

    std::string socket_path_;
    std::chrono::milliseconds timeout_;
    mutable std::mutex mutex_;
    mutable int socket_fd_{-1};
    mutable std::uint64_t next_request_id_{1};
    mutable std::uint64_t device_generation_{};
};

}  // namespace ocs
