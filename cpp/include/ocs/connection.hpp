#pragma once

#include <cstdint>
#include <string>

namespace ocs {

using PortId = std::uint16_t;

enum class ConnectionOperation {
    kUpsert,
    kRemove,
};

struct ConnectionCommand {
    ConnectionOperation operation{ConnectionOperation::kUpsert};
    std::string id;
    PortId input_port{};
    PortId output_port{};
    std::uint64_t desired_version{};
};

struct AppliedConnection {
    std::string id;
    PortId input_port{};
    PortId output_port{};
    std::uint64_t applied_version{};

    bool operator==(const AppliedConnection&) const = default;
};

}  // namespace ocs
