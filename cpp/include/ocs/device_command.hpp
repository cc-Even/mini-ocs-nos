#pragma once

#include "ocs/device_types.hpp"

#include <string>
#include <vector>

namespace ocs {

struct DeviceCommandBatch {
    std::vector<ConnectionCommand> commands;
    ApplyOptions options;
};

[[nodiscard]] std::string encodeDeviceCommand(const DeviceCommandBatch& batch);
[[nodiscard]] DeviceCommandBatch decodeDeviceCommand(const std::string& payload);

}  // namespace ocs
