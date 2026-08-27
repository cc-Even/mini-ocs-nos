#include "ocs/connection_state_machine.hpp"
#include "ocs/device_command.hpp"
#include "ocs/errors.hpp"
#include "ocs/redis_repository.hpp"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

TEST(ErrorContractTest, RoundTripsEveryStableErrorCode) {
    constexpr std::array codes{
        ocs::ErrorCode::kOk,
        ocs::ErrorCode::kInvalidArgument,
        ocs::ErrorCode::kInvalidPort,
        ocs::ErrorCode::kConnectionNotFound,
        ocs::ErrorCode::kConnectionExists,
        ocs::ErrorCode::kInputConflict,
        ocs::ErrorCode::kOutputConflict,
        ocs::ErrorCode::kPortDisabled,
        ocs::ErrorCode::kPortDown,
        ocs::ErrorCode::kDeviceNotReady,
        ocs::ErrorCode::kApplyTimeout,
        ocs::ErrorCode::kApplyFailed,
        ocs::ErrorCode::kVersionStale,
        ocs::ErrorCode::kProtocolMalformed,
        ocs::ErrorCode::kProtocolVersion,
        ocs::ErrorCode::kPayloadTooLarge,
        ocs::ErrorCode::kUnsupported,
        ocs::ErrorCode::kInternalError,
    };

    std::set<std::string> names;
    for (const auto code : codes) {
        const auto name = std::string(ocs::toString(code));
        EXPECT_TRUE(names.insert(name).second) << name;
        EXPECT_EQ(ocs::errorCodeFromString(name), code);
    }
    EXPECT_EQ(ocs::errorCodeFromString("OCS_UNKNOWN"), ocs::ErrorCode::kInternalError);
}

TEST(ConnectionStateContractTest, RoundTripsStatusesAndDefinesEveryAllowedTransition) {
    using Status = ocs::ConnectionApplyStatus;
    constexpr std::array statuses{
        Status::kAbsent,
        Status::kPendingCreate,
        Status::kApplying,
        Status::kActive,
        Status::kPendingUpdate,
        Status::kPendingDelete,
        Status::kRemoving,
        Status::kFailed,
        Status::kRetryWait,
        Status::kDrifted,
        Status::kReconciling,
    };
    const std::set<std::pair<Status, Status>> allowed{
        {Status::kAbsent, Status::kPendingCreate},
        {Status::kPendingCreate, Status::kApplying},
        {Status::kPendingCreate, Status::kFailed},
        {Status::kApplying, Status::kActive},
        {Status::kApplying, Status::kFailed},
        {Status::kActive, Status::kPendingUpdate},
        {Status::kActive, Status::kPendingDelete},
        {Status::kActive, Status::kDrifted},
        {Status::kPendingUpdate, Status::kApplying},
        {Status::kPendingUpdate, Status::kFailed},
        {Status::kPendingDelete, Status::kRemoving},
        {Status::kPendingDelete, Status::kFailed},
        {Status::kRemoving, Status::kAbsent},
        {Status::kRemoving, Status::kFailed},
        {Status::kFailed, Status::kRetryWait},
        {Status::kRetryWait, Status::kApplying},
        {Status::kRetryWait, Status::kRemoving},
        {Status::kDrifted, Status::kReconciling},
        {Status::kReconciling, Status::kActive},
        {Status::kReconciling, Status::kFailed},
    };

    std::set<std::string> names;
    for (const auto status : statuses) {
        const auto name = std::string(ocs::toString(status));
        EXPECT_TRUE(names.insert(name).second) << name;
        EXPECT_EQ(ocs::connectionApplyStatusFromString(name), status);
        for (const auto destination : statuses) {
            EXPECT_EQ(
                ocs::canTransition(status, destination),
                allowed.contains({status, destination}))
                << name << " -> " << ocs::toString(destination);
        }
    }
}

TEST(DeviceCommandContractTest, RoundTripsAtomicMixedOperationBatch) {
    const ocs::DeviceCommandBatch expected{
        .commands = {
            {
                .operation = ocs::ConnectionOperation::kUpsert,
                .id = "upserted",
                .input_port = 3,
                .output_port = 11,
                .desired_version = 7,
            },
            {
                .operation = ocs::ConnectionOperation::kRemove,
                .id = "removed",
                .desired_version = 8,
            },
        },
        .options = {
            .atomic = true,
            .timeout = std::chrono::milliseconds(725),
            .operation_id = "operation-42",
        },
    };

    const auto actual = ocs::decodeDeviceCommand(ocs::encodeDeviceCommand(expected));

    ASSERT_EQ(actual.commands.size(), expected.commands.size());
    for (std::size_t index = 0; index < expected.commands.size(); ++index) {
        EXPECT_EQ(actual.commands[index].operation, expected.commands[index].operation);
        EXPECT_EQ(actual.commands[index].id, expected.commands[index].id);
        EXPECT_EQ(actual.commands[index].input_port, expected.commands[index].input_port);
        EXPECT_EQ(actual.commands[index].output_port, expected.commands[index].output_port);
        EXPECT_EQ(actual.commands[index].desired_version, expected.commands[index].desired_version);
    }
    EXPECT_EQ(actual.options.atomic, expected.options.atomic);
    EXPECT_EQ(actual.options.timeout, expected.options.timeout);
    EXPECT_EQ(actual.options.operation_id, expected.options.operation_id);
}

TEST(DeviceCommandContractTest, RejectsInvalidOperationTimeoutAndEmptyBatch) {
    for (const auto& payload : {
             R"({"commands":[{"operation":"MOVE","id":"a","desired_version":1}],"atomic":true,"timeout_ms":1})",
             R"({"commands":[{"operation":"REMOVE","id":"a","desired_version":1}],"atomic":true,"timeout_ms":0})",
             R"({"commands":[],"atomic":true,"timeout_ms":1})",
         }) {
        EXPECT_THROW(static_cast<void>(ocs::decodeDeviceCommand(payload)), std::invalid_argument);
    }
}

TEST(RedisEndpointContractTest, ParsesUnixAndTcpTargets) {
    const auto legacy_unix = ocs::redis::parseRedisEndpoint("/run/mini-ocs/redis.sock");
    EXPECT_EQ(legacy_unix.unix_socket, "/run/mini-ocs/redis.sock");

    const auto unix_uri = ocs::redis::parseRedisEndpoint("unix:///tmp/redis.sock");
    EXPECT_EQ(unix_uri.unix_socket, "/tmp/redis.sock");

    const auto tcp = ocs::redis::parseRedisEndpoint("tcp://redis:6380");
    EXPECT_EQ(tcp.host, "redis");
    EXPECT_EQ(tcp.port, 6380);
    EXPECT_TRUE(tcp.unix_socket.empty());
}

TEST(RedisEndpointContractTest, RejectsMalformedOrUnboundedTargets) {
    for (const auto* target : {
             "redis:6379",
             "tcp://:6379",
             "tcp://redis",
             "tcp://redis:0",
             "tcp://redis:65536",
             "tcp://redis:not-a-port",
             "unix://relative.sock",
         }) {
        EXPECT_THROW(
            static_cast<void>(ocs::redis::parseRedisEndpoint(target)),
            std::invalid_argument)
            << target;
    }
}

}  // namespace
