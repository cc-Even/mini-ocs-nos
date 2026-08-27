#include "ocs/device_command.hpp"
#include "ocs/redis_keys.hpp"
#include "ocs/redis_repository.hpp"
#include "ocs/simulated_ocs_device.hpp"
#include "ocs/syncd_service.hpp"
#include "ocs/uds_device_backend.hpp"
#include "ocs/uds_server.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
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

class SyncdIntegrationTest : public testing::Test {
protected:
    void SetUp() override {
        const char* redis_socket = std::getenv("OCS_REDIS_SOCKET");
        if (redis_socket == nullptr) {
            GTEST_SKIP() << "set OCS_REDIS_SOCKET to run syncd integration tests";
        }
        endpoint_.unix_socket = redis_socket;

        char directory_template[] = "/tmp/mini-ocs-syncd-test-XXXXXX";
        const char* created = ::mkdtemp(directory_template);
        ASSERT_NE(created, nullptr);
        runtime_directory_ = created;
        hwsim_socket_ = runtime_directory_ + "/hwsim.sock";

        simulated_device_ = std::make_shared<ocs::SimulatedOcsDevice>(defaultDeviceInfo());
        hwsim_ = std::make_unique<ocs::UdsServer>(hwsim_socket_, simulated_device_);
        ASSERT_TRUE(hwsim_->start().ok());

        device_db_ = std::make_unique<ocs::redis::RedisRepository>(
            endpoint_, ocs::redis::LogicalDb::kDevice);
        state_db_ =
            std::make_unique<ocs::redis::RedisRepository>(endpoint_, ocs::redis::LogicalDb::kState);
        counters_db_ = std::make_unique<ocs::redis::RedisRepository>(
            endpoint_, ocs::redis::LogicalDb::kCounters);
        device_db_->flushForTest();
        state_db_->flushForTest();
        counters_db_->flushForTest();

        auto backend = std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_);
        syncd_ = std::make_unique<ocs::SyncdService>(endpoint_, std::move(backend));
        syncd_->initialize();
    }

    void TearDown() override {
        syncd_.reset();
        device_db_.reset();
        state_db_.reset();
        counters_db_.reset();
        if (hwsim_) {
            hwsim_->stop();
        }
        std::error_code error;
        std::filesystem::remove_all(runtime_directory_, error);
    }

    ocs::redis::RedisEndpoint endpoint_;
    std::string runtime_directory_;
    std::string hwsim_socket_;
    std::shared_ptr<ocs::SimulatedOcsDevice> simulated_device_;
    std::unique_ptr<ocs::UdsServer> hwsim_;
    std::unique_ptr<ocs::redis::RedisRepository> device_db_;
    std::unique_ptr<ocs::redis::RedisRepository> state_db_;
    std::unique_ptr<ocs::redis::RedisRepository> counters_db_;
    std::unique_ptr<ocs::SyncdService> syncd_;
};

TEST_F(SyncdIntegrationTest, AppliesCommandPublishesStateAndResultThenAcknowledges) {
    const std::string result_group = "result-test";
    const std::string state_group = "state-test";
    device_db_->createConsumerGroup(std::string(ocs::redis::kDeviceResults), result_group);
    state_db_->createConsumerGroup(std::string(ocs::redis::kStateEvents), state_group);
    const ocs::DeviceCommandBatch command_batch{
        .commands = {{
            .id = "conn-001",
            .input_port = 3,
            .output_port = 11,
            .desired_version = 7,
        }},
        .options = {},
    };
    const ocs::redis::EventEnvelope command_event{
        .event_schema_version = 1,
        .event_id = "command-001",
        .request_id = "request-001",
        .timestamp_ns = 1780000000000000000ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-001",
        .operation = "UPSERT",
        .desired_version = 7,
        .payload = ocs::encodeDeviceCommand(command_batch),
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), command_event));

    ASSERT_TRUE(syncd_->processOne("syncd-test"));

    const auto actual = simulated_device_->getConnections();
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual.front().id, "conn-001");
    EXPECT_EQ(actual.front().applied_version, 7);

    const auto state = state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-001"));
    EXPECT_EQ(state.at("apply_status"), "ACTIVE");
    EXPECT_EQ(state.at("desired_version"), "7");
    EXPECT_EQ(state.at("applied_version"), "7");

    const auto state_events = state_db_->readGroup(
        std::string(ocs::redis::kStateEvents), state_group, "state-consumer");
    ASSERT_EQ(state_events.size(), 1);
    EXPECT_EQ(state_events.front().event.resource_type, "connection");
    EXPECT_EQ(state_events.front().event.resource_id, "conn-001");
    EXPECT_EQ(state_events.front().event.operation, "UPSERT");
    EXPECT_EQ(state_events.front().event.desired_version, 7);

    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("device_apply_total"), "1");
    EXPECT_EQ(counters.at("device_apply_success_total"), "1");
    EXPECT_EQ(counters.at("active_connections"), "1");

    const auto results = device_db_->readGroup(
        std::string(ocs::redis::kDeviceResults), result_group, "result-consumer");
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results.front().event.request_id, "request-001");
    EXPECT_EQ(results.front().event.operation, "APPLY_RESULT");
    EXPECT_EQ(device_db_->pendingCount(std::string(ocs::redis::kDeviceCommands), "ocs-syncd"), 0);
}

TEST_F(SyncdIntegrationTest, FailedUpdatePreservesLastConfirmedActualConnection) {
    const auto append_command = [this](
                                    std::string event_id,
                                    ocs::PortId input_port,
                                    ocs::PortId output_port,
                                    std::uint64_t desired_version) {
        const ocs::DeviceCommandBatch batch{
            .commands = {{
                .id = "conn-preserve",
                .input_port = input_port,
                .output_port = output_port,
                .desired_version = desired_version,
            }},
            .options = {},
        };
        const ocs::redis::EventEnvelope event{
            .event_schema_version = 1,
            .event_id = std::move(event_id),
            .request_id = "request-preserve-" + std::to_string(desired_version),
            .timestamp_ns = 1780000000000000030ULL + desired_version,
            .device = "ocs0",
            .resource_type = "connection",
            .resource_id = "conn-preserve",
            .operation = "UPSERT",
            .desired_version = desired_version,
            .payload = ocs::encodeDeviceCommand(batch),
        };
        static_cast<void>(device_db_->appendEvent(
            std::string(ocs::redis::kDeviceCommands), event));
    };

    append_command("command-preserve-001", 1, 9, 1);
    ASSERT_TRUE(syncd_->processOne("syncd-test-preserve"));
    ASSERT_TRUE(simulated_device_->injectFault({.type = ocs::FaultType::kNextApplyError}).error.ok());
    append_command("command-preserve-002", 2, 10, 2);
    ASSERT_TRUE(syncd_->processOne("syncd-test-preserve"));

    const auto state =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-preserve"));
    EXPECT_EQ(state.at("input_port"), "1");
    EXPECT_EQ(state.at("output_port"), "9");
    EXPECT_EQ(state.at("desired_version"), "2");
    EXPECT_EQ(state.at("applied_version"), "1");
    EXPECT_EQ(state.at("apply_status"), "FAILED");
    EXPECT_EQ(state.at("last_error_code"), "OCS_APPLY_FAILED");
    const auto actual = simulated_device_->getConnections();
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual.front().input_port, 1);
    EXPECT_EQ(actual.front().output_port, 9);
    EXPECT_EQ(actual.front().applied_version, 1);
}

TEST_F(SyncdIntegrationTest, ActiveCounterTracksActualPresenceAcrossFailedRecovery) {
    const auto append_command = [this](
                                    std::string event_id,
                                    ocs::ConnectionOperation operation,
                                    ocs::PortId input_port,
                                    ocs::PortId output_port,
                                    std::uint64_t desired_version) {
        const ocs::DeviceCommandBatch batch{
            .commands = {{
                .operation = operation,
                .id = "conn-accounting",
                .input_port = input_port,
                .output_port = output_port,
                .desired_version = desired_version,
            }},
            .options = {},
        };
        const ocs::redis::EventEnvelope event{
            .event_schema_version = 1,
            .event_id = std::move(event_id),
            .request_id = "request-accounting-" + std::to_string(desired_version),
            .timestamp_ns = 1780000000000000050ULL + desired_version,
            .device = "ocs0",
            .resource_type = "connection",
            .resource_id = "conn-accounting",
            .operation = operation == ocs::ConnectionOperation::kRemove ? "REMOVE" : "UPSERT",
            .desired_version = desired_version,
            .payload = ocs::encodeDeviceCommand(batch),
        };
        static_cast<void>(device_db_->appendEvent(
            std::string(ocs::redis::kDeviceCommands), event));
        ASSERT_TRUE(syncd_->processOne("accounting-consumer"));
    };
    const auto active_count = [this] {
        return counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"))
            .at("active_connections");
    };

    append_command("accounting-1", ocs::ConnectionOperation::kUpsert, 1, 9, 1);
    EXPECT_EQ(active_count(), "1");

    ASSERT_TRUE(simulated_device_->injectFault({.type = ocs::FaultType::kNextApplyError}).error.ok());
    append_command("accounting-2", ocs::ConnectionOperation::kUpsert, 2, 10, 2);
    EXPECT_EQ(active_count(), "1");
    EXPECT_EQ(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-accounting"))
            .at("actual_present"),
        "true");

    append_command("accounting-3", ocs::ConnectionOperation::kUpsert, 2, 10, 3);
    EXPECT_EQ(active_count(), "1");

    ASSERT_TRUE(simulated_device_->injectFault({.type = ocs::FaultType::kNextApplyError}).error.ok());
    append_command("accounting-4", ocs::ConnectionOperation::kRemove, 0, 0, 4);
    EXPECT_EQ(active_count(), "1");
    EXPECT_EQ(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-accounting"))
            .at("actual_present"),
        "true");

    append_command("accounting-5", ocs::ConnectionOperation::kRemove, 0, 0, 5);
    EXPECT_EQ(active_count(), "0");
    EXPECT_TRUE(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-accounting")).empty());
}

TEST_F(SyncdIntegrationTest, ClaimsProcessedCommandAfterCrashWithoutRepeatingSideEffects) {
    const std::string result_group = "crash-result-test";
    const std::string state_group = "crash-state-test";
    device_db_->createConsumerGroup(std::string(ocs::redis::kDeviceResults), result_group);
    state_db_->createConsumerGroup(std::string(ocs::redis::kStateEvents), state_group);
    syncd_.reset();
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_,
        std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_),
        std::chrono::milliseconds(0));
    syncd_->initialize();

    const ocs::DeviceCommandBatch command_batch{
        .commands = {{
            .id = "conn-crash",
            .input_port = 4,
            .output_port = 12,
            .desired_version = 1,
        }},
        .options = {},
    };
    const ocs::redis::EventEnvelope command_event{
        .event_schema_version = 1,
        .event_id = "command-crash-001",
        .request_id = "request-crash-001",
        .timestamp_ns = 1780000000000000100ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-crash",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = ocs::encodeDeviceCommand(command_batch),
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), command_event));

    struct SimulatedCrash {};
    EXPECT_THROW(
        static_cast<void>(syncd_->processOne("crashing-consumer", [] {
            throw SimulatedCrash{};
        })),
        SimulatedCrash);
    EXPECT_EQ(device_db_->pendingCount(std::string(ocs::redis::kDeviceCommands), "ocs-syncd"), 1);
    EXPECT_FALSE(
        device_db_->getHash(ocs::redis::processedDeviceCommandKey("command-crash-001")).empty());

    const auto state_events = state_db_->readGroup(
        std::string(ocs::redis::kStateEvents), state_group, "state-before-recovery");
    const auto results = device_db_->readGroup(
        std::string(ocs::redis::kDeviceResults), result_group, "result-before-recovery");
    ASSERT_EQ(state_events.size(), 1);
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0")).at(
                  "device_apply_total"),
              "1");

    syncd_.reset();
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_,
        std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_),
        std::chrono::milliseconds(0));
    syncd_->initialize();
    ASSERT_TRUE(syncd_->processOne("recovery-consumer"));

    EXPECT_EQ(device_db_->pendingCount(std::string(ocs::redis::kDeviceCommands), "ocs-syncd"), 0);
    EXPECT_TRUE(state_db_->readGroup(
                             std::string(ocs::redis::kStateEvents),
                             state_group,
                             "state-after-recovery")
                    .empty());
    EXPECT_TRUE(device_db_->readGroup(
                              std::string(ocs::redis::kDeviceResults),
                              result_group,
                              "result-after-recovery")
                    .empty());
    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("device_apply_total"), "1");
    EXPECT_EQ(counters.at("active_connections"), "1");
    const auto actual = simulated_device_->getConnections();
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual.front().id, "conn-crash");
    EXPECT_EQ(actual.front().applied_version, 1);
}

TEST_F(SyncdIntegrationTest, RecoversIdempotentlyAfterEveryPersistenceBoundary) {
    const std::string result_group = "phase-result-test";
    const std::string state_group = "phase-state-test";
    device_db_->createConsumerGroup(std::string(ocs::redis::kDeviceResults), result_group);
    state_db_->createConsumerGroup(std::string(ocs::redis::kStateEvents), state_group);
    const std::vector<std::string> phases{
        "device-apply", "apply-result", "state", "counters", "result", "processed"};

    struct SimulatedCrash {};
    for (std::size_t index = 0; index < phases.size(); ++index) {
        const auto desired_version = static_cast<std::uint64_t>(index + 1);
        const std::string connection_id = "phase-" + std::to_string(index + 1);
        const std::string command_id = "command-phase-" + std::to_string(index + 1);
        const ocs::DeviceCommandBatch batch{
            .commands = {{
                .id = connection_id,
                .input_port = static_cast<ocs::PortId>(index + 1),
                .output_port = static_cast<ocs::PortId>(index + 9),
                .desired_version = desired_version,
            }},
            .options = {},
        };
        const ocs::redis::EventEnvelope event{
            .event_schema_version = 1,
            .event_id = command_id,
            .request_id = "request-phase-" + std::to_string(index + 1),
            .timestamp_ns = 1780000000000000200ULL + index,
            .device = "ocs0",
            .resource_type = "connection",
            .resource_id = connection_id,
            .operation = "UPSERT",
            .desired_version = desired_version,
            .payload = ocs::encodeDeviceCommand(batch),
        };
        static_cast<void>(device_db_->appendEvent(
            std::string(ocs::redis::kDeviceCommands), event));

        const auto crash_phase = phases.at(index);
        EXPECT_THROW(
            static_cast<void>(syncd_->processOne(
                "phase-crashing-consumer",
                {},
                [&crash_phase](std::string_view phase) {
                    if (phase == crash_phase) {
                        throw SimulatedCrash{};
                    }
                })),
            SimulatedCrash);
        EXPECT_EQ(
            device_db_->pendingCount(std::string(ocs::redis::kDeviceCommands), "ocs-syncd"),
            1);

        syncd_.reset();
        syncd_ = std::make_unique<ocs::SyncdService>(
            endpoint_,
            std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_),
            std::chrono::milliseconds(0));
        syncd_->initialize();
        ASSERT_TRUE(syncd_->processOne("phase-recovery-consumer"));
        EXPECT_EQ(
            device_db_->pendingCount(std::string(ocs::redis::kDeviceCommands), "ocs-syncd"),
            0);
        EXPECT_EQ(
            state_db_->getHash(ocs::redis::connectionStateKey("ocs0", connection_id))
                .at("apply_status"),
            "ACTIVE");
    }

    const auto state_events = state_db_->readGroup(
        std::string(ocs::redis::kStateEvents), state_group, "phase-state-consumer", 100);
    const auto result_events = device_db_->readGroup(
        std::string(ocs::redis::kDeviceResults), result_group, "phase-result-consumer", 100);
    EXPECT_EQ(state_events.size(), phases.size());
    EXPECT_EQ(result_events.size(), phases.size());
    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("device_apply_total"), std::to_string(phases.size()));
    EXPECT_EQ(counters.at("device_apply_success_total"), std::to_string(phases.size()));
    EXPECT_EQ(counters.at("active_connections"), std::to_string(phases.size()));
    EXPECT_EQ(simulated_device_->getConnections().size(), phases.size());
}

TEST_F(SyncdIntegrationTest, RejectsCommandForDifferentConnectedDevice) {
    const std::string result_group = "identity-result-test";
    device_db_->createConsumerGroup(std::string(ocs::redis::kDeviceResults), result_group);
    const ocs::DeviceCommandBatch batch{
        .commands = {{
            .id = "wrong-device",
            .input_port = 1,
            .output_port = 9,
            .desired_version = 1,
        }},
        .options = {},
    };
    const ocs::redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = "command-wrong-device",
        .request_id = "request-wrong-device",
        .timestamp_ns = 1780000000000000300ULL,
        .device = "ocs1",
        .resource_type = "connection",
        .resource_id = "wrong-device",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = ocs::encodeDeviceCommand(batch),
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), event));

    ASSERT_TRUE(syncd_->processOne("identity-consumer"));

    EXPECT_TRUE(simulated_device_->getConnections().empty());
    const auto results = device_db_->readGroup(
        std::string(ocs::redis::kDeviceResults), result_group, "identity-result-consumer");
    ASSERT_EQ(results.size(), 1);
    EXPECT_NE(results.front().event.payload.find("OCS_DEVICE_NOT_READY"), std::string::npos);
    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs1"));
    EXPECT_EQ(counters.at("device_apply_total"), "1");
    EXPECT_EQ(counters.at("device_apply_failure_total"), "1");
}

}  // namespace
