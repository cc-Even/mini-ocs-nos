#include "ocs/device_command.hpp"
#include "ocs/redis_keys.hpp"
#include "ocs/redis_repository.hpp"
#include "ocs/simulated_ocs_device.hpp"
#include "ocs/syncd_service.hpp"
#include "ocs/uds_device_backend.hpp"
#include "ocs/uds_server.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

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

}  // namespace
