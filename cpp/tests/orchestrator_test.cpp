#include "ocs/connection_state_machine.hpp"
#include "ocs/device_command.hpp"
#include "ocs/orchestrator_service.hpp"
#include "ocs/redis_keys.hpp"
#include "ocs/redis_repository.hpp"
#include "ocs/syncd_service.hpp"
#include "ocs/uds_device_backend.hpp"

#include <gtest/gtest.h>

#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <algorithm>
#include <csignal>
#include <condition_variable>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace {

TEST(ConnectionStateMachineTest, AcceptsCreateUpdateDeleteAndRecoveryPaths) {
    using Status = ocs::ConnectionApplyStatus;

    EXPECT_TRUE(ocs::canTransition(Status::kAbsent, Status::kPendingCreate));
    EXPECT_TRUE(ocs::canTransition(Status::kPendingCreate, Status::kApplying));
    EXPECT_TRUE(ocs::canTransition(Status::kApplying, Status::kActive));
    EXPECT_TRUE(ocs::canTransition(Status::kActive, Status::kPendingUpdate));
    EXPECT_TRUE(ocs::canTransition(Status::kActive, Status::kPendingDelete));
    EXPECT_TRUE(ocs::canTransition(Status::kRemoving, Status::kAbsent));
    EXPECT_TRUE(ocs::canTransition(Status::kFailed, Status::kRetryWait));
    EXPECT_TRUE(ocs::canTransition(Status::kDrifted, Status::kReconciling));
}

TEST(ConnectionStateMachineTest, RejectsInvalidTransitionsAndUnknownNames) {
    using Status = ocs::ConnectionApplyStatus;

    EXPECT_FALSE(ocs::canTransition(Status::kAbsent, Status::kActive));
    EXPECT_THROW(ocs::requireTransition(Status::kActive, Status::kApplying), std::logic_error);
    EXPECT_THROW(
        static_cast<void>(ocs::connectionApplyStatusFromString("BROKEN")),
        std::invalid_argument);
}

TEST(DeviceCommandTest, RejectsPortsThatWouldNarrowIntoTheSupportedRange) {
    const std::string payload = R"({
        "commands":[{
            "operation":"UPSERT",
            "id":"conn-overflow",
            "input_port":65537,
            "output_port":9,
            "desired_version":1
        }],
        "atomic":true,
        "timeout_ms":1000
    })";

    EXPECT_THROW(static_cast<void>(ocs::decodeDeviceCommand(payload)), std::invalid_argument);
}

class OrchestratorIntegrationTest : public testing::Test {
protected:
    void SetUp() override {
        const char* redis_socket = std::getenv("OCS_REDIS_SOCKET");
        if (redis_socket == nullptr) {
            GTEST_SKIP() << "set OCS_REDIS_SOCKET to run orchestrator integration tests";
        }
        endpoint_.unix_socket = redis_socket;

        char directory_template[] = "/tmp/mini-ocs-orch-test-XXXXXX";
        const char* created = ::mkdtemp(directory_template);
        ASSERT_NE(created, nullptr);
        runtime_directory_ = created;
        hwsim_socket_ = runtime_directory_ + "/hwsim.sock";

        startHwsim();

        config_db_ = std::make_unique<ocs::redis::RedisRepository>(
            endpoint_, ocs::redis::LogicalDb::kConfig);
        appl_db_ =
            std::make_unique<ocs::redis::RedisRepository>(endpoint_, ocs::redis::LogicalDb::kAppl);
        device_db_ = std::make_unique<ocs::redis::RedisRepository>(
            endpoint_, ocs::redis::LogicalDb::kDevice);
        state_db_ =
            std::make_unique<ocs::redis::RedisRepository>(endpoint_, ocs::redis::LogicalDb::kState);
        counters_db_ = std::make_unique<ocs::redis::RedisRepository>(
            endpoint_, ocs::redis::LogicalDb::kCounters);
        alarm_db_ =
            std::make_unique<ocs::redis::RedisRepository>(endpoint_, ocs::redis::LogicalDb::kAlarm);
        config_db_->flushForTest();
        appl_db_->flushForTest();
        device_db_->flushForTest();
        state_db_->flushForTest();
        counters_db_->flushForTest();
        alarm_db_->flushForTest();

        startServices();
    }

    void startHwsim() {
        hwsim_pid_ = ::fork();
        ASSERT_GE(hwsim_pid_, 0);
        if (hwsim_pid_ == 0) {
            ::execl(OCS_HWSIM_PATH, OCS_HWSIM_PATH, hwsim_socket_.c_str(), nullptr);
            std::_Exit(127);
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!std::filesystem::exists(hwsim_socket_) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE(std::filesystem::exists(hwsim_socket_));
    }

    void stopHwsim() {
        ASSERT_GT(hwsim_pid_, 0);
        ASSERT_EQ(::kill(hwsim_pid_, SIGTERM), 0);
        int status = 0;
        ASSERT_EQ(::waitpid(hwsim_pid_, &status, 0), hwsim_pid_);
        hwsim_pid_ = -1;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::filesystem::exists(hwsim_socket_) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_FALSE(std::filesystem::exists(hwsim_socket_));
    }

    void startServices() {
        orch_ = std::make_unique<ocs::OrchestratorService>(endpoint_);
        orch_->initialize();
        auto backend = std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_);
        syncd_ = std::make_unique<ocs::SyncdService>(endpoint_, std::move(backend));
        syncd_->initialize();
    }

    void restartServices() {
        syncd_.reset();
        orch_.reset();
        startServices();
    }

    void applyConfigEvent(const ocs::redis::EventEnvelope& event) {
        static_cast<void>(config_db_->appendEvent(
            std::string(ocs::redis::kConfigEvents), event));
        ASSERT_TRUE(orch_->processConfigOne("orch-test-config"));
        ASSERT_TRUE(syncd_->processOne("syncd-test"));
        ASSERT_TRUE(orch_->processResultOne("orch-test-result"));
    }

    void TearDown() override {
        syncd_.reset();
        orch_.reset();
        config_db_.reset();
        appl_db_.reset();
        device_db_.reset();
        state_db_.reset();
        counters_db_.reset();
        alarm_db_.reset();
        if (hwsim_pid_ > 0) {
            ::kill(hwsim_pid_, SIGTERM);
            int status = 0;
            static_cast<void>(::waitpid(hwsim_pid_, &status, 0));
        }
        std::error_code error;
        std::filesystem::remove_all(runtime_directory_, error);
    }

    ocs::redis::RedisEndpoint endpoint_;
    std::string runtime_directory_;
    std::string hwsim_socket_;
    pid_t hwsim_pid_{-1};
    std::unique_ptr<ocs::redis::RedisRepository> config_db_;
    std::unique_ptr<ocs::redis::RedisRepository> appl_db_;
    std::unique_ptr<ocs::redis::RedisRepository> device_db_;
    std::unique_ptr<ocs::redis::RedisRepository> state_db_;
    std::unique_ptr<ocs::redis::RedisRepository> counters_db_;
    std::unique_ptr<ocs::redis::RedisRepository> alarm_db_;
    std::unique_ptr<ocs::OrchestratorService> orch_;
    std::unique_ptr<ocs::SyncdService> syncd_;
};

TEST_F(OrchestratorIntegrationTest, ConfigEventReachesActiveThroughStandaloneHwsim) {
    const ocs::redis::EventEnvelope config_event{
        .event_schema_version = 1,
        .event_id = "event-vertical-001",
        .request_id = "request-vertical-001",
        .timestamp_ns = 1780000000000000000ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-vertical-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":3,"output_port":11})",
    };
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), config_event));

    ASSERT_TRUE(orch_->processConfigOne("orch-test-config"));
    const auto applying =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-vertical-001"));
    EXPECT_EQ(applying.at("apply_status"), "APPLYING");
    ASSERT_TRUE(syncd_->processOne("syncd-test"));
    ASSERT_TRUE(orch_->processResultOne("orch-test-result"));

    const auto state =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-vertical-001"));
    EXPECT_EQ(state.at("apply_status"), "ACTIVE");
    EXPECT_EQ(state.at("desired_version"), "1");
    EXPECT_EQ(state.at("applied_version"), "1");
    const auto active =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-vertical-001"));
    EXPECT_EQ(active.at("apply_status"), "ACTIVE");
    EXPECT_EQ(
        config_db_->pendingCount(std::string(ocs::redis::kConfigEvents), "ocs-orch"), 0);
    EXPECT_EQ(
        device_db_->pendingCount(std::string(ocs::redis::kDeviceCommands), "ocs-syncd"), 0);
    EXPECT_EQ(
        device_db_->pendingCount(std::string(ocs::redis::kDeviceResults), "ocs-orch"), 0);
}

TEST_F(OrchestratorIntegrationTest, RecoversConfirmedStateAfterStandaloneHwsimRestart) {
    const ocs::redis::EventEnvelope create{
        .event_schema_version = 1,
        .event_id = "event-hwsim-restart-001",
        .request_id = "request-hwsim-restart-001",
        .timestamp_ns = 1780000000000000060ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-hwsim-restart",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":3,"output_port":11})",
    };
    config_db_->putHash(
        ocs::redis::connectionConfigKey("ocs0", "conn-hwsim-restart"),
        {
            {"device", "ocs0"},
            {"id", "conn-hwsim-restart"},
            {"input_port", "3"},
            {"output_port", "11"},
            {"desired_version", "1"},
        });
    applyConfigEvent(create);
    const auto first_device_state =
        state_db_->getHash(ocs::redis::deviceStateKey("ocs0"));
    const auto first_generation = std::stoull(first_device_state.at("device_generation"));

    stopHwsim();
    EXPECT_FALSE(syncd_->pollDevice());
    const auto unavailable = state_db_->getHash(ocs::redis::deviceStateKey("ocs0"));
    EXPECT_EQ(unavailable.at("oper_status"), "FAILED");
    EXPECT_EQ(unavailable.at("last_error_code"), "OCS_DEVICE_NOT_READY");

    const ocs::redis::EventEnvelope update{
        .event_schema_version = 1,
        .event_id = "event-hwsim-restart-002",
        .request_id = "request-hwsim-restart-002",
        .timestamp_ns = 1780000000000000061ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-hwsim-restart",
        .operation = "UPSERT",
        .desired_version = 2,
        .payload = R"({"input_port":4,"output_port":12})",
    };
    config_db_->putHash(
        ocs::redis::connectionConfigKey("ocs0", "conn-hwsim-restart"),
        {
            {"device", "ocs0"},
            {"id", "conn-hwsim-restart"},
            {"input_port", "4"},
            {"output_port", "12"},
            {"desired_version", "2"},
        });
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), update));
    ASSERT_TRUE(orch_->processConfigOne("orch-hwsim-down"));
    ASSERT_TRUE(syncd_->processOne("syncd-hwsim-down"));
    ASSERT_TRUE(orch_->processResultOne("orch-hwsim-down-result"));
    const auto failed =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-hwsim-restart"));
    EXPECT_EQ(failed.at("apply_status"), "FAILED");
    EXPECT_EQ(failed.at("last_error_code"), "OCS_DEVICE_NOT_READY");
    const auto unconfirmed =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-hwsim-restart"));
    EXPECT_EQ(unconfirmed.at("apply_status"), "FAILED");
    EXPECT_EQ(unconfirmed.at("applied_version"), "1");
    EXPECT_EQ(unconfirmed.at("actual_present"), "true");

    startHwsim();
    ASSERT_TRUE(syncd_->pollDevice());
    const auto restarted = state_db_->getHash(ocs::redis::deviceStateKey("ocs0"));
    const auto restarted_generation = std::stoull(restarted.at("device_generation"));
    EXPECT_NE(restarted_generation, first_generation);
    EXPECT_EQ(restarted.at("oper_status"), "READY");
    EXPECT_EQ(restarted.at("actual_connection_count"), "0");
    const auto refreshed =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-hwsim-restart"));
    EXPECT_EQ(refreshed.at("actual_present"), "false");
    EXPECT_EQ(refreshed.at("applied_version"), "0");
    EXPECT_EQ(
        counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"))
            .at("active_connections"),
        "0");

    const auto recovery = device_db_->getHash(
        ocs::redis::syncdGenerationRecoveryKey("ocs0", restarted_generation));
    ASSERT_EQ(recovery.at("recovery_required"), "true");
    ASSERT_TRUE(syncd_->pollDevice());
    EXPECT_EQ(
        device_db_->pendingCount(std::string(ocs::redis::kDeviceCommands), "ocs-syncd"),
        0);
    ASSERT_TRUE(syncd_->processOne("syncd-hwsim-recovery"));
    EXPECT_FALSE(syncd_->processOne("syncd-hwsim-recovery-duplicate"));
    ASSERT_TRUE(orch_->processResultOne("orch-hwsim-recovery-result"));
    ASSERT_TRUE(syncd_->pollDevice());

    const auto active =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-hwsim-restart"));
    EXPECT_EQ(active.at("apply_status"), "ACTIVE");
    const auto confirmed =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-hwsim-restart"));
    EXPECT_EQ(confirmed.at("apply_status"), "ACTIVE");
    EXPECT_EQ(confirmed.at("input_port"), "4");
    EXPECT_EQ(confirmed.at("output_port"), "12");
    EXPECT_EQ(confirmed.at("desired_version"), "2");
    EXPECT_EQ(confirmed.at("applied_version"), "2");
    EXPECT_EQ(confirmed.at("actual_present"), "true");
    EXPECT_EQ(
        counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"))
            .at("active_connections"),
        "1");
    EXPECT_EQ(
        state_db_->getHash(ocs::redis::deviceStateKey("ocs0"))
            .at("actual_connection_count"),
        "1");
    const auto version = device_db_->getHash(
        ocs::redis::syncdConnectionVersionKey("ocs0", "conn-hwsim-restart"));
    EXPECT_EQ(version.at("device_generation"), std::to_string(restarted_generation));

    syncd_.reset();
    ocs::UdsDeviceBackend verifier(hwsim_socket_);
    const auto actual = verifier.getConnections();
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual.front().id, "conn-hwsim-restart");
    EXPECT_EQ(actual.front().input_port, 4);
    EXPECT_EQ(actual.front().output_port, 12);
    EXPECT_EQ(actual.front().applied_version, 2);
}

TEST_F(OrchestratorIntegrationTest, ReconcilesOutOfBandDriftWithFullDesiredSnapshot) {
    const auto create = [this](
                            std::string id,
                            ocs::PortId input_port,
                            ocs::PortId output_port,
                            std::uint64_t timestamp) {
        config_db_->putHash(
            ocs::redis::connectionConfigKey("ocs0", id),
            {
                {"device", "ocs0"},
                {"id", id},
                {"input_port", std::to_string(input_port)},
                {"output_port", std::to_string(output_port)},
                {"desired_version", "1"},
            });
        applyConfigEvent({
            .event_schema_version = 1,
            .event_id = "event-drift-" + id,
            .request_id = "request-drift-" + id,
            .timestamp_ns = timestamp,
            .device = "ocs0",
            .resource_type = "connection",
            .resource_id = id,
            .operation = "UPSERT",
            .desired_version = 1,
            .payload = "{\"input_port\":" + std::to_string(input_port) +
                       ",\"output_port\":" + std::to_string(output_port) + "}",
        });
    };
    create("drift-a", 3, 11, 1780000000000000070ULL);
    create("drift-b", 4, 12, 1780000000000000071ULL);
    ASSERT_TRUE(syncd_->pollDevice());
    EXPECT_EQ(
        counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"))
            .at("active_connections"),
        "2");

    const std::string state_group = "drift-state-events";
    const std::string alarm_group = "drift-alarm-events";
    state_db_->createConsumerGroup(std::string(ocs::redis::kStateEvents), state_group, "$");
    alarm_db_->createConsumerGroup(std::string(ocs::redis::kAlarmEvents), alarm_group, "$");

    syncd_.reset();
    {
        ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
        ASSERT_TRUE(
            fault_backend.injectFault({.type = ocs::FaultType::kOutOfBandDrift}).error.ok());
        ASSERT_EQ(fault_backend.getConnections().size(), 1);
    }
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_, std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_));
    syncd_->initialize();

    ASSERT_TRUE(syncd_->pollDevice());
    const auto control =
        device_db_->getHash(ocs::redis::syncdReconciliationKey("ocs0"));
    ASSERT_EQ(control.at("status"), "PUBLISHED");
    const auto command_id = control.at("command_id");
    const auto drifted =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "drift-a"));
    EXPECT_EQ(drifted.at("apply_status"), "RECONCILING");
    EXPECT_EQ(drifted.at("actual_present"), "false");
    EXPECT_EQ(drifted.at("applied_version"), "0");
    const auto state_events = state_db_->readGroup(
        std::string(ocs::redis::kStateEvents), state_group, "drift-state-reader", 10);
    ASSERT_EQ(state_events.size(), 3);
    EXPECT_NE(state_events.at(0).event.payload.find("\"apply_status\":\"DRIFTED\""),
              std::string::npos);
    EXPECT_NE(state_events.at(1).event.payload.find("\"apply_status\":\"RECONCILING\""),
              std::string::npos);
    EXPECT_EQ(state_events.at(2).event.resource_type, "device");

    const auto alarm_key = ocs::redis::activeAlarmKey(
        "ocs0", ocs::redis::desiredActualDriftAlarmId());
    const auto alarm = alarm_db_->getHash(alarm_key);
    EXPECT_EQ(alarm.at("active"), "true");
    EXPECT_EQ(alarm.at("drift_count"), "1");
    const auto raised = alarm_db_->readGroup(
        std::string(ocs::redis::kAlarmEvents), alarm_group, "drift-alarm-reader");
    ASSERT_EQ(raised.size(), 1);
    EXPECT_EQ(raised.front().event.operation, "UPSERT");

    const auto before = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(before.at("active_connections"), "1");
    EXPECT_EQ(before.at("active_alarms"), "1");
    EXPECT_EQ(before.at("drift_detected_total"), "1");
    EXPECT_EQ(before.at("reconciliation_total"), "1");

    ASSERT_TRUE(syncd_->processOne("syncd-drift-reconcile"));
    const auto attempt = device_db_->getHash(ocs::redis::deviceApplyAttemptKey(command_id));
    const auto full_snapshot = ocs::decodeDeviceCommand(attempt.at("payload"));
    ASSERT_EQ(full_snapshot.commands.size(), 2);
    EXPECT_EQ(full_snapshot.commands.at(0).id, "drift-a");
    EXPECT_EQ(full_snapshot.commands.at(1).id, "drift-b");
    ASSERT_TRUE(orch_->processResultOne("orch-drift-result-a"));
    ASSERT_TRUE(orch_->processResultOne("orch-drift-result-b"));
    ASSERT_TRUE(syncd_->pollDevice());

    EXPECT_EQ(
        device_db_->getHash(ocs::redis::syncdReconciliationKey("ocs0")).at("status"),
        "CONVERGED");
    EXPECT_TRUE(alarm_db_->getHash(alarm_key).empty());
    const auto cleared = alarm_db_->readGroup(
        std::string(ocs::redis::kAlarmEvents), alarm_group, "drift-alarm-reader");
    ASSERT_EQ(cleared.size(), 1);
    EXPECT_EQ(cleared.front().event.operation, "REMOVE");
    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("active_connections"), "2");
    EXPECT_EQ(counters.at("active_alarms"), "0");
    EXPECT_EQ(counters.at("drift_detected_total"), "1");
    EXPECT_EQ(counters.at("reconciliation_total"), "1");
    EXPECT_EQ(counters.at("reconciliation_success_total"), "1");
    EXPECT_EQ(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "drift-a"))
            .at("apply_status"),
        "ACTIVE");
    EXPECT_EQ(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "drift-b"))
            .at("apply_status"),
        "ACTIVE");

    syncd_.reset();
    ocs::UdsDeviceBackend verifier(hwsim_socket_);
    const auto actual = verifier.getConnections();
    ASSERT_EQ(actual.size(), 2);
    EXPECT_EQ(actual.at(0).id, "drift-a");
    EXPECT_EQ(actual.at(1).id, "drift-b");
}

TEST_F(OrchestratorIntegrationTest, ReconciliationRecoversSameVersionFailedApplication) {
    syncd_.reset();
    {
        ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
        ASSERT_TRUE(
            fault_backend.injectFault({.type = ocs::FaultType::kNextApplyError}).error.ok());
    }
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_, std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_));
    syncd_->initialize();

    config_db_->putHash(
        ocs::redis::connectionConfigKey("ocs0", "reconcile-failed"),
        {
            {"device", "ocs0"},
            {"id", "reconcile-failed"},
            {"input_port", "6"},
            {"output_port", "14"},
            {"desired_version", "1"},
        });
    const ocs::redis::EventEnvelope create{
        .event_schema_version = 1,
        .event_id = "event-reconcile-failed",
        .request_id = "request-reconcile-failed",
        .timestamp_ns = 1780000000000000072ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "reconcile-failed",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":6,"output_port":14})",
    };
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), create));
    ASSERT_TRUE(orch_->processConfigOne("orch-reconcile-failed"));
    ASSERT_TRUE(syncd_->processOne("syncd-reconcile-failed"));
    ASSERT_TRUE(orch_->processResultOne("orch-reconcile-failed-result"));
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "reconcile-failed"))
            .at("apply_status"),
        "FAILED");

    ASSERT_TRUE(syncd_->pollDevice());
    EXPECT_EQ(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "reconcile-failed"))
            .at("apply_status"),
        "FAILED");
    ASSERT_TRUE(syncd_->processOne("syncd-reconcile-failed-recovery"));
    ASSERT_TRUE(orch_->processResultOne("orch-reconcile-failed-recovery-result"));
    ASSERT_TRUE(syncd_->pollDevice());

    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "reconcile-failed"))
            .at("apply_status"),
        "ACTIVE");
    EXPECT_EQ(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "reconcile-failed"))
            .at("apply_status"),
        "ACTIVE");
    EXPECT_TRUE(
        alarm_db_
            ->getHash(ocs::redis::activeAlarmKey(
                "ocs0", ocs::redis::desiredActualDriftAlarmId()))
            .empty());
}

TEST_F(OrchestratorIntegrationTest, PortDownPublishesStateAndAlarmThenReconcilesAfterClear) {
    config_db_->putHash(
        ocs::redis::connectionConfigKey("ocs0", "port-fault"),
        {
            {"device", "ocs0"},
            {"id", "port-fault"},
            {"input_port", "3"},
            {"output_port", "11"},
            {"desired_version", "1"},
        });
    applyConfigEvent({
        .event_schema_version = 1,
        .event_id = "event-port-fault",
        .request_id = "request-port-fault",
        .timestamp_ns = 1780000000000000073ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "port-fault",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":3,"output_port":11})",
    });
    ASSERT_TRUE(syncd_->pollDevice());

    const std::string state_group = "port-fault-state-events";
    const std::string alarm_group = "port-fault-alarm-events";
    state_db_->createConsumerGroup(std::string(ocs::redis::kStateEvents), state_group, "$");
    alarm_db_->createConsumerGroup(std::string(ocs::redis::kAlarmEvents), alarm_group, "$");

    const auto run_fault = [this, &state_group, &alarm_group](
                               ocs::FaultType type,
                               std::string_view direction,
                               ocs::PortId port_id,
                               std::string_view suffix) {
        {
            ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
            ASSERT_TRUE(fault_backend.injectFault({.type = type, .port_id = port_id}).error.ok());
        }
        ASSERT_TRUE(syncd_->pollDevice());

        const auto port_id_text = std::to_string(port_id);
        const auto port_key = direction == "input"
                                  ? ocs::redis::inputPortStateKey("ocs0", port_id_text)
                                  : ocs::redis::outputPortStateKey("ocs0", port_id_text);
        EXPECT_EQ(state_db_->getHash(port_key).at("oper_status"), "DOWN");
        const auto reconciling =
            state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "port-fault"));
        EXPECT_EQ(reconciling.at("apply_status"), "RECONCILING");
        EXPECT_EQ(reconciling.at("actual_present"), "false");

        const auto alarm_id = ocs::redis::portDownAlarmId(direction, port_id_text);
        const auto alarm = alarm_db_->getHash(ocs::redis::activeAlarmKey("ocs0", alarm_id));
        EXPECT_EQ(alarm.at("active"), "true");
        EXPECT_EQ(alarm.at("affected_connection_count"), "1");
        EXPECT_EQ(alarm.at("error_code"), "OCS_PORT_DOWN");

        const auto down_events = state_db_->readGroup(
            std::string(ocs::redis::kStateEvents),
            state_group,
            "port-fault-state-reader-" + std::string(suffix),
            10);
        EXPECT_TRUE(std::ranges::any_of(down_events, [&](const auto& message) {
            return message.event.resource_type == std::string(direction) + "-port" &&
                   message.event.resource_id == port_id_text &&
                   message.event.payload.find("\"oper_status\":\"DOWN\"") !=
                       std::string::npos;
        }));
        const auto raised = alarm_db_->readGroup(
            std::string(ocs::redis::kAlarmEvents),
            alarm_group,
            "port-fault-alarm-reader-" + std::string(suffix),
            10);
        EXPECT_TRUE(std::ranges::any_of(raised, [&](const auto& message) {
            return message.event.resource_id == alarm_id && message.event.operation == "UPSERT";
        }));

        const auto failed_command =
            device_db_->getHash(ocs::redis::syncdReconciliationKey("ocs0")).at("command_id");
        ASSERT_TRUE(syncd_->processOne("syncd-port-fault-" + std::string(suffix)));
        ASSERT_TRUE(orch_->processResultOne("orch-port-fault-" + std::string(suffix)));
        const auto failed =
            state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "port-fault"));
        EXPECT_EQ(failed.at("apply_status"), "FAILED");
        EXPECT_EQ(failed.at("last_error_code"), "OCS_PORT_DOWN");
        EXPECT_EQ(
            device_db_->getHash(ocs::redis::processedDeviceCommandKey(failed_command))
                .at("applied"),
            "false");

        {
            ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
            ASSERT_TRUE(fault_backend.clearFault({.type = type, .port_id = port_id}).error.ok());
        }
        ASSERT_TRUE(syncd_->pollDevice());
        EXPECT_EQ(state_db_->getHash(port_key).at("oper_status"), "UP");
        EXPECT_TRUE(alarm_db_->getHash(ocs::redis::activeAlarmKey("ocs0", alarm_id)).empty());
        const auto recovery_command =
            device_db_->getHash(ocs::redis::syncdReconciliationKey("ocs0")).at("command_id");
        EXPECT_NE(recovery_command, failed_command);
        ASSERT_TRUE(syncd_->processOne("syncd-port-recovery-" + std::string(suffix)));
        ASSERT_TRUE(orch_->processResultOne("orch-port-recovery-" + std::string(suffix)));
        ASSERT_TRUE(syncd_->pollDevice());

        const auto active =
            state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "port-fault"));
        EXPECT_EQ(active.at("apply_status"), "ACTIVE");
        EXPECT_EQ(active.at("actual_present"), "true");
        EXPECT_EQ(active.at("desired_version"), active.at("applied_version"));
        EXPECT_EQ(
            device_db_->getHash(ocs::redis::syncdReconciliationKey("ocs0")).at("status"),
            "CONVERGED");
    };

    run_fault(ocs::FaultType::kInputPortDown, "input", 3, "input");
    run_fault(ocs::FaultType::kOutputPortDown, "output", 11, "output");

    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("active_connections"), "1");
    EXPECT_EQ(counters.at("active_alarms"), "0");
    EXPECT_EQ(counters.at("port_down_total"), "2");
    EXPECT_EQ(counters.at("reconciliation_total"), "4");
    EXPECT_EQ(counters.at("reconciliation_success_total"), "2");
}

TEST_F(OrchestratorIntegrationTest, ApplyTimeoutRaisesAlarmAndRecoversAfterDurableRetry) {
    orch_ = std::make_unique<ocs::OrchestratorService>(
        endpoint_,
        std::chrono::milliseconds(0),
        ocs::ApplyRetryPolicy{
            .max_retries = 2,
            .base_backoff = std::chrono::milliseconds(20),
            .max_backoff = std::chrono::milliseconds(40),
        });
    orch_->initialize();
    syncd_.reset();
    {
        ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
        ASSERT_TRUE(
            fault_backend.injectFault({.type = ocs::FaultType::kNextApplyTimeout}).error.ok());
    }
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_, std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_));
    syncd_->initialize();

    const ocs::redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = "event-timeout-recovery-001",
        .request_id = "request-timeout-recovery-001",
        .timestamp_ns = 1780000000000000030ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-timeout-recovery-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":6,"output_port":13})",
    };
    static_cast<void>(
        config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), event));
    ASSERT_TRUE(orch_->processConfigOne("orch-timeout-config"));
    ASSERT_TRUE(syncd_->processOne("syncd-timeout"));
    ASSERT_TRUE(orch_->processResultOne("orch-timeout-result"));

    const auto application = appl_db_->getHash(
        ocs::redis::connectionAppKey("ocs0", "conn-timeout-recovery-001"));
    EXPECT_EQ(application.at("apply_status"), "RETRY_WAIT");
    EXPECT_EQ(application.at("last_error_code"), "OCS_APPLY_TIMEOUT");
    const auto failed_state = state_db_->getHash(
        ocs::redis::connectionStateKey("ocs0", "conn-timeout-recovery-001"));
    EXPECT_EQ(failed_state.at("apply_status"), "FAILED");
    EXPECT_EQ(failed_state.at("actual_present"), "false");
    EXPECT_EQ(failed_state.at("applied_version"), "0");
    const auto alarm_id = ocs::redis::applyTimeoutAlarmId("conn-timeout-recovery-001");
    const auto alarm = alarm_db_->getHash(ocs::redis::activeAlarmKey("ocs0", alarm_id));
    EXPECT_EQ(alarm.at("active"), "true");
    EXPECT_EQ(alarm.at("error_code"), "OCS_APPLY_TIMEOUT");
    auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("device_apply_timeout_total"), "1");
    EXPECT_EQ(counters.at("active_alarms"), "1");

    orch_.reset();
    orch_ = std::make_unique<ocs::OrchestratorService>(
        endpoint_,
        std::chrono::milliseconds(0),
        ocs::ApplyRetryPolicy{
            .max_retries = 2,
            .base_backoff = std::chrono::milliseconds(20),
            .max_backoff = std::chrono::milliseconds(40),
        });
    orch_->initialize();
    syncd_.reset();
    {
        ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
        ASSERT_TRUE(
            fault_backend.clearFault({.type = ocs::FaultType::kNextApplyTimeout}).error.ok());
    }
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_, std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_));
    syncd_->initialize();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    bool retry_published = false;
    while (!retry_published && std::chrono::steady_clock::now() < deadline) {
        retry_published = orch_->processRetryOne("orch-timeout-retry");
        if (!retry_published) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }
    ASSERT_TRUE(retry_published);
    ASSERT_TRUE(syncd_->processOne("syncd-timeout-retry"));
    ASSERT_TRUE(orch_->processResultOne("orch-timeout-retry-result"));

    const auto active = appl_db_->getHash(
        ocs::redis::connectionAppKey("ocs0", "conn-timeout-recovery-001"));
    EXPECT_EQ(active.at("apply_status"), "ACTIVE");
    const auto recovered_state = state_db_->getHash(
        ocs::redis::connectionStateKey("ocs0", "conn-timeout-recovery-001"));
    EXPECT_EQ(recovered_state.at("apply_status"), "ACTIVE");
    EXPECT_EQ(recovered_state.at("desired_version"), recovered_state.at("applied_version"));
    EXPECT_TRUE(
        alarm_db_->getHash(ocs::redis::activeAlarmKey("ocs0", alarm_id)).empty());
    counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("device_apply_total"), "2");
    EXPECT_EQ(counters.at("device_apply_timeout_total"), "1");
    EXPECT_EQ(counters.at("active_alarms"), "0");
    EXPECT_FALSE(orch_->processResultOne("orch-timeout-retry-result"));
    EXPECT_FALSE(orch_->processRetryOne("orch-timeout-retry"));
}

TEST_F(OrchestratorIntegrationTest, ExhaustsConfiguredApplyTimeoutRetries) {
    orch_ = std::make_unique<ocs::OrchestratorService>(
        endpoint_,
        std::chrono::milliseconds(0),
        ocs::ApplyRetryPolicy{
            .max_retries = 2,
            .base_backoff = std::chrono::milliseconds(0),
            .max_backoff = std::chrono::milliseconds(0),
        });
    orch_->initialize();
    const auto inject_timeout = [this] {
        syncd_.reset();
        {
            ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
            ASSERT_TRUE(
                fault_backend.injectFault({.type = ocs::FaultType::kNextApplyTimeout}).error.ok());
        }
        syncd_ = std::make_unique<ocs::SyncdService>(
            endpoint_, std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_));
        syncd_->initialize();
    };

    inject_timeout();
    const ocs::redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = "event-timeout-exhausted-001",
        .request_id = "request-timeout-exhausted-001",
        .timestamp_ns = 1780000000000000031ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-timeout-exhausted-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":6,"output_port":13})",
    };
    static_cast<void>(
        config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), event));
    ASSERT_TRUE(orch_->processConfigOne("orch-timeout-exhausted-config"));
    ASSERT_TRUE(syncd_->processOne("syncd-timeout-exhausted"));
    ASSERT_TRUE(orch_->processResultOne("orch-timeout-exhausted-result"));

    for (int retry = 0; retry < 2; ++retry) {
        ASSERT_TRUE(orch_->processRetryOne("orch-timeout-exhausted-retry"));
        inject_timeout();
        ASSERT_TRUE(syncd_->processOne("syncd-timeout-exhausted"));
        ASSERT_TRUE(orch_->processResultOne("orch-timeout-exhausted-result"));
    }

    const auto failed = appl_db_->getHash(
        ocs::redis::connectionAppKey("ocs0", "conn-timeout-exhausted-001"));
    EXPECT_EQ(failed.at("apply_status"), "FAILED");
    EXPECT_EQ(failed.at("retry_attempt"), "2");
    EXPECT_FALSE(orch_->processRetryOne("orch-timeout-exhausted-retry"));
    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("device_apply_total"), "3");
    EXPECT_EQ(counters.at("device_apply_timeout_total"), "3");
    EXPECT_EQ(counters.at("device_apply_failure_total"), "3");
    EXPECT_EQ(counters.at("active_alarms"), "1");
}

TEST_F(OrchestratorIntegrationTest, PreservesAtomicOutputSwapAsOneDeviceBatch) {
    const auto initial_event = [](std::string id, ocs::PortId input, ocs::PortId output) {
        return ocs::redis::EventEnvelope{
            .event_schema_version = 1,
            .event_id = "event-swap-initial-" + id,
            .request_id = "request-swap-initial-" + id,
            .timestamp_ns = 1780000000000000010ULL,
            .device = "ocs0",
            .resource_type = "connection",
            .resource_id = id,
            .operation = "UPSERT",
            .desired_version = 1,
            .payload = "{\"input_port\":" + std::to_string(input) +
                       ",\"output_port\":" + std::to_string(output) + "}",
        };
    };
    applyConfigEvent(initial_event("swap-a", 1, 9));
    applyConfigEvent(initial_event("swap-b", 2, 10));

    const ocs::DeviceCommandBatch swap{
        .commands = {
            {
                .id = "swap-a",
                .input_port = 1,
                .output_port = 10,
                .desired_version = 2,
            },
            {
                .id = "swap-b",
                .input_port = 2,
                .output_port = 9,
                .desired_version = 2,
            },
        },
        .options = {},
    };
    const ocs::redis::EventEnvelope batch_event{
        .event_schema_version = 1,
        .event_id = "event-swap-batch-002",
        .request_id = "request-swap-batch-002",
        .timestamp_ns = 1780000000000000012ULL,
        .device = "ocs0",
        .resource_type = "connection-batch",
        .resource_id = "request-swap-batch-002",
        .operation = "APPLY_BATCH",
        .desired_version = 2,
        .payload = ocs::encodeDeviceCommand(swap),
    };
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), batch_event));

    ASSERT_TRUE(orch_->processConfigOne("orch-test-swap"));
    ASSERT_TRUE(syncd_->processOne("syncd-test-swap"));
    ASSERT_TRUE(orch_->processResultOne("orch-test-swap-result"));
    ASSERT_TRUE(orch_->processResultOne("orch-test-swap-result"));

    const auto swap_a =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "swap-a"));
    const auto swap_b =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "swap-b"));
    EXPECT_EQ(swap_a.at("output_port"), "10");
    EXPECT_EQ(swap_b.at("output_port"), "9");
    EXPECT_EQ(swap_a.at("applied_version"), "2");
    EXPECT_EQ(swap_b.at("applied_version"), "2");
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "swap-a")).at("apply_status"),
        "ACTIVE");
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "swap-b")).at("apply_status"),
        "ACTIVE");
}

TEST_F(OrchestratorIntegrationTest, RetriesTimedOutOutputSwapAsOneAtomicBatch) {
    const auto initial_event = [](std::string id, ocs::PortId input, ocs::PortId output) {
        return ocs::redis::EventEnvelope{
            .event_schema_version = 1,
            .event_id = "event-timeout-swap-initial-" + id,
            .request_id = "request-timeout-swap-initial-" + id,
            .timestamp_ns = 1780000000000000032ULL,
            .device = "ocs0",
            .resource_type = "connection",
            .resource_id = id,
            .operation = "UPSERT",
            .desired_version = 1,
            .payload = "{\"input_port\":" + std::to_string(input) +
                       ",\"output_port\":" + std::to_string(output) + "}",
        };
    };
    applyConfigEvent(initial_event("timeout-swap-a", 1, 9));
    applyConfigEvent(initial_event("timeout-swap-b", 2, 10));
    orch_ = std::make_unique<ocs::OrchestratorService>(
        endpoint_,
        std::chrono::milliseconds(0),
        ocs::ApplyRetryPolicy{
            .max_retries = 1,
            .base_backoff = std::chrono::milliseconds(0),
            .max_backoff = std::chrono::milliseconds(0),
        });
    orch_->initialize();
    syncd_.reset();
    {
        ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
        ASSERT_TRUE(
            fault_backend.injectFault({.type = ocs::FaultType::kNextApplyTimeout}).error.ok());
    }
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_, std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_));
    syncd_->initialize();

    const ocs::DeviceCommandBatch swap{
        .commands = {
            {
                .id = "timeout-swap-a",
                .input_port = 1,
                .output_port = 10,
                .desired_version = 2,
            },
            {
                .id = "timeout-swap-b",
                .input_port = 2,
                .output_port = 9,
                .desired_version = 2,
            },
        },
        .options = {},
    };
    const ocs::redis::EventEnvelope batch_event{
        .event_schema_version = 1,
        .event_id = "event-timeout-swap-batch-002",
        .request_id = "request-timeout-swap-batch-002",
        .timestamp_ns = 1780000000000000033ULL,
        .device = "ocs0",
        .resource_type = "connection-batch",
        .resource_id = "request-timeout-swap-batch-002",
        .operation = "APPLY_BATCH",
        .desired_version = 2,
        .payload = ocs::encodeDeviceCommand(swap),
    };
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), batch_event));
    ASSERT_TRUE(orch_->processConfigOne("orch-timeout-swap"));
    ASSERT_TRUE(syncd_->processOne("syncd-timeout-swap"));
    ASSERT_TRUE(orch_->processResultOne("orch-timeout-swap-result"));
    ASSERT_TRUE(orch_->processResultOne("orch-timeout-swap-result"));
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "timeout-swap-a"))
            .at("apply_status"),
        "RETRY_WAIT");
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "timeout-swap-b"))
            .at("apply_status"),
        "RETRY_WAIT");

    ASSERT_TRUE(orch_->processRetryOne("orch-timeout-swap-retry"));
    ASSERT_TRUE(syncd_->processOne("syncd-timeout-swap-retry"));
    ASSERT_TRUE(orch_->processResultOne("orch-timeout-swap-retry-result"));
    ASSERT_TRUE(orch_->processResultOne("orch-timeout-swap-retry-result"));

    const auto swap_a =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "timeout-swap-a"));
    const auto swap_b =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "timeout-swap-b"));
    EXPECT_EQ(swap_a.at("output_port"), "10");
    EXPECT_EQ(swap_b.at("output_port"), "9");
    EXPECT_EQ(swap_a.at("applied_version"), "2");
    EXPECT_EQ(swap_b.at("applied_version"), "2");
    EXPECT_EQ(
        counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"))
            .at("device_apply_total"),
        "4");
}

TEST_F(OrchestratorIntegrationTest, RecoversWholeAtomicBatchAfterApplicationPhaseCrash) {
    const auto initial_event = [](std::string id, ocs::PortId input, ocs::PortId output) {
        return ocs::redis::EventEnvelope{
            .event_schema_version = 1,
            .event_id = "event-crash-swap-initial-" + id,
            .request_id = "request-crash-swap-initial-" + id,
            .timestamp_ns = 1780000000000000040ULL,
            .device = "ocs0",
            .resource_type = "connection",
            .resource_id = id,
            .operation = "UPSERT",
            .desired_version = 1,
            .payload = "{\"input_port\":" + std::to_string(input) +
                       ",\"output_port\":" + std::to_string(output) + "}",
        };
    };
    applyConfigEvent(initial_event("crash-swap-a", 1, 9));
    applyConfigEvent(initial_event("crash-swap-b", 2, 10));

    const ocs::DeviceCommandBatch swap{
        .commands = {
            {
                .id = "crash-swap-a",
                .input_port = 1,
                .output_port = 10,
                .desired_version = 2,
            },
            {
                .id = "crash-swap-b",
                .input_port = 2,
                .output_port = 9,
                .desired_version = 2,
            },
        },
        .options = {},
    };
    const ocs::redis::EventEnvelope batch_event{
        .event_schema_version = 1,
        .event_id = "event-crash-swap-batch-002",
        .request_id = "request-crash-swap-batch-002",
        .timestamp_ns = 1780000000000000042ULL,
        .device = "ocs0",
        .resource_type = "connection-batch",
        .resource_id = "request-crash-swap-batch-002",
        .operation = "APPLY_BATCH",
        .desired_version = 2,
        .payload = ocs::encodeDeviceCommand(swap),
    };
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), batch_event));

    struct SimulatedCrash {};
    EXPECT_THROW(
        static_cast<void>(orch_->processConfigOne(
            "orch-crash-swap",
            [](std::string_view phase) {
                if (phase == "application") {
                    throw SimulatedCrash{};
                }
            })),
        SimulatedCrash);
    EXPECT_EQ(config_db_->pendingCount(std::string(ocs::redis::kConfigEvents), "ocs-orch"), 1);
    EXPECT_FALSE(syncd_->processOne("syncd-before-orch-recovery"));
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "crash-swap-a"))
            .at("desired_version"),
        "2");
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "crash-swap-b"))
            .at("desired_version"),
        "2");

    orch_.reset();
    orch_ = std::make_unique<ocs::OrchestratorService>(endpoint_, std::chrono::milliseconds(0));
    orch_->initialize();
    ASSERT_TRUE(orch_->processConfigOne("orch-crash-swap-recovery"));
    ASSERT_TRUE(syncd_->processOne("syncd-crash-swap-recovery"));
    ASSERT_TRUE(orch_->processResultOne("orch-crash-swap-result"));
    ASSERT_TRUE(orch_->processResultOne("orch-crash-swap-result"));

    const auto state_a =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "crash-swap-a"));
    const auto state_b =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "crash-swap-b"));
    EXPECT_EQ(state_a.at("output_port"), "10");
    EXPECT_EQ(state_b.at("output_port"), "9");
    EXPECT_EQ(state_a.at("applied_version"), "2");
    EXPECT_EQ(state_b.at("applied_version"), "2");
    EXPECT_EQ(config_db_->pendingCount(std::string(ocs::redis::kConfigEvents), "ocs-orch"), 0);
}

TEST_F(OrchestratorIntegrationTest, MalformedDeviceCommandPublishesTerminalFailure) {
    const std::map<std::string, std::string> application{
        {"device", "ocs0"},
        {"id", "malformed"},
        {"input_port", "1"},
        {"output_port", "9"},
        {"desired_version", "1"},
        {"apply_status", "APPLYING"},
        {"operation", "UPSERT"},
        {"request_id", "request-malformed"},
        {"event_id", "event-malformed"},
        {"command_id", "command-malformed"},
        {"last_error_code", ""},
        {"last_error_message", ""},
    };
    appl_db_->putHash(
        ocs::redis::connectionAppKey("ocs0", "malformed"), application);
    device_db_->putHash(
        ocs::redis::connectionDeviceKey("ocs0", "malformed"), application);
    const ocs::redis::EventEnvelope malformed{
        .event_schema_version = 1,
        .event_id = "command-malformed",
        .request_id = "request-malformed",
        .timestamp_ns = 1780000000000000045ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "malformed",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = "{not-json",
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), malformed));

    ASSERT_TRUE(syncd_->processOne("syncd-malformed"));
    ASSERT_TRUE(orch_->processResultOne("orch-malformed-result"));

    const auto failed =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "malformed"));
    EXPECT_EQ(failed.at("apply_status"), "FAILED");
    EXPECT_EQ(failed.at("last_error_code"), "OCS_PROTOCOL_MALFORMED");
}

TEST_F(OrchestratorIntegrationTest, MalformedBatchFailsEveryPreparedMember) {
    const ocs::DeviceCommandBatch batch{
        .commands = {
            {.id = "malformed-a", .input_port = 1, .output_port = 9, .desired_version = 1},
            {.id = "malformed-b", .input_port = 2, .output_port = 10, .desired_version = 1},
        },
        .options = {},
    };
    for (const auto& command : batch.commands) {
        const std::map<std::string, std::string> application{
            {"device", "ocs0"},
            {"id", command.id},
            {"input_port", std::to_string(command.input_port)},
            {"output_port", std::to_string(command.output_port)},
            {"desired_version", "1"},
            {"apply_status", "APPLYING"},
            {"operation", "UPSERT"},
            {"request_id", "request-malformed-batch"},
            {"event_id", "event-malformed-batch"},
            {"command_id", "event-malformed-batch:command"},
            {"last_error_code", ""},
            {"last_error_message", ""},
        };
        appl_db_->putHash(ocs::redis::connectionAppKey("ocs0", command.id), application);
        device_db_->putHash(ocs::redis::connectionDeviceKey("ocs0", command.id), application);
    }
    device_db_->putHash(
        ocs::redis::orchConfigBatchKey("event-malformed-batch"),
        {{"payload", ocs::encodeDeviceCommand(batch)}});
    const ocs::redis::EventEnvelope malformed{
        .event_schema_version = 1,
        .event_id = "event-malformed-batch:command",
        .request_id = "request-malformed-batch",
        .timestamp_ns = 1780000000000000046ULL,
        .device = "ocs0",
        .resource_type = "connection-batch",
        .resource_id = "request-malformed-batch",
        .operation = "APPLY_BATCH",
        .desired_version = 1,
        .payload = "{not-json",
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), malformed));

    ASSERT_TRUE(syncd_->processOne("syncd-malformed-batch"));
    ASSERT_TRUE(orch_->processResultOne("orch-malformed-batch-result"));
    ASSERT_TRUE(orch_->processResultOne("orch-malformed-batch-result"));

    for (const auto& command : batch.commands) {
        const auto failed =
            appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", command.id));
        EXPECT_EQ(failed.at("apply_status"), "FAILED");
        EXPECT_EQ(failed.at("last_error_code"), "OCS_PROTOCOL_MALFORMED");
    }
}

TEST_F(OrchestratorIntegrationTest, OrphanMalformedBatchResultIsAcknowledged) {
    const ocs::redis::EventEnvelope malformed{
        .event_schema_version = 1,
        .event_id = "external-malformed-batch",
        .request_id = "request-external-malformed-batch",
        .timestamp_ns = 1780000000000000047ULL,
        .device = "ocs0",
        .resource_type = "connection-batch",
        .resource_id = "request-external-malformed-batch",
        .operation = "APPLY_BATCH",
        .desired_version = 1,
        .payload = "{not-json",
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), malformed));

    ASSERT_TRUE(syncd_->processOne("syncd-orphan-malformed-batch"));
    bool processed = false;
    EXPECT_NO_THROW(processed = orch_->processResultOne("orch-orphan-malformed-batch-result"));
    EXPECT_TRUE(processed);
    EXPECT_EQ(device_db_->pendingCount(std::string(ocs::redis::kDeviceResults), "ocs-orch"), 0);
}

TEST_F(OrchestratorIntegrationTest, ConcurrentClaimCannotRegressCompletedApplication) {
    const ocs::redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = "event-concurrent-claim",
        .request_id = "request-concurrent-claim",
        .timestamp_ns = 1780000000000000048ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "concurrent-claim",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":3,"output_port":11})",
    };
    static_cast<void>(config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), event));

    std::mutex mutex;
    std::condition_variable condition;
    bool prepared = false;
    bool resume = false;
    std::exception_ptr first_error;
    std::thread first([&] {
        try {
            static_cast<void>(orch_->processConfigOne(
                "orch-concurrent-first",
                [&](std::string_view phase) {
                    if (phase != "prepared") {
                        return;
                    }
                    std::unique_lock lock(mutex);
                    prepared = true;
                    condition.notify_all();
                    condition.wait(lock, [&] { return resume; });
                }));
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    bool first_reached_prepared = false;
    {
        std::unique_lock lock(mutex);
        first_reached_prepared = condition.wait_for(
            lock, std::chrono::seconds(2), [&] { return prepared; });
    }

    ocs::OrchestratorService second(endpoint_, std::chrono::milliseconds(0));
    second.initialize();
    const bool second_configured =
        first_reached_prepared && second.processConfigOne("orch-concurrent-second");
    const bool device_processed = second_configured && syncd_->processOne("syncd-concurrent");
    const bool result_processed =
        device_processed && second.processResultOne("orch-concurrent-result");
    {
        std::lock_guard lock(mutex);
        resume = true;
    }
    condition.notify_all();
    first.join();

    EXPECT_TRUE(first_reached_prepared);
    EXPECT_TRUE(second_configured);
    EXPECT_TRUE(device_processed);
    EXPECT_TRUE(result_processed);
    EXPECT_EQ(first_error, nullptr);
    const auto application =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "concurrent-claim"));
    EXPECT_EQ(application.at("apply_status"), "ACTIVE");
    EXPECT_EQ(
        counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"))
            .at("device_apply_total"),
        "1");
}

TEST_F(OrchestratorIntegrationTest, NewerEventCannotOvertakePendingAtomicBatch) {
    const ocs::DeviceCommandBatch first_batch{
        .commands = {
            {.id = "ordered-a", .input_port = 1, .output_port = 9, .desired_version = 1},
            {.id = "ordered-b", .input_port = 2, .output_port = 10, .desired_version = 1},
        },
        .options = {},
    };
    const ocs::redis::EventEnvelope first_event{
        .event_schema_version = 1,
        .event_id = "event-ordered-batch-1",
        .request_id = "request-ordered-batch-1",
        .timestamp_ns = 1780000000000000050ULL,
        .device = "ocs0",
        .resource_type = "connection-batch",
        .resource_id = "request-ordered-batch-1",
        .operation = "APPLY_BATCH",
        .desired_version = 1,
        .payload = ocs::encodeDeviceCommand(first_batch),
    };
    const ocs::redis::EventEnvelope second_event{
        .event_schema_version = 1,
        .event_id = "event-ordered-a-2",
        .request_id = "request-ordered-a-2",
        .timestamp_ns = 1780000000000000051ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "ordered-a",
        .operation = "UPSERT",
        .desired_version = 2,
        .payload = R"({"input_port":3,"output_port":11})",
    };
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), first_event));
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), second_event));

    std::mutex mutex;
    std::condition_variable condition;
    bool prepared = false;
    bool resume = false;
    bool first_processed = false;
    std::exception_ptr first_error;
    std::thread first([&] {
        try {
            first_processed = orch_->processConfigOne(
                "orch-ordered-first",
                [&](std::string_view phase) {
                    if (phase != "prepared") {
                        return;
                    }
                    std::unique_lock lock(mutex);
                    prepared = true;
                    condition.notify_all();
                    condition.wait(lock, [&] { return resume; });
                });
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    bool first_reached_prepared = false;
    {
        std::unique_lock lock(mutex);
        first_reached_prepared = condition.wait_for(
            lock, std::chrono::seconds(2), [&] { return prepared; });
    }

    ocs::OrchestratorService second(endpoint_, std::chrono::hours(1));
    second.initialize();
    bool newer_overtook = false;
    std::exception_ptr second_error;
    try {
        newer_overtook =
            first_reached_prepared && second.processConfigOne("orch-ordered-second");
    } catch (...) {
        second_error = std::current_exception();
    }
    {
        std::lock_guard lock(mutex);
        resume = true;
    }
    condition.notify_all();
    first.join();

    EXPECT_TRUE(first_reached_prepared);
    EXPECT_FALSE(newer_overtook);
    EXPECT_TRUE(first_processed);
    EXPECT_EQ(first_error, nullptr);
    EXPECT_EQ(second_error, nullptr);
    ASSERT_TRUE(syncd_->processOne("syncd-ordered-first"));
    ASSERT_TRUE(orch_->processResultOne("orch-ordered-first-result"));
    ASSERT_TRUE(orch_->processResultOne("orch-ordered-first-result"));

    ASSERT_TRUE(second.processConfigOne("orch-ordered-second"));
    ASSERT_TRUE(syncd_->processOne("syncd-ordered-second"));
    ASSERT_TRUE(second.processResultOne("orch-ordered-second-result"));
    const auto ordered_a =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "ordered-a"));
    const auto ordered_b =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "ordered-b"));
    EXPECT_EQ(ordered_a.at("desired_version"), "2");
    EXPECT_EQ(ordered_a.at("apply_status"), "ACTIVE");
    EXPECT_EQ(ordered_b.at("desired_version"), "1");
    EXPECT_EQ(ordered_b.at("apply_status"), "ACTIVE");
}

TEST_F(OrchestratorIntegrationTest, RecoversResultAfterDeviceDatabasePhaseCrash) {
    const ocs::redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = "event-result-crash",
        .request_id = "request-result-crash",
        .timestamp_ns = 1780000000000000049ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "result-crash",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":4,"output_port":12})",
    };
    static_cast<void>(config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), event));
    ASSERT_TRUE(orch_->processConfigOne("orch-result-crash-config"));
    ASSERT_TRUE(syncd_->processOne("syncd-result-crash"));

    struct SimulatedCrash {};
    EXPECT_THROW(
        static_cast<void>(orch_->processResultOne(
            "orch-result-crash-first",
            [](std::string_view phase) {
                if (phase == "device-result") {
                    throw SimulatedCrash{};
                }
            })),
        SimulatedCrash);
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "result-crash"))
            .at("apply_status"),
        "APPLYING");
    EXPECT_EQ(
        device_db_->getHash(ocs::redis::connectionDeviceKey("ocs0", "result-crash"))
            .at("apply_status"),
        "ACTIVE");
    EXPECT_EQ(device_db_->pendingCount(std::string(ocs::redis::kDeviceResults), "ocs-orch"), 1);

    orch_ = std::make_unique<ocs::OrchestratorService>(endpoint_, std::chrono::milliseconds(0));
    orch_->initialize();
    ASSERT_TRUE(orch_->processResultOne("orch-result-crash-recovery"));
    EXPECT_EQ(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "result-crash"))
            .at("apply_status"),
        "ACTIVE");
    EXPECT_EQ(device_db_->pendingCount(std::string(ocs::redis::kDeviceResults), "ocs-orch"), 0);
}

TEST_F(OrchestratorIntegrationTest, CoalescesConsecutiveVersionsWhileApplyIsOutstanding) {
    const ocs::redis::EventEnvelope first{
        .event_schema_version = 1,
        .event_id = "event-consecutive-001",
        .request_id = "request-consecutive-001",
        .timestamp_ns = 1780000000000000020ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-consecutive-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":1,"output_port":9})",
    };
    auto second = first;
    second.event_id = "event-consecutive-002";
    second.request_id = "request-consecutive-002";
    second.timestamp_ns += 1;
    second.desired_version = 2;
    second.payload = R"({"input_port":2,"output_port":10})";
    static_cast<void>(
        config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), first));
    static_cast<void>(
        config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), second));

    ASSERT_TRUE(orch_->processConfigOne("orch-test-consecutive"));
    ASSERT_TRUE(orch_->processConfigOne("orch-test-consecutive"));
    const auto applying =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-consecutive-001"));
    EXPECT_EQ(applying.at("desired_version"), "2");
    EXPECT_EQ(applying.at("apply_status"), "APPLYING");

    ASSERT_TRUE(syncd_->processOne("syncd-test-consecutive"));
    ASSERT_TRUE(orch_->processResultOne("orch-test-consecutive-result"));
    ASSERT_TRUE(syncd_->processOne("syncd-test-consecutive"));
    ASSERT_TRUE(orch_->processResultOne("orch-test-consecutive-result"));

    const auto state =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-consecutive-001"));
    EXPECT_EQ(state.at("input_port"), "2");
    EXPECT_EQ(state.at("output_port"), "10");
    EXPECT_EQ(state.at("desired_version"), "2");
    EXPECT_EQ(state.at("applied_version"), "2");
    EXPECT_EQ(state.at("apply_status"), "ACTIVE");
    const auto active =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-consecutive-001"));
    EXPECT_EQ(active.at("desired_version"), "2");
    EXPECT_EQ(active.at("apply_status"), "ACTIVE");
}

TEST_F(OrchestratorIntegrationTest, RejectsConfigPortThatWouldNarrowIntoTheMatrix) {
    const ocs::redis::EventEnvelope event{
        .event_schema_version = 1,
        .event_id = "event-overflow-001",
        .request_id = "request-overflow-001",
        .timestamp_ns = 1780000000000000022ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-overflow-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":65537,"output_port":9})",
    };
    static_cast<void>(
        config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), event));

    EXPECT_THROW(
        static_cast<void>(orch_->processConfigOne("orch-test-overflow")),
        std::invalid_argument);
    EXPECT_TRUE(
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-overflow-001")).empty());
    EXPECT_FALSE(syncd_->processOne("syncd-test-overflow"));
}

TEST_F(OrchestratorIntegrationTest, LatestRemoveConvergesWhenOutstandingCreateFails) {
    const ocs::redis::EventEnvelope create{
        .event_schema_version = 1,
        .event_id = "event-create-remove-001",
        .request_id = "request-create-remove-001",
        .timestamp_ns = 1780000000000000023ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-create-remove-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":3,"output_port":11})",
    };
    auto remove = create;
    remove.event_id = "event-create-remove-002";
    remove.request_id = "request-create-remove-002";
    remove.timestamp_ns += 1;
    remove.operation = "REMOVE";
    remove.desired_version = 2;
    remove.payload = "{}";
    static_cast<void>(
        config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), create));
    static_cast<void>(
        config_db_->appendEvent(std::string(ocs::redis::kConfigEvents), remove));
    ASSERT_TRUE(orch_->processConfigOne("orch-test-create-remove"));
    ASSERT_TRUE(orch_->processConfigOne("orch-test-create-remove"));

    syncd_.reset();
    {
        ocs::UdsDeviceBackend fault_backend(hwsim_socket_);
        ASSERT_TRUE(
            fault_backend.injectFault({.type = ocs::FaultType::kNextApplyError}).error.ok());
    }
    syncd_ = std::make_unique<ocs::SyncdService>(
        endpoint_, std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_));
    syncd_->initialize();
    ASSERT_TRUE(syncd_->processOne("syncd-test-create-remove"));
    ASSERT_TRUE(orch_->processResultOne("orch-test-create-remove-result"));
    ASSERT_TRUE(syncd_->processOne("syncd-test-create-remove"));
    ASSERT_TRUE(orch_->processResultOne("orch-test-create-remove-result"));

    EXPECT_TRUE(
        state_db_->getHash(
            ocs::redis::connectionStateKey("ocs0", "conn-create-remove-001"))
            .empty());
    const auto application =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-create-remove-001"));
    EXPECT_EQ(application.at("desired_version"), "2");
    EXPECT_EQ(application.at("apply_status"), "ABSENT");
}

TEST_F(OrchestratorIntegrationTest, ReloadsVersionGapAndSuppressesStaleAndDuplicateWork) {
    const ocs::redis::EventEnvelope initial_event{
        .event_schema_version = 1,
        .event_id = "event-recovery-001",
        .request_id = "request-recovery-001",
        .timestamp_ns = 1780000000000000000ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-recovery-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":1,"output_port":9})",
    };
    applyConfigEvent(initial_event);

    config_db_->putHash(
        ocs::redis::connectionConfigKey("ocs0", "conn-recovery-001"),
        {
            {"input_port", "4"},
            {"output_port", "12"},
            {"desired_version", "3"},
        });
    restartServices();

    const ocs::redis::EventEnvelope gap_event{
        .event_schema_version = 1,
        .event_id = "event-recovery-003",
        .request_id = "request-recovery-003",
        .timestamp_ns = 1780000000000000003ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-recovery-001",
        .operation = "UPSERT",
        .desired_version = 3,
        .payload = R"({"input_port":2,"output_port":10})",
    };
    applyConfigEvent(gap_event);

    const auto recovered =
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-recovery-001"));
    EXPECT_EQ(recovered.at("input_port"), "4");
    EXPECT_EQ(recovered.at("output_port"), "12");
    EXPECT_EQ(recovered.at("desired_version"), "3");
    EXPECT_EQ(recovered.at("applied_version"), "3");

    const ocs::redis::EventEnvelope stale_event{
        .event_schema_version = 1,
        .event_id = "event-recovery-002-late",
        .request_id = "request-recovery-002",
        .timestamp_ns = 1780000000000000002ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-recovery-001",
        .operation = "UPSERT",
        .desired_version = 2,
        .payload = R"({"input_port":7,"output_port":15})",
    };
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), stale_event));
    ASSERT_TRUE(orch_->processConfigOne("orch-test-config-after-restart"));
    EXPECT_FALSE(syncd_->processOne("syncd-test-after-stale"));

    const ocs::redis::EventEnvelope stale_result{
        .event_schema_version = 1,
        .event_id = "event-recovery-001:command:late-result",
        .request_id = "request-recovery-001",
        .timestamp_ns = 1780000000000000005ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-recovery-001",
        .operation = "APPLY_RESULT",
        .desired_version = 1,
        .payload = R"({"success":false,"error_code":"OCS_APPLY_FAILED","error_message":"late"})",
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceResults), stale_result));
    ASSERT_TRUE(orch_->processResultOne("orch-test-stale-result"));
    const auto after_stale_result =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-recovery-001"));
    EXPECT_EQ(after_stale_result.at("apply_status"), "ACTIVE");
    EXPECT_EQ(after_stale_result.at("desired_version"), "3");

    syncd_.reset();
    auto backend = std::make_unique<ocs::UdsDeviceBackend>(hwsim_socket_);
    syncd_ = std::make_unique<ocs::SyncdService>(endpoint_, std::move(backend));
    syncd_->initialize();

    const ocs::DeviceCommandBatch stale_batch{
        .commands = {{
            .id = "conn-recovery-001",
            .input_port = 8,
            .output_port = 16,
            .desired_version = 2,
        }},
        .options = {},
    };
    const ocs::redis::EventEnvelope stale_command{
        .event_schema_version = 1,
        .event_id = "manual-stale-command-002",
        .request_id = "request-recovery-stale-command",
        .timestamp_ns = 1780000000000000006ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-recovery-001",
        .operation = "UPSERT",
        .desired_version = 2,
        .payload = ocs::encodeDeviceCommand(stale_batch),
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), stale_command));
    ASSERT_TRUE(syncd_->processOne("syncd-test-stale-version"));

    const ocs::DeviceCommandBatch duplicate_batch{
        .commands = {{
            .id = "conn-recovery-001",
            .input_port = 4,
            .output_port = 12,
            .desired_version = 3,
        }},
        .options = {},
    };
    const ocs::redis::EventEnvelope duplicate_command{
        .event_schema_version = 1,
        .event_id = "event-recovery-003:command",
        .request_id = "request-recovery-003",
        .timestamp_ns = 1780000000000000004ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-recovery-001",
        .operation = "UPSERT",
        .desired_version = 3,
        .payload = ocs::encodeDeviceCommand(duplicate_batch),
    };
    static_cast<void>(device_db_->appendEvent(
        std::string(ocs::redis::kDeviceCommands), duplicate_command));
    ASSERT_TRUE(syncd_->processOne("syncd-test-after-restart"));

    const auto counters = counters_db_->getHash(ocs::redis::deviceCountersKey("ocs0"));
    EXPECT_EQ(counters.at("device_apply_total"), "2");
    EXPECT_EQ(counters.at("device_apply_success_total"), "2");
    EXPECT_FALSE(orch_->processResultOne("orch-test-no-duplicate-result"));
    const auto processed = device_db_->getHash(
        ocs::redis::processedDeviceCommandKey("event-recovery-003:command"));
    EXPECT_EQ(processed.at("applied"), "true");
    const auto stale_processed = device_db_->getHash(
        ocs::redis::processedDeviceCommandKey("manual-stale-command-002"));
    EXPECT_EQ(stale_processed.at("applied"), "false");
    const auto syncd_version = device_db_->getHash(
        ocs::redis::syncdConnectionVersionKey("ocs0", "conn-recovery-001"));
    EXPECT_EQ(syncd_version.at("last_successful_version"), "3");
}

TEST_F(OrchestratorIntegrationTest, DeleteTombstoneRejectsLateConfigurationEvent) {
    const ocs::redis::EventEnvelope create_event{
        .event_schema_version = 1,
        .event_id = "event-delete-001",
        .request_id = "request-delete-001",
        .timestamp_ns = 1780000000000000010ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-delete-001",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":6,"output_port":14})",
    };
    applyConfigEvent(create_event);

    const ocs::redis::EventEnvelope delete_event{
        .event_schema_version = 1,
        .event_id = "event-delete-002",
        .request_id = "request-delete-002",
        .timestamp_ns = 1780000000000000011ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-delete-001",
        .operation = "REMOVE",
        .desired_version = 2,
        .payload = "{}",
    };
    applyConfigEvent(delete_event);

    EXPECT_TRUE(
        state_db_->getHash(ocs::redis::connectionStateKey("ocs0", "conn-delete-001")).empty());
    const auto tombstone =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-delete-001"));
    EXPECT_EQ(tombstone.at("apply_status"), "ABSENT");
    EXPECT_EQ(tombstone.at("desired_version"), "2");

    const auto late_event = create_event;
    static_cast<void>(config_db_->appendEvent(
        std::string(ocs::redis::kConfigEvents), late_event));
    ASSERT_TRUE(orch_->processConfigOne("orch-test-after-delete"));
    EXPECT_FALSE(syncd_->processOne("syncd-test-after-delete"));
    const auto retained =
        appl_db_->getHash(ocs::redis::connectionAppKey("ocs0", "conn-delete-001"));
    EXPECT_EQ(retained.at("apply_status"), "ABSENT");
    EXPECT_EQ(retained.at("desired_version"), "2");
}

}  // namespace
