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
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <memory>
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
        config_db_->flushForTest();
        appl_db_->flushForTest();
        device_db_->flushForTest();
        state_db_->flushForTest();
        counters_db_->flushForTest();

        startServices();
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
    orch_ = std::make_unique<ocs::OrchestratorService>(endpoint_);
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
