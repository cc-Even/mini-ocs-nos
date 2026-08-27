#include "ocs/in_process_sim_backend.hpp"

#include <gtest/gtest.h>

#include <memory>

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

std::unique_ptr<ocs::OcsDeviceApi> makeBackend() {
    return std::make_unique<ocs::InProcessSimBackend>(defaultDeviceInfo());
}

TEST(InProcessSimBackendTest, ReportsConfiguredDevice) {
    const auto backend = makeBackend();

    const auto info = backend->getDeviceInfo();

    EXPECT_EQ(info.name, "ocs0");
    EXPECT_EQ(info.input_port_count, 16);
    EXPECT_EQ(info.output_port_count, 16);
    EXPECT_EQ(backend->getHealth().status, ocs::DeviceOperStatus::kReady);
}

TEST(InProcessSimBackendTest, CreatesQueriesAndDeletesConnection) {
    auto backend = makeBackend();
    const ocs::ConnectionCommand create{
        .operation = ocs::ConnectionOperation::kUpsert,
        .id = "conn-001",
        .input_port = 3,
        .output_port = 11,
        .desired_version = 7,
    };

    const auto create_result = backend->applyConnections({create}, {});

    ASSERT_TRUE(create_result.ok());
    ASSERT_EQ(create_result.connections.size(), 1);
    EXPECT_EQ(create_result.connections.front().id, "conn-001");
    EXPECT_EQ(backend->getConnections(), create_result.connections);

    const ocs::ConnectionCommand remove{
        .operation = ocs::ConnectionOperation::kRemove,
        .id = "conn-001",
    };
    const auto remove_result = backend->applyConnections({remove}, {});

    EXPECT_TRUE(remove_result.ok());
    EXPECT_TRUE(backend->getConnections().empty());
}

TEST(InProcessSimBackendTest, RejectsPortsOutsideMatrix) {
    auto backend = makeBackend();
    const ocs::ConnectionCommand invalid_input{
        .id = "bad-input",
        .input_port = 0,
        .output_port = 1,
        .desired_version = 1,
    };
    const ocs::ConnectionCommand invalid_output{
        .id = "bad-output",
        .input_port = 1,
        .output_port = 17,
        .desired_version = 1,
    };

    EXPECT_EQ(
        backend->applyConnections({invalid_input}, {}).error.code,
        ocs::ErrorCode::kInvalidPort);
    EXPECT_EQ(
        backend->applyConnections({invalid_output}, {}).error.code,
        ocs::ErrorCode::kInvalidPort);
    EXPECT_TRUE(backend->getConnections().empty());
}

TEST(InProcessSimBackendTest, HardResetClearsMatrixAndAdvancesGeneration) {
    auto backend = makeBackend();
    ASSERT_TRUE(
        backend
            ->applyConnections(
                {{
                    .id = "conn-001",
                    .input_port = 1,
                    .output_port = 9,
                    .desired_version = 1,
                }},
                {})
            .ok());

    const auto result = backend->reset(ocs::ResetMode::kHard);

    EXPECT_TRUE(result.error.ok());
    EXPECT_EQ(result.generation, 2);
    EXPECT_TRUE(backend->getConnections().empty());
}

TEST(InProcessSimBackendTest, AppliesValidBatchAtomically) {
    auto backend = makeBackend();
    const std::vector<ocs::ConnectionCommand> commands{
        {.id = "conn-001", .input_port = 1, .output_port = 9, .desired_version = 1},
        {.id = "conn-002", .input_port = 2, .output_port = 10, .desired_version = 1},
    };

    const auto result = backend->applyConnections(commands, {});

    ASSERT_TRUE(result.ok());
    EXPECT_EQ(backend->getConnections().size(), 2);
}

TEST(InProcessSimBackendTest, RejectsBatchInputConflictWithoutPartialApply) {
    auto backend = makeBackend();
    const std::vector<ocs::ConnectionCommand> commands{
        {.id = "conn-001", .input_port = 1, .output_port = 9, .desired_version = 1},
        {.id = "conn-002", .input_port = 1, .output_port = 10, .desired_version = 1},
    };

    const auto result = backend->applyConnections(commands, {});

    EXPECT_EQ(result.error.code, ocs::ErrorCode::kInputConflict);
    EXPECT_TRUE(backend->getConnections().empty());
}

TEST(InProcessSimBackendTest, RejectsBatchOutputConflictWithoutPartialApply) {
    auto backend = makeBackend();
    const std::vector<ocs::ConnectionCommand> commands{
        {.id = "conn-001", .input_port = 1, .output_port = 9, .desired_version = 1},
        {.id = "conn-002", .input_port = 2, .output_port = 9, .desired_version = 1},
    };

    const auto result = backend->applyConnections(commands, {});

    EXPECT_EQ(result.error.code, ocs::ErrorCode::kOutputConflict);
    EXPECT_TRUE(backend->getConnections().empty());
}

TEST(InProcessSimBackendTest, PreservesOldMatrixWhenInjectedApplyFails) {
    auto backend = makeBackend();
    ASSERT_TRUE(
        backend
            ->applyConnections(
                {{.id = "existing", .input_port = 1, .output_port = 9, .desired_version = 1}},
                {})
            .ok());
    ASSERT_TRUE(backend->injectFault({.type = ocs::FaultType::kNextApplyError}).error.ok());

    const auto result = backend->applyConnections(
        {{.id = "new", .input_port = 2, .output_port = 10, .desired_version = 1}}, {});

    EXPECT_EQ(result.error.code, ocs::ErrorCode::kApplyFailed);
    const auto actual = backend->getConnections();
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual.front().id, "existing");
}

TEST(InProcessSimBackendTest, RejectsDownAndDisabledPorts) {
    auto device = std::make_shared<ocs::SimulatedOcsDevice>(defaultDeviceInfo());
    ocs::InProcessSimBackend backend(device);
    ASSERT_TRUE(
        backend.injectFault({.type = ocs::FaultType::kInputPortDown, .port_id = 3}).error.ok());

    const auto down_result = backend.applyConnections(
        {{.id = "down", .input_port = 3, .output_port = 11, .desired_version = 1}}, {});
    EXPECT_EQ(down_result.error.code, ocs::ErrorCode::kPortDown);
    EXPECT_EQ(backend.getInputPortState(3).oper_status, ocs::PortOperStatus::kDown);

    ASSERT_TRUE(device->setPortAdminState(ocs::PortDirection::kOutput, 12, false).ok());
    const auto disabled_result = backend.applyConnections(
        {{.id = "disabled", .input_port = 4, .output_port = 12, .desired_version = 1}}, {});
    EXPECT_EQ(disabled_result.error.code, ocs::ErrorCode::kPortDisabled);
    EXPECT_TRUE(backend.getConnections().empty());
}

TEST(InProcessSimBackendTest, UpsertMovesExistingConnectionAndReleasesOldPorts) {
    auto backend = makeBackend();
    ASSERT_TRUE(
        backend
            ->applyConnections(
                {{.id = "conn-001", .input_port = 1, .output_port = 9, .desired_version = 1}},
                {})
            .ok());

    const auto update = backend->applyConnections(
        {{.id = "conn-001", .input_port = 2, .output_port = 10, .desired_version = 2},
         {.id = "conn-002", .input_port = 1, .output_port = 9, .desired_version = 1}},
        {});

    ASSERT_TRUE(update.ok());
    const auto actual = backend->getConnections();
    ASSERT_EQ(actual.size(), 2);
    EXPECT_EQ(actual.at(0).id, "conn-001");
    EXPECT_EQ(actual.at(0).applied_version, 2);
    EXPECT_EQ(actual.at(1).id, "conn-002");
}

TEST(InProcessSimBackendTest, AtomicallySwapsOutputsWithoutCommandOrderDependency) {
    auto backend = makeBackend();
    ASSERT_TRUE(
        backend
            ->applyConnections(
                {
                    {.id = "conn-001", .input_port = 1, .output_port = 9, .desired_version = 1},
                    {.id = "conn-002", .input_port = 2, .output_port = 10, .desired_version = 1},
                },
                {})
            .ok());

    const auto result = backend->applyConnections(
        {
            {.id = "conn-001", .input_port = 1, .output_port = 10, .desired_version = 2},
            {.id = "conn-002", .input_port = 2, .output_port = 9, .desired_version = 2},
        },
        {});

    ASSERT_TRUE(result.ok());
    const auto actual = backend->getConnections();
    ASSERT_EQ(actual.size(), 2);
    EXPECT_EQ(actual.at(0).output_port, 10);
    EXPECT_EQ(actual.at(1).output_port, 9);
}

TEST(InProcessSimBackendTest, RemovingAbsentConnectionIsIdempotent) {
    auto backend = makeBackend();

    const auto result = backend->applyConnections(
        {{.operation = ocs::ConnectionOperation::kRemove,
          .id = "already-absent",
          .desired_version = 2}},
        {});

    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(backend->getConnections().empty());
}

}  // namespace
