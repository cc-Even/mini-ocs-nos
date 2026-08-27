#include "ocs/reconciler.hpp"

#include <gtest/gtest.h>

namespace {

TEST(ReconcilerTest, ReturnsNoWorkForMatchingSnapshots) {
    const auto plan = ocs::buildReconciliationPlan(
        {{.id = "a", .input_port = 1, .output_port = 9, .desired_version = 3}},
        {{.id = "a", .input_port = 1, .output_port = 9, .applied_version = 3}});

    EXPECT_TRUE(plan.converged());
    ASSERT_EQ(plan.full_snapshot.commands.size(), 1);
    EXPECT_EQ(plan.full_snapshot.commands.front().id, "a");
}

TEST(ReconcilerTest, BuildsFullSnapshotForMissingMismatchedAndUnexpectedActual) {
    const auto plan = ocs::buildReconciliationPlan(
        {
            {.id = "missing", .input_port = 1, .output_port = 9, .desired_version = 4},
            {.id = "moved", .input_port = 2, .output_port = 10, .desired_version = 5},
            {.id = "stable", .input_port = 3, .output_port = 11, .desired_version = 6},
        },
        {
            {.id = "moved", .input_port = 4, .output_port = 12, .applied_version = 4},
            {.id = "stable", .input_port = 3, .output_port = 11, .applied_version = 6},
            {.id = "unexpected", .input_port = 5, .output_port = 13, .applied_version = 2},
        });

    ASSERT_EQ(plan.drifts.size(), 3);
    EXPECT_EQ(plan.drifts.at(0).kind, ocs::ConnectionDriftKind::kMissing);
    EXPECT_EQ(plan.drifts.at(1).kind, ocs::ConnectionDriftKind::kMismatched);
    EXPECT_EQ(plan.drifts.at(2).kind, ocs::ConnectionDriftKind::kUnexpected);
    ASSERT_EQ(plan.full_snapshot.commands.size(), 4);
    EXPECT_EQ(plan.full_snapshot.commands.at(0).id, "missing");
    EXPECT_EQ(plan.full_snapshot.commands.at(1).id, "moved");
    EXPECT_EQ(plan.full_snapshot.commands.at(2).id, "stable");
    EXPECT_EQ(plan.full_snapshot.commands.at(3).operation, ocs::ConnectionOperation::kRemove);
    EXPECT_EQ(plan.full_snapshot.commands.at(3).id, "unexpected");
}

}  // namespace
