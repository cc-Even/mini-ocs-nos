#include "ocs/version.hpp"

#include <gtest/gtest.h>

namespace {

TEST(ProjectVersionTest, ReturnsSemanticVersion) {
    EXPECT_EQ(ocs::projectVersion(), "0.1.0");
}

}  // namespace
