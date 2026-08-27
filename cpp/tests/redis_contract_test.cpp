#include "ocs/redis_keys.hpp"
#include "ocs/redis_repository.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <map>
#include <string>

namespace {

class RedisContractTest : public testing::Test {
protected:
    void SetUp() override {
        const char* socket = std::getenv("OCS_REDIS_SOCKET");
        if (socket == nullptr) {
            GTEST_SKIP() << "set OCS_REDIS_SOCKET to run Redis integration tests";
        }
        endpoint_.unix_socket = socket;

        config_ = std::make_unique<ocs::redis::RedisRepository>(
            endpoint_, ocs::redis::LogicalDb::kConfig);
        state_ =
            std::make_unique<ocs::redis::RedisRepository>(endpoint_, ocs::redis::LogicalDb::kState);
        config_->ping();
        state_->ping();
        config_->flushForTest();
        state_->flushForTest();
    }

    ocs::redis::RedisEndpoint endpoint_;
    std::unique_ptr<ocs::redis::RedisRepository> config_;
    std::unique_ptr<ocs::redis::RedisRepository> state_;
};

TEST_F(RedisContractTest, SeparatesSnapshotsByLogicalDatabase) {
    const auto key = ocs::redis::connectionConfigKey("ocs0", "conn-001");
    const std::map<std::string, std::string> desired{
        {"input_port", "3"},
        {"output_port", "11"},
        {"desired_version", "7"},
    };

    config_->putHash(key, desired);

    EXPECT_EQ(config_->getHash(key), desired);
    EXPECT_TRUE(state_->getHash(key).empty());
    EXPECT_TRUE(config_->deleteKey(key));
    EXPECT_TRUE(config_->getHash(key).empty());
}

TEST_F(RedisContractTest, DeliversVersionedEventThroughConsumerGroupAndAck) {
    const ocs::redis::EventEnvelope expected{
        .event_schema_version = 1,
        .event_id = "event-001",
        .request_id = "request-001",
        .timestamp_ns = 1780000000000000000ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-001",
        .operation = "UPSERT",
        .desired_version = 7,
        .payload = R"({"input_port":3,"output_port":11})",
    };
    const std::string stream(ocs::redis::kConfigEvents);
    config_->createConsumerGroup(stream, "ocs-orch-test");

    const auto event_id = config_->appendEvent(stream, expected);
    const auto messages = config_->readGroup(stream, "ocs-orch-test", "consumer-1");

    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages.front().id, event_id);
    EXPECT_EQ(messages.front().event, expected);
    EXPECT_EQ(config_->pendingCount(stream, "ocs-orch-test"), 1);
    EXPECT_EQ(config_->acknowledge(stream, "ocs-orch-test", event_id), 1);
    EXPECT_EQ(config_->pendingCount(stream, "ocs-orch-test"), 0);
}

TEST_F(RedisContractTest, ConsumerGroupCreationIsIdempotent) {
    const std::string stream(ocs::redis::kConfigEvents);

    EXPECT_NO_THROW(config_->createConsumerGroup(stream, "ocs-orch-test"));
    EXPECT_NO_THROW(config_->createConsumerGroup(stream, "ocs-orch-test"));
}

TEST_F(RedisContractTest, RejectsUnsupportedEventSchemaBeforeAppend) {
    const ocs::redis::EventEnvelope invalid{
        .event_schema_version = 2,
        .event_id = "event-001",
        .request_id = "request-001",
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-001",
        .operation = "UPSERT",
        .payload = "{}",
    };

    EXPECT_THROW(
        static_cast<void>(
            config_->appendEvent(std::string(ocs::redis::kConfigEvents), invalid)),
        std::invalid_argument);
}

}  // namespace
