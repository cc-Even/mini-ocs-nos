#include "ocs/redis_keys.hpp"
#include "ocs/redis_repository.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <map>
#include <string>
#include <thread>

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

TEST_F(RedisContractTest, ReplacesHashWithoutRetainingOldFields) {
    const auto key = ocs::redis::connectionConfigKey("ocs0", "conn-replace");
    config_->putHash(key, {{"desired_version", "1"}, {"obsolete", "value"}});

    const std::map<std::string, std::string> replacement{{"desired_version", "2"}};
    config_->putHash(key, replacement);

    EXPECT_EQ(config_->getHash(key), replacement);
}

TEST_F(RedisContractTest, InitializesAndRecordsApplyCountersWithoutDuplicateEffects) {
    ocs::redis::RedisRepository counters(endpoint_, ocs::redis::LogicalDb::kCounters);
    counters.flushForTest();
    const auto key = ocs::redis::deviceCountersKey("ocs0");
    counters.ensureHashFields(
        key,
        {
            {"device_apply_total", "0"},
            {"last_apply_latency_ms", "0"},
            {"max_apply_latency_ms", "0"},
        });

    EXPECT_TRUE(counters.recordApplyCountersOnce(
        "OCS_TEST_APPLY_COUNTERS|first",
        {{"command_id", "first"}},
        key,
        {{"device_apply_total", 1}},
        12));
    EXPECT_FALSE(counters.recordApplyCountersOnce(
        "OCS_TEST_APPLY_COUNTERS|first",
        {{"command_id", "first"}},
        key,
        {{"device_apply_total", 1}},
        99));
    EXPECT_TRUE(counters.recordApplyCountersOnce(
        "OCS_TEST_APPLY_COUNTERS|second",
        {{"command_id", "second"}},
        key,
        {{"device_apply_total", 1}},
        5));
    counters.ensureHashFields(
        key,
        {
            {"device_apply_total", "0"},
            {"last_apply_latency_ms", "0"},
            {"max_apply_latency_ms", "0"},
        });

    const auto values = counters.getHash(key);
    EXPECT_EQ(values.at("device_apply_total"), "2");
    EXPECT_EQ(values.at("last_apply_latency_ms"), "5");
    EXPECT_EQ(values.at("max_apply_latency_ms"), "12");
}

TEST_F(RedisContractTest, ScansMatchingKeysWithinExplicitResultBound) {
    const auto first = ocs::redis::connectionConfigKey("ocs0", "scan-a");
    const auto second = ocs::redis::connectionConfigKey("ocs0", "scan-b");
    config_->putHash(first, {{"desired_version", "1"}});
    config_->putHash(second, {{"desired_version", "1"}});
    config_->putHash(
        ocs::redis::connectionConfigKey("ocs1", "scan-other"),
        {{"desired_version", "1"}});

    EXPECT_EQ(
        config_->scanKeys("OCS_CONNECTION|ocs0|*", 2),
        (std::vector<std::string>{first, second}));
    EXPECT_THROW(
        static_cast<void>(config_->scanKeys("OCS_CONNECTION|ocs0|*", 1)),
        std::runtime_error);
}

TEST_F(RedisContractTest, VersionedOncePublicationCannotOverwriteNewerHash) {
    const auto key = ocs::redis::connectionConfigKey("ocs0", "conn-version-fence");
    const auto marker = "OCS_TEST_VERSIONED_MARKER";
    const std::map<std::string, std::string> newer{
        {"desired_version", "2"}, {"apply_status", "ACTIVE"}};
    config_->putHash(key, newer);

    EXPECT_TRUE(config_->putVersionedHashesAtomicallyIfMarkerAbsent(
        marker,
        {{"event_id", "older-event"}, {"desired_version", "1"}},
        {{key, {{"desired_version", "1"}, {"apply_status", "APPLYING"}}}}));

    EXPECT_EQ(config_->getHash(key), newer);
    EXPECT_EQ(config_->getHash(marker).at("desired_version"), "1");
    EXPECT_FALSE(config_->putVersionedHashesAtomicallyIfMarkerAbsent(
        marker,
        {{"event_id", "older-event"}, {"desired_version", "1"}},
        {{key, {{"desired_version", "1"}, {"apply_status", "APPLYING"}}}}));
}

TEST_F(RedisContractTest, NeverExposesIntermediateMissingHashDuringReplacement) {
    const auto key = ocs::redis::connectionConfigKey("ocs0", "conn-atomic-replace");
    config_->putHash(key, {{"desired_version", "0"}, {"marker", "initial"}});
    ocs::redis::RedisRepository observer(endpoint_, ocs::redis::LogicalDb::kConfig);
    std::barrier start(2);
    std::atomic<bool> writer_done{false};
    std::atomic<bool> observed_missing{false};
    std::jthread reader([&] {
        start.arrive_and_wait();
        while (!writer_done.load()) {
            if (observer.getHash(key).empty()) {
                observed_missing.store(true);
                return;
            }
        }
    });

    start.arrive_and_wait();
    for (int version = 1; version <= 2000; ++version) {
        config_->putHash(
            key,
            {{"desired_version", std::to_string(version)}, {"marker", "replacement"}});
    }
    writer_done.store(true);
    reader.join();

    EXPECT_FALSE(observed_missing.load());
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

TEST_F(RedisContractTest, ClaimsAnIdlePendingEntryForAnotherConsumer) {
    const ocs::redis::EventEnvelope expected{
        .event_schema_version = 1,
        .event_id = "event-claim-001",
        .request_id = "request-claim-001",
        .timestamp_ns = 1780000000000000001ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "conn-claim",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":1,"output_port":9})",
    };
    const std::string stream(ocs::redis::kConfigEvents);
    config_->createConsumerGroup(stream, "claim-test");
    const auto message_id = config_->appendEvent(stream, expected);
    ASSERT_EQ(config_->readGroup(stream, "claim-test", "failed-consumer").size(), 1);

    const auto claimed = config_->claimPending(
        stream, "claim-test", "recovery-consumer", std::chrono::milliseconds(0));

    ASSERT_EQ(claimed.size(), 1);
    EXPECT_EQ(claimed.front().id, message_id);
    EXPECT_EQ(claimed.front().event, expected);
    EXPECT_EQ(config_->pendingCount(stream, "claim-test"), 1);
    EXPECT_EQ(config_->acknowledge(stream, "claim-test", message_id), 1);
    EXPECT_EQ(config_->pendingCount(stream, "claim-test"), 0);
}

TEST_F(RedisContractTest, SerializesNewGroupReadsBeforeCreatingAnotherPendingEntry) {
    const std::string stream(ocs::redis::kConfigEvents);
    const std::string group = "serialized-read-test";
    config_->createConsumerGroup(stream, group);
    const ocs::redis::EventEnvelope first_event{
        .event_schema_version = 1,
        .event_id = "serialized-event-1",
        .request_id = "serialized-request-1",
        .timestamp_ns = 1780000000000000010ULL,
        .device = "ocs0",
        .resource_type = "connection",
        .resource_id = "serialized-1",
        .operation = "UPSERT",
        .desired_version = 1,
        .payload = R"({"input_port":1,"output_port":9})",
    };
    auto second_event = first_event;
    second_event.event_id = "serialized-event-2";
    second_event.request_id = "serialized-request-2";
    second_event.resource_id = "serialized-2";
    second_event.timestamp_ns += 1;
    static_cast<void>(config_->appendEvent(stream, first_event));
    static_cast<void>(config_->appendEvent(stream, second_event));

    ocs::redis::RedisRepository reader_a(endpoint_, ocs::redis::LogicalDb::kConfig);
    ocs::redis::RedisRepository reader_b(endpoint_, ocs::redis::LogicalDb::kConfig);
    std::barrier start(3);
    std::vector<ocs::redis::StreamMessage> messages_a;
    std::vector<ocs::redis::StreamMessage> messages_b;
    std::jthread thread_a([&] {
        start.arrive_and_wait();
        messages_a = reader_a.readGroupIfNoPending(stream, group, "serialized-a");
    });
    std::jthread thread_b([&] {
        start.arrive_and_wait();
        messages_b = reader_b.readGroupIfNoPending(stream, group, "serialized-b");
    });
    start.arrive_and_wait();
    thread_a.join();
    thread_b.join();

    ASSERT_EQ(messages_a.size() + messages_b.size(), 1);
    EXPECT_EQ(config_->pendingCount(stream, group), 1);
    const auto& first_message = messages_a.empty() ? messages_b.front() : messages_a.front();
    EXPECT_EQ(first_message.event.event_id, "serialized-event-1");
    EXPECT_EQ(config_->acknowledge(stream, group, first_message.id), 1);

    const auto remaining =
        config_->readGroupIfNoPending(stream, group, "serialized-after-ack");
    ASSERT_EQ(remaining.size(), 1);
    EXPECT_EQ(remaining.front().event.event_id, "serialized-event-2");
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
