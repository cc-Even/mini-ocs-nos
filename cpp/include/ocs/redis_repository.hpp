#pragma once

#include "ocs/redis_keys.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ocs::redis {

struct RedisEndpoint {
    std::string host{"127.0.0.1"};
    int port{6379};
    std::string unix_socket;
    std::chrono::milliseconds connect_timeout{1000};
    std::chrono::milliseconds socket_timeout{1000};
};

struct EventEnvelope {
    std::uint32_t event_schema_version{1};
    std::string event_id;
    std::string request_id;
    std::uint64_t timestamp_ns{};
    std::string device;
    std::string resource_type;
    std::string resource_id;
    std::string operation;
    std::uint64_t desired_version{};
    std::string payload;

    bool operator==(const EventEnvelope&) const = default;
};

struct StreamMessage {
    std::string id;
    EventEnvelope event;
};

class RedisRepository {
public:
    RedisRepository(RedisEndpoint endpoint, LogicalDb database);
    ~RedisRepository();

    RedisRepository(const RedisRepository&) = delete;
    RedisRepository& operator=(const RedisRepository&) = delete;
    RedisRepository(RedisRepository&&) noexcept;
    RedisRepository& operator=(RedisRepository&&) noexcept;

    void ping();
    void putHash(const std::string& key, const std::map<std::string, std::string>& fields);
    [[nodiscard]] bool putHashIfAbsent(
        const std::string& key,
        const std::map<std::string, std::string>& fields);
    [[nodiscard]] std::map<std::string, std::string> getHash(const std::string& key);
    [[nodiscard]] long long incrementHashField(
        const std::string& key,
        const std::string& field,
        long long increment = 1);
    [[nodiscard]] bool deleteKey(const std::string& key);
    void replaceHashAndAppendEvent(
        const std::string& key,
        const std::map<std::string, std::string>& fields,
        const std::string& stream,
        const EventEnvelope& event);
    [[nodiscard]] bool replaceHashAndAppendEventOnce(
        const std::string& marker_key,
        const std::map<std::string, std::string>& marker_fields,
        const std::string& key,
        const std::map<std::string, std::string>& fields,
        const std::string& stream,
        const EventEnvelope& event);
    [[nodiscard]] bool appendEventOnce(
        const std::string& marker_key,
        const std::map<std::string, std::string>& marker_fields,
        const std::string& stream,
        const EventEnvelope& event);
    [[nodiscard]] bool incrementHashFieldsOnce(
        const std::string& marker_key,
        const std::map<std::string, std::string>& marker_fields,
        const std::string& key,
        const std::map<std::string, long long>& increments);
    void putHashesAtomically(
        const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& hashes);

    [[nodiscard]] std::string appendEvent(const std::string& stream, const EventEnvelope& event);
    void createConsumerGroup(
        const std::string& stream,
        const std::string& group,
        const std::string& start_id = "0-0");
    [[nodiscard]] std::vector<StreamMessage> readGroup(
        const std::string& stream,
        const std::string& group,
        const std::string& consumer,
        std::size_t count = 1);
    [[nodiscard]] std::vector<StreamMessage> claimPending(
        const std::string& stream,
        const std::string& group,
        const std::string& consumer,
        std::chrono::milliseconds min_idle_time,
        std::size_t count = 1);
    [[nodiscard]] long long acknowledge(
        const std::string& stream,
        const std::string& group,
        const std::string& message_id);
    [[nodiscard]] long long pendingCount(const std::string& stream, const std::string& group);

    void flushForTest();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ocs::redis
