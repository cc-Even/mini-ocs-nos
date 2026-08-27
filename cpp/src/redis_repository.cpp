#include "ocs/redis_repository.hpp"

#include <sw/redis++/redis++.h>

#include <iterator>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <utility>

namespace ocs::redis {
namespace {

using FieldMap = std::unordered_map<std::string, std::string>;
using StreamItem = std::pair<std::string, FieldMap>;
using StreamResult = std::unordered_map<std::string, std::vector<StreamItem>>;

std::vector<std::pair<std::string, std::string>> eventFields(const EventEnvelope& event) {
    return {
        {"event_schema_version", std::to_string(event.event_schema_version)},
        {"event_id", event.event_id},
        {"request_id", event.request_id},
        {"timestamp_ns", std::to_string(event.timestamp_ns)},
        {"device", event.device},
        {"resource_type", event.resource_type},
        {"resource_id", event.resource_id},
        {"operation", event.operation},
        {"desired_version", std::to_string(event.desired_version)},
        {"payload", event.payload},
    };
}

EventEnvelope parseEvent(const FieldMap& fields) {
    const auto required = [&fields](const std::string& name) -> const std::string& {
        const auto value = fields.find(name);
        if (value == fields.end()) {
            throw std::runtime_error("Redis stream event is missing field: " + name);
        }
        return value->second;
    };
    return {
        .event_schema_version = static_cast<std::uint32_t>(std::stoul(required("event_schema_version"))),
        .event_id = required("event_id"),
        .request_id = required("request_id"),
        .timestamp_ns = std::stoull(required("timestamp_ns")),
        .device = required("device"),
        .resource_type = required("resource_type"),
        .resource_id = required("resource_id"),
        .operation = required("operation"),
        .desired_version = std::stoull(required("desired_version")),
        .payload = required("payload"),
    };
}

void validateEvent(const EventEnvelope& event) {
    if (event.event_schema_version != 1) {
        throw std::invalid_argument("unsupported Redis event schema version");
    }
    if (event.event_id.empty() || event.request_id.empty() || event.device.empty() ||
        event.resource_type.empty() || event.resource_id.empty() || event.operation.empty()) {
        throw std::invalid_argument("Redis event is missing a required envelope field");
    }
}

}  // namespace

class RedisRepository::Impl {
public:
    Impl(const RedisEndpoint& endpoint, LogicalDb database)
        : redis([&endpoint, database] {
              sw::redis::ConnectionOptions options;
              options.host = endpoint.host;
              options.port = endpoint.port;
              if (!endpoint.unix_socket.empty()) {
                  options.type = sw::redis::ConnectionType::UNIX;
                  options.path = endpoint.unix_socket;
              }
              options.db = static_cast<int>(database);
              options.connect_timeout = endpoint.connect_timeout;
              options.socket_timeout = endpoint.socket_timeout;
              return options;
          }()) {}

    sw::redis::Redis redis;
};

RedisRepository::RedisRepository(RedisEndpoint endpoint, LogicalDb database)
    : impl_(std::make_unique<Impl>(endpoint, database)) {}

RedisRepository::~RedisRepository() = default;
RedisRepository::RedisRepository(RedisRepository&&) noexcept = default;
RedisRepository& RedisRepository::operator=(RedisRepository&&) noexcept = default;

void RedisRepository::ping() {
    impl_->redis.ping();
}

void RedisRepository::putHash(
    const std::string& key,
    const std::map<std::string, std::string>& fields) {
    auto transaction = impl_->redis.transaction();
    transaction.del(key);
    if (!fields.empty()) {
        transaction.hmset(key, fields.begin(), fields.end());
    }
    static_cast<void>(transaction.exec());
}

std::map<std::string, std::string> RedisRepository::getHash(const std::string& key) {
    std::map<std::string, std::string> result;
    impl_->redis.hgetall(key, std::inserter(result, result.end()));
    return result;
}

long long RedisRepository::incrementHashField(
    const std::string& key,
    const std::string& field,
    long long increment) {
    return impl_->redis.hincrby(key, field, increment);
}

bool RedisRepository::deleteKey(const std::string& key) {
    return impl_->redis.del(key) == 1;
}

void RedisRepository::replaceHashAndAppendEvent(
    const std::string& key,
    const std::map<std::string, std::string>& fields,
    const std::string& stream,
    const EventEnvelope& event) {
    validateEvent(event);
    const auto event_fields = eventFields(event);
    auto transaction = impl_->redis.transaction();
    transaction.del(key);
    if (!fields.empty()) {
        transaction.hmset(key, fields.begin(), fields.end());
    }
    transaction.xadd(stream, "*", event_fields.begin(), event_fields.end());
    static_cast<void>(transaction.exec());
}

std::string RedisRepository::appendEvent(const std::string& stream, const EventEnvelope& event) {
    validateEvent(event);
    const auto fields = eventFields(event);
    return impl_->redis.xadd(stream, "*", fields.begin(), fields.end());
}

void RedisRepository::createConsumerGroup(
    const std::string& stream,
    const std::string& group,
    const std::string& start_id) {
    try {
        impl_->redis.xgroup_create(stream, group, start_id, true);
    } catch (const sw::redis::ReplyError& error) {
        if (std::string(error.what()).find("BUSYGROUP") == std::string::npos) {
            throw;
        }
    }
}

std::vector<StreamMessage> RedisRepository::readGroup(
    const std::string& stream,
    const std::string& group,
    const std::string& consumer,
    std::size_t count) {
    StreamResult raw;
    impl_->redis.xreadgroup(
        group,
        consumer,
        stream,
        ">",
        static_cast<long long>(count),
        std::inserter(raw, raw.end()));

    std::vector<StreamMessage> result;
    const auto messages = raw.find(stream);
    if (messages == raw.end()) {
        return result;
    }
    result.reserve(messages->second.size());
    for (const auto& [id, fields] : messages->second) {
        result.push_back({id, parseEvent(fields)});
    }
    return result;
}

long long RedisRepository::acknowledge(
    const std::string& stream,
    const std::string& group,
    const std::string& message_id) {
    return impl_->redis.xack(stream, group, message_id);
}

long long RedisRepository::pendingCount(const std::string& stream, const std::string& group) {
    using PendingEntry = std::tuple<std::string, std::string, long long, long long>;
    std::vector<PendingEntry> pending;
    impl_->redis.xpending(
        stream,
        group,
        "-",
        "+",
        std::numeric_limits<long long>::max(),
        std::back_inserter(pending));
    return static_cast<long long>(pending.size());
}

void RedisRepository::flushForTest() {
    impl_->redis.flushdb();
}

}  // namespace ocs::redis
