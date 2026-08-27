#include "ocs/redis_repository.hpp"

#include <sw/redis++/redis++.h>
#include <algorithm>
#include <charconv>
#include <iterator>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace ocs::redis {
namespace {

using FieldMap = std::unordered_map<std::string, std::string>;
using StreamItem = std::pair<std::string, FieldMap>;
using StreamResult = std::unordered_map<std::string, std::vector<StreamItem>>;
inline constexpr int kMaxWatchRetries = 8;
inline constexpr std::string_view kReadGroupIfNoPendingScript = R"(
local pending = redis.call('XPENDING', KEYS[1], ARGV[1])
if pending[1] ~= 0 then
    return {}
end
local messages = redis.call(
    'XREADGROUP', 'GROUP', ARGV[1], ARGV[2], 'COUNT', ARGV[3],
    'STREAMS', KEYS[1], '>')
if not messages then
    return {}
end
return messages
)";

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

RedisEndpoint parseRedisEndpoint(std::string_view target) {
    constexpr std::string_view tcp_prefix = "tcp://";
    constexpr std::string_view unix_prefix = "unix://";
    if (target.starts_with(tcp_prefix)) {
        const auto address = target.substr(tcp_prefix.size());
        const auto separator = address.rfind(':');
        if (separator == std::string_view::npos || separator == 0 ||
            separator + 1 == address.size()) {
            throw std::invalid_argument("Redis TCP endpoint must use tcp://HOST:PORT");
        }
        int port = 0;
        const auto port_text = address.substr(separator + 1);
        const auto [end, error] =
            std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
        if (error != std::errc{} || end != port_text.data() + port_text.size() ||
            port < 1 || port > 65535) {
            throw std::invalid_argument("Redis TCP endpoint port is outside 1..65535");
        }
        return {
            .host = std::string(address.substr(0, separator)),
            .port = port,
            .unix_socket = "",
        };
    }
    if (target.starts_with(unix_prefix)) {
        target.remove_prefix(unix_prefix.size());
    }
    if (target.empty() || target.front() != '/') {
        throw std::invalid_argument(
            "Redis endpoint must be an absolute path, unix:///PATH, or tcp://HOST:PORT");
    }
    return {.unix_socket = std::string(target)};
}

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

bool RedisRepository::putHashIfAbsent(
    const std::string& key,
    const std::map<std::string, std::string>& fields) {
    if (fields.empty()) {
        throw std::invalid_argument("conditional Redis hash fields must not be empty");
    }
    for (int attempt = 0; attempt < kMaxWatchRetries; ++attempt) {
        auto transaction = impl_->redis.transaction();
        auto watched = transaction.redis();
        watched.watch(key);
        if (watched.exists(key) != 0) {
            watched.unwatch();
            return false;
        }
        transaction.hmset(key, fields.begin(), fields.end());
        try {
            static_cast<void>(transaction.exec());
            return true;
        } catch (const sw::redis::WatchError&) {
            continue;
        }
    }
    throw std::runtime_error("Redis conditional hash write exceeded retry limit");
}

void RedisRepository::ensureHashFields(
    const std::string& key,
    const std::map<std::string, std::string>& fields) {
    if (fields.empty()) {
        return;
    }
    auto transaction = impl_->redis.transaction();
    for (const auto& [field, value] : fields) {
        transaction.hsetnx(key, field, value);
    }
    static_cast<void>(transaction.exec());
}

std::map<std::string, std::string> RedisRepository::getHash(const std::string& key) {
    std::map<std::string, std::string> result;
    impl_->redis.hgetall(key, std::inserter(result, result.end()));
    return result;
}

std::vector<std::string> RedisRepository::scanKeys(
    const std::string& pattern,
    std::size_t max_results) {
    if (pattern.empty() || max_results == 0) {
        throw std::invalid_argument("Redis key scan requires a pattern and positive bound");
    }
    sw::redis::Cursor cursor = 0;
    std::vector<std::string> result;
    std::unordered_set<std::string> seen;
    std::size_t page_count = 0;
    do {
        if (++page_count > 4096) {
            throw std::runtime_error("Redis key scan exceeded page bound");
        }
        std::vector<std::string> page;
        cursor = impl_->redis.scan(cursor, pattern, 128, std::back_inserter(page));
        for (auto& key : page) {
            if (!seen.insert(key).second) {
                continue;
            }
            if (result.size() == max_results) {
                throw std::runtime_error("Redis key scan exceeded result bound");
            }
            result.push_back(std::move(key));
        }
    } while (cursor != 0);
    std::ranges::sort(result);
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

bool RedisRepository::replaceHashAndAppendEventOnce(
    const std::string& marker_key,
    const std::map<std::string, std::string>& marker_fields,
    const std::string& key,
    const std::map<std::string, std::string>& fields,
    const std::string& stream,
    const EventEnvelope& event) {
    validateEvent(event);
    const auto event_fields = eventFields(event);
    for (int attempt = 0; attempt < kMaxWatchRetries; ++attempt) {
        auto transaction = impl_->redis.transaction();
        auto watched = transaction.redis();
        watched.watch(marker_key);
        if (watched.exists(marker_key) != 0) {
            watched.unwatch();
            return false;
        }
        transaction.del(key);
        if (!fields.empty()) {
            transaction.hmset(key, fields.begin(), fields.end());
        }
        transaction.xadd(stream, "*", event_fields.begin(), event_fields.end());
        transaction.hmset(marker_key, marker_fields.begin(), marker_fields.end());
        try {
            static_cast<void>(transaction.exec());
            return true;
        } catch (const sw::redis::WatchError&) {
            continue;
        }
    }
    throw std::runtime_error("Redis state publication exceeded retry limit");
}

bool RedisRepository::appendEventOnce(
    const std::string& marker_key,
    const std::map<std::string, std::string>& marker_fields,
    const std::string& stream,
    const EventEnvelope& event) {
    validateEvent(event);
    const auto event_fields = eventFields(event);
    for (int attempt = 0; attempt < kMaxWatchRetries; ++attempt) {
        auto transaction = impl_->redis.transaction();
        auto watched = transaction.redis();
        watched.watch(marker_key);
        if (watched.exists(marker_key) != 0) {
            watched.unwatch();
            return false;
        }
        transaction.xadd(stream, "*", event_fields.begin(), event_fields.end());
        transaction.hmset(marker_key, marker_fields.begin(), marker_fields.end());
        try {
            static_cast<void>(transaction.exec());
            return true;
        } catch (const sw::redis::WatchError&) {
            continue;
        }
    }
    throw std::runtime_error("Redis event publication exceeded retry limit");
}

bool RedisRepository::incrementHashFieldsOnce(
    const std::string& marker_key,
    const std::map<std::string, std::string>& marker_fields,
    const std::string& key,
    const std::map<std::string, long long>& increments) {
    for (int attempt = 0; attempt < kMaxWatchRetries; ++attempt) {
        auto transaction = impl_->redis.transaction();
        auto watched = transaction.redis();
        watched.watch(marker_key);
        if (watched.exists(marker_key) != 0) {
            watched.unwatch();
            return false;
        }
        for (const auto& [field, increment] : increments) {
            if (increment != 0) {
                transaction.hincrby(key, field, increment);
            }
        }
        transaction.hmset(marker_key, marker_fields.begin(), marker_fields.end());
        try {
            static_cast<void>(transaction.exec());
            return true;
        } catch (const sw::redis::WatchError&) {
            continue;
        }
    }
    throw std::runtime_error("Redis counter publication exceeded retry limit");
}

bool RedisRepository::recordApplyCountersOnce(
    const std::string& marker_key,
    const std::map<std::string, std::string>& marker_fields,
    const std::string& key,
    const std::map<std::string, long long>& increments,
    long long latency_ms) {
    if (latency_ms < 0) {
        throw std::invalid_argument("device apply latency must not be negative");
    }
    for (int attempt = 0; attempt < kMaxWatchRetries; ++attempt) {
        auto transaction = impl_->redis.transaction();
        auto watched = transaction.redis();
        watched.watch(marker_key);
        watched.watch(key);
        if (watched.exists(marker_key) != 0) {
            watched.unwatch();
            return false;
        }
        const auto existing_max = watched.hget(key, "max_apply_latency_ms");
        const auto maximum = existing_max
                                 ? std::max(latency_ms, std::stoll(*existing_max))
                                 : latency_ms;
        for (const auto& [field, increment] : increments) {
            if (increment != 0) {
                transaction.hincrby(key, field, increment);
            }
        }
        const std::map<std::string, std::string> latency_fields{
            {"last_apply_latency_ms", std::to_string(latency_ms)},
            {"max_apply_latency_ms", std::to_string(maximum)},
        };
        transaction.hmset(key, latency_fields.begin(), latency_fields.end());
        transaction.hmset(marker_key, marker_fields.begin(), marker_fields.end());
        try {
            static_cast<void>(transaction.exec());
            return true;
        } catch (const sw::redis::WatchError&) {
            continue;
        }
    }
    throw std::runtime_error("Redis apply counter publication exceeded retry limit");
}

bool RedisRepository::putVersionedHashesAtomicallyIfMarkerAbsent(
    const std::string& marker_key,
    const std::map<std::string, std::string>& marker_fields,
    const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& hashes) {
    const auto marker_version = marker_fields.find("desired_version");
    if (marker_version == marker_fields.end()) {
        throw std::invalid_argument("versioned Redis hash marker is missing desired_version");
    }
    const auto desired_version = std::stoull(marker_version->second);
    for (int attempt = 0; attempt < kMaxWatchRetries; ++attempt) {
        auto transaction = impl_->redis.transaction();
        auto watched = transaction.redis();
        watched.watch(marker_key);
        for (const auto& [key, fields] : hashes) {
            static_cast<void>(fields);
            watched.watch(key);
        }
        if (watched.exists(marker_key) != 0) {
            watched.unwatch();
            return false;
        }
        std::vector<bool> publish;
        publish.reserve(hashes.size());
        for (const auto& [key, fields] : hashes) {
            const auto fields_version = fields.find("desired_version");
            if (fields_version != fields.end() &&
                std::stoull(fields_version->second) != desired_version) {
                watched.unwatch();
                throw std::invalid_argument(
                    "versioned Redis hash does not match marker desired_version");
            }
            const auto existing_version = watched.hget(key, "desired_version");
            publish.push_back(
                !existing_version || std::stoull(*existing_version) <= desired_version);
        }
        for (std::size_t index = 0; index < hashes.size(); ++index) {
            if (!publish[index]) {
                continue;
            }
            const auto& [key, fields] = hashes[index];
            transaction.del(key);
            if (!fields.empty()) {
                transaction.hmset(key, fields.begin(), fields.end());
            }
        }
        transaction.hmset(marker_key, marker_fields.begin(), marker_fields.end());
        try {
            static_cast<void>(transaction.exec());
            return true;
        } catch (const sw::redis::WatchError&) {
            continue;
        }
    }
    throw std::runtime_error("Redis versioned hash publication exceeded retry limit");
}

void RedisRepository::putHashesAtomically(
    const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& hashes) {
    auto transaction = impl_->redis.transaction();
    for (const auto& [key, fields] : hashes) {
        transaction.del(key);
        if (!fields.empty()) {
            transaction.hmset(key, fields.begin(), fields.end());
        }
    }
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

std::vector<StreamMessage> RedisRepository::readGroupIfNoPending(
    const std::string& stream,
    const std::string& group,
    const std::string& consumer,
    std::size_t count) {
    if (count == 0) {
        throw std::invalid_argument("serialized stream read count must be positive");
    }
    const auto raw = impl_->redis.eval<StreamResult>(
        std::string(kReadGroupIfNoPendingScript),
        {stream},
        {group, consumer, std::to_string(count)});
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

std::vector<StreamMessage> RedisRepository::claimPending(
    const std::string& stream,
    const std::string& group,
    const std::string& consumer,
    std::chrono::milliseconds min_idle_time,
    std::size_t count) {
    if (count == 0 || min_idle_time.count() < 0) {
        throw std::invalid_argument("pending claim count must be positive and idle non-negative");
    }
    using PendingEntry = std::tuple<std::string, std::string, long long, long long>;
    std::vector<PendingEntry> pending;
    const auto scan_count = static_cast<long long>(std::min<std::size_t>(100, count * 10));
    impl_->redis.xpending(
        stream,
        group,
        "-",
        "+",
        scan_count,
        std::back_inserter(pending));

    std::vector<std::string> ids;
    ids.reserve(count);
    for (const auto& [id, owner, idle_ms, deliveries] : pending) {
        static_cast<void>(owner);
        static_cast<void>(deliveries);
        if (idle_ms >= min_idle_time.count()) {
            ids.push_back(id);
            if (ids.size() == count) {
                break;
            }
        }
    }
    if (ids.empty()) {
        return {};
    }

    std::vector<StreamItem> claimed;
    impl_->redis.xclaim(
        stream,
        group,
        consumer,
        min_idle_time,
        ids.begin(),
        ids.end(),
        std::back_inserter(claimed));
    std::vector<StreamMessage> result;
    result.reserve(claimed.size());
    for (const auto& [id, fields] : claimed) {
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
    std::vector<std::pair<std::string, std::string>> consumers;
    const auto [count, first_id, last_id] =
        impl_->redis.xpending(stream, group, std::back_inserter(consumers));
    static_cast<void>(first_id);
    static_cast<void>(last_id);
    return count;
}

void RedisRepository::flushForTest() {
    impl_->redis.flushdb();
}

}  // namespace ocs::redis
