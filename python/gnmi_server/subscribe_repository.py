"""Redis snapshot and stream access for gNMI ON_CHANGE subscriptions."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from gnmi_server.errors import DeadlineExceededError, DependencyUnavailableError, NotFoundError
from gnmi_server.get_repository import RedisGetRepository
from gnmi_server.path_parser import NativePath
from gnmi_server.proto import gnmi_pb2
from gnmi_server.redis_keys import ALARM_DB, ALARM_EVENTS, STATE_DB, STATE_EVENTS
from gnmi_server.redis_repository import RedisSettings, create_redis_client


@dataclass(frozen=True)
class StreamEvent:
    stream: str
    message_id: str
    timestamp_ns: int
    device: str
    resource_type: str
    resource_id: str
    operation: str


class RedisSubscribeRepository:
    """Tail per-database streams while rereading authoritative snapshots."""

    def __init__(self, settings: RedisSettings | None = None) -> None:
        self.settings = settings or RedisSettings.from_environment()
        self._snapshots = RedisGetRepository(self.settings)
        self._clients = {
            STATE_EVENTS: create_redis_client(self.settings, STATE_DB),
            ALARM_EVENTS: create_redis_client(self.settings, ALARM_DB),
        }
        self._block_ms = max(
            1,
            min(250, int(self.settings.socket_timeout_seconds * 500)),
        )

    async def close(self) -> None:
        await asyncio.gather(
            self._snapshots.close(),
            *(client.aclose() for client in self._clients.values()),
        )

    async def snapshot(self, path: NativePath) -> dict[str, Any] | None:
        try:
            return (await self._snapshots.read_many((path,), gnmi_pb2.GetRequest.STATE))[0]
        except NotFoundError:
            return None

    async def cursors(self, streams: frozenset[str]) -> dict[str, str]:
        try:
            async with asyncio.timeout(self.settings.transaction_timeout_seconds):
                result: dict[str, str] = {}
                for stream in sorted(streams):
                    messages = await self._clients[stream].xrevrange(stream, count=1)
                    result[stream] = messages[0][0] if messages else "0-0"
                return result
        except (TimeoutError, RedisTimeoutError) as error:
            raise DeadlineExceededError("Redis subscription cursor") from error
        except RedisError as error:
            raise DependencyUnavailableError("Redis subscription cursor") from error

    async def next_events(self, cursors: dict[str, str]) -> tuple[StreamEvent, ...]:
        try:
            async with asyncio.timeout(self.settings.transaction_timeout_seconds):
                batches = await asyncio.gather(
                    *(
                        self._clients[stream].xread(
                            {stream: message_id},
                            count=100,
                            block=self._block_ms,
                        )
                        for stream, message_id in sorted(cursors.items())
                    )
                )
        except (TimeoutError, RedisTimeoutError) as error:
            raise DeadlineExceededError("Redis subscription read") from error
        except RedisError as error:
            raise DependencyUnavailableError("Redis subscription read") from error

        events: list[StreamEvent] = []
        for batch in batches:
            for stream, messages in batch:
                for message_id, fields in messages:
                    cursors[stream] = message_id
                    event = self._parse_event(stream, message_id, fields)
                    if event is not None:
                        events.append(event)
        return tuple(
            sorted(events, key=lambda event: (event.timestamp_ns, event.stream, event.message_id))
        )

    @staticmethod
    def _parse_event(
        stream: str, message_id: str, fields: dict[str, str]
    ) -> StreamEvent | None:
        required = {
            "event_schema_version",
            "event_id",
            "request_id",
            "timestamp_ns",
            "device",
            "resource_type",
            "resource_id",
            "operation",
            "desired_version",
            "payload",
        }
        if not required.issubset(fields) or fields["event_schema_version"] != "1":
            return None
        numeric = {"event_schema_version", "timestamp_ns", "desired_version"}
        if not all(fields[name] for name in required - numeric):
            return None
        try:
            timestamp_ns = int(fields["timestamp_ns"])
            desired_version = int(fields["desired_version"])
        except ValueError:
            return None
        if timestamp_ns < 0 or timestamp_ns > 2**63 - 1 or desired_version < 0:
            return None
        return StreamEvent(
            stream=stream,
            message_id=message_id,
            timestamp_ns=timestamp_ns,
            device=fields["device"],
            resource_type=fields["resource_type"],
            resource_id=fields["resource_id"],
            operation=fields["operation"],
        )
