"""Deadline-bounded CONFIG_DB persistence for atomic gNMI Set requests."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

import redis.asyncio as redis_async
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError, WatchError
from redis.exceptions import TimeoutError as RedisTimeoutError

from gnmi_server.errors import (
    ConflictError,
    DeadlineExceededError,
    DependencyUnavailableError,
    NotFoundError,
)
from gnmi_server.redis_keys import (
    CONFIG_DB,
    CONFIG_EVENTS,
    config_revision_key,
    connection_config_key,
    connection_config_pattern,
    device_config_key,
    input_port_config_key,
    output_port_config_key,
)
from gnmi_server.set_transaction import (
    CandidateChange,
    ConnectionConfig,
    SetCommitResult,
    SetPlan,
    build_candidate,
)

EVENT_SCHEMA_VERSION: Final = 1


@dataclass(frozen=True)
class RedisSettings:
    host: str = "127.0.0.1"
    port: int = 6379
    unix_socket: str = ""
    connect_timeout_seconds: float = 1.0
    socket_timeout_seconds: float = 1.0
    transaction_timeout_seconds: float = 3.0
    max_watch_retries: int = 3

    @classmethod
    def from_environment(cls) -> RedisSettings:
        return cls(
            host=os.getenv("OCS_REDIS_HOST", "127.0.0.1"),
            port=int(os.getenv("OCS_REDIS_PORT", "6379")),
            unix_socket=os.getenv("OCS_REDIS_SOCKET", ""),
            connect_timeout_seconds=float(os.getenv("OCS_REDIS_CONNECT_TIMEOUT_SECONDS", "1")),
            socket_timeout_seconds=float(os.getenv("OCS_REDIS_SOCKET_TIMEOUT_SECONDS", "1")),
            transaction_timeout_seconds=float(
                os.getenv("OCS_REDIS_TRANSACTION_TIMEOUT_SECONDS", "3")
            ),
            max_watch_retries=int(os.getenv("OCS_REDIS_MAX_WATCH_RETRIES", "3")),
        )


def create_redis_client(settings: RedisSettings, database: int) -> redis_async.Redis:
    """Create a deadline-bounded client without implicit command retries."""

    connection_options = {
        "db": database,
        "decode_responses": True,
        "socket_connect_timeout": settings.connect_timeout_seconds,
        "socket_timeout": settings.socket_timeout_seconds,
        "retry": Retry(NoBackoff(), 0),
    }
    if settings.unix_socket:
        return redis_async.Redis(
            unix_socket_path=settings.unix_socket,
            **connection_options,
        )
    return redis_async.Redis(
        host=settings.host,
        port=settings.port,
        **connection_options,
    )


class RedisConfigRepository:
    """Persist a complete candidate and its reliable events in one MULTI/EXEC."""

    def __init__(
        self,
        settings: RedisSettings | None = None,
        *,
        timestamp_factory: Callable[[], int] = time.time_ns,
        event_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self.settings = settings or RedisSettings.from_environment()
        if self.settings.max_watch_retries <= 0:
            raise ValueError("max_watch_retries must be positive")
        if (
            self.settings.connect_timeout_seconds <= 0
            or self.settings.socket_timeout_seconds <= 0
            or self.settings.transaction_timeout_seconds <= 0
        ):
            raise ValueError("Redis deadlines must be positive")
        self._client = create_redis_client(self.settings, CONFIG_DB)
        self._timestamp_factory = timestamp_factory
        self._event_id_factory = event_id_factory

    async def close(self) -> None:
        await self._client.aclose()

    async def flush_for_test(self) -> None:
        await self._client.flushdb()

    async def commit(self, plan: SetPlan) -> SetCommitResult:
        try:
            async with asyncio.timeout(self.settings.transaction_timeout_seconds):
                for _ in range(self.settings.max_watch_retries):
                    try:
                        return await self._commit_once(plan)
                    except WatchError:
                        continue
        except (TimeoutError, RedisTimeoutError) as error:
            raise DeadlineExceededError("CONFIG_DB transaction") from error
        except RedisError as error:
            raise DependencyUnavailableError("CONFIG_DB") from error
        raise DependencyUnavailableError("CONFIG_DB changed during all transaction retries")

    async def _commit_once(self, plan: SetPlan) -> SetCommitResult:
        revision_key = config_revision_key(plan.device)
        device_key = device_config_key(plan.device)
        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.watch(revision_key, device_key)
            raw_revision = await pipe.get(revision_key)
            try:
                current_revision = int(raw_revision) if raw_revision is not None else 0
            except ValueError as error:
                raise RuntimeError("CONFIG_DB revision is not an integer") from error
            current = await self._load_connections(pipe, plan.device)
            input_count, output_count = await self._load_port_counts(
                pipe, plan.device, device_key
            )
            timestamp_ns = self._timestamp_factory()
            candidate = build_candidate(
                current,
                plan,
                revision=current_revision + 1,
                timestamp_ns=timestamp_ns,
                input_port_count=input_count,
                output_port_count=output_count,
            )
            await self._validate_ports_enabled(pipe, plan.device, candidate.connections)

            if not candidate.changes:
                await pipe.unwatch()
                return SetCommitResult(timestamp_ns=timestamp_ns, operations=plan.operations)

            pipe.multi()
            for change in candidate.changes:
                key = connection_config_key(plan.device, change.connection_id)
                pipe.delete(key)
                if change.config is not None:
                    pipe.hset(key, mapping=change.config.redis_fields(plan.device))
            pipe.set(revision_key, candidate.revision)
            pipe.xadd(
                CONFIG_EVENTS,
                self._event_fields(
                    plan,
                    candidate.revision,
                    candidate.timestamp_ns,
                    candidate.changes,
                ),
            )
            await pipe.execute()
            return SetCommitResult(timestamp_ns=timestamp_ns, operations=plan.operations)

    async def _load_connections(self, pipe, device: str) -> dict[str, ConnectionConfig]:
        prefix = connection_config_key(device, "")
        keys = await pipe.keys(connection_config_pattern(device))
        current: dict[str, ConnectionConfig] = {}
        for key in sorted(keys):
            if not key.startswith(prefix):
                continue
            connection_id = key[len(prefix) :]
            fields = await pipe.hgetall(key)
            if fields:
                current[connection_id] = ConnectionConfig.from_redis(connection_id, fields)
        return current

    async def _load_port_counts(
        self, pipe, device: str, device_key: str
    ) -> tuple[int, int]:
        fields = await pipe.hgetall(device_key)
        if not fields:
            raise NotFoundError(f"device {device!r}")
        configured_name = fields.get("name", device)
        if configured_name != device:
            raise RuntimeError("CONFIG_DB device name does not match its key")
        if fields.get("admin_status", "ENABLED") != "ENABLED":
            raise ConflictError(f"device {device!r} is administratively disabled")
        try:
            input_count = int(fields["input_port_count"])
            output_count = int(fields["output_port_count"])
        except KeyError as error:
            raise RuntimeError("CONFIG_DB device port count is missing") from error
        except ValueError as error:
            raise RuntimeError("CONFIG_DB device port count is not an integer") from error
        if input_count <= 0 or output_count <= 0:
            raise RuntimeError("CONFIG_DB device port counts must be positive")
        return input_count, output_count

    async def _validate_ports_enabled(
        self,
        pipe,
        device: str,
        connections: tuple[ConnectionConfig, ...],
    ) -> None:
        ports = {
            (connection.input_port, connection.output_port)
            for connection in connections
        }
        keys = [
            key
            for input_port, output_port in sorted(ports)
            for key in (
                input_port_config_key(device, input_port),
                output_port_config_key(device, output_port),
            )
        ]
        if keys:
            await pipe.watch(*keys)
        for input_port, output_port in sorted(ports):
            input_fields = await pipe.hgetall(input_port_config_key(device, input_port))
            output_fields = await pipe.hgetall(output_port_config_key(device, output_port))
            if input_fields.get("admin_status", "ENABLED") != "ENABLED":
                raise ConflictError(f"input port {input_port} is administratively disabled")
            if output_fields.get("admin_status", "ENABLED") != "ENABLED":
                raise ConflictError(f"output port {output_port} is administratively disabled")

    def _event_fields(
        self,
        plan: SetPlan,
        desired_version: int,
        timestamp_ns: int,
        changes: tuple[CandidateChange, ...],
    ) -> dict[str, str]:
        commands = []
        for change in changes:
            command: dict[str, int | str] = {
                "operation": change.operation,
                "id": change.connection_id,
                "desired_version": desired_version,
            }
            if change.config is not None:
                command["input_port"] = change.config.input_port
                command["output_port"] = change.config.output_port
            commands.append(command)
        return {
            "event_schema_version": str(EVENT_SCHEMA_VERSION),
            "event_id": str(self._event_id_factory()),
            "request_id": plan.request_id,
            "timestamp_ns": str(timestamp_ns),
            "device": plan.device,
            "resource_type": "connection-batch",
            "resource_id": plan.request_id,
            "operation": "APPLY_BATCH",
            "desired_version": str(desired_version),
            "payload": json.dumps(
                {"commands": commands, "atomic": True, "timeout_ms": 1000},
                separators=(",", ":"),
                sort_keys=True,
            ),
        }
