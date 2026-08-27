"""Deadline-bounded multi-database reads for the native gNMI tree."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, Final

from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from gnmi_server.errors import DeadlineExceededError, DependencyUnavailableError, NotFoundError
from gnmi_server.path_parser import NativePath, PathKind
from gnmi_server.proto import gnmi_pb2
from gnmi_server.redis_keys import (
    ALARM_DB,
    CONFIG_DB,
    COUNTERS_DB,
    STATE_DB,
    active_alarm_key,
    active_alarm_pattern,
    connection_config_key,
    connection_config_pattern,
    connection_state_key,
    connection_state_pattern,
    device_config_key,
    device_config_pattern,
    device_counters_key,
    device_counters_pattern,
    device_state_key,
    device_state_pattern,
    input_port_state_key,
    input_port_state_pattern,
    output_port_state_key,
    output_port_state_pattern,
)
from gnmi_server.redis_repository import RedisSettings, create_redis_client

_INTEGER_FIELDS: Final = {
    "active_alarms",
    "active_connections",
    "applied_version",
    "created_at_ns",
    "desired_version",
    "device_generation",
    "input_port",
    "input_port_count",
    "last_change_ns",
    "output_port",
    "output_port_count",
    "timestamp_ns",
    "updated_at_ns",
}
_FLOAT_FIELDS: Final = {
    "apply_latency_ms",
    "low_power_threshold_dbm",
    "optical_power_dbm",
}


def _native_fields(fields: Mapping[str, str]) -> dict[str, Any]:
    """Translate Redis strings into the documented JSON_IETF field representation."""

    result: dict[str, Any] = {}
    for redis_name, raw_value in sorted(fields.items()):
        json_name = redis_name.replace("_", "-")
        try:
            if redis_name in _INTEGER_FIELDS or redis_name.endswith("_total"):
                result[json_name] = int(raw_value)
            elif redis_name in _FLOAT_FIELDS:
                result[json_name] = float(raw_value)
            elif raw_value in {"true", "false"}:
                result[json_name] = raw_value == "true"
            else:
                result[json_name] = raw_value
        except ValueError as error:
            raise RuntimeError(f"invalid numeric Redis field {redis_name!r}") from error
    return result


def _include_config(request_type: int) -> bool:
    return request_type in {gnmi_pb2.GetRequest.ALL, gnmi_pb2.GetRequest.CONFIG}


def _include_state(request_type: int) -> bool:
    return request_type in {
        gnmi_pb2.GetRequest.ALL,
        gnmi_pb2.GetRequest.STATE,
        gnmi_pb2.GetRequest.OPERATIONAL,
    }


class RedisGetRepository:
    """Read stable native resources from their authoritative Redis databases."""

    def __init__(self, settings: RedisSettings | None = None) -> None:
        self.settings = settings or RedisSettings.from_environment()
        if (
            self.settings.connect_timeout_seconds <= 0
            or self.settings.socket_timeout_seconds <= 0
            or self.settings.transaction_timeout_seconds <= 0
        ):
            raise ValueError("Redis deadlines must be positive")
        self._config = create_redis_client(self.settings, CONFIG_DB)
        self._state = create_redis_client(self.settings, STATE_DB)
        self._counters = create_redis_client(self.settings, COUNTERS_DB)
        self._alarm = create_redis_client(self.settings, ALARM_DB)

    async def close(self) -> None:
        await asyncio.gather(
            self._config.aclose(),
            self._state.aclose(),
            self._counters.aclose(),
            self._alarm.aclose(),
        )

    async def read_many(
        self, paths: tuple[NativePath, ...], request_type: int
    ) -> tuple[dict[str, Any], ...]:
        try:
            async with asyncio.timeout(self.settings.transaction_timeout_seconds):
                return tuple([await self._read(path, request_type) for path in paths])
        except (TimeoutError, RedisTimeoutError) as error:
            raise DeadlineExceededError("Redis Get snapshot") from error
        except RedisError as error:
            raise DependencyUnavailableError("Redis Get snapshot") from error

    async def _read(self, path: NativePath, request_type: int) -> dict[str, Any]:
        if path.kind in {PathKind.ROOT, PathKind.DEVICES}:
            devices = [
                await self._device(device, request_type)
                for device in await self._device_names()
            ]
            return {"device": devices}

        assert path.device is not None
        device = path.device
        if path.kind is PathKind.DEVICE:
            if not await self._device_exists(device):
                raise NotFoundError(f"device {device!r}")
            return await self._device(device, request_type)
        if path.kind is PathKind.DEVICE_STATE:
            return await self._required_hash(
                self._state, device_state_key(device), f"device state {device!r}"
            )
        if path.kind is PathKind.CONNECTIONS:
            await self._require_device(device)
            return {"connection": await self._connections(device, request_type)}
        if path.kind in {
            PathKind.CONNECTION,
            PathKind.CONNECTION_CONFIG,
            PathKind.CONNECTION_STATE,
        }:
            assert path.connection_id is not None
            return await self._connection(device, path.connection_id, path.kind, request_type)
        if path.kind is PathKind.PORTS:
            await self._require_device(device)
            return {
                "input-port": await self._port_list(device, input_port_state_pattern(device)),
                "output-port": await self._port_list(device, output_port_state_pattern(device)),
            }
        if path.kind in {PathKind.INPUT_PORT, PathKind.INPUT_PORT_STATE}:
            assert path.port_id is not None
            return await self._required_port_hash(
                input_port_state_key(device, path.port_id),
                path.port_id,
                f"input port {device!r}/{path.port_id}",
            )
        if path.kind in {PathKind.OUTPUT_PORT, PathKind.OUTPUT_PORT_STATE}:
            assert path.port_id is not None
            return await self._required_port_hash(
                output_port_state_key(device, path.port_id),
                path.port_id,
                f"output port {device!r}/{path.port_id}",
            )
        if path.kind is PathKind.COUNTERS:
            return await self._required_hash(
                self._counters, device_counters_key(device), f"counters {device!r}"
            )
        if path.kind is PathKind.ALARMS:
            await self._require_device(device)
            return {"alarm": await self._alarm_list(device)}
        if path.kind is PathKind.ALARM:
            assert path.alarm_id is not None
            return await self._required_hash(
                self._alarm,
                active_alarm_key(device, path.alarm_id),
                f"alarm {device!r}/{path.alarm_id!r}",
            )
        raise AssertionError(f"unhandled path kind {path.kind}")

    async def _device(self, device: str, request_type: int) -> dict[str, Any]:
        result: dict[str, Any] = {"name": device}
        if _include_config(request_type):
            config = await self._config.hgetall(device_config_key(device))
            result["config"] = _native_fields(config)
        if _include_state(request_type):
            state = await self._state.hgetall(device_state_key(device))
            result["state"] = _native_fields(state)
        return result

    async def _connection(
        self, device: str, connection_id: str, kind: PathKind, request_type: int
    ) -> dict[str, Any]:
        config = await self._config.hgetall(connection_config_key(device, connection_id))
        state = await self._state.hgetall(connection_state_key(device, connection_id))
        if kind is PathKind.CONNECTION_CONFIG:
            if not config:
                raise NotFoundError(f"connection config {device!r}/{connection_id!r}")
            return _native_fields(config)
        if kind is PathKind.CONNECTION_STATE:
            if not state:
                raise NotFoundError(f"connection state {device!r}/{connection_id!r}")
            return _native_fields(state)
        selected_exists = (_include_config(request_type) and bool(config)) or (
            _include_state(request_type) and bool(state)
        )
        if not selected_exists:
            raise NotFoundError(f"connection {device!r}/{connection_id!r}")
        result: dict[str, Any] = {"id": connection_id}
        if _include_config(request_type) and config:
            result["config"] = _native_fields(config)
        if _include_state(request_type) and state:
            result["state"] = _native_fields(state)
        return result

    async def _connections(self, device: str, request_type: int) -> list[dict[str, Any]]:
        ids: set[str] = set()
        if _include_config(request_type):
            ids.update(await self._resource_ids(self._config, connection_config_pattern(device)))
        if _include_state(request_type):
            ids.update(await self._resource_ids(self._state, connection_state_pattern(device)))
        connections: list[dict[str, Any]] = []
        for connection_id in sorted(ids):
            try:
                connections.append(
                    await self._connection(
                        device, connection_id, PathKind.CONNECTION, request_type
                    )
                )
            except NotFoundError:
                # A concurrent delete after SCAN makes this entry absent, not an error
                # for the enclosing collection.
                continue
        return connections

    async def _device_names(self) -> list[str]:
        names: set[str] = set()
        for client, patterns in (
            (self._config, (device_config_pattern(), "OCS_CONNECTION|*|*")),
            (
                self._state,
                (
                    device_state_pattern(),
                    "OCS_CONNECTION_STATE|*|*",
                    "OCS_INPUT_PORT_STATE|*|*",
                    "OCS_OUTPUT_PORT_STATE|*|*",
                ),
            ),
            (self._counters, (device_counters_pattern(),)),
            (self._alarm, ("OCS_ACTIVE_ALARM|*|*",)),
        ):
            for pattern in patterns:
                async for key in client.scan_iter(match=pattern, count=100):
                    parts = key.split("|")
                    if len(parts) >= 2:
                        names.add(parts[1])
        return sorted(names)

    async def _device_exists(self, device: str) -> bool:
        if any(
            await asyncio.gather(
                self._config.exists(device_config_key(device)),
                self._state.exists(device_state_key(device)),
                self._counters.exists(device_counters_key(device)),
            )
        ):
            return True
        for client, pattern in (
            (self._config, connection_config_pattern(device)),
            (self._state, connection_state_pattern(device)),
            (self._state, input_port_state_pattern(device)),
            (self._state, output_port_state_pattern(device)),
            (self._alarm, active_alarm_pattern(device)),
        ):
            async for _ in client.scan_iter(match=pattern, count=1):
                return True
        return False

    async def _require_device(self, device: str) -> None:
        if not await self._device_exists(device):
            raise NotFoundError(f"device {device!r}")

    async def _required_hash(self, client, key: str, label: str) -> dict[str, Any]:
        fields = await client.hgetall(key)
        if not fields:
            raise NotFoundError(label)
        return _native_fields(fields)

    async def _resource_ids(self, client, pattern: str) -> set[str]:
        ids: set[str] = set()
        async for key in client.scan_iter(match=pattern, count=100):
            ids.add(key.rsplit("|", 1)[1])
        return ids

    async def _required_port_hash(self, key: str, port_id: int, label: str) -> dict[str, Any]:
        fields = await self._state.hgetall(key)
        if not fields:
            raise NotFoundError(label)
        payload = _native_fields(fields)
        payload["id"] = port_id
        return payload

    async def _port_list(self, device: str, pattern: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        async for key in self._state.scan_iter(match=pattern, count=100):
            fields = await self._state.hgetall(key)
            if not fields:
                continue
            payload = _native_fields(fields)
            payload["id"] = int(key.rsplit("|", 1)[1])
            result.append(payload)
        return sorted(result, key=lambda item: item["id"])

    async def _alarm_list(self, device: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        async for key in self._alarm.scan_iter(match=active_alarm_pattern(device), count=100):
            fields = await self._alarm.hgetall(key)
            if not fields:
                continue
            payload = _native_fields(fields)
            payload.setdefault("id", key.rsplit("|", 1)[1])
            result.append(payload)
        return sorted(result, key=lambda item: item["id"])
