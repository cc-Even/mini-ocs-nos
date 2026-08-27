"""Simulation-only, gNMI-fronted fault command transaction."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, Protocol

from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from gnmi_server.errors import (
    DeadlineExceededError,
    DependencyUnavailableError,
    InvalidArgumentError,
    PermissionDeniedError,
)
from gnmi_server.path_parser import NativePath, PathKind, parse_path
from gnmi_server.proto import gnmi_pb2
from gnmi_server.redis_keys import DEVICE_DB, FAULT_COMMANDS, fault_result_key
from gnmi_server.redis_repository import RedisSettings, create_redis_client
from gnmi_server.set_transaction import SetCommitResult, SetOperation, SetOperationKind

EVENT_SCHEMA_VERSION: Final = 1
_FAULT_KINDS: Final = {PathKind.FAULTS, PathKind.FAULT, PathKind.FAULT_CONFIG}


@dataclass(frozen=True)
class FaultCommand:
    device: str
    fault_id: str
    fault_type: str
    port_id: int
    operation: str
    request_id: str
    response_path: gnmi_pb2.Path
    response_kind: SetOperationKind


class FaultCommandRepository(Protocol):
    async def execute(self, command: FaultCommand) -> int: ...

    async def close(self) -> None: ...


def _copy_path(path: gnmi_pb2.Path) -> gnmi_pb2.Path:
    copied = gnmi_pb2.Path()
    copied.CopyFrom(path)
    return copied


def _decode_inject_payload(value: gnmi_pb2.TypedValue) -> None:
    if value.WhichOneof("value") != "json_ietf_val":
        raise InvalidArgumentError("fault Set values must use TypedValue.json_ietf_val")
    if len(value.json_ietf_val) > 4096:
        raise InvalidArgumentError("fault JSON_IETF payload exceeds 4096 bytes")
    try:
        decoded = json.loads(value.json_ietf_val)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidArgumentError("fault Set value is not valid JSON_IETF") from error
    if decoded != {"operation": "INJECT"}:
        raise InvalidArgumentError('fault inject payload must be {"operation":"INJECT"}')


def _fault_parts(fault_id: str, *, allow_all: bool) -> tuple[str, int]:
    if fault_id == "all" and allow_all:
        return "ALL", 0
    names = {
        "next-apply-timeout": "NEXT_APPLY_TIMEOUT",
        "next-apply-error": "NEXT_APPLY_ERROR",
    }
    if fault_id in names:
        return names[fault_id], 0
    for prefix, fault_type in (
        ("input-port-down-", "INPUT_PORT_DOWN"),
        ("output-port-down-", "OUTPUT_PORT_DOWN"),
    ):
        if fault_id.startswith(prefix):
            raw_port = fault_id.removeprefix(prefix)
            if not raw_port.isascii() or not raw_port.isdecimal():
                break
            port_id = int(raw_port)
            if 0 < port_id <= 2**31 - 1:
                return fault_type, port_id
            break
    raise InvalidArgumentError(f"unsupported simulator fault {fault_id!r}")


def _all_paths(request: gnmi_pb2.SetRequest) -> list[gnmi_pb2.Path]:
    return [
        *request.delete,
        *(update.path for update in request.replace),
        *(update.path for update in request.update),
        *(update.path for update in request.union_replace),
    ]


def is_fault_request(request: gnmi_pb2.SetRequest) -> bool:
    """Return true when any valid operation path targets the fault subtree."""

    for path in _all_paths(request):
        try:
            if parse_path(path, prefix=request.prefix).kind in _FAULT_KINDS:
                return True
        except Exception:
            continue
    return False


def parse_fault_request(
    request: gnmi_pb2.SetRequest,
    *,
    request_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
) -> FaultCommand:
    if request.union_replace or request.replace:
        raise InvalidArgumentError("fault Set does not support replace or union_replace")
    if len(request.delete) + len(request.update) != 1:
        raise InvalidArgumentError("fault Set requires exactly one operation")

    request_id = str(request_id_factory())
    if request.update:
        update = request.update[0]
        native = parse_path(update.path, prefix=request.prefix)
        if native.kind is not PathKind.FAULT_CONFIG or native.fault_id is None:
            raise InvalidArgumentError("fault inject requires a keyed fault config path")
        _decode_inject_payload(update.val)
        fault_type, port_id = _fault_parts(native.fault_id, allow_all=False)
        return FaultCommand(
            device=str(native.device),
            fault_id=native.fault_id,
            fault_type=fault_type,
            port_id=port_id,
            operation="INJECT",
            request_id=request_id,
            response_path=_copy_path(update.path),
            response_kind=SetOperationKind.UPDATE,
        )

    path = request.delete[0]
    native = parse_path(path, prefix=request.prefix)
    if native.kind is PathKind.FAULTS:
        fault_id = "all"
    elif native.kind in {PathKind.FAULT, PathKind.FAULT_CONFIG} and native.fault_id:
        fault_id = native.fault_id
    else:
        raise InvalidArgumentError("fault clear requires the fault container or keyed fault path")
    fault_type, port_id = _fault_parts(fault_id, allow_all=True)
    return FaultCommand(
        device=str(native.device),
        fault_id=fault_id,
        fault_type=fault_type,
        port_id=port_id,
        operation="CLEAR",
        request_id=request_id,
        response_path=_copy_path(path),
        response_kind=SetOperationKind.DELETE,
    )


class RedisFaultRepository:
    """Append one reliable command and wait for syncd's confirmed UDS result."""

    def __init__(
        self,
        settings: RedisSettings | None = None,
        *,
        enabled: bool | None = None,
        timestamp_factory: Callable[[], int] = time.time_ns,
        event_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self.settings = settings or RedisSettings.from_environment()
        if (
            self.settings.connect_timeout_seconds <= 0
            or self.settings.socket_timeout_seconds <= 0
            or self.settings.transaction_timeout_seconds <= 0
        ):
            raise ValueError("Redis deadlines must be positive")
        self.enabled = (
            os.getenv("OCS_ENABLE_FAULT_API", "0") == "1" if enabled is None else enabled
        )
        self._client = create_redis_client(self.settings, DEVICE_DB)
        self._timestamp_factory = timestamp_factory
        self._event_id_factory = event_id_factory

    async def close(self) -> None:
        await self._client.aclose()

    async def execute(self, command: FaultCommand) -> int:
        if not self.enabled:
            raise PermissionDeniedError("simulator fault API is disabled")
        event_id = str(self._event_id_factory())
        timestamp_ns = self._timestamp_factory()
        fields = {
            "event_schema_version": str(EVENT_SCHEMA_VERSION),
            "event_id": event_id,
            "request_id": command.request_id,
            "timestamp_ns": str(timestamp_ns),
            "device": command.device,
            "resource_type": "fault",
            "resource_id": command.fault_id,
            "operation": command.operation,
            "desired_version": "0",
            "payload": json.dumps(
                {
                    "fault_type": command.fault_type,
                    "operation": command.operation,
                    "port_id": command.port_id,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        }
        try:
            async with asyncio.timeout(self.settings.transaction_timeout_seconds):
                await self._client.xadd(FAULT_COMMANDS, fields)
                key = fault_result_key(event_id)
                while True:
                    result = await self._client.hgetall(key)
                    if result:
                        if result.get("success") != "true":
                            code = result.get("error_code", "OCS_INTERNAL_ERROR")
                            message = result.get("error_message", "fault command failed")
                            if code == "OCS_DEVICE_NOT_READY":
                                raise DependencyUnavailableError(message)
                            raise InvalidArgumentError(f"{code}: {message}")
                        return timestamp_ns
                    await asyncio.sleep(0.02)
        except (TimeoutError, RedisTimeoutError) as error:
            raise DeadlineExceededError("simulator fault command") from error
        except RedisError as error:
            raise DependencyUnavailableError("DEVICE_DB fault command") from error


class FaultTransaction:
    def __init__(
        self,
        repository: FaultCommandRepository,
        *,
        request_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self._repository = repository
        self._request_id_factory = request_id_factory

    async def apply(self, request: gnmi_pb2.SetRequest) -> SetCommitResult:
        command = parse_fault_request(request, request_id_factory=self._request_id_factory)
        timestamp_ns = await self._repository.execute(command)
        operation = SetOperation(
            command.response_kind,
            NativePath(PathKind.FAULT, device=command.device, fault_id=command.fault_id),
            command.response_path,
            values={},
        )
        return SetCommitResult(timestamp_ns=timestamp_ns, operations=(operation,))

    async def close(self) -> None:
        await self._repository.close()
