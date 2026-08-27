"""gNMI Set parsing and pure final-candidate configuration semantics."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Protocol

from gnmi_server.errors import ConflictError, InvalidArgumentError
from gnmi_server.path_parser import NativePath, PathKind, parse_path
from gnmi_server.proto import gnmi_pb2

MAX_JSON_IETF_BYTES: Final = 64 * 1024
DEFAULT_PORT_COUNT: Final = 16
ADMIN_ENABLED: Final = "ENABLED"


class SetOperationKind(StrEnum):
    DELETE = "DELETE"
    REPLACE = "REPLACE"
    UPDATE = "UPDATE"


@dataclass(frozen=True)
class ConnectionConfig:
    connection_id: str
    input_port: int
    output_port: int
    admin_status: str = ADMIN_ENABLED
    desired_version: int = 0
    created_at_ns: int = 0
    updated_at_ns: int = 0

    @classmethod
    def from_redis(cls, connection_id: str, fields: Mapping[str, str]) -> ConnectionConfig:
        try:
            return cls(
                connection_id=connection_id,
                input_port=int(fields["input_port"]),
                output_port=int(fields["output_port"]),
                admin_status=fields.get("admin_status", ADMIN_ENABLED),
                desired_version=int(fields.get("desired_version", "0")),
                created_at_ns=int(fields.get("created_at_ns", "0")),
                updated_at_ns=int(fields.get("updated_at_ns", "0")),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError(f"invalid Redis connection snapshot for {connection_id}") from error

    def redis_fields(self, device: str) -> dict[str, str]:
        return {
            "device": device,
            "id": self.connection_id,
            "input_port": str(self.input_port),
            "output_port": str(self.output_port),
            "admin_status": self.admin_status,
            "desired_version": str(self.desired_version),
            "created_at_ns": str(self.created_at_ns),
            "updated_at_ns": str(self.updated_at_ns),
        }

    def event_payload(self) -> str:
        return json.dumps(
            {"input_port": self.input_port, "output_port": self.output_port},
            separators=(",", ":"),
            sort_keys=True,
        )


@dataclass(frozen=True)
class SetOperation:
    kind: SetOperationKind
    native_path: NativePath
    response_path: gnmi_pb2.Path
    values: Mapping[str, int | str]

    @property
    def connection_id(self) -> str:
        assert self.native_path.connection_id is not None
        return self.native_path.connection_id


@dataclass(frozen=True)
class SetPlan:
    device: str
    request_id: str
    operations: tuple[SetOperation, ...]


@dataclass(frozen=True)
class CandidateChange:
    connection_id: str
    operation: str
    config: ConnectionConfig | None


@dataclass(frozen=True)
class CandidateMutation:
    revision: int
    timestamp_ns: int
    changes: tuple[CandidateChange, ...]


@dataclass(frozen=True)
class SetCommitResult:
    timestamp_ns: int
    operations: tuple[SetOperation, ...]


class ConfigTransactionRepository(Protocol):
    async def commit(self, plan: SetPlan) -> SetCommitResult: ...

    async def close(self) -> None: ...


@dataclass
class _MutableConnection:
    input_port: int | None = None
    output_port: int | None = None
    admin_status: str = ADMIN_ENABLED
    created_at_ns: int = 0


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InvalidArgumentError(f"JSON_IETF payload repeats field {key!r}")
        result[key] = value
    return result


def decode_json_ietf(value: gnmi_pb2.TypedValue) -> dict[str, int | str]:
    """Decode and strictly validate one connection config payload."""

    if value.WhichOneof("value") != "json_ietf_val":
        raise InvalidArgumentError("Set values must use TypedValue.json_ietf_val")
    payload = value.json_ietf_val
    if len(payload) > MAX_JSON_IETF_BYTES:
        raise InvalidArgumentError("JSON_IETF payload exceeds 65536 bytes")
    try:
        decoded = json.loads(payload, object_pairs_hook=_unique_object)
    except InvalidArgumentError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidArgumentError("Set value is not valid JSON_IETF") from error
    if not isinstance(decoded, dict):
        raise InvalidArgumentError("connection config must be a JSON object")
    allowed = {"input-port", "output-port", "admin-status"}
    unexpected = set(decoded) - allowed
    if unexpected:
        raise InvalidArgumentError(f"unsupported connection fields: {','.join(sorted(unexpected))}")
    if not decoded:
        raise InvalidArgumentError("connection config update must not be empty")

    values: dict[str, int | str] = {}
    for json_name, field_name in (
        ("input-port", "input_port"),
        ("output-port", "output_port"),
    ):
        if json_name not in decoded:
            continue
        port = decoded[json_name]
        if isinstance(port, bool) or not isinstance(port, int):
            raise InvalidArgumentError(f"{json_name} must be an integer")
        if port <= 0 or port > 2**31 - 1:
            raise InvalidArgumentError(f"{json_name} is outside the supported range")
        values[field_name] = port
    if "admin-status" in decoded:
        admin_status = decoded["admin-status"]
        if not isinstance(admin_status, str):
            raise InvalidArgumentError("admin-status must be a string")
        if admin_status != ADMIN_ENABLED:
            raise InvalidArgumentError(
                "admin-status must be ENABLED; delete the connection to disable it"
            )
        values["admin_status"] = admin_status
    return values


def _copy_path(path: gnmi_pb2.Path) -> gnmi_pb2.Path:
    copied = gnmi_pb2.Path()
    copied.CopyFrom(path)
    return copied


def _connection_path(
    path: gnmi_pb2.Path,
    prefix: gnmi_pb2.Path,
    *,
    allow_connection_container: bool,
) -> NativePath:
    native = parse_path(path, prefix=prefix)
    allowed = {PathKind.CONNECTION_CONFIG}
    if allow_connection_container:
        allowed.add(PathKind.CONNECTION)
    if native.kind not in allowed:
        raise InvalidArgumentError("Set only supports keyed connection config paths")
    return native


def parse_set_request(
    request: gnmi_pb2.SetRequest,
    *,
    request_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
) -> SetPlan:
    """Parse operations in the gNMI-defined delete, replace, update order."""

    if request.union_replace:
        raise InvalidArgumentError("union_replace is not supported")
    if not request.delete and not request.replace and not request.update:
        raise InvalidArgumentError("SetRequest requires at least one operation")

    operations: list[SetOperation] = []
    for path in request.delete:
        native = _connection_path(path, request.prefix, allow_connection_container=True)
        operations.append(
            SetOperation(SetOperationKind.DELETE, native, _copy_path(path), values={})
        )
    for update in request.replace:
        native = _connection_path(update.path, request.prefix, allow_connection_container=False)
        values = decode_json_ietf(update.val)
        if "input_port" not in values or "output_port" not in values:
            raise InvalidArgumentError("replace requires input-port and output-port")
        operations.append(
            SetOperation(SetOperationKind.REPLACE, native, _copy_path(update.path), values)
        )
    for update in request.update:
        native = _connection_path(update.path, request.prefix, allow_connection_container=False)
        operations.append(
            SetOperation(
                SetOperationKind.UPDATE,
                native,
                _copy_path(update.path),
                decode_json_ietf(update.val),
            )
        )

    devices = {operation.native_path.device for operation in operations}
    if None in devices or len(devices) != 1:
        raise InvalidArgumentError("one SetRequest must target exactly one device")
    return SetPlan(
        device=next(iter(devices)),
        request_id=str(request_id_factory()),
        operations=tuple(operations),
    )


def build_candidate(
    current: Mapping[str, ConnectionConfig],
    plan: SetPlan,
    *,
    revision: int,
    timestamp_ns: int,
    input_port_count: int = DEFAULT_PORT_COUNT,
    output_port_count: int = DEFAULT_PORT_COUNT,
) -> CandidateMutation:
    """Apply all operations, then validate the complete candidate atomically."""

    if revision <= 0:
        raise RuntimeError("configuration revision must be positive")
    working = {
        connection_id: _MutableConnection(
            config.input_port,
            config.output_port,
            config.admin_status,
            config.created_at_ns,
        )
        for connection_id, config in current.items()
    }
    touched: dict[str, None] = {}
    for operation in plan.operations:
        connection_id = operation.connection_id
        touched.setdefault(connection_id, None)
        if operation.kind is SetOperationKind.DELETE:
            working.pop(connection_id, None)
            continue
        if operation.kind is SetOperationKind.REPLACE:
            existing = current.get(connection_id)
            working[connection_id] = _MutableConnection(
                input_port=int(operation.values["input_port"]),
                output_port=int(operation.values["output_port"]),
                admin_status=str(operation.values.get("admin_status", ADMIN_ENABLED)),
                created_at_ns=(
                    existing.created_at_ns if existing is not None else timestamp_ns
                ),
            )
            continue
        candidate = working.setdefault(
            connection_id,
            _MutableConnection(created_at_ns=timestamp_ns),
        )
        for field, value in operation.values.items():
            setattr(candidate, field, value)

    finalized: dict[str, ConnectionConfig] = {}
    occupied_inputs: dict[int, str] = {}
    occupied_outputs: dict[int, str] = {}
    for connection_id, candidate in working.items():
        if candidate.input_port is None or candidate.output_port is None:
            raise InvalidArgumentError(
                f"connection {connection_id!r} requires input-port and output-port"
            )
        if not 1 <= candidate.input_port <= input_port_count:
            raise InvalidArgumentError(
                f"connection {connection_id!r} input-port is outside 1..{input_port_count}"
            )
        if not 1 <= candidate.output_port <= output_port_count:
            raise InvalidArgumentError(
                f"connection {connection_id!r} output-port is outside 1..{output_port_count}"
            )
        if previous := occupied_inputs.get(candidate.input_port):
            raise ConflictError(
                f"input port {candidate.input_port} is used by {previous!r} and {connection_id!r}"
            )
        if previous := occupied_outputs.get(candidate.output_port):
            raise ConflictError(
                f"output port {candidate.output_port} is used by {previous!r} and {connection_id!r}"
            )
        occupied_inputs[candidate.input_port] = connection_id
        occupied_outputs[candidate.output_port] = connection_id
        existing = current.get(connection_id)
        if connection_id in touched:
            desired_version = revision
            updated_at_ns = timestamp_ns
        else:
            if existing is None:
                raise RuntimeError("untouched candidate has no current snapshot")
            desired_version = existing.desired_version
            updated_at_ns = existing.updated_at_ns
        finalized[connection_id] = ConnectionConfig(
            connection_id=connection_id,
            input_port=candidate.input_port,
            output_port=candidate.output_port,
            admin_status=candidate.admin_status,
            desired_version=desired_version,
            created_at_ns=candidate.created_at_ns or timestamp_ns,
            updated_at_ns=updated_at_ns,
        )

    changes = tuple(
        CandidateChange(
            connection_id=connection_id,
            operation="UPSERT" if connection_id in finalized else "REMOVE",
            config=finalized.get(connection_id),
        )
        for connection_id in touched
    )
    return CandidateMutation(revision=revision, timestamp_ns=timestamp_ns, changes=changes)


class SetTransaction:
    """Parse Set requests and delegate their atomic persistence."""

    def __init__(
        self,
        repository: ConfigTransactionRepository,
        *,
        request_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self._repository = repository
        self._request_id_factory = request_id_factory

    async def apply(self, request: gnmi_pb2.SetRequest) -> SetCommitResult:
        plan = parse_set_request(request, request_id_factory=self._request_id_factory)
        return await self._repository.commit(plan)

    async def close(self) -> None:
        await self._repository.close()
