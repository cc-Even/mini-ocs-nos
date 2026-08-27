"""gNMI Get validation and stable JSON_IETF response construction."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any, Final, Protocol

from gnmi_server import __version__
from gnmi_server.errors import InvalidArgumentError
from gnmi_server.path_parser import NativePath, PathKind, parse_paths
from gnmi_server.proto import gnmi_pb2

MODEL_NAME: Final = "mini-ocs-native"
MODEL_ORGANIZATION: Final = "mini-ocs-nos"
MODEL_VERSION: Final = __version__
_STATE_ONLY_KINDS: Final = {
    PathKind.DEVICE_STATE,
    PathKind.PORTS,
    PathKind.INPUT_PORT,
    PathKind.INPUT_PORT_STATE,
    PathKind.OUTPUT_PORT,
    PathKind.OUTPUT_PORT_STATE,
    PathKind.CONNECTION_STATE,
    PathKind.ALARMS,
    PathKind.ALARM,
    PathKind.COUNTERS,
    PathKind.DIAGNOSTICS,
}


class GetRepository(Protocol):
    async def read_many(
        self, paths: tuple[NativePath, ...], request_type: int
    ) -> tuple[dict[str, Any], ...]: ...

    async def close(self) -> None: ...


def _copy_path(path: gnmi_pb2.Path) -> gnmi_pb2.Path:
    copied = gnmi_pb2.Path()
    copied.CopyFrom(path)
    return copied


def validate_models(models) -> None:
    """Validate a request's optional native model restrictions."""

    for model in models:
        if model.name != MODEL_NAME:
            raise InvalidArgumentError(f"unsupported model {model.name!r}")
        if model.organization and model.organization != MODEL_ORGANIZATION:
            raise InvalidArgumentError(f"unsupported model organization {model.organization!r}")
        if model.version and model.version != MODEL_VERSION:
            raise InvalidArgumentError(f"unsupported model version {model.version!r}")


def _validate_type(path: NativePath, request_type: int) -> None:
    if path.kind is PathKind.CONNECTION_CONFIG and request_type in {
        gnmi_pb2.GetRequest.STATE,
        gnmi_pb2.GetRequest.OPERATIONAL,
    }:
        raise InvalidArgumentError("configuration paths do not support a state-only Get type")
    if path.kind in _STATE_ONLY_KINDS and request_type == gnmi_pb2.GetRequest.CONFIG:
        raise InvalidArgumentError("operational paths do not support the CONFIG Get type")


class GetTransaction:
    """Validate a complete request before performing any external reads."""

    def __init__(
        self,
        repository: GetRepository,
        *,
        timestamp_factory: Callable[[], int] = time.time_ns,
    ) -> None:
        self._repository = repository
        self._timestamp_factory = timestamp_factory

    async def close(self) -> None:
        await self._repository.close()

    async def read(self, request: gnmi_pb2.GetRequest) -> gnmi_pb2.GetResponse:
        if not request.path:
            raise InvalidArgumentError("GetRequest requires at least one path")
        paths = parse_paths(request.path, prefix=request.prefix)
        if request.encoding != gnmi_pb2.JSON_IETF:
            raise InvalidArgumentError("GetRequest encoding must be JSON_IETF")
        validate_models(request.use_models)
        for path in paths:
            _validate_type(path, request.type)

        payloads = await self._repository.read_many(paths, request.type)
        updates = [
            gnmi_pb2.Update(
                path=_copy_path(request.path[index]),
                val=gnmi_pb2.TypedValue(
                    json_ietf_val=json.dumps(
                        payload,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                ),
            )
            for index, payload in enumerate(payloads)
        ]
        prefix = gnmi_pb2.Path()
        prefix.CopyFrom(request.prefix)
        return gnmi_pb2.GetResponse(
            notification=[
                gnmi_pb2.Notification(
                    timestamp=self._timestamp_factory(),
                    prefix=prefix,
                    update=updates,
                )
            ]
        )
