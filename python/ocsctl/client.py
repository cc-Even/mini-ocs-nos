"""Deadline-bounded gNMI client used by every ocsctl command."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any

import grpc
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc


@dataclass(frozen=True)
class ConnectionSpec:
    connection_id: str
    input_port: int
    output_port: int


def connection_path(device: str, connection_id: str, leaf: str = "") -> str:
    suffix = f"/{leaf}" if leaf else ""
    return (
        f"/ocs/devices/device[name={device}]/connections/"
        f"connection[id={connection_id}]{suffix}"
    )


def _path_text(path: gnmi_pb2.Path) -> str:
    segments: list[str] = []
    for element in path.elem:
        keys = "".join(f"[{key}={value}]" for key, value in sorted(element.key.items()))
        segments.append(f"{element.name}{keys}")
    return "/" + "/".join(segments)


class GnmiClient:
    """Small native-model client; it has no Redis dependency by design."""

    def __init__(
        self,
        target: str = "127.0.0.1:50051",
        *,
        timeout_seconds: float = 3.0,
        stub=None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.target = target
        self.timeout_seconds = timeout_seconds
        self._channel: grpc.aio.Channel | None = None
        self._stub = stub

    async def __aenter__(self) -> GnmiClient:
        if self._stub is None:
            self._channel = grpc.aio.insecure_channel(self.target)
            self._stub = gnmi_pb2_grpc.gNMIStub(self._channel)
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        if self._channel is not None:
            await self._channel.close()

    async def capabilities(self) -> dict[str, Any]:
        response = await self._stub.Capabilities(
            gnmi_pb2.CapabilityRequest(), timeout=self.timeout_seconds
        )
        return {
            "gnmi-version": response.gNMI_version,
            "encodings": [gnmi_pb2.Encoding.Name(value) for value in response.supported_encodings],
            "models": [
                {
                    "name": model.name,
                    "organization": model.organization,
                    "version": model.version,
                }
                for model in response.supported_models
            ],
        }

    async def get(self, path: str, *, data_type: int = gnmi_pb2.GetRequest.ALL) -> Any:
        response = await self._stub.Get(
            gnmi_pb2.GetRequest(
                path=[protobuf_path(path)],
                type=data_type,
                encoding=gnmi_pb2.JSON_IETF,
            ),
            timeout=self.timeout_seconds,
        )
        if len(response.notification) != 1 or len(response.notification[0].update) != 1:
            raise RuntimeError("gNMI Get returned an unexpected notification shape")
        value = response.notification[0].update[0].val
        if value.WhichOneof("value") != "json_ietf_val":
            raise RuntimeError("gNMI Get did not return JSON_IETF")
        return json.loads(value.json_ietf_val)

    async def set_connections(
        self,
        device: str,
        connections: Sequence[ConnectionSpec],
        *,
        replace: bool = False,
    ) -> dict[str, Any]:
        if not connections:
            raise ValueError("at least one connection is required")
        updates = [self._connection_update(device, connection) for connection in connections]
        request = gnmi_pb2.SetRequest()
        (request.replace if replace else request.update).extend(updates)
        response = await self._stub.Set(request, timeout=self.timeout_seconds)
        return {
            "timestamp": response.timestamp,
            "operations": [
                {
                    "path": _path_text(result.path),
                    "operation": gnmi_pb2.UpdateResult.Operation.Name(result.op),
                }
                for result in response.response
            ],
        }

    async def delete_connection(self, device: str, connection_id: str) -> dict[str, Any]:
        response = await self._stub.Set(
            gnmi_pb2.SetRequest(delete=[protobuf_path(connection_path(device, connection_id))]),
            timeout=self.timeout_seconds,
        )
        return {
            "timestamp": response.timestamp,
            "operations": [
                {
                    "path": _path_text(result.path),
                    "operation": gnmi_pb2.UpdateResult.Operation.Name(result.op),
                }
                for result in response.response
            ],
        }

    async def wait_for_connection(
        self,
        device: str,
        connection_id: str,
        *,
        apply_status: str = "ACTIVE",
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.timeout_seconds
        state_path = connection_path(device, connection_id, "state")
        last_state: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            try:
                last_state = await self.get(state_path, data_type=gnmi_pb2.GetRequest.STATE)
            except grpc.aio.AioRpcError as error:
                if error.code() is not grpc.StatusCode.NOT_FOUND:
                    raise
            if last_state is not None and last_state.get("apply-status") == apply_status:
                return last_state
            await asyncio.sleep(0.05)
        actual = last_state.get("apply-status") if last_state else "MISSING"
        raise TimeoutError(
            f"connection {device}/{connection_id} did not reach {apply_status}; last={actual}"
        )

    async def subscribe(self, path: str) -> AsyncIterator[dict[str, Any]]:
        request_path = protobuf_path(path)

        async def requests():
            yield gnmi_pb2.SubscribeRequest(
                subscribe=gnmi_pb2.SubscriptionList(
                    subscription=[
                        gnmi_pb2.Subscription(path=request_path, mode=gnmi_pb2.ON_CHANGE)
                    ],
                    mode=gnmi_pb2.SubscriptionList.STREAM,
                    encoding=gnmi_pb2.JSON_IETF,
                )
            )
            await asyncio.Future()

        call = self._stub.Subscribe(requests(), timeout=self.timeout_seconds)
        try:
            async for response in call:
                response_kind = response.WhichOneof("response")
                if response_kind == "sync_response":
                    yield {"sync-response": response.sync_response}
                    continue
                if response_kind != "update":
                    raise RuntimeError("gNMI Subscribe returned an unexpected response")
                notification = response.update
                for update in notification.update:
                    yield {
                        "timestamp": notification.timestamp,
                        "path": _path_text(update.path),
                        "value": json.loads(update.val.json_ietf_val),
                    }
                for deleted in notification.delete:
                    yield {
                        "timestamp": notification.timestamp,
                        "path": _path_text(deleted),
                        "deleted": True,
                    }
        finally:
            call.cancel()

    @staticmethod
    def _connection_update(device: str, connection: ConnectionSpec) -> gnmi_pb2.Update:
        payload = json.dumps(
            {
                "admin-status": "ENABLED",
                "input-port": connection.input_port,
                "output-port": connection.output_port,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return gnmi_pb2.Update(
            path=protobuf_path(connection_path(device, connection.connection_id, "config")),
            val=gnmi_pb2.TypedValue(json_ietf_val=payload),
        )
