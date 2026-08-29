"""gNMI-only adapter used by every REST and WebSocket operation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from ocsctl.client import ConnectionSpec, GnmiClient


class ManagementClient(Protocol):
    async def capabilities(self) -> dict[str, Any]: ...

    async def get(self, path: str, *, data_type: int = ...) -> Any: ...

    async def set_connections(
        self,
        device: str,
        connections: Sequence[ConnectionSpec],
        *,
        replace: bool = False,
    ) -> dict[str, Any]: ...

    async def delete_connection(self, device: str, connection_id: str) -> dict[str, Any]: ...

    async def inject_fault(self, device: str, fault_id: str) -> dict[str, Any]: ...

    async def clear_fault(
        self, device: str, fault_id: str | None = None
    ) -> dict[str, Any]: ...

    def subscribe(self, paths: str | Sequence[str]) -> AsyncIterator[dict[str, Any]]: ...


ClientFactory = Callable[[float], AbstractAsyncContextManager[ManagementClient]]


def default_client_factory(target: str) -> ClientFactory:
    def create(timeout_seconds: float) -> GnmiClient:
        return GnmiClient(target, timeout_seconds=timeout_seconds)

    return create


def device_path(device: str, branch: str = "") -> str:
    suffix = f"/{branch}" if branch else ""
    return f"/ocs/devices/device[name={device}]{suffix}"


class GatewayService:
    """Translate the bounded web contract into public gNMI operations."""

    def __init__(self, client_factory: ClientFactory, rpc_timeout_seconds: float) -> None:
        self.client_factory = client_factory
        self.rpc_timeout_seconds = rpc_timeout_seconds

    async def health(self) -> dict[str, Any]:
        async with self.client_factory(self.rpc_timeout_seconds) as client:
            capabilities, diagnostics = await asyncio.gather(
                client.capabilities(), client.get(device_path("ocs0", "diagnostics"))
            )
        if not diagnostics.get("core-services-online", False):
            raise RuntimeError("gNMI dependencies are not ready")
        return {
            "status": "ready",
            "dependency": "gnmi",
            "model": capabilities["models"][0]["name"],
            "gnmi-version": capabilities["gnmi-version"],
            "device-health": diagnostics.get("device-health", "UNKNOWN"),
        }

    async def snapshot(self, device: str) -> dict[str, Any]:
        branches = ("", "ports", "connections", "alarms", "counters", "diagnostics")
        async with self.client_factory(self.rpc_timeout_seconds) as client:
            values = await asyncio.gather(
                *(client.get(device_path(device, branch)) for branch in branches)
            )
        return dict(
            zip(
                ("device", "ports", "connections", "alarms", "counters", "diagnostics"),
                values,
                strict=True,
            )
        )

    async def write_connections(
        self,
        device: str,
        connections: Sequence[ConnectionSpec],
        *,
        replace: bool,
    ) -> dict[str, Any]:
        async with self.client_factory(self.rpc_timeout_seconds) as client:
            return await client.set_connections(device, connections, replace=replace)

    async def delete_connection(self, device: str, connection_id: str) -> dict[str, Any]:
        async with self.client_factory(self.rpc_timeout_seconds) as client:
            return await client.delete_connection(device, connection_id)

    async def inject_fault(self, device: str, fault_id: str) -> dict[str, Any]:
        async with self.client_factory(self.rpc_timeout_seconds) as client:
            return await client.inject_fault(device, fault_id)

    async def clear_faults(self, device: str) -> dict[str, Any]:
        async with self.client_factory(self.rpc_timeout_seconds) as client:
            return await client.clear_fault(device)
