from __future__ import annotations

import asyncio
import json
import os

import grpc
import httpx
import pytest
from ocsctl.client import ConnectionSpec, GnmiClient
from websockets.asyncio.client import connect

TARGET = os.getenv("OCS_COMPOSE_GNMI_TARGET", "")
WEB_TARGET = os.getenv("OCS_COMPOSE_WEB_TARGET", "")
pytestmark = pytest.mark.skipif(
    not TARGET or not WEB_TARGET,
    reason="set OCS_COMPOSE_GNMI_TARGET and OCS_COMPOSE_WEB_TARGET for Compose E2E",
)


@pytest.mark.asyncio
async def test_packaged_runtime_traverses_gnmi_redis_and_uds() -> None:
    created = (
        ConnectionSpec("compose-a", 1, 9),
        ConnectionSpec("compose-b", 2, 10),
        ConnectionSpec("compose-c", 3, 11),
    )
    async with GnmiClient(TARGET, timeout_seconds=10.0) as client:
        capabilities = await client.capabilities()
        assert capabilities["models"][0]["name"] == "mini-ocs-native"

        diagnostics = await client.get("/ocs/devices/device[name=ocs0]/diagnostics")
        assert diagnostics["device-health"] == "READY"
        assert diagnostics["core-services-online"] is True

        try:
            await client.set_connections("ocs0", created)
            for connection in created:
                state = await client.wait_for_connection("ocs0", connection.connection_id)
                assert state["input-port"] == connection.input_port
                assert state["output-port"] == connection.output_port
                assert state["desired-version"] == state["applied-version"]

            counters = await client.get("/ocs/devices/device[name=ocs0]/counters")
            assert counters["active-connections"] == 3

            with pytest.raises(grpc.aio.AioRpcError) as conflict:
                await client.set_connections(
                    "ocs0",
                    (
                        ConnectionSpec("compose-conflict-a", 4, 12),
                        ConnectionSpec("compose-conflict-b", 5, 12),
                    ),
                )
            assert conflict.value.code() is grpc.StatusCode.INVALID_ARGUMENT

            for connection_id in ("compose-conflict-a", "compose-conflict-b"):
                with pytest.raises(grpc.aio.AioRpcError) as missing:
                    await client.get(
                        "/ocs/devices/device[name=ocs0]/connections/"
                        f"connection[id={connection_id}]/config"
                    )
                assert missing.value.code() is grpc.StatusCode.NOT_FOUND
        finally:
            for connection in created:
                await client.delete_connection("ocs0", connection.connection_id)


@pytest.mark.asyncio
async def test_packaged_web_gateway_uses_the_gnmi_management_boundary() -> None:
    async with (
        httpx.AsyncClient(base_url=WEB_TARGET, timeout=10.0) as web,
        GnmiClient(TARGET, timeout_seconds=10.0) as gnmi,
    ):
        health = await web.get("/healthz")
        assert health.status_code == 200
        assert health.json()["dependency"] == "gnmi"

        snapshot = await web.get("/api/v1/devices/ocs0/snapshot")
        assert snapshot.status_code == 200
        assert snapshot.json()["device"]["name"] == "ocs0"

        try:
            websocket_url = WEB_TARGET.replace("http://", "ws://", 1)
            async with connect(
                f"{websocket_url}/api/v1/devices/ocs0/events?duration_seconds=10",
                open_timeout=5,
                max_size=65_536,
            ) as websocket:
                assert json.loads(await websocket.recv())["type"] == "ready"
                while True:
                    initial = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
                    if initial["type"] == "sync":
                        break

                accepted = await web.put(
                    "/api/v1/devices/ocs0/connections/compose-web",
                    json={"input-port": 8, "output-port": 16},
                )
                assert accepted.status_code == 202
                state = await gnmi.wait_for_connection("ocs0", "compose-web")
                assert state["input-port"] == 8
                assert state["output-port"] == 16

                while True:
                    event = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
                    if event.get("type") != "update":
                        continue
                    connections = event.get("value", {}).get("connection", [])
                    if any(item.get("id") == "compose-web" for item in connections):
                        break
        finally:
            deleted = await web.delete(
                "/api/v1/devices/ocs0/connections/compose-web"
            )
            assert deleted.status_code == 202
