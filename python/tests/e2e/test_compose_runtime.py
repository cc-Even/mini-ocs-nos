from __future__ import annotations

import os

import grpc
import pytest
from ocsctl.client import ConnectionSpec, GnmiClient

TARGET = os.getenv("OCS_COMPOSE_GNMI_TARGET", "")
pytestmark = pytest.mark.skipif(
    not TARGET,
    reason="set OCS_COMPOSE_GNMI_TARGET to run the packaged Compose E2E test",
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
