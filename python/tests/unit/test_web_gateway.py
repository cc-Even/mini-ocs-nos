from __future__ import annotations

import ast
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
import pytest
from fastapi.testclient import TestClient
from web_gateway.app import create_app
from web_gateway.config import GatewaySettings


def rpc_error(code: grpc.StatusCode, details: str = "dependency failed"):
    return grpc.aio.AioRpcError(code, (), (), details)


class FakeManagementClient:
    def __init__(self, owner: FakeClientFactory) -> None:
        self.owner = owner

    async def capabilities(self):
        if self.owner.failure is not None:
            raise self.owner.failure
        self.owner.calls.append(("capabilities",))
        return {
            "gnmi-version": "0.10.0",
            "models": [{"name": "mini-ocs-native"}],
        }

    async def get(self, path: str, *, data_type=None):
        del data_type
        if self.owner.failure is not None:
            raise self.owner.failure
        self.owner.calls.append(("get", path))
        if path.endswith("/diagnostics"):
            return {"core-services-online": True, "device-health": "READY"}
        return {"path": path}

    async def set_connections(self, device, connections, *, replace=False):
        if self.owner.failure is not None:
            raise self.owner.failure
        self.owner.calls.append(("set", device, tuple(connections), replace))
        return {"operations": [{"operation": "REPLACE" if replace else "UPDATE"}]}

    async def delete_connection(self, device, connection_id):
        self.owner.calls.append(("delete", device, connection_id))
        return {"operations": [{"operation": "DELETE"}]}

    async def inject_fault(self, device, fault_id):
        self.owner.calls.append(("inject", device, fault_id))
        return {"operations": [{"operation": "UPDATE"}]}

    async def clear_fault(self, device, fault_id=None):
        self.owner.calls.append(("clear", device, fault_id))
        return {"operations": [{"operation": "DELETE"}]}

    async def subscribe(self, paths):
        self.owner.subscribe_calls += 1
        call = self.owner.subscribe_calls
        self.owner.calls.append(("subscribe", tuple(paths)))
        try:
            if call <= self.owner.subscription_failures:
                raise rpc_error(grpc.StatusCode.UNAVAILABLE, "gNMI is restarting")
            yield {"sync-response": True}
            await self.owner.never_complete()
        finally:
            self.owner.subscription_closes += 1


class FakeClientFactory:
    def __init__(self) -> None:
        self.calls = []
        self.timeouts = []
        self.failure = None
        self.subscription_failures = 0
        self.subscribe_calls = 0
        self.subscription_closes = 0
        self.context_exits = 0

    @staticmethod
    async def never_complete():
        import asyncio

        await asyncio.Future()

    @asynccontextmanager
    async def __call__(self, timeout_seconds):
        self.timeouts.append(timeout_seconds)
        try:
            yield FakeManagementClient(self)
        finally:
            self.context_exits += 1


def settings(**overrides) -> GatewaySettings:
    values = {
        **GatewaySettings().__dict__,
        "rpc_timeout_seconds": 1.25,
        "websocket_max_seconds": 5,
        "websocket_reconnect_backoff_seconds": 0,
        **overrides,
    }
    return GatewaySettings(**values)


def test_rest_snapshot_and_mutations_translate_only_to_bounded_gnmi_calls() -> None:
    factory = FakeClientFactory()
    with TestClient(create_app(settings(), factory)) as client:
        health = client.get("/healthz")
        snapshot = client.get("/api/v1/devices/ocs0/snapshot")
        updated = client.put(
            "/api/v1/devices/ocs0/connections/conn-1",
            json={"input-port": 3, "output-port": 11},
        )
        replaced = client.post(
            "/api/v1/devices/ocs0/connections:batch",
            json={
                "operation": "REPLACE",
                "connections": [
                    {"id": "conn-1", "input-port": 4, "output-port": 12},
                    {"id": "conn-2", "input-port": 5, "output-port": 13},
                ],
            },
        )
        deleted = client.delete("/api/v1/devices/ocs0/connections/conn-1")
        injected = client.post(
            "/api/v1/devices/ocs0/faults",
            json={"fault": "INPUT_PORT_DOWN", "port": 3},
        )
        cleared = client.delete("/api/v1/devices/ocs0/faults")

    assert health.status_code == 200
    assert health.json()["dependency"] == "gnmi"
    assert snapshot.status_code == 200
    assert set(snapshot.json()) == {
        "device",
        "ports",
        "connections",
        "alarms",
        "counters",
        "diagnostics",
    }
    assert updated.status_code == 202
    assert replaced.status_code == 202
    assert deleted.status_code == 202
    assert injected.status_code == 200
    assert cleared.status_code == 200
    set_calls = [call for call in factory.calls if call[0] == "set"]
    assert set_calls[0][3] is False
    assert set_calls[1][3] is True
    assert len(set_calls[1][2]) == 2
    assert ("inject", "ocs0", "input-port-down-3") in factory.calls
    assert all(timeout == 1.25 for timeout in factory.timeouts)


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        ("/api/v1/devices/bad$name/snapshot", None),
        (
            "/api/v1/devices/ocs0/connections/conn-1",
            {"input-port": 0, "output-port": 1},
        ),
        (
            "/api/v1/devices/ocs0/connections:batch",
            {
                "connections": [
                    {"id": "duplicate", "input-port": 1, "output-port": 9},
                    {"id": "duplicate", "input-port": 2, "output-port": 10},
                ]
            },
        ),
        (
            "/api/v1/devices/ocs0/faults",
            {"fault": "NEXT_APPLY_TIMEOUT", "port": 1},
        ),
    ],
)
def test_rest_contract_rejects_unsafe_or_inconsistent_requests(path, payload) -> None:
    factory = FakeClientFactory()
    with TestClient(create_app(settings(), factory)) as client:
        if payload is None:
            response = client.get(path)
        elif "connections:batch" in path or path.endswith("faults"):
            response = client.post(path, json=payload)
        else:
            response = client.put(path, json=payload)
    assert response.status_code == 422
    assert factory.calls == []


def test_request_body_limit_and_grpc_dependency_error_are_stable() -> None:
    factory = FakeClientFactory()
    app = create_app(settings(max_request_body_bytes=1024), factory)
    with TestClient(app) as client:
        too_large = client.put(
            "/api/v1/devices/ocs0/connections/conn-1",
            content=b"x" * 1025,
            headers={"content-type": "application/json"},
        )
        factory.failure = rpc_error(grpc.StatusCode.UNAVAILABLE, "gNMI offline")
        unavailable = client.get("/healthz")

    assert too_large.status_code == 413
    assert too_large.json()["error"]["code"] == "REQUEST_TOO_LARGE"
    assert unavailable.status_code == 503
    assert unavailable.json() == {
        "error": {
            "code": "UNAVAILABLE",
            "message": "gNMI offline",
            "retryable": True,
        }
    }


def test_websocket_reconnects_and_cancels_the_gnmi_subscription_on_disconnect() -> None:
    factory = FakeClientFactory()
    factory.subscription_failures = 1
    app = create_app(settings(websocket_reconnect_attempts=2), factory)

    with (
        TestClient(app) as client,
        client.websocket_connect(
            "/api/v1/devices/ocs0/events?duration_seconds=4"
        ) as websocket,
    ):
        assert websocket.receive_json()["type"] == "ready"
        reconnecting = websocket.receive_json()
        synchronized = websocket.receive_json()
        assert reconnecting == {
            "type": "reconnecting",
            "attempt": 1,
            "code": "UNAVAILABLE",
        }
        assert synchronized == {"type": "sync", "sync-response": True}

    assert factory.subscribe_calls == 2
    assert factory.subscription_closes == 2
    assert factory.context_exits == 2
    assert all(0 < timeout <= 4 for timeout in factory.timeouts)
    subscribe_paths = next(call[1] for call in factory.calls if call[0] == "subscribe")
    assert subscribe_paths == (
        "/ocs/devices/device[name=ocs0]/connections",
        "/ocs/devices/device[name=ocs0]/ports",
        "/ocs/devices/device[name=ocs0]/alarms",
    )


def test_websocket_reports_terminal_dependency_failure_after_bounded_reconnects() -> None:
    factory = FakeClientFactory()
    factory.subscription_failures = 3
    app = create_app(settings(websocket_reconnect_attempts=1), factory)

    with (
        TestClient(app) as client,
        client.websocket_connect("/api/v1/devices/ocs0/events") as websocket,
    ):
        assert websocket.receive_json()["type"] == "ready"
        assert websocket.receive_json()["type"] == "reconnecting"
        terminal = websocket.receive_json()

    assert terminal == {
        "type": "error",
        "error": {
            "code": "UNAVAILABLE",
            "message": "gNMI is restarting",
            "retryable": True,
        },
    }
    assert factory.subscribe_calls == 2
    assert factory.subscription_closes == 2


def test_gateway_package_has_no_redis_or_uds_import_boundary() -> None:
    package_root = Path(__file__).parents[2] / "web_gateway"
    imported_modules: set[str] = set()
    source_text = ""
    for source_path in package_root.glob("*.py"):
        text = source_path.read_text(encoding="utf-8")
        source_text += text
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.add(node.module)

    assert not any(module == "redis" or module.startswith("redis.") for module in imported_modules)
    assert "UdsDeviceBackend" not in source_text
    assert "ocs-hwsim" not in source_text
