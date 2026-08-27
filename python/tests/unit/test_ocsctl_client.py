from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2
from ocsctl.client import ConnectionSpec, GnmiClient, connection_path
from ocsctl.main import app
from typer.testing import CliRunner


class FakeStub:
    def __init__(self) -> None:
        self.calls = []

    async def Capabilities(self, request, *, timeout):
        self.calls.append(("capabilities", request, timeout))
        return gnmi_pb2.CapabilityResponse(
            supported_models=[
                gnmi_pb2.ModelData(
                    name="mini-ocs-native",
                    organization="mini-ocs-nos",
                    version="0.1.0",
                )
            ],
            supported_encodings=[gnmi_pb2.JSON_IETF],
            gNMI_version="0.10.0",
        )

    async def Get(self, request, *, timeout):
        self.calls.append(("get", request, timeout))
        return gnmi_pb2.GetResponse(
            notification=[
                gnmi_pb2.Notification(
                    timestamp=123,
                    update=[
                        gnmi_pb2.Update(
                            path=request.path[0],
                            val=gnmi_pb2.TypedValue(
                                json_ietf_val=b'{"applied-version":7,"apply-status":"ACTIVE"}'
                            ),
                        )
                    ],
                )
            ]
        )

    async def Set(self, request, *, timeout):
        self.calls.append(("set", request, timeout))
        if request.delete:
            path = request.delete[0]
            operation = gnmi_pb2.UpdateResult.DELETE
        elif request.replace:
            path = request.replace[0].path
            operation = gnmi_pb2.UpdateResult.REPLACE
        else:
            path = request.update[0].path
            operation = gnmi_pb2.UpdateResult.UPDATE
        return gnmi_pb2.SetResponse(
            timestamp=456,
            response=[gnmi_pb2.UpdateResult(path=path, op=operation)],
        )


async def test_client_builds_deadline_bounded_native_get_and_capabilities() -> None:
    stub = FakeStub()
    client = GnmiClient(timeout_seconds=1.5, stub=stub)

    capabilities = await client.capabilities()
    state = await client.get(connection_path("ocs0", "conn-1", "state"))

    assert capabilities == {
        "encodings": ["JSON_IETF"],
        "gnmi-version": "0.10.0",
        "models": [
            {
                "name": "mini-ocs-native",
                "organization": "mini-ocs-nos",
                "version": "0.1.0",
            }
        ],
    }
    assert state == {"applied-version": 7, "apply-status": "ACTIVE"}
    _, get_request, timeout = stub.calls[1]
    assert get_request.encoding == gnmi_pb2.JSON_IETF
    assert timeout == 1.5


async def test_client_builds_update_replace_and_delete_requests() -> None:
    stub = FakeStub()
    client = GnmiClient(timeout_seconds=2.0, stub=stub)
    connection = ConnectionSpec("conn-1", 3, 11)

    updated = await client.set_connections("ocs0", [connection])
    replaced = await client.set_connections("ocs0", [connection], replace=True)
    deleted = await client.delete_connection("ocs0", "conn-1")

    update_request = stub.calls[0][1]
    assert not update_request.replace
    assert json.loads(update_request.update[0].val.json_ietf_val) == {
        "admin-status": "ENABLED",
        "input-port": 3,
        "output-port": 11,
    }
    assert stub.calls[1][1].replace
    assert stub.calls[2][1].delete
    assert updated["operations"][0]["operation"] == "UPDATE"
    assert replaced["operations"][0]["operation"] == "REPLACE"
    assert deleted["operations"][0]["operation"] == "DELETE"
    assert all(call[2] == 2.0 for call in stub.calls)


async def test_client_validates_deadline_batch_and_get_response_shape() -> None:
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        GnmiClient(timeout_seconds=0)

    client = GnmiClient(timeout_seconds=1.0, stub=FakeStub())
    with pytest.raises(ValueError, match="at least one connection"):
        await client.set_connections("ocs0", [])

    class InvalidGetStub(FakeStub):
        def __init__(self, response: gnmi_pb2.GetResponse) -> None:
            super().__init__()
            self.response = response

        async def Get(self, request, *, timeout):
            self.calls.append(("get", request, timeout))
            return self.response

    path = connection_path("ocs0", "conn-1", "state")
    malformed_responses = (
        gnmi_pb2.GetResponse(),
        gnmi_pb2.GetResponse(
            notification=[
                gnmi_pb2.Notification(
                    update=[
                        gnmi_pb2.Update(
                            val=gnmi_pb2.TypedValue(string_val="not-json-ietf")
                        )
                    ]
                )
            ]
        ),
    )
    for response in malformed_responses:
        invalid_client = GnmiClient(stub=InvalidGetStub(response))
        with pytest.raises(RuntimeError, match="gNMI Get"):
            await invalid_client.get(path)


async def test_wait_for_connection_and_subscribe_decode_operational_events() -> None:
    client = GnmiClient(timeout_seconds=1.0, stub=FakeStub())
    state = await client.wait_for_connection("ocs0", "conn-1")
    assert state["apply-status"] == "ACTIVE"

    path = connection_path("ocs0", "conn-1", "state")
    protobuf_state_path = protobuf_path(path)

    class FakeSubscribeCall:
        def __init__(self) -> None:
            self.cancelled = False
            self.responses = iter(
                (
                    gnmi_pb2.SubscribeResponse(sync_response=True),
                    gnmi_pb2.SubscribeResponse(
                        update=gnmi_pb2.Notification(
                            timestamp=789,
                            update=[
                                gnmi_pb2.Update(
                                    path=protobuf_state_path,
                                    val=gnmi_pb2.TypedValue(
                                        json_ietf_val=b'{"apply-status":"ACTIVE"}'
                                    ),
                                )
                            ],
                        )
                    ),
                    gnmi_pb2.SubscribeResponse(
                        update=gnmi_pb2.Notification(
                            timestamp=790,
                            delete=[protobuf_state_path],
                        )
                    ),
                )
            )

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.responses)
            except StopIteration as error:
                raise StopAsyncIteration from error

        def cancel(self) -> None:
            self.cancelled = True

    class FakeSubscribeStub:
        def __init__(self) -> None:
            self.call = FakeSubscribeCall()
            self.requests = None
            self.timeout = None

        def Subscribe(self, requests, *, timeout):
            self.requests = requests
            self.timeout = timeout
            return self.call

    stub = FakeSubscribeStub()
    subscribing_client = GnmiClient(timeout_seconds=2.0, stub=stub)
    events = [event async for event in subscribing_client.subscribe(path)]

    assert events == [
        {"sync-response": True},
        {
            "timestamp": 789,
            "path": path,
            "value": {"apply-status": "ACTIVE"},
        },
        {"timestamp": 790, "path": path, "deleted": True},
    ]
    assert stub.timeout == 2.0
    assert stub.call.cancelled is True
    request = await anext(stub.requests)
    assert request.subscribe.mode == gnmi_pb2.SubscriptionList.STREAM
    assert request.subscribe.subscription[0].mode == gnmi_pb2.ON_CHANGE
    await stub.requests.aclose()


def test_cli_exposes_management_commands_and_has_no_redis_imports() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    connection_help = runner.invoke(app, ["connection", "--help"])

    assert result.exit_code == 0
    assert "capabilities" in result.stdout
    assert "connection" in result.stdout
    assert "diagnostics" in result.stdout
    assert connection_help.exit_code == 0
    for command in ("batch", "create", "delete", "list", "replace", "watch"):
        assert command in connection_help.stdout

    package_root = Path(__file__).parents[2] / "ocsctl"
    imported_modules: set[str] = set()
    for source_path in package_root.glob("*.py"):
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.add(node.module)
    assert not any(module == "redis" or module.startswith("redis.") for module in imported_modules)
