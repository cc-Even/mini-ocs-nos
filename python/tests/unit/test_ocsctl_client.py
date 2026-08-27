from __future__ import annotations

import ast
import json
from pathlib import Path

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
