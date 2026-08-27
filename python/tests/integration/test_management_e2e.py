from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import grpc
import pytest
from gnmi_server.get_repository import RedisGetRepository
from gnmi_server.get_transaction import GetTransaction
from gnmi_server.redis_keys import device_config_key
from gnmi_server.redis_repository import RedisConfigRepository, RedisSettings, create_redis_client
from gnmi_server.server import create_server
from gnmi_server.service import GnmiService
from gnmi_server.set_transaction import SetTransaction
from gnmi_server.subscribe_repository import RedisSubscribeRepository
from gnmi_server.subscribe_transaction import SubscribeTransaction
from ocsctl.client import GnmiClient, connection_path

pytestmark = pytest.mark.skipif(
    not os.getenv("OCS_REDIS_SOCKET"),
    reason="set OCS_REDIS_SOCKET to run management E2E tests",
)

REPOSITORY_ROOT = Path(__file__).parents[3]


async def _wait_for_path(path: Path, timeout_seconds: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        if path.exists():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError(f"process did not create {path}")


async def _stop_process(process: asyncio.subprocess.Process) -> str:
    if process.returncode is None:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except TimeoutError:
            process.kill()
            await process.wait()
    output = await process.stdout.read() if process.stdout is not None else b""
    return output.decode("utf-8", errors="replace")


async def _run_cli(target: str, *arguments: str) -> tuple[int, str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "python")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "ocsctl.main",
        "--target",
        target,
        "--timeout-seconds",
        "5",
        "--json",
        *arguments,
        cwd=REPOSITORY_ROOT,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


async def test_scenarios_a_and_b_through_ocsctl_and_gnmi(tmp_path: Path) -> None:
    redis_socket = os.environ["OCS_REDIS_SOCKET"]
    settings = RedisSettings(unix_socket=redis_socket)
    cleanup_clients = [create_redis_client(settings, database) for database in (0, 1, 2, 4, 6, 8)]
    for client in cleanup_clients:
        await client.flushdb()
    await cleanup_clients[3].hset(
        device_config_key("ocs0"),
        mapping={
            "name": "ocs0",
            "input_port_count": "16",
            "output_port_count": "16",
            "admin_status": "ENABLED",
        },
    )

    hwsim_socket = tmp_path / "ocs-hwsim.sock"
    executable_root = REPOSITORY_ROOT / "build" / "dev" / "cpp"
    processes: list[asyncio.subprocess.Process] = []
    service = GnmiService(
        SetTransaction(RedisConfigRepository(settings)),
        GetTransaction(RedisGetRepository(settings)),
        SubscribeTransaction(RedisSubscribeRepository(settings)),
    )
    server, port = create_server("127.0.0.1:0", service)
    await server.start()
    target = f"127.0.0.1:{port}"
    process_logs: list[str] = []
    try:
        hwsim = await asyncio.create_subprocess_exec(
            executable_root / "ocs-hwsim",
            hwsim_socket,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        processes.append(hwsim)
        await _wait_for_path(hwsim_socket)
        processes.append(
            await asyncio.create_subprocess_exec(
                executable_root / "ocs-orch",
                redis_socket,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        )
        processes.append(
            await asyncio.create_subprocess_exec(
                executable_root / "ocs-syncd",
                redis_socket,
                hwsim_socket,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        )
        await asyncio.sleep(0.1)

        code, stdout, stderr = await _run_cli(target, "capabilities")
        assert code == 0, stderr
        assert json.loads(stdout)["models"][0]["name"] == "mini-ocs-native"

        code, stdout, stderr = await _run_cli(
            target,
            "connection",
            "batch",
            "ocs0",
            "--connection",
            "conn-1:1:9",
            "--connection",
            "conn-2:2:10",
            "--connection",
            "conn-3:3:11",
        )
        assert code == 0, stderr
        assert len(json.loads(stdout)["operations"]) == 3

        async with GnmiClient(target, timeout_seconds=5.0) as client:
            states = [
                await client.wait_for_connection("ocs0", f"conn-{index}")
                for index in (1, 2, 3)
            ]
            assert all(state["apply-status"] == "ACTIVE" for state in states)
            assert all(
                state["desired-version"] == state["applied-version"] == 1
                for state in states
            )
            counters = await client.get("/ocs/devices/device[name=ocs0]/counters")
            assert counters["active-connections"] == 3

        code, stdout, stderr = await _run_cli(
            target,
            "get",
            connection_path("ocs0", "conn-1", "state"),
        )
        assert code == 0, stderr
        cli_state = json.loads(stdout)
        assert cli_state["desired-version"] == cli_state["applied-version"] == 1

        watch_environment = os.environ.copy()
        watch_environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "python")
        watch = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "ocsctl.main",
            "--target",
            target,
            "--json",
            "connection",
            "watch",
            "ocs0",
            "--duration-seconds",
            "2",
            cwd=REPOSITORY_ROOT,
            env=watch_environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.sleep(0.3)
        code, _, stderr = await _run_cli(
            target,
            "connection",
            "replace",
            "ocs0",
            "conn-2",
            "--input",
            "2",
            "--output",
            "12",
        )
        assert code == 0, stderr
        watch_stdout, watch_stderr = await watch.communicate()
        assert watch.returncode == 0, watch_stderr.decode()
        assert '"sync-response": true' in watch_stdout.decode()
        assert '"applied-version": 2' in watch_stdout.decode()

        code, _, stderr = await _run_cli(target, "connection", "delete", "ocs0", "conn-2")
        assert code == 0, stderr
        async with GnmiClient(target, timeout_seconds=5.0) as client:
            deadline = asyncio.get_running_loop().time() + 5.0
            while True:
                try:
                    await client.get(connection_path("ocs0", "conn-2", "state"))
                except grpc.aio.AioRpcError as error:
                    if error.code() is grpc.StatusCode.NOT_FOUND:
                        break
                    raise
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("deleted connection state remained present")
                await asyncio.sleep(0.05)
            before_conflict = await client.get(
                "/ocs/devices/device[name=ocs0]/connections"
            )
            counters_before = await client.get("/ocs/devices/device[name=ocs0]/counters")
            assert counters_before["active-connections"] == 2

        code, _, stderr = await _run_cli(
            target,
            "connection",
            "batch",
            "ocs0",
            "--connection",
            "conflict-1:4:12",
            "--connection",
            "conflict-2:5:12",
        )
        assert code == 1
        assert "INVALID_ARGUMENT" in stderr

        async with GnmiClient(target, timeout_seconds=5.0) as client:
            assert await client.get("/ocs/devices/device[name=ocs0]/connections") == before_conflict
            assert await client.get("/ocs/devices/device[name=ocs0]/counters") == counters_before
            for connection_id in ("conflict-1", "conflict-2"):
                with pytest.raises(grpc.aio.AioRpcError) as missing:
                    await client.get(connection_path("ocs0", connection_id, "config"))
                assert missing.value.code() is grpc.StatusCode.NOT_FOUND
    finally:
        await server.stop(0.5)
        await service.close()
        for process in reversed(processes):
            process_logs.append(await _stop_process(process))
        for client in cleanup_clients:
            await client.aclose()
        if any(process.returncode not in {0, -15} for process in processes):
            pytest.fail("service process failed:\n" + "\n".join(process_logs))
