from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from gnmi_server.get_repository import RedisGetRepository
from gnmi_server.get_transaction import GetTransaction
from gnmi_server.redis_keys import (
    APPL_DB,
    CONFIG_DB,
    COUNTERS_DB,
    DEVICE_COMMANDS,
    DEVICE_DB,
    DEVICE_RESULTS,
    STATE_DB,
    STATE_EVENTS,
    device_counters_key,
)
from gnmi_server.redis_repository import RedisConfigRepository, RedisSettings, create_redis_client
from gnmi_server.server import create_server
from gnmi_server.service import GnmiService
from gnmi_server.set_transaction import SetTransaction
from gnmi_server.subscribe_repository import RedisSubscribeRepository
from gnmi_server.subscribe_transaction import SubscribeTransaction
from ocsctl.client import ConnectionSpec, GnmiClient

pytestmark = pytest.mark.skipif(
    not os.getenv("OCS_REDIS_SOCKET"),
    reason="set OCS_REDIS_SOCKET to run pending recovery E2E tests",
)

REPOSITORY_ROOT = Path(__file__).parents[3]


async def _wait_for_path(path: Path) -> None:
    for _ in range(100):
        if path.exists():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError(f"process did not create {path}")


async def _stop(process: asyncio.subprocess.Process) -> str:
    if process.returncode is None:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except TimeoutError:
            process.kill()
            await process.wait()
    output = await process.stdout.read() if process.stdout is not None else b""
    return output.decode("utf-8", errors="replace")


async def test_crashed_syncd_claims_pending_without_duplicate_side_effects(
    tmp_path: Path,
) -> None:
    redis_socket = os.environ["OCS_REDIS_SOCKET"]
    settings = RedisSettings(unix_socket=redis_socket)
    clients = {
        database: create_redis_client(settings, database)
        for database in (APPL_DB, DEVICE_DB, COUNTERS_DB, CONFIG_DB, STATE_DB)
    }
    for client in clients.values():
        await client.flushdb()

    service = GnmiService(
        SetTransaction(RedisConfigRepository(settings)),
        GetTransaction(RedisGetRepository(settings)),
        SubscribeTransaction(RedisSubscribeRepository(settings)),
    )
    server, port = create_server("127.0.0.1:0", service)
    await server.start()
    executable_root = REPOSITORY_ROOT / "build" / "dev" / "cpp"
    hwsim_socket = tmp_path / "recovery-hwsim.sock"
    processes: list[asyncio.subprocess.Process] = []
    logs: list[str] = []
    crashing_syncd: asyncio.subprocess.Process | None = None
    try:
        hwsim = await asyncio.create_subprocess_exec(
            executable_root / "ocs-hwsim",
            hwsim_socket,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        processes.append(hwsim)
        await _wait_for_path(hwsim_socket)
        orch = await asyncio.create_subprocess_exec(
            executable_root / "ocs-orch",
            redis_socket,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        processes.append(orch)
        crash_environment = os.environ.copy()
        crash_environment["OCS_SYNCD_CRASH_BEFORE_ACK_ONCE"] = "1"
        crash_environment["OCS_SYNCD_PENDING_MIN_IDLE_MS"] = "100"
        crashing_syncd = await asyncio.create_subprocess_exec(
            executable_root / "ocs-syncd",
            redis_socket,
            hwsim_socket,
            env=crash_environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        processes.append(crashing_syncd)
        await asyncio.sleep(0.1)

        async with GnmiClient(f"127.0.0.1:{port}", timeout_seconds=5.0) as client:
            await client.set_connections("ocs0", [ConnectionSpec("conn-recovery", 6, 13)])
        assert await asyncio.wait_for(crashing_syncd.wait(), timeout=5.0) == 86

        pending = await clients[DEVICE_DB].xpending_range(
            DEVICE_COMMANDS, "ocs-syncd", "-", "+", 10
        )
        assert len(pending) == 1
        counters_before = await clients[COUNTERS_DB].hgetall(device_counters_key("ocs0"))
        assert counters_before["device_apply_total"] == "1"
        assert counters_before["active_connections"] == "1"
        assert await clients[STATE_DB].xlen(STATE_EVENTS) == 1
        assert await clients[DEVICE_DB].xlen(DEVICE_RESULTS) == 1

        recovery_environment = os.environ.copy()
        recovery_environment["OCS_SYNCD_PENDING_MIN_IDLE_MS"] = "100"
        recovered_syncd = await asyncio.create_subprocess_exec(
            executable_root / "ocs-syncd",
            redis_socket,
            hwsim_socket,
            env=recovery_environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        processes.append(recovered_syncd)
        deadline = asyncio.get_running_loop().time() + 5.0
        while await clients[DEVICE_DB].xpending_range(
            DEVICE_COMMANDS, "ocs-syncd", "-", "+", 10
        ):
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("restarted syncd did not claim pending command")
            await asyncio.sleep(0.05)

        async with GnmiClient(f"127.0.0.1:{port}", timeout_seconds=2.0) as client:
            state = await client.wait_for_connection("ocs0", "conn-recovery")
        assert state["apply-status"] == "ACTIVE"
        assert state["desired-version"] == state["applied-version"] == 1
        assert await clients[COUNTERS_DB].hgetall(
            device_counters_key("ocs0")
        ) == counters_before
        assert await clients[STATE_DB].xlen(STATE_EVENTS) == 1
        assert await clients[DEVICE_DB].xlen(DEVICE_RESULTS) == 1
    finally:
        await server.stop(0.5)
        await service.close()
        for process in reversed(processes):
            logs.append(await _stop(process))
        for client in clients.values():
            await client.aclose()
        unexpected = [
            process.returncode
            for process in processes
            if process is not crashing_syncd and process.returncode not in {0, -15}
        ]
        if unexpected:
            pytest.fail(f"service exit codes {unexpected}:\n" + "\n".join(logs))
