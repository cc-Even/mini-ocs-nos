"""Bounded health probes for the packaged mini-ocs-nos services."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections.abc import Sequence
from typing import Any, Final

from ocsctl.client import GnmiClient

from gnmi_server.redis_keys import (
    CONFIG_DB,
    CONFIG_EVENTS,
    DEVICE_COMMANDS,
    DEVICE_DB,
    DEVICE_RESULTS,
    DEVICE_RETRIES,
    STATE_DB,
    device_state_key,
    service_state_key,
)
from gnmi_server.redis_repository import RedisSettings, create_redis_client

DEFAULT_MAX_HEARTBEAT_AGE_SECONDS: Final = 2.0
DEFAULT_MAX_STREAM_LAG: Final = 1000


def _heartbeat_ready(fields: dict[str, str], now_ns: int, max_age_seconds: float) -> bool:
    try:
        last_seen_ns = int(fields.get("last_seen_ns", "0"))
    except ValueError:
        return False
    age_ns = now_ns - last_seen_ns
    return fields.get("status") == "ONLINE" and 0 <= age_ns <= max_age_seconds * 1e9


async def _group_status(client, stream: str, group: str) -> dict[str, int]:
    groups = await client.xinfo_groups(stream)
    matching = next((entry for entry in groups if entry.get("name") == group), None)
    if matching is None:
        raise RuntimeError(f"consumer group {group!r} is missing for {stream!r}")
    lag = matching.get("lag")
    return {
        "lag": int(lag) if lag is not None else 0,
        "pending": int(matching.get("pending", 0)),
    }


async def _redis_health() -> dict[str, Any]:
    settings = RedisSettings.from_environment()
    client = create_redis_client(settings, CONFIG_DB)
    try:
        pong = await client.ping()
        if not pong:
            raise RuntimeError("Redis ping did not return PONG")
        return {"redis": "PONG"}
    finally:
        await client.aclose()


async def _service_health(service: str) -> dict[str, Any]:
    settings = RedisSettings.from_environment()
    state = create_redis_client(settings, STATE_DB)
    config = create_redis_client(settings, CONFIG_DB)
    device = create_redis_client(settings, DEVICE_DB)
    try:
        await state.ping()
        now_ns = time.time_ns()
        heartbeat = await state.hgetall(service_state_key(service))
        max_age = float(
            os.getenv(
                "OCS_HEALTH_MAX_HEARTBEAT_AGE_SECONDS",
                str(DEFAULT_MAX_HEARTBEAT_AGE_SECONDS),
            )
        )
        if max_age <= 0 or not _heartbeat_ready(heartbeat, now_ns, max_age):
            raise RuntimeError(f"{service} heartbeat is missing, stale, or offline")

        max_lag = int(os.getenv("OCS_HEALTH_MAX_STREAM_LAG", str(DEFAULT_MAX_STREAM_LAG)))
        if max_lag < 0:
            raise RuntimeError("OCS_HEALTH_MAX_STREAM_LAG must not be negative")
        groups: dict[str, dict[str, int]] = {}
        if service == "ocs-orch":
            groups = {
                "config-events": await _group_status(config, CONFIG_EVENTS, "ocs-orch"),
                "device-results": await _group_status(device, DEVICE_RESULTS, "ocs-orch"),
                "device-retries": await _group_status(
                    device, DEVICE_RETRIES, "ocs-orch-retry"
                ),
            }
        elif service == "ocs-syncd":
            groups = {
                "device-commands": await _group_status(
                    device, DEVICE_COMMANDS, "ocs-syncd"
                )
            }
            hwsim = await state.hgetall(service_state_key("ocs-hwsim"))
            if not _heartbeat_ready(hwsim, now_ns, max_age):
                raise RuntimeError("ocs-syncd has no ready UDS device dependency")
            device_state = await state.hgetall(device_state_key("ocs0"))
            if (
                device_state.get("oper_status") != "READY"
                or int(device_state.get("device_generation", "0")) <= 0
            ):
                raise RuntimeError("ocs-syncd has not confirmed a ready device generation")
        else:
            raise RuntimeError(f"unsupported service health probe: {service}")

        if any(status["lag"] > max_lag for status in groups.values()):
            raise RuntimeError(f"{service} consumer lag exceeds {max_lag}")
        return {"service": service, "status": "READY", "consumer-groups": groups}
    finally:
        await asyncio.gather(state.aclose(), config.aclose(), device.aclose())


async def _gnmi_health(target: str) -> dict[str, Any]:
    timeout = float(os.getenv("OCS_HEALTH_RPC_TIMEOUT_SECONDS", "1"))
    if timeout <= 0:
        raise RuntimeError("OCS_HEALTH_RPC_TIMEOUT_SECONDS must be positive")
    async with GnmiClient(target, timeout_seconds=timeout) as client:
        capabilities = await client.capabilities()
        diagnostics = await client.get("/ocs/devices/device[name=ocs0]/diagnostics")
    if diagnostics.get("device-health") != "READY":
        raise RuntimeError("gNMI dependency device is not READY")
    if not diagnostics.get("core-services-online"):
        raise RuntimeError("gNMI diagnostics report an offline core service")
    return {
        "service": "gnmi-server",
        "status": "READY",
        "gnmi-version": capabilities["gnmi-version"],
        "dependencies": diagnostics["services"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe packaged service readiness")
    subcommands = parser.add_subparsers(dest="probe", required=True)
    subcommands.add_parser("redis")
    service = subcommands.add_parser("service")
    service.add_argument("name", choices=("ocs-orch", "ocs-syncd"))
    gnmi = subcommands.add_parser("gnmi")
    gnmi.add_argument("--target", default="127.0.0.1:50051")
    return parser


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.probe == "redis":
        return await _redis_health()
    if args.probe == "service":
        return await _service_health(args.name)
    return await _gnmi_health(args.target)


def main(argv: Sequence[str] | None = None) -> None:
    """Run one bounded readiness probe and emit its machine-readable result."""

    args = _parser().parse_args(argv)
    try:
        result = asyncio.run(_run(args))
    except Exception as error:
        print(json.dumps({"status": "NOT_READY", "error": str(error)}, sort_keys=True))
        raise SystemExit(1) from error
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
