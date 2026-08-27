"""Human and JSON command-line workflows over the gNMI management API."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Annotated, Any

import grpc
import typer

from ocsctl.client import ConnectionSpec, GnmiClient

app = typer.Typer(no_args_is_help=True, help="Manage mini-ocs-nos exclusively through gNMI.")
device_app = typer.Typer(no_args_is_help=True)
port_app = typer.Typer(no_args_is_help=True)
connection_app = typer.Typer(no_args_is_help=True)
alarm_app = typer.Typer(no_args_is_help=True)
counters_app = typer.Typer(no_args_is_help=True)
diagnostics_app = typer.Typer(no_args_is_help=True)
fault_app = typer.Typer(no_args_is_help=True)
app.add_typer(device_app, name="device")
app.add_typer(port_app, name="port")
app.add_typer(connection_app, name="connection")
app.add_typer(alarm_app, name="alarm")
app.add_typer(counters_app, name="counters")
app.add_typer(diagnostics_app, name="diagnostics")
app.add_typer(fault_app, name="fault")


@dataclass(frozen=True)
class CliSettings:
    target: str
    timeout_seconds: float
    json_output: bool


@app.callback()
def configure(
    context: typer.Context,
    target: Annotated[str, typer.Option("--target", help="gNMI host:port.")] =
    "127.0.0.1:50051",
    timeout_seconds: Annotated[
        float, typer.Option("--timeout-seconds", min=0.1, help="RPC deadline.")
    ] = 3.0,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    context.obj = CliSettings(target, timeout_seconds, json_output)


def _run(operation) -> Any:
    try:
        return asyncio.run(operation)
    except grpc.aio.AioRpcError as error:
        typer.echo(f"{error.code().name}: {error.details()}", err=True)
        raise typer.Exit(1) from error
    except (TimeoutError, ValueError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(1) from error


def _emit(value: Any, settings: CliSettings) -> None:
    if settings.json_output:
        typer.echo(json.dumps(value, indent=2, sort_keys=True))
        return
    if isinstance(value, dict):
        for key, item in value.items():
            rendered = json.dumps(item, sort_keys=True) if isinstance(item, dict | list) else item
            typer.echo(f"{key}\t{rendered}")
        return
    if isinstance(value, list):
        for item in value:
            typer.echo(json.dumps(item, sort_keys=True))
        return
    typer.echo(value)


async def _get(settings: CliSettings, path: str) -> Any:
    async with GnmiClient(settings.target, timeout_seconds=settings.timeout_seconds) as client:
        return await client.get(path)


@app.command()
def capabilities(context: typer.Context) -> None:
    settings: CliSettings = context.obj

    async def operation():
        async with GnmiClient(settings.target, timeout_seconds=settings.timeout_seconds) as client:
            return await client.capabilities()

    _emit(_run(operation()), settings)


@device_app.command("show")
def device_show(context: typer.Context, device: str) -> None:
    settings: CliSettings = context.obj
    _emit(_run(_get(settings, f"/ocs/devices/device[name={device}]")), settings)


@port_app.command("list")
def port_list(context: typer.Context, device: str) -> None:
    settings: CliSettings = context.obj
    _emit(_run(_get(settings, f"/ocs/devices/device[name={device}]/ports")), settings)


@connection_app.command("list")
def connection_list(context: typer.Context, device: str) -> None:
    settings: CliSettings = context.obj
    value = _run(_get(settings, f"/ocs/devices/device[name={device}]/connections"))
    _emit(value["connection"], settings)


async def _set_one(
    settings: CliSettings,
    device: str,
    connection_id: str,
    input_port: int,
    output_port: int,
    *,
    replace: bool,
    wait_active: bool,
) -> dict[str, Any]:
    async with GnmiClient(settings.target, timeout_seconds=settings.timeout_seconds) as client:
        result = await client.set_connections(
            device,
            [ConnectionSpec(connection_id, input_port, output_port)],
            replace=replace,
        )
        if wait_active:
            result["state"] = await client.wait_for_connection(device, connection_id)
        return result


@connection_app.command("create")
def connection_create(
    context: typer.Context,
    device: str,
    connection_id: str,
    input_port: Annotated[int, typer.Option("--input", min=1)],
    output_port: Annotated[int, typer.Option("--output", min=1)],
    wait_active: Annotated[bool, typer.Option("--wait-active/--no-wait-active")] = True,
) -> None:
    settings: CliSettings = context.obj
    result = _run(
        _set_one(
            settings,
            device,
            connection_id,
            input_port,
            output_port,
            replace=False,
            wait_active=wait_active,
        )
    )
    _emit(result, settings)


@connection_app.command("replace")
def connection_replace(
    context: typer.Context,
    device: str,
    connection_id: str,
    input_port: Annotated[int, typer.Option("--input", min=1)],
    output_port: Annotated[int, typer.Option("--output", min=1)],
    wait_active: Annotated[bool, typer.Option("--wait-active/--no-wait-active")] = True,
) -> None:
    settings: CliSettings = context.obj
    result = _run(
        _set_one(
            settings,
            device,
            connection_id,
            input_port,
            output_port,
            replace=True,
            wait_active=wait_active,
        )
    )
    _emit(result, settings)


def _parse_connection(value: str) -> ConnectionSpec:
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError("connections must use ID:INPUT:OUTPUT")
    try:
        return ConnectionSpec(parts[0], int(parts[1]), int(parts[2]))
    except ValueError as error:
        raise ValueError("connection ports must be integers") from error


@connection_app.command("batch")
def connection_batch(
    context: typer.Context,
    device: str,
    connections: Annotated[
        list[str], typer.Option("--connection", help="Repeat ID:INPUT:OUTPUT.")
    ],
    replace: Annotated[bool, typer.Option("--replace")] = False,
) -> None:
    settings: CliSettings = context.obj

    async def operation():
        async with GnmiClient(settings.target, timeout_seconds=settings.timeout_seconds) as client:
            return await client.set_connections(
                device,
                [_parse_connection(value) for value in connections],
                replace=replace,
            )

    _emit(_run(operation()), settings)


@connection_app.command("delete")
def connection_delete(context: typer.Context, device: str, connection_id: str) -> None:
    settings: CliSettings = context.obj

    async def operation():
        async with GnmiClient(settings.target, timeout_seconds=settings.timeout_seconds) as client:
            return await client.delete_connection(device, connection_id)

    _emit(_run(operation()), settings)


@connection_app.command("watch")
def connection_watch(
    context: typer.Context,
    device: str,
    duration_seconds: Annotated[
        float, typer.Option("--duration-seconds", min=0.1, help="Bounded watch deadline.")
    ] = 30.0,
) -> None:
    settings: CliSettings = context.obj
    watch_settings = CliSettings(settings.target, duration_seconds, settings.json_output)

    async def operation():
        async with GnmiClient(
            watch_settings.target, timeout_seconds=watch_settings.timeout_seconds
        ) as client:
            try:
                async for event in client.subscribe(
                    f"/ocs/devices/device[name={device}]/connections"
                ):
                    _emit(event, watch_settings)
            except grpc.aio.AioRpcError as error:
                if error.code() is not grpc.StatusCode.DEADLINE_EXCEEDED:
                    raise

    _run(operation())


@alarm_app.command("list")
def alarm_list(context: typer.Context, device: str) -> None:
    settings: CliSettings = context.obj
    value = _run(_get(settings, f"/ocs/devices/device[name={device}]/alarms"))
    _emit(value["alarm"], settings)


@counters_app.command("show")
def counters_show(context: typer.Context, device: str) -> None:
    settings: CliSettings = context.obj
    _emit(_run(_get(settings, f"/ocs/devices/device[name={device}]/counters")), settings)


@diagnostics_app.command("show")
def diagnostics_show(context: typer.Context, device: str) -> None:
    """Show bounded health, convergence, alarm, pending, and service status."""

    settings: CliSettings = context.obj
    _emit(
        _run(_get(settings, f"/ocs/devices/device[name={device}]/diagnostics")),
        settings,
    )


def _fault_id(fault: str, port: int | None) -> str:
    normalized = fault.lower().replace("_", "-")
    if normalized in {"next-apply-timeout", "next-apply-error"}:
        if port is not None:
            raise typer.BadParameter(f"{normalized} does not accept --port")
        return normalized
    if normalized in {"input-port-down", "output-port-down"}:
        if port is None or port <= 0:
            raise typer.BadParameter(f"{normalized} requires a positive --port")
        return f"{normalized}-{port}"
    raise typer.BadParameter(f"unsupported simulator fault {fault!r}")


@fault_app.command("inject")
def fault_inject(
    context: typer.Context,
    device: str,
    fault: str,
    port: Annotated[int | None, typer.Option("--port", min=1)] = None,
) -> None:
    """Inject a development-only simulator fault through gNMI and syncd."""

    settings: CliSettings = context.obj

    async def operation():
        async with GnmiClient(settings.target, timeout_seconds=settings.timeout_seconds) as client:
            return await client.inject_fault(device, _fault_id(fault, port))

    _emit(_run(operation()), settings)


@fault_app.command("clear")
def fault_clear(
    context: typer.Context,
    device: str,
    all_faults: Annotated[bool, typer.Option("--all")] = False,
    fault: Annotated[str | None, typer.Option("--fault")] = None,
    port: Annotated[int | None, typer.Option("--port", min=1)] = None,
) -> None:
    """Clear all faults or one selected development-only simulator fault."""

    if all_faults == (fault is not None):
        raise typer.BadParameter("select exactly one of --all or --fault")
    if all_faults and port is not None:
        raise typer.BadParameter("--all does not accept --port")
    fault_id = None if all_faults else _fault_id(str(fault), port)
    settings: CliSettings = context.obj

    async def operation():
        async with GnmiClient(settings.target, timeout_seconds=settings.timeout_seconds) as client:
            return await client.clear_fault(device, fault_id)

    _emit(_run(operation()), settings)


@app.command("get")
def raw_get(context: typer.Context, path: str) -> None:
    """Read any supported native path."""

    settings: CliSettings = context.obj
    _emit(_run(_get(settings, path)), settings)


if __name__ == "__main__":
    app()
