"""Versioned REST and WebSocket surface for the visualization client."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from typing import Annotated, Any, Final

import grpc
from fastapi import FastAPI, Path, Query, Request, WebSocket, status
from fastapi.responses import JSONResponse
from ocsctl.client import ConnectionSpec
from starlette.websockets import WebSocketState

from web_gateway import __version__
from web_gateway.config import GatewaySettings
from web_gateway.middleware import RequestBodyLimitMiddleware
from web_gateway.models import (
    ConnectionBatchWrite,
    ConnectionWrite,
    FaultWrite,
    SafeIdentifier,
)
from web_gateway.service import (
    ClientFactory,
    GatewayService,
    default_client_factory,
    device_path,
)

_HTTP_STATUS: Final = {
    grpc.StatusCode.INVALID_ARGUMENT: status.HTTP_422_UNPROCESSABLE_ENTITY,
    grpc.StatusCode.NOT_FOUND: status.HTTP_404_NOT_FOUND,
    grpc.StatusCode.ALREADY_EXISTS: status.HTTP_409_CONFLICT,
    grpc.StatusCode.PERMISSION_DENIED: status.HTTP_403_FORBIDDEN,
    grpc.StatusCode.UNAUTHENTICATED: status.HTTP_401_UNAUTHORIZED,
    grpc.StatusCode.RESOURCE_EXHAUSTED: status.HTTP_429_TOO_MANY_REQUESTS,
    grpc.StatusCode.DEADLINE_EXCEEDED: status.HTTP_504_GATEWAY_TIMEOUT,
    grpc.StatusCode.UNAVAILABLE: status.HTTP_503_SERVICE_UNAVAILABLE,
}
_RETRYABLE: Final = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
}


def _rpc_error(error: grpc.aio.AioRpcError) -> dict[str, Any]:
    return {
        "error": {
            "code": error.code().name,
            "message": error.details() or "gNMI request failed",
            "retryable": error.code() in _RETRYABLE,
        }
    }


def _event_paths(device: str) -> tuple[str, str, str]:
    return (
        device_path(device, "connections"),
        device_path(device, "ports"),
        device_path(device, "alarms"),
    )


async def _produce_events(
    queue: asyncio.Queue[dict[str, Any] | None],
    client_factory: ClientFactory,
    settings: GatewaySettings,
    device: str,
    duration_seconds: float,
) -> None:
    deadline = time.monotonic() + duration_seconds
    reconnects = 0
    try:
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                async with client_factory(remaining) as client:
                    async for event in client.subscribe(_event_paths(device)):
                        event_type = "sync" if "sync-response" in event else "update"
                        await queue.put({"type": event_type, **event})
                if time.monotonic() >= deadline:
                    break
                raise RuntimeError("gNMI Subscribe ended before its deadline")
            except asyncio.CancelledError:
                raise
            except grpc.aio.AioRpcError as error:
                stream_deadline = error.code() is grpc.StatusCode.DEADLINE_EXCEEDED
                if stream_deadline and time.monotonic() >= deadline:
                    break
                retries_exhausted = reconnects >= settings.websocket_reconnect_attempts
                if error.code() not in _RETRYABLE or retries_exhausted:
                    await queue.put({"type": "error", **_rpc_error(error)})
                    return
                reconnects += 1
                await queue.put(
                    {
                        "type": "reconnecting",
                        "attempt": reconnects,
                        "code": error.code().name,
                    }
                )
            except Exception:
                await queue.put(
                    {
                        "type": "error",
                        "error": {
                            "code": "BAD_GATEWAY",
                            "message": "gNMI subscription ended unexpectedly",
                            "retryable": False,
                        },
                    }
                )
                return

            delay = min(
                settings.websocket_reconnect_backoff_seconds * 2 ** (reconnects - 1),
                max(0.0, deadline - time.monotonic()),
            )
            if delay:
                await asyncio.sleep(delay)
        await queue.put(None)
    except asyncio.CancelledError:
        raise


def create_app(
    settings: GatewaySettings | None = None,
    client_factory: ClientFactory | None = None,
) -> FastAPI:
    settings = settings or GatewaySettings.from_environment()
    factory = client_factory or default_client_factory(settings.gnmi_target)
    service = GatewayService(factory, settings.rpc_timeout_seconds)
    app = FastAPI(
        title="mini-ocs web gateway",
        version=__version__,
        description="Bounded browser API translated exclusively to gNMI.",
    )
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=settings.max_request_body_bytes)

    @app.exception_handler(grpc.aio.AioRpcError)
    async def grpc_error_handler(
        request: Request, error: grpc.aio.AioRpcError
    ) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=_HTTP_STATUS.get(error.code(), status.HTTP_502_BAD_GATEWAY),
            content=_rpc_error(error),
        )

    @app.exception_handler(TimeoutError)
    async def timeout_handler(request: Request, error: TimeoutError) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={
                "error": {
                    "code": "DEADLINE_EXCEEDED",
                    "message": "gateway operation exceeded its deadline",
                    "retryable": True,
                }
            },
        )

    @app.exception_handler(RuntimeError)
    async def malformed_dependency_handler(
        request: Request, error: RuntimeError
    ) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": {
                    "code": "BAD_GATEWAY",
                    "message": "gNMI returned an invalid response",
                    "retryable": False,
                }
            },
        )

    @app.get("/healthz")
    async def health() -> dict[str, Any]:
        return await service.health()

    @app.get("/api/v1/devices/{device}/snapshot")
    async def snapshot(
        device: Annotated[SafeIdentifier, Path(description="Native OCS device name.")],
    ) -> dict[str, Any]:
        return await service.snapshot(device)

    @app.put(
        "/api/v1/devices/{device}/connections/{connection_id}",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def write_connection(
        device: Annotated[SafeIdentifier, Path()],
        connection_id: Annotated[SafeIdentifier, Path()],
        request: ConnectionWrite,
    ) -> dict[str, Any]:
        return await service.write_connections(
            device,
            [ConnectionSpec(connection_id, request.input_port, request.output_port)],
            replace=request.operation == "REPLACE",
        )

    @app.post(
        "/api/v1/devices/{device}/connections:batch",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def write_connection_batch(
        device: Annotated[SafeIdentifier, Path()], request: ConnectionBatchWrite
    ) -> dict[str, Any]:
        connections = [
            ConnectionSpec(item.id, item.input_port, item.output_port)
            for item in request.connections
        ]
        return await service.write_connections(
            device,
            connections,
            replace=request.operation == "REPLACE",
        )

    @app.delete(
        "/api/v1/devices/{device}/connections/{connection_id}",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def delete_connection(
        device: Annotated[SafeIdentifier, Path()],
        connection_id: Annotated[SafeIdentifier, Path()],
    ) -> dict[str, Any]:
        return await service.delete_connection(device, connection_id)

    @app.post("/api/v1/devices/{device}/faults")
    async def inject_fault(
        device: Annotated[SafeIdentifier, Path()], request: FaultWrite
    ) -> dict[str, Any]:
        return await service.inject_fault(device, request.fault_id)

    @app.delete("/api/v1/devices/{device}/faults")
    async def clear_faults(
        device: Annotated[SafeIdentifier, Path()],
    ) -> dict[str, Any]:
        return await service.clear_faults(device)

    @app.websocket("/api/v1/devices/{device}/events")
    async def events(
        websocket: WebSocket,
        device: Annotated[SafeIdentifier, Path()],
        duration_seconds: Annotated[float | None, Query(ge=1, le=3600)] = None,
    ) -> None:
        requested_duration = duration_seconds or settings.websocket_max_seconds
        duration = min(requested_duration, settings.websocket_max_seconds)
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(
            maxsize=settings.websocket_queue_size
        )
        await websocket.accept()
        await websocket.send_json(
            {
                "type": "ready",
                "device": device,
                "streams": ["connections", "ports", "alarms"],
                "max-duration-seconds": duration,
            }
        )
        producer = asyncio.create_task(
            _produce_events(queue, factory, settings, device, duration)
        )
        receive_task: asyncio.Task | None = asyncio.create_task(websocket.receive())
        event_task: asyncio.Task | None = asyncio.create_task(queue.get())
        disconnected = False
        try:
            while True:
                assert receive_task is not None and event_task is not None
                done, _ = await asyncio.wait(
                    {receive_task, event_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if receive_task in done:
                    message = receive_task.result()
                    if message["type"] == "websocket.disconnect":
                        disconnected = True
                        break
                    await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                    disconnected = True
                    break

                event = event_task.result()
                event_task = None
                if event is None:
                    break
                await websocket.send_json(event)
                if event.get("type") == "error":
                    await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
                    disconnected = True
                    break
                event_task = asyncio.create_task(queue.get())
        finally:
            for task in (receive_task, event_task, producer):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (receive_task, event_task, producer) if task is not None),
                return_exceptions=True,
            )
            if not disconnected and websocket.application_state is WebSocketState.CONNECTED:
                with suppress(RuntimeError):
                    await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)

    return app


app = create_app()
