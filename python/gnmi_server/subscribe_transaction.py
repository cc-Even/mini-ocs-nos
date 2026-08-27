"""Validated gNMI STREAM/ON_CHANGE subscription lifecycle."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Final, Protocol

from gnmi_server.errors import InvalidArgumentError
from gnmi_server.get_transaction import validate_models
from gnmi_server.path_parser import NativePath, PathKind, parse_path
from gnmi_server.proto import gnmi_pb2
from gnmi_server.redis_keys import ALARM_EVENTS, STATE_EVENTS
from gnmi_server.subscribe_repository import StreamEvent

_SUPPORTED_KINDS: Final = {
    PathKind.CONNECTIONS,
    PathKind.CONNECTION_STATE,
    PathKind.PORTS,
    PathKind.INPUT_PORT_STATE,
    PathKind.OUTPUT_PORT_STATE,
    PathKind.ALARMS,
    PathKind.ALARM,
}
_MISSING: Final = object()


class SubscribeRepository(Protocol):
    async def close(self) -> None: ...

    async def snapshot(self, path: NativePath) -> dict[str, Any] | None: ...

    async def cursors(self, streams: frozenset[str]) -> dict[str, str]: ...

    async def next_events(self, cursors: dict[str, str]) -> tuple[StreamEvent, ...]: ...


@dataclass(frozen=True)
class SubscriptionSpec:
    native_path: NativePath
    response_path: gnmi_pb2.Path

    @property
    def stream(self) -> str:
        if self.native_path.kind in {PathKind.ALARMS, PathKind.ALARM}:
            return ALARM_EVENTS
        return STATE_EVENTS

    def matches(self, event: StreamEvent) -> bool:
        path = self.native_path
        if event.device != path.device:
            return False
        if path.kind in {PathKind.CONNECTIONS, PathKind.CONNECTION_STATE}:
            return event.resource_type == "connection" and (
                path.kind is PathKind.CONNECTIONS
                or event.resource_id == path.connection_id
            )
        if path.kind in {PathKind.PORTS, PathKind.INPUT_PORT_STATE, PathKind.OUTPUT_PORT_STATE}:
            expected_type = {
                PathKind.INPUT_PORT_STATE: "input-port",
                PathKind.OUTPUT_PORT_STATE: "output-port",
            }.get(path.kind)
            return event.resource_type in {"input-port", "output-port"} and (
                expected_type is None
                or (
                    event.resource_type == expected_type
                    and event.resource_id == str(path.port_id)
                )
            )
        return event.resource_type == "alarm" and (
            path.kind is PathKind.ALARMS or event.resource_id == path.alarm_id
        )


@dataclass(frozen=True)
class SubscriptionPlan:
    prefix: gnmi_pb2.Path
    subscriptions: tuple[SubscriptionSpec, ...]
    streams: frozenset[str]
    updates_only: bool


def _copy_path(path: gnmi_pb2.Path) -> gnmi_pb2.Path:
    copied = gnmi_pb2.Path()
    copied.CopyFrom(path)
    return copied


def parse_subscribe_request(request: gnmi_pb2.SubscribeRequest) -> SubscriptionPlan:
    if request.WhichOneof("request") != "subscribe":
        raise InvalidArgumentError("first SubscribeRequest must contain a subscription list")
    subscription_list = request.subscribe
    if subscription_list.mode != gnmi_pb2.SubscriptionList.STREAM:
        raise InvalidArgumentError("Subscribe only supports STREAM mode")
    if subscription_list.encoding != gnmi_pb2.JSON_IETF:
        raise InvalidArgumentError("Subscribe encoding must be JSON_IETF")
    if not subscription_list.subscription:
        raise InvalidArgumentError("subscription list requires at least one path")
    if subscription_list.allow_aggregation:
        raise InvalidArgumentError("subscription aggregation is not supported")
    if subscription_list.qos.marking:
        raise InvalidArgumentError("subscription QoS marking is not supported")
    validate_models(subscription_list.use_models)

    specs: list[SubscriptionSpec] = []
    for subscription in subscription_list.subscription:
        if subscription.mode not in {
            gnmi_pb2.TARGET_DEFINED,
            gnmi_pb2.ON_CHANGE,
        }:
            raise InvalidArgumentError("subscriptions only support ON_CHANGE mode")
        if (
            subscription.sample_interval
            or subscription.suppress_redundant
            or subscription.heartbeat_interval
        ):
            raise InvalidArgumentError("sample, heartbeat, and suppression options are unsupported")
        native_path = parse_path(subscription.path, prefix=subscription_list.prefix)
        if native_path.kind not in _SUPPORTED_KINDS:
            raise InvalidArgumentError(
                "Subscribe only supports connection, port, and alarm state paths"
            )
        specs.append(SubscriptionSpec(native_path, _copy_path(subscription.path)))

    prefix = _copy_path(subscription_list.prefix)
    return SubscriptionPlan(
        prefix=prefix,
        subscriptions=tuple(specs),
        streams=frozenset(spec.stream for spec in specs),
        updates_only=subscription_list.updates_only,
    )


def _json_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _notification_response(
    plan: SubscriptionPlan,
    spec: SubscriptionSpec,
    payload: bytes | object,
    timestamp_ns: int,
) -> gnmi_pb2.SubscribeResponse:
    if payload is _MISSING:
        notification = gnmi_pb2.Notification(
            timestamp=timestamp_ns,
            prefix=plan.prefix,
            delete=[spec.response_path],
        )
    else:
        assert isinstance(payload, bytes)
        notification = gnmi_pb2.Notification(
            timestamp=timestamp_ns,
            prefix=plan.prefix,
            update=[
                gnmi_pb2.Update(
                    path=spec.response_path,
                    val=gnmi_pb2.TypedValue(json_ietf_val=payload),
                )
            ],
        )
    return gnmi_pb2.SubscribeResponse(update=notification)


class SubscribeTransaction:
    """Run a subscription with race-safe baseline capture and bounded stream reads."""

    def __init__(
        self,
        repository: SubscribeRepository,
        *,
        timestamp_factory: Callable[[], int] = time.time_ns,
    ) -> None:
        self._repository = repository
        self._timestamp_factory = timestamp_factory

    async def close(self) -> None:
        await self._repository.close()

    async def stream(
        self, request_iterator: AsyncIterator[gnmi_pb2.SubscribeRequest]
    ) -> AsyncIterator[gnmi_pb2.SubscribeResponse]:
        try:
            first_request = await anext(request_iterator)
        except StopAsyncIteration as error:
            raise InvalidArgumentError("Subscribe requires an initial request") from error
        plan = parse_subscribe_request(first_request)
        cursors = await self._repository.cursors(plan.streams)

        previous: list[bytes | object] = []
        for spec in plan.subscriptions:
            snapshot = await self._repository.snapshot(spec.native_path)
            payload: bytes | object = _MISSING if snapshot is None else _json_payload(snapshot)
            previous.append(payload)
            if not plan.updates_only and payload is not _MISSING:
                yield _notification_response(
                    plan,
                    spec,
                    payload,
                    self._timestamp_factory(),
                )
        yield gnmi_pb2.SubscribeResponse(sync_response=True)

        input_task: asyncio.Task | None = asyncio.create_task(anext(request_iterator))
        event_task: asyncio.Task | None = None
        try:
            while True:
                event_task = asyncio.create_task(self._repository.next_events(cursors))
                wait_for = {event_task}
                if input_task is not None:
                    wait_for.add(input_task)
                done, _ = await asyncio.wait(wait_for, return_when=asyncio.FIRST_COMPLETED)

                if input_task is not None and input_task in done:
                    try:
                        input_task.result()
                    except StopAsyncIteration:
                        input_task = None
                    else:
                        raise InvalidArgumentError(
                            "STREAM subscriptions do not accept subsequent requests"
                        )
                if event_task not in done:
                    event_task.cancel()
                    await asyncio.gather(event_task, return_exceptions=True)
                    event_task = None
                    continue

                events = event_task.result()
                event_task = None
                for event in events:
                    for index, spec in enumerate(plan.subscriptions):
                        if not spec.matches(event):
                            continue
                        snapshot = await self._repository.snapshot(spec.native_path)
                        payload = _MISSING if snapshot is None else _json_payload(snapshot)
                        if payload == previous[index]:
                            continue
                        previous[index] = payload
                        yield _notification_response(plan, spec, payload, event.timestamp_ns)
        finally:
            for task in (input_task, event_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (input_task, event_task) if task is not None),
                return_exceptions=True,
            )
