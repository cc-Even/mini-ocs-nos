from __future__ import annotations

import asyncio
import json

import pytest
from gnmi_server.errors import InvalidArgumentError
from gnmi_server.path_parser import PathKind, protobuf_path
from gnmi_server.proto import gnmi_pb2
from gnmi_server.redis_keys import ALARM_EVENTS, STATE_EVENTS
from gnmi_server.subscribe_repository import StreamEvent
from gnmi_server.subscribe_transaction import SubscribeTransaction, parse_subscribe_request


class FakeSubscribeRepository:
    def __init__(self) -> None:
        self.snapshots = {}
        self.events: asyncio.Queue[tuple[StreamEvent, ...]] = asyncio.Queue()
        self.cursor_streams = None
        self.active_reads = 0
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def snapshot(self, path):
        key = (path.kind, path.device, path.connection_id, path.port_id, path.alarm_id)
        return self.snapshots.get(key)

    async def cursors(self, streams):
        self.cursor_streams = streams
        return {stream: "1-0" for stream in streams}

    async def next_events(self, cursors):
        del cursors
        self.active_reads += 1
        try:
            return await self.events.get()
        finally:
            self.active_reads -= 1


def _subscribe_request(
    path: str,
    *,
    subscription_mode: int = gnmi_pb2.ON_CHANGE,
    list_mode: int = gnmi_pb2.SubscriptionList.STREAM,
    encoding: int = gnmi_pb2.JSON_IETF,
    updates_only: bool = False,
) -> gnmi_pb2.SubscribeRequest:
    return gnmi_pb2.SubscribeRequest(
        subscribe=gnmi_pb2.SubscriptionList(
            subscription=[
                gnmi_pb2.Subscription(
                    path=protobuf_path(path),
                    mode=subscription_mode,
                )
            ],
            mode=list_mode,
            encoding=encoding,
            updates_only=updates_only,
        )
    )


async def _requests(request):
    yield request
    await asyncio.Future()


def _event(
    resource_id: str,
    *,
    resource_type: str = "connection",
    operation: str = "UPSERT",
    timestamp_ns: int = 200,
) -> StreamEvent:
    return StreamEvent(
        stream=ALARM_EVENTS if resource_type == "alarm" else STATE_EVENTS,
        message_id="2-0",
        timestamp_ns=timestamp_ns,
        device="ocs0",
        resource_type=resource_type,
        resource_id=resource_id,
        operation=operation,
    )


async def test_on_change_sends_snapshot_sync_filtered_update_and_cleans_tasks() -> None:
    repository = FakeSubscribeRepository()
    key = (PathKind.CONNECTION_STATE, "ocs0", "conn-1", None, None)
    repository.snapshots[key] = {"apply-status": "ACTIVE", "applied-version": 1}
    transaction = SubscribeTransaction(repository, timestamp_factory=lambda: 100)
    responses = transaction.stream(
        _requests(
            _subscribe_request(
                "/ocs/devices/device[name=ocs0]/connections/connection[id=conn-1]/state"
            )
        )
    )

    initial = await anext(responses)
    assert initial.update.timestamp == 100
    assert json.loads(initial.update.update[0].val.json_ietf_val) == {
        "applied-version": 1,
        "apply-status": "ACTIVE",
    }
    assert (await anext(responses)).sync_response is True
    assert repository.cursor_streams == frozenset({STATE_EVENTS})

    repository.snapshots[key] = {"apply-status": "FAILED", "applied-version": 1}
    next_response = asyncio.create_task(anext(responses))
    await repository.events.put((_event("other"),))
    await asyncio.sleep(0)
    assert not next_response.done()
    await repository.events.put((_event("conn-1", timestamp_ns=201),))
    changed = await asyncio.wait_for(next_response, timeout=1.0)
    assert changed.update.timestamp == 201
    assert json.loads(changed.update.update[0].val.json_ietf_val)["apply-status"] == "FAILED"

    pending = asyncio.create_task(anext(responses))
    for _ in range(10):
        if repository.active_reads:
            break
        await asyncio.sleep(0)
    assert repository.active_reads == 1
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    assert repository.active_reads == 0


async def test_missing_initial_resource_can_be_created_and_deleted() -> None:
    repository = FakeSubscribeRepository()
    key = (PathKind.ALARM, "ocs0", None, None, "alarm-1")
    transaction = SubscribeTransaction(repository)
    responses = transaction.stream(
        _requests(
            _subscribe_request(
                "/ocs/devices/device[name=ocs0]/alarms/alarm[id=alarm-1]"
            )
        )
    )

    assert (await anext(responses)).sync_response is True
    repository.snapshots[key] = {"id": "alarm-1", "severity": "MAJOR"}
    created_task = asyncio.create_task(anext(responses))
    await repository.events.put((_event("alarm-1", resource_type="alarm"),))
    created = await asyncio.wait_for(created_task, timeout=1.0)
    assert json.loads(created.update.update[0].val.json_ietf_val)["severity"] == "MAJOR"

    repository.snapshots.pop(key)
    deleted_task = asyncio.create_task(anext(responses))
    await repository.events.put(
        (_event("alarm-1", resource_type="alarm", operation="REMOVE"),)
    )
    deleted = await asyncio.wait_for(deleted_task, timeout=1.0)
    assert list(deleted.update.delete) == [
        protobuf_path("/ocs/devices/device[name=ocs0]/alarms/alarm[id=alarm-1]")
    ]
    await responses.aclose()


async def test_updates_only_captures_baseline_and_suppresses_unchanged_event() -> None:
    repository = FakeSubscribeRepository()
    key = (PathKind.CONNECTION_STATE, "ocs0", "conn-2", None, None)
    repository.snapshots[key] = {"apply-status": "ACTIVE", "applied-version": 1}
    transaction = SubscribeTransaction(repository)
    responses = transaction.stream(
        _requests(
            _subscribe_request(
                "/ocs/devices/device[name=ocs0]/connections/connection[id=conn-2]/state",
                updates_only=True,
            )
        )
    )

    assert (await anext(responses)).sync_response is True
    changed_task = asyncio.create_task(anext(responses))
    await repository.events.put((_event("conn-2"),))
    await asyncio.sleep(0)
    assert not changed_task.done()
    repository.snapshots[key] = {"apply-status": "ACTIVE", "applied-version": 2}
    await repository.events.put((_event("conn-2", timestamp_ns=202),))
    changed = await asyncio.wait_for(changed_task, timeout=1.0)
    assert json.loads(changed.update.update[0].val.json_ietf_val)["applied-version"] == 2
    await responses.aclose()


@pytest.mark.parametrize(
    ("case_request", "message"),
    [
        (gnmi_pb2.SubscribeRequest(poll=gnmi_pb2.Poll()), "first SubscribeRequest"),
        (
            _subscribe_request(
                "/ocs/devices/device[name=ocs0]/state",
            ),
            "only supports connection, port, and alarm",
        ),
        (
            _subscribe_request(
                "/ocs/devices/device[name=ocs0]/alarms",
                subscription_mode=gnmi_pb2.SAMPLE,
            ),
            "only support ON_CHANGE",
        ),
        (
            _subscribe_request(
                "/ocs/devices/device[name=ocs0]/alarms",
                list_mode=gnmi_pb2.SubscriptionList.ONCE,
            ),
            "STREAM mode",
        ),
        (
            _subscribe_request(
                "/ocs/devices/device[name=ocs0]/alarms",
                encoding=gnmi_pb2.JSON,
            ),
            "encoding must be JSON_IETF",
        ),
    ],
)
def test_subscribe_rejects_unsupported_modes_and_paths(case_request, message: str) -> None:
    with pytest.raises(InvalidArgumentError, match=message):
        parse_subscribe_request(case_request)
