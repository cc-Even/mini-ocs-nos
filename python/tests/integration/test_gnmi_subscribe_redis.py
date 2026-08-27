from __future__ import annotations

import asyncio
import json
import os

import grpc
import pytest
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc
from gnmi_server.redis_keys import STATE_DB, STATE_EVENTS, connection_state_key
from gnmi_server.redis_repository import RedisSettings, create_redis_client
from gnmi_server.server import create_server
from gnmi_server.service import GnmiService
from gnmi_server.subscribe_repository import RedisSubscribeRepository
from gnmi_server.subscribe_transaction import SubscribeTransaction

pytestmark = pytest.mark.skipif(
    not os.getenv("OCS_REDIS_SOCKET"),
    reason="set OCS_REDIS_SOCKET to run Redis integration tests",
)


def _event_fields(resource_id: str, operation: str, timestamp_ns: int) -> dict[str, str]:
    return {
        "event_schema_version": "1",
        "event_id": f"event-{timestamp_ns}",
        "request_id": f"request-{timestamp_ns}",
        "timestamp_ns": str(timestamp_ns),
        "device": "ocs0",
        "resource_type": "connection",
        "resource_id": resource_id,
        "operation": operation,
        "desired_version": "2",
        "payload": "{}",
    }


async def test_subscribe_streams_initial_sync_update_and_delete_from_state_events() -> None:
    settings = RedisSettings(unix_socket=os.environ["OCS_REDIS_SOCKET"])
    state = create_redis_client(settings, STATE_DB)
    await state.flushdb()
    state_key = connection_state_key("ocs0", "conn-1")
    await state.hset(
        state_key,
        mapping={
            "device": "ocs0",
            "id": "conn-1",
            "input_port": "3",
            "output_port": "11",
            "desired_version": "1",
            "applied_version": "1",
            "apply_status": "ACTIVE",
        },
    )

    subscribe_repository = RedisSubscribeRepository(settings)
    service = GnmiService(
        subscribe_transaction=SubscribeTransaction(subscribe_repository)
    )
    server, port = create_server("127.0.0.1:0", service)
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = gnmi_pb2_grpc.gNMIStub(channel)
    request_path = protobuf_path(
        "/ocs/devices/device[name=ocs0]/connections/connection[id=conn-1]/state"
    )

    async def requests():
        yield gnmi_pb2.SubscribeRequest(
            subscribe=gnmi_pb2.SubscriptionList(
                subscription=[
                    gnmi_pb2.Subscription(path=request_path, mode=gnmi_pb2.ON_CHANGE)
                ],
                mode=gnmi_pb2.SubscriptionList.STREAM,
                encoding=gnmi_pb2.JSON_IETF,
            )
        )
        await asyncio.Future()

    call = stub.Subscribe(requests(), timeout=5.0)
    try:
        initial = await call.read()
        assert json.loads(initial.update.update[0].val.json_ietf_val)["apply-status"] == "ACTIVE"
        assert (await call.read()).sync_response is True

        await state.xadd(STATE_EVENTS, _event_fields("other", "UPSERT", 300))
        await state.hset(
            state_key,
            mapping={
                "device": "ocs0",
                "id": "conn-1",
                "input_port": "4",
                "output_port": "12",
                "desired_version": "2",
                "applied_version": "2",
                "apply_status": "ACTIVE",
            },
        )
        await state.xadd(STATE_EVENTS, _event_fields("conn-1", "UPSERT", 301))
        changed = await call.read()
        changed_payload = json.loads(changed.update.update[0].val.json_ietf_val)
        assert changed.update.timestamp == 301
        assert changed_payload["input-port"] == 4
        assert changed_payload["applied-version"] == 2

        await state.delete(state_key)
        await state.xadd(STATE_EVENTS, _event_fields("conn-1", "REMOVE", 302))
        deleted = await call.read()
        assert list(deleted.update.delete) == [request_path]
    finally:
        call.cancel()
        await channel.close()
        await server.stop(0.5)
        await service.close()
        await state.aclose()
