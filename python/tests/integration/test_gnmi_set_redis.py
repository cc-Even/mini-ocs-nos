from __future__ import annotations

import asyncio
import json
import os

import grpc
import pytest
import redis.asyncio as redis_async
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc
from gnmi_server.redis_keys import (
    CONFIG_DB,
    CONFIG_EVENTS,
    config_revision_key,
    connection_config_key,
)
from gnmi_server.redis_repository import RedisConfigRepository, RedisSettings
from gnmi_server.server import create_server
from gnmi_server.service import GnmiService
from gnmi_server.set_transaction import SetTransaction

pytestmark = pytest.mark.skipif(
    not os.getenv("OCS_REDIS_SOCKET"),
    reason="set OCS_REDIS_SOCKET to run Redis integration tests",
)


def _config_path(connection_id: str) -> gnmi_pb2.Path:
    return protobuf_path(
        f"/ocs/devices/device[name=ocs0]/connections/connection[id={connection_id}]/config"
    )


def _update(connection_id: str, input_port: int, output_port: int) -> gnmi_pb2.Update:
    payload = json.dumps(
        {
            "input-port": input_port,
            "output-port": output_port,
            "admin-status": "ENABLED",
        },
        separators=(",", ":"),
    ).encode()
    return gnmi_pb2.Update(
        path=_config_path(connection_id),
        val=gnmi_pb2.TypedValue(json_ietf_val=payload),
    )


async def test_conflicting_set_leaves_snapshot_revision_and_stream_unchanged() -> None:
    socket_path = os.environ["OCS_REDIS_SOCKET"]
    settings = RedisSettings(unix_socket=socket_path)
    repository = RedisConfigRepository(settings)
    observer = redis_async.Redis(
        unix_socket_path=socket_path,
        db=CONFIG_DB,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    )
    service = GnmiService(SetTransaction(repository))
    server, port = create_server("127.0.0.1:0", service)
    await repository.flush_for_test()
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = gnmi_pb2_grpc.gNMIStub(channel)
    try:
        accepted = await stub.Set(
            gnmi_pb2.SetRequest(update=[_update("existing", 1, 9)]),
            timeout=2.0,
        )
        assert accepted.timestamp > 0
        assert [response.op for response in accepted.response] == [gnmi_pb2.UpdateResult.UPDATE]

        async with asyncio.timeout(2.0):
            revision_before = await observer.get(config_revision_key("ocs0"))
            stream_length_before = await observer.xlen(CONFIG_EVENTS)
            existing_before = await observer.hgetall(
                connection_config_key("ocs0", "existing")
            )
        assert revision_before == "1"
        assert stream_length_before == 1
        assert existing_before["desired_version"] == "1"

        conflict = gnmi_pb2.SetRequest(
            update=[
                _update("conflict-a", 2, 12),
                _update("conflict-b", 3, 12),
            ]
        )
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.Set(conflict, timeout=2.0)
        assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
        assert raised.value.details().startswith("configuration conflict:")

        async with asyncio.timeout(2.0):
            assert await observer.get(config_revision_key("ocs0")) == revision_before
            assert await observer.xlen(CONFIG_EVENTS) == stream_length_before
            assert await observer.hgetall(
                connection_config_key("ocs0", "existing")
            ) == existing_before
            assert not await observer.exists(connection_config_key("ocs0", "conflict-a"))
            assert not await observer.exists(connection_config_key("ocs0", "conflict-b"))

            events = await observer.xrange(CONFIG_EVENTS)
        assert len(events) == 1
        event = events[0][1]
        assert event["operation"] == "UPSERT"
        assert event["resource_id"] == "existing"
        assert json.loads(event["payload"]) == {"input_port": 1, "output_port": 9}
    finally:
        await channel.close()
        await server.stop(0.5)
        await service.close()
        await observer.aclose()
