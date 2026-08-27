from __future__ import annotations

import asyncio
import json
import os

import grpc
import pytest
import redis.asyncio as redis_async
from gnmi_server.errors import ConflictError
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc
from gnmi_server.redis_keys import (
    CONFIG_DB,
    CONFIG_EVENTS,
    COUNTERS_DB,
    config_revision_key,
    connection_config_key,
    device_config_key,
    device_counters_key,
    input_port_config_key,
    output_port_config_key,
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
    counter_observer = redis_async.Redis(
        unix_socket_path=socket_path,
        db=COUNTERS_DB,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    )
    service = GnmiService(SetTransaction(repository))
    server, port = create_server("127.0.0.1:0", service)
    await repository.flush_for_test()
    await observer.hset(
        device_config_key("ocs0"),
        mapping={
            "name": "ocs0",
            "input_port_count": "16",
            "output_port_count": "16",
            "admin_status": "ENABLED",
        },
    )
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = gnmi_pb2_grpc.gNMIStub(channel)
    try:
        accepted = await stub.Set(
            gnmi_pb2.SetRequest(
                update=[_update("existing", 1, 9), _update("peer", 2, 10)]
            ),
            timeout=2.0,
        )
        assert accepted.timestamp > 0
        assert [response.op for response in accepted.response] == [
            gnmi_pb2.UpdateResult.UPDATE,
            gnmi_pb2.UpdateResult.UPDATE,
        ]

        swapped = await stub.Set(
            gnmi_pb2.SetRequest(
                update=[_update("existing", 1, 10), _update("peer", 2, 9)]
            ),
            timeout=2.0,
        )
        assert len(swapped.response) == 2

        async with asyncio.timeout(2.0):
            revision_before = await observer.get(config_revision_key("ocs0"))
            stream_length_before = await observer.xlen(CONFIG_EVENTS)
            existing_before = await observer.hgetall(
                connection_config_key("ocs0", "existing")
            )
        assert revision_before == "2"
        assert stream_length_before == 2
        assert existing_before["desired_version"] == "2"
        assert existing_before["output_port"] == "10"

        conflict = gnmi_pb2.SetRequest(
            update=[
                _update("conflict-a", 3, 12),
                _update("conflict-b", 4, 12),
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
        assert len(events) == 2
        event = events[-1][1]
        assert event["operation"] == "APPLY_BATCH"
        assert event["resource_type"] == "connection-batch"
        payload = json.loads(event["payload"])
        assert payload["atomic"] is True
        assert payload["commands"] == [
            {
                "desired_version": 2,
                "id": "existing",
                "input_port": 1,
                "operation": "UPSERT",
                "output_port": 10,
            },
            {
                "desired_version": 2,
                "id": "peer",
                "input_port": 2,
                "operation": "UPSERT",
                "output_port": 9,
            },
        ]
        counters = await counter_observer.hgetall(device_counters_key("ocs0"))
        assert counters["config_requests_total"] == "3"
        assert counters["config_rejected_total"] == "1"
    finally:
        await channel.close()
        await server.stop(0.5)
        await service.close()
        await observer.aclose()
        await counter_observer.aclose()


async def test_set_rejects_unknown_device_and_disabled_port_without_writes() -> None:
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
    await repository.flush_for_test()
    try:
        service = GnmiService(SetTransaction(repository))
        server, port = create_server("127.0.0.1:0", service)
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        stub = gnmi_pb2_grpc.gNMIStub(channel)
        try:
            with pytest.raises(grpc.aio.AioRpcError) as unknown:
                await stub.Set(
                    gnmi_pb2.SetRequest(update=[_update("unknown", 1, 9)]),
                    timeout=2.0,
                )
            assert unknown.value.code() is grpc.StatusCode.NOT_FOUND

            await observer.hset(
                device_config_key("ocs0"),
                mapping={
                    "name": "ocs0",
                    "input_port_count": "16",
                    "output_port_count": "16",
                },
            )
            await observer.hset(
                input_port_config_key("ocs0", 1),
                mapping={"admin_status": "DISABLED"},
            )
            with pytest.raises(grpc.aio.AioRpcError) as disabled:
                await stub.Set(
                    gnmi_pb2.SetRequest(update=[_update("disabled", 1, 9)]),
                    timeout=2.0,
                )
            assert disabled.value.code() is grpc.StatusCode.INVALID_ARGUMENT
            assert "administratively disabled" in disabled.value.details()

            await observer.hset(
                input_port_config_key("ocs0", 1),
                mapping={"admin_status": "ENABLED"},
            )
            for _ in range(2):
                deleted = await stub.Set(
                    gnmi_pb2.SetRequest(delete=[_config_path("missing")]),
                    timeout=2.0,
                )
                assert [response.op for response in deleted.response] == [
                    gnmi_pb2.UpdateResult.DELETE
                ]

            await stub.Set(
                gnmi_pb2.SetRequest(update=[_update("existing", 1, 9)]),
                timeout=2.0,
            )
            await observer.hset(
                output_port_config_key("ocs0", 9),
                mapping={"admin_status": "DISABLED"},
            )
            for request in (
                gnmi_pb2.SetRequest(update=[_update("existing", 1, 9)]),
                gnmi_pb2.SetRequest(update=[_update("unrelated", 2, 10)]),
            ):
                with pytest.raises(grpc.aio.AioRpcError) as final_candidate:
                    await stub.Set(request, timeout=2.0)
                assert final_candidate.value.code() is grpc.StatusCode.INVALID_ARGUMENT
                assert "output port 9" in final_candidate.value.details()

            assert await observer.xlen(CONFIG_EVENTS) == 1
            assert await observer.get(config_revision_key("ocs0")) == "1"
        finally:
            await channel.close()
            await server.stop(0.5)
            await service.close()
    finally:
        await observer.aclose()


async def test_port_admin_change_racing_commit_is_revalidated() -> None:
    socket_path = os.environ["OCS_REDIS_SOCKET"]
    settings = RedisSettings(unix_socket=socket_path)
    observer = redis_async.Redis(
        unix_socket_path=socket_path,
        db=CONFIG_DB,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    )

    class RacingRepository(RedisConfigRepository):
        raced = False

        async def _validate_ports_enabled(self, pipe, device, connections) -> None:
            await super()._validate_ports_enabled(pipe, device, connections)
            if not self.raced:
                self.raced = True
                await observer.hset(
                    output_port_config_key(device, 9),
                    mapping={"admin_status": "DISABLED"},
                )

    repository = RacingRepository(settings)
    await repository.flush_for_test()
    await observer.hset(
        device_config_key("ocs0"),
        mapping={
            "name": "ocs0",
            "input_port_count": "16",
            "output_port_count": "16",
        },
    )
    try:
        transaction = SetTransaction(repository)
        with pytest.raises(ConflictError, match="output port 9"):
            await transaction.apply(
                gnmi_pb2.SetRequest(update=[_update("racing", 1, 9)])
            )
        assert repository.raced is True
        assert await observer.xlen(CONFIG_EVENTS) == 0
        assert await observer.get(config_revision_key("ocs0")) is None
        assert not await observer.exists(connection_config_key("ocs0", "racing"))
    finally:
        await repository.close()
        await observer.aclose()
