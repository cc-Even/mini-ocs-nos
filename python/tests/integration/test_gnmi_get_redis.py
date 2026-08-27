from __future__ import annotations

import json
import os

import grpc
import pytest
from gnmi_server.get_repository import RedisGetRepository
from gnmi_server.get_transaction import GetTransaction
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc
from gnmi_server.redis_keys import (
    ALARM_DB,
    CONFIG_DB,
    COUNTERS_DB,
    STATE_DB,
    active_alarm_key,
    connection_config_key,
    connection_state_key,
    device_config_key,
    device_counters_key,
    device_state_key,
    input_port_state_key,
    output_port_state_key,
)
from gnmi_server.redis_repository import RedisSettings, create_redis_client
from gnmi_server.server import create_server
from gnmi_server.service import GnmiService

pytestmark = pytest.mark.skipif(
    not os.getenv("OCS_REDIS_SOCKET"),
    reason="set OCS_REDIS_SOCKET to run Redis integration tests",
)


def _get_request(*paths: str) -> gnmi_pb2.GetRequest:
    return gnmi_pb2.GetRequest(
        path=[protobuf_path(path) for path in paths],
        encoding=gnmi_pb2.JSON_IETF,
    )


async def test_get_reads_stable_snapshots_from_all_operational_databases() -> None:
    settings = RedisSettings(unix_socket=os.environ["OCS_REDIS_SOCKET"])
    clients = {
        database: create_redis_client(settings, database)
        for database in (CONFIG_DB, STATE_DB, COUNTERS_DB, ALARM_DB)
    }
    for client in clients.values():
        await client.flushdb()

    await clients[CONFIG_DB].hset(
        device_config_key("ocs0"),
        mapping={"name": "ocs0", "input_port_count": "16", "output_port_count": "16"},
    )
    await clients[CONFIG_DB].hset(
        connection_config_key("ocs0", "conn-1"),
        mapping={
            "device": "ocs0",
            "id": "conn-1",
            "input_port": "3",
            "output_port": "11",
            "admin_status": "ENABLED",
            "desired_version": "7",
        },
    )
    await clients[STATE_DB].hset(
        device_state_key("ocs0"),
        mapping={"name": "ocs0", "oper_status": "UP", "device_generation": "4"},
    )
    await clients[STATE_DB].hset(
        connection_state_key("ocs0", "conn-1"),
        mapping={
            "device": "ocs0",
            "id": "conn-1",
            "input_port": "3",
            "output_port": "11",
            "desired_version": "7",
            "applied_version": "7",
            "apply_status": "ACTIVE",
        },
    )
    await clients[STATE_DB].hset(
        input_port_state_key("ocs0", 3),
        mapping={"id": "3", "oper_status": "UP", "optical_power_dbm": "-1.25"},
    )
    await clients[STATE_DB].hset(
        output_port_state_key("ocs0", 11),
        mapping={"id": "11", "oper_status": "UP"},
    )
    await clients[COUNTERS_DB].hset(
        device_counters_key("ocs0"),
        mapping={"device_apply_total": "9", "device_apply_success_total": "8"},
    )
    await clients[ALARM_DB].hset(
        active_alarm_key("ocs0", "alarm-1"),
        mapping={
            "id": "alarm-1",
            "severity": "MAJOR",
            "active": "true",
            "last_change_ns": "123456",
        },
    )

    service = GnmiService(get_transaction=GetTransaction(RedisGetRepository(settings)))
    server, port = create_server("127.0.0.1:0", service)
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = gnmi_pb2_grpc.gNMIStub(channel)
    try:
        response = await stub.Get(
            _get_request(
                "/ocs/devices/device[name=ocs0]",
                "/ocs/devices/device[name=ocs0]/connections",
                "/ocs/devices/device[name=ocs0]/connections/connection[id=conn-1]/config",
                "/ocs/devices/device[name=ocs0]/connections/connection[id=conn-1]/state",
                "/ocs/devices/device[name=ocs0]/ports/input-port[id=3]/state",
                "/ocs/devices/device[name=ocs0]/ports/output-port[id=11]/state",
                "/ocs/devices/device[name=ocs0]/counters",
                "/ocs/devices/device[name=ocs0]/alarms",
                "/ocs/devices/device[name=ocs0]/alarms/alarm[id=alarm-1]",
            ),
            timeout=2.0,
        )
        updates = response.notification[0].update
        payloads = [json.loads(update.val.json_ietf_val) for update in updates]

        assert payloads[0] == {
            "config": {"input-port-count": 16, "name": "ocs0", "output-port-count": 16},
            "name": "ocs0",
            "state": {"device-generation": 4, "name": "ocs0", "oper-status": "UP"},
        }
        assert payloads[1]["connection"][0]["config"]["desired-version"] == 7
        assert payloads[1]["connection"][0]["state"]["apply-status"] == "ACTIVE"
        assert payloads[2]["input-port"] == 3
        assert payloads[3]["applied-version"] == 7
        assert payloads[4] == {"id": 3, "oper-status": "UP", "optical-power-dbm": -1.25}
        assert payloads[5] == {"id": 11, "oper-status": "UP"}
        assert payloads[6] == {"device-apply-success-total": 8, "device-apply-total": 9}
        assert payloads[7]["alarm"] == [
            {"active": True, "id": "alarm-1", "last-change-ns": 123456, "severity": "MAJOR"}
        ]
        assert payloads[8] == payloads[7]["alarm"][0]

        with pytest.raises(grpc.aio.AioRpcError) as missing:
            await stub.Get(
                _get_request(
                    "/ocs/devices/device[name=ocs0]/connections/connection[id=missing]/state"
                ),
                timeout=2.0,
            )
        assert missing.value.code() is grpc.StatusCode.NOT_FOUND

        with pytest.raises(grpc.aio.AioRpcError) as malformed:
            await stub.Get(
                _get_request("/ocs/devices/device[name=ocs0]/ports/input-port[id=zero]/state"),
                timeout=2.0,
            )
        assert malformed.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    finally:
        await channel.close()
        await server.stop(0.5)
        await service.close()
        for client in clients.values():
            await client.aclose()
