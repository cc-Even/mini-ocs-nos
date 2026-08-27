from __future__ import annotations

import json
import uuid

import pytest
from gnmi_server.errors import InvalidArgumentError
from gnmi_server.fault_transaction import (
    FaultTransaction,
    RedisFaultRepository,
    parse_fault_request,
)
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2
from gnmi_server.redis_repository import RedisSettings


def _inject(fault_id: str) -> gnmi_pb2.SetRequest:
    path = protobuf_path(
        f"/ocs/devices/device[name=ocs0]/faults/fault[id={fault_id}]/config"
    )
    return gnmi_pb2.SetRequest(
        update=[
            gnmi_pb2.Update(
                path=path,
                val=gnmi_pb2.TypedValue(
                    json_ietf_val=json.dumps({"operation": "INJECT"}).encode()
                ),
            )
        ]
    )


def test_fault_request_parses_timeout_port_and_clear_all() -> None:
    def request_id() -> uuid.UUID:
        return uuid.UUID("00000000-0000-0000-0000-000000000053")

    timeout = parse_fault_request(_inject("next-apply-timeout"), request_id_factory=request_id)
    port = parse_fault_request(_inject("input-port-down-3"), request_id_factory=request_id)
    clear = parse_fault_request(
        gnmi_pb2.SetRequest(
            delete=[protobuf_path("/ocs/devices/device[name=ocs0]/faults")]
        ),
        request_id_factory=request_id,
    )

    assert (timeout.fault_type, timeout.port_id, timeout.operation) == (
        "NEXT_APPLY_TIMEOUT",
        0,
        "INJECT",
    )
    assert (port.fault_type, port.port_id) == ("INPUT_PORT_DOWN", 3)
    assert (clear.fault_type, clear.operation) == ("ALL", "CLEAR")


@pytest.mark.parametrize(
    "set_request",
    [
        _inject("out-of-band-drift"),
        gnmi_pb2.SetRequest(update=[*_inject("next-apply-timeout").update] * 2),
        gnmi_pb2.SetRequest(
            replace=[*_inject("next-apply-timeout").update]
        ),
    ],
)
def test_fault_request_rejects_unsupported_or_non_atomic_shapes(set_request) -> None:
    with pytest.raises(InvalidArgumentError):
        parse_fault_request(set_request)


def test_fault_repository_rejects_non_positive_deadline() -> None:
    with pytest.raises(ValueError, match="deadlines must be positive"):
        RedisFaultRepository(
            RedisSettings(transaction_timeout_seconds=0), enabled=True
        )


async def test_fault_transaction_returns_confirmed_set_result() -> None:
    class Repository:
        def __init__(self) -> None:
            self.command = None

        async def execute(self, command):
            self.command = command
            return 1780000000000000053

        async def close(self) -> None:
            pass

    repository = Repository()
    transaction = FaultTransaction(repository)
    result = await transaction.apply(_inject("next-apply-timeout"))

    assert repository.command.device == "ocs0"
    assert result.timestamp_ns == 1780000000000000053
    assert len(result.operations) == 1
