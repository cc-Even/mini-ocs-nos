from __future__ import annotations

import json

import pytest
from gnmi_server.errors import InvalidArgumentError, NotFoundError
from gnmi_server.get_transaction import GetTransaction
from gnmi_server.path_parser import PathKind, protobuf_path
from gnmi_server.proto import gnmi_pb2


class FakeRepository:
    def __init__(self, payloads=({"z-value": 2, "a-value": 1},)) -> None:
        self.payloads = payloads
        self.calls = []
        self.closed = False

    async def read_many(self, paths, request_type):
        self.calls.append((paths, request_type))
        return self.payloads

    async def close(self) -> None:
        self.closed = True


def _request(path: str, **kwargs) -> gnmi_pb2.GetRequest:
    return gnmi_pb2.GetRequest(
        path=[protobuf_path(path)],
        encoding=gnmi_pb2.JSON_IETF,
        **kwargs,
    )


async def test_get_builds_one_stable_notification_after_reading_all_paths() -> None:
    repository = FakeRepository(
        payloads=(
            {"z-value": 2, "a-value": 1},
            {"apply-status": "ACTIVE", "applied-version": 7},
        )
    )
    transaction = GetTransaction(repository, timestamp_factory=lambda: 1234)
    prefix = protobuf_path("/ocs/devices/device[name=ocs0]")
    request = gnmi_pb2.GetRequest(
        prefix=prefix,
        path=[
            protobuf_path("/connections/connection[id=conn-1]/config"),
            protobuf_path("/connections/connection[id=conn-1]/state"),
        ],
        encoding=gnmi_pb2.JSON_IETF,
    )

    response = await transaction.read(request)

    assert len(repository.calls) == 1
    paths, request_type = repository.calls[0]
    assert [path.kind for path in paths] == [
        PathKind.CONNECTION_CONFIG,
        PathKind.CONNECTION_STATE,
    ]
    assert request_type == gnmi_pb2.GetRequest.ALL
    notification = response.notification[0]
    assert notification.timestamp == 1234
    assert notification.prefix == prefix
    assert notification.update[0].path == request.path[0]
    assert notification.update[0].val.json_ietf_val == b'{"a-value":1,"z-value":2}'
    assert json.loads(notification.update[1].val.json_ietf_val) == {
        "applied-version": 7,
        "apply-status": "ACTIVE",
    }


@pytest.mark.parametrize(
    ("case_request", "message"),
    [
        (gnmi_pb2.GetRequest(encoding=gnmi_pb2.JSON_IETF), "at least one path"),
        (
            gnmi_pb2.GetRequest(
                path=[protobuf_path("/ocs/devices")],
                encoding=gnmi_pb2.JSON,
            ),
            "encoding must be JSON_IETF",
        ),
        (
            _request(
                "/ocs/devices",
                use_models=[gnmi_pb2.ModelData(name="openconfig-interfaces")],
            ),
            "unsupported model",
        ),
        (
            _request(
                "/ocs/devices/device[name=ocs0]/state",
                type=gnmi_pb2.GetRequest.CONFIG,
            ),
            "operational paths",
        ),
        (
            _request(
                "/ocs/devices/device[name=ocs0]/connections/connection[id=c1]/config",
                type=gnmi_pb2.GetRequest.STATE,
            ),
            "configuration paths",
        ),
    ],
)
async def test_get_rejects_invalid_request_before_read(case_request, message: str) -> None:
    repository = FakeRepository()
    transaction = GetTransaction(repository)

    with pytest.raises(InvalidArgumentError, match=message):
        await transaction.read(case_request)

    assert repository.calls == []


async def test_get_does_not_create_a_partial_response_when_repository_fails() -> None:
    class MissingRepository(FakeRepository):
        async def read_many(self, paths, request_type):
            del paths, request_type
            raise NotFoundError("connection 'missing'")

    transaction = GetTransaction(MissingRepository())

    with pytest.raises(NotFoundError, match="missing"):
        await transaction.read(
            _request(
                "/ocs/devices/device[name=ocs0]/connections/connection[id=missing]/state"
            )
        )
