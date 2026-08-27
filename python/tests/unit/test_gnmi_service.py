import json

import grpc
import pytest
from gnmi_server.get_transaction import GetTransaction
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc
from gnmi_server.server import create_server
from gnmi_server.service import (
    GNMI_VERSION,
    MODEL_NAME,
    MODEL_ORGANIZATION,
    MODEL_VERSION,
    GnmiService,
)


class FakeGetRepository:
    async def read_many(self, paths, request_type):
        del request_type
        return tuple({"name": path.device, "oper-status": "UP"} for path in paths)

    async def close(self) -> None:
        pass


@pytest.fixture
async def gnmi_stub():
    service = GnmiService(get_transaction=GetTransaction(FakeGetRepository()))
    server, port = create_server("127.0.0.1:0", service)
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield gnmi_pb2_grpc.gNMIStub(channel)
    finally:
        await channel.close()
        await server.stop(0.5)
        await service.close()


async def test_capabilities_are_served_over_official_gnmi_rpc(gnmi_stub) -> None:
    response = await gnmi_stub.Capabilities(gnmi_pb2.CapabilityRequest(), timeout=1.0)

    assert response.gNMI_version == GNMI_VERSION == "0.10.0"
    assert list(response.supported_encodings) == [gnmi_pb2.JSON_IETF]
    assert len(response.supported_models) == 1
    model = response.supported_models[0]
    assert model.name == MODEL_NAME == "mini-ocs-native"
    assert model.organization == MODEL_ORGANIZATION == "mini-ocs-nos"
    assert model.version == MODEL_VERSION == "0.1.0"


async def test_invalid_get_path_maps_to_invalid_argument(gnmi_stub) -> None:
    request = gnmi_pb2.GetRequest(path=[protobuf_path("/interfaces")])

    with pytest.raises(grpc.aio.AioRpcError) as raised:
        await gnmi_stub.Get(request, timeout=1.0)

    assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    assert raised.value.details().startswith("invalid path:")


async def test_valid_get_returns_json_ietf(gnmi_stub) -> None:
    request = gnmi_pb2.GetRequest(
        path=[protobuf_path("/ocs/devices/device[name=ocs0]/state")],
        encoding=gnmi_pb2.JSON_IETF,
    )

    response = await gnmi_stub.Get(request, timeout=1.0)

    assert len(response.notification) == 1
    assert len(response.notification[0].update) == 1
    assert json.loads(response.notification[0].update[0].val.json_ietf_val) == {
        "name": "ocs0",
        "oper-status": "UP",
    }


async def test_invalid_set_path_maps_to_invalid_argument(gnmi_stub) -> None:
    request = gnmi_pb2.SetRequest(delete=[protobuf_path("/ocs/devices/device")])

    with pytest.raises(grpc.aio.AioRpcError) as raised:
        await gnmi_stub.Set(request, timeout=1.0)

    assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    assert raised.value.details().startswith("invalid path:")


async def test_fault_set_is_default_closed(gnmi_stub) -> None:
    path = protobuf_path(
        "/ocs/devices/device[name=ocs0]/faults/fault[id=next-apply-timeout]/config"
    )
    request = gnmi_pb2.SetRequest(
        update=[
            gnmi_pb2.Update(
                path=path,
                val=gnmi_pb2.TypedValue(json_ietf_val=b'{"operation":"INJECT"}'),
            )
        ]
    )

    with pytest.raises(grpc.aio.AioRpcError) as raised:
        await gnmi_stub.Set(request, timeout=1.0)

    assert raised.value.code() is grpc.StatusCode.PERMISSION_DENIED
