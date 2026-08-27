"""Base implementation of the official OpenConfig gNMI service."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Final

import grpc

from gnmi_server import __version__
from gnmi_server.errors import abort_rpc
from gnmi_server.get_repository import RedisGetRepository
from gnmi_server.get_transaction import GetTransaction
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc
from gnmi_server.redis_repository import RedisConfigRepository
from gnmi_server.set_transaction import SetOperationKind, SetTransaction

MODEL_NAME: Final = "mini-ocs-native"
MODEL_ORGANIZATION: Final = "mini-ocs-nos"
MODEL_VERSION: Final = __version__
GNMI_VERSION: Final = gnmi_pb2.DESCRIPTOR.GetOptions().Extensions[gnmi_pb2.gnmi_service]


class GnmiService(gnmi_pb2_grpc.gNMIServicer):
    """gNMI management surface for desired configuration and operational state."""

    def __init__(
        self,
        set_transaction: SetTransaction | None = None,
        get_transaction: GetTransaction | None = None,
    ) -> None:
        self._set_transaction = set_transaction or SetTransaction(RedisConfigRepository())
        self._get_transaction = get_transaction or GetTransaction(RedisGetRepository())

    async def close(self) -> None:
        await asyncio.gather(self._set_transaction.close(), self._get_transaction.close())

    async def Capabilities(
        self,
        request: gnmi_pb2.CapabilityRequest,
        context: grpc.aio.ServicerContext,
    ) -> gnmi_pb2.CapabilityResponse:
        del request, context
        return gnmi_pb2.CapabilityResponse(
            supported_models=[
                gnmi_pb2.ModelData(
                    name=MODEL_NAME,
                    organization=MODEL_ORGANIZATION,
                    version=MODEL_VERSION,
                )
            ],
            supported_encodings=[gnmi_pb2.JSON_IETF],
            gNMI_version=GNMI_VERSION,
        )

    async def Get(
        self,
        request: gnmi_pb2.GetRequest,
        context: grpc.aio.ServicerContext,
    ) -> gnmi_pb2.GetResponse:
        try:
            return await self._get_transaction.read(request)
        except Exception as error:
            await abort_rpc(context, error)
            raise AssertionError("gRPC abort returned unexpectedly") from error

    async def Set(
        self,
        request: gnmi_pb2.SetRequest,
        context: grpc.aio.ServicerContext,
    ) -> gnmi_pb2.SetResponse:
        try:
            result = await self._set_transaction.apply(request)
        except Exception as error:
            await abort_rpc(context, error)
            raise AssertionError("gRPC abort returned unexpectedly") from error

        operation_codes = {
            SetOperationKind.DELETE: gnmi_pb2.UpdateResult.DELETE,
            SetOperationKind.REPLACE: gnmi_pb2.UpdateResult.REPLACE,
            SetOperationKind.UPDATE: gnmi_pb2.UpdateResult.UPDATE,
        }
        return gnmi_pb2.SetResponse(
            prefix=request.prefix,
            response=[
                gnmi_pb2.UpdateResult(
                    path=operation.response_path,
                    op=operation_codes[operation.kind],
                )
                for operation in result.operations
            ],
            timestamp=result.timestamp_ns,
        )

    async def Subscribe(
        self,
        request_iterator: AsyncIterator[gnmi_pb2.SubscribeRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[gnmi_pb2.SubscribeResponse]:
        del request_iterator
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED,
            "Subscribe is not implemented in Iteration 32",
        )
        yield gnmi_pb2.SubscribeResponse()
