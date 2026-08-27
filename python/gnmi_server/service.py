"""Base implementation of the official OpenConfig gNMI service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Final

import grpc

from gnmi_server import __version__
from gnmi_server.errors import InvalidArgumentError, abort_rpc
from gnmi_server.path_parser import parse_paths
from gnmi_server.proto import gnmi_pb2, gnmi_pb2_grpc

MODEL_NAME: Final = "mini-ocs-native"
MODEL_ORGANIZATION: Final = "mini-ocs-nos"
MODEL_VERSION: Final = __version__
GNMI_VERSION: Final = gnmi_pb2.DESCRIPTOR.GetOptions().Extensions[gnmi_pb2.gnmi_service]


class GnmiService(gnmi_pb2_grpc.gNMIServicer):
    """Iteration-30 gNMI surface with Capabilities and boundary validation."""

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
            if not request.path:
                raise InvalidArgumentError("GetRequest requires at least one path")
            parse_paths(request.path, prefix=request.prefix)
        except Exception as error:
            await abort_rpc(context, error)
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Get is not implemented in Iteration 30")

    async def Set(
        self,
        request: gnmi_pb2.SetRequest,
        context: grpc.aio.ServicerContext,
    ) -> gnmi_pb2.SetResponse:
        paths = [*request.delete]
        paths.extend(update.path for update in request.replace)
        paths.extend(update.path for update in request.update)
        paths.extend(update.path for update in request.union_replace)
        try:
            if not paths:
                raise InvalidArgumentError("SetRequest requires at least one operation")
            parse_paths(paths, prefix=request.prefix)
        except Exception as error:
            await abort_rpc(context, error)
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Set is not implemented in Iteration 30")

    async def Subscribe(
        self,
        request_iterator: AsyncIterator[gnmi_pb2.SubscribeRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[gnmi_pb2.SubscribeResponse]:
        del request_iterator
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED,
            "Subscribe is not implemented in Iteration 30",
        )
        yield gnmi_pb2.SubscribeResponse()
