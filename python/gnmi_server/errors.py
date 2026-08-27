"""Stable management-plane errors and their public gRPC representation."""

from __future__ import annotations

from dataclasses import dataclass

import grpc


class GnmiServiceError(Exception):
    """Base class for expected failures that are safe to return to clients."""

    status_code = grpc.StatusCode.INTERNAL
    detail_prefix = "internal error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    @property
    def public_details(self) -> str:
        return f"{self.detail_prefix}: {self.message}"


class InvalidArgumentError(GnmiServiceError):
    """The request does not conform to the native schema."""

    status_code = grpc.StatusCode.INVALID_ARGUMENT
    detail_prefix = "invalid argument"


class NotFoundError(GnmiServiceError):
    """An exact native resource does not exist."""

    status_code = grpc.StatusCode.NOT_FOUND
    detail_prefix = "not found"


class ConflictError(GnmiServiceError):
    """A candidate configuration violates an OCS constraint."""

    status_code = grpc.StatusCode.INVALID_ARGUMENT
    detail_prefix = "configuration conflict"


class DependencyUnavailableError(GnmiServiceError):
    """A required external service is currently unavailable."""

    status_code = grpc.StatusCode.UNAVAILABLE
    detail_prefix = "dependency unavailable"


class DeadlineExceededError(GnmiServiceError):
    """An external operation exceeded its configured deadline."""

    status_code = grpc.StatusCode.DEADLINE_EXCEEDED
    detail_prefix = "deadline exceeded"


class PermissionDeniedError(GnmiServiceError):
    """A development-only management surface is disabled."""

    status_code = grpc.StatusCode.PERMISSION_DENIED
    detail_prefix = "permission denied"


@dataclass(frozen=True)
class RpcStatus:
    """Sanitized status returned at the gRPC boundary."""

    code: grpc.StatusCode
    details: str


def map_exception(error: Exception) -> RpcStatus:
    """Map expected failures and hide unexpected implementation details."""

    if isinstance(error, GnmiServiceError):
        return RpcStatus(error.status_code, error.public_details)
    return RpcStatus(grpc.StatusCode.INTERNAL, "internal error")


async def abort_rpc(context: grpc.aio.ServicerContext, error: Exception) -> None:
    """Abort an asynchronous RPC using the centralized status mapping."""

    status = map_exception(error)
    await context.abort(status.code, status.details)
