import grpc
import pytest
from gnmi_server.errors import (
    ConflictError,
    DeadlineExceededError,
    DependencyUnavailableError,
    InvalidArgumentError,
    NotFoundError,
    map_exception,
)
from gnmi_server.path_parser import PathParseError


@pytest.mark.parametrize(
    ("error", "code", "details"),
    [
        (
            InvalidArgumentError("bad payload"),
            grpc.StatusCode.INVALID_ARGUMENT,
            "invalid argument: bad payload",
        ),
        (
            PathParseError("unknown branch"),
            grpc.StatusCode.INVALID_ARGUMENT,
            "invalid path: unknown branch",
        ),
        (NotFoundError("connection x"), grpc.StatusCode.NOT_FOUND, "not found: connection x"),
        (
            ConflictError("output 2 is occupied"),
            grpc.StatusCode.INVALID_ARGUMENT,
            "configuration conflict: output 2 is occupied",
        ),
        (
            DependencyUnavailableError("Redis"),
            grpc.StatusCode.UNAVAILABLE,
            "dependency unavailable: Redis",
        ),
        (
            DeadlineExceededError("Redis read"),
            grpc.StatusCode.DEADLINE_EXCEEDED,
            "deadline exceeded: Redis read",
        ),
    ],
)
def test_expected_errors_have_stable_grpc_status(
    error: Exception, code: grpc.StatusCode, details: str
) -> None:
    status = map_exception(error)

    assert status.code is code
    assert status.details == details


def test_unexpected_errors_do_not_leak_details() -> None:
    status = map_exception(RuntimeError("secret implementation detail"))

    assert status.code is grpc.StatusCode.INTERNAL
    assert status.details == "internal error"
