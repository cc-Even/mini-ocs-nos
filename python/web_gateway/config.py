"""Environment-backed configuration with explicit operational bounds."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

DEFAULT_LISTEN_ADDRESS: Final = "127.0.0.1:8080"
DEFAULT_GNMI_TARGET: Final = "127.0.0.1:50051"


def _number(name: str, default: str, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, default)
    try:
        value = float(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be a number") from error
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _integer(name: str, default: str, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, default)
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True)
class GatewaySettings:
    """Runtime limits shared by the ASGI app and its gNMI adapter."""

    listen_address: str = DEFAULT_LISTEN_ADDRESS
    gnmi_target: str = DEFAULT_GNMI_TARGET
    rpc_timeout_seconds: float = 3.0
    websocket_max_seconds: float = 300.0
    websocket_reconnect_attempts: int = 3
    websocket_reconnect_backoff_seconds: float = 0.2
    websocket_queue_size: int = 64
    max_request_body_bytes: int = 16_384
    max_websocket_message_bytes: int = 4_096

    @classmethod
    def from_environment(cls) -> GatewaySettings:
        return cls(
            listen_address=os.getenv("OCS_WEB_LISTEN", DEFAULT_LISTEN_ADDRESS),
            gnmi_target=os.getenv("OCS_WEB_GNMI_TARGET", DEFAULT_GNMI_TARGET),
            rpc_timeout_seconds=_number(
                "OCS_WEB_RPC_TIMEOUT_SECONDS", "3", minimum=0.1, maximum=30
            ),
            websocket_max_seconds=_number(
                "OCS_WEB_MAX_STREAM_SECONDS", "300", minimum=1, maximum=3600
            ),
            websocket_reconnect_attempts=_integer(
                "OCS_WEB_RECONNECT_ATTEMPTS", "3", minimum=0, maximum=10
            ),
            websocket_reconnect_backoff_seconds=_number(
                "OCS_WEB_RECONNECT_BACKOFF_SECONDS", "0.2", minimum=0, maximum=5
            ),
            websocket_queue_size=_integer(
                "OCS_WEB_EVENT_QUEUE_SIZE", "64", minimum=1, maximum=1024
            ),
            max_request_body_bytes=_integer(
                "OCS_WEB_MAX_REQUEST_BYTES", "16384", minimum=1024, maximum=1_048_576
            ),
            max_websocket_message_bytes=_integer(
                "OCS_WEB_MAX_MESSAGE_BYTES", "4096", minimum=256, maximum=65_536
            ),
        )
