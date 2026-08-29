"""Executable ASGI server for the separately packaged web gateway."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

import uvicorn

from web_gateway.app import create_app
from web_gateway.config import GatewaySettings

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {"()": "gnmi_server.logging_config.JsonFormatter"},
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "stream": "ext://sys.stdout",
        }
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"handlers": ["default"], "level": "INFO", "propagate": False},
    },
}


def _split_listen(value: str) -> tuple[str, int]:
    host, separator, raw_port = value.rpartition(":")
    if not separator or not host:
        raise ValueError("listen address must use HOST:PORT")
    try:
        port = int(raw_port)
    except ValueError as error:
        raise ValueError("listen port must be an integer") from error
    if not 0 < port <= 65535:
        raise ValueError("listen port must be between 1 and 65535")
    return host, port


def _parser() -> argparse.ArgumentParser:
    defaults = GatewaySettings.from_environment()
    parser = argparse.ArgumentParser(description="mini-ocs bounded gNMI web gateway")
    parser.add_argument("--listen", default=defaults.listen_address)
    parser.add_argument("--gnmi-target", default=defaults.gnmi_target)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    settings = GatewaySettings.from_environment()
    settings = GatewaySettings(
        **{
            **settings.__dict__,
            "listen_address": args.listen,
            "gnmi_target": args.gnmi_target,
        }
    )
    try:
        host, port = _split_listen(settings.listen_address)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    uvicorn.run(
        create_app(settings),
        host=host,
        port=port,
        log_level="info",
        log_config=LOG_CONFIG,
        ws_max_size=settings.max_websocket_message_bytes,
        ws_max_queue=1,
        limit_concurrency=100,
        timeout_graceful_shutdown=5,
    )


if __name__ == "__main__":
    main()
