"""Executable asynchronous gNMI server."""

from __future__ import annotations

import argparse
import asyncio
import signal
from collections.abc import Sequence
from typing import Final

import grpc

from gnmi_server.logging_config import configure_logging
from gnmi_server.proto import gnmi_pb2_grpc
from gnmi_server.service import GnmiService

DEFAULT_LISTEN_ADDRESS: Final = "127.0.0.1:50051"
DEFAULT_SHUTDOWN_GRACE_SECONDS: Final = 5.0


def create_server(listen_address: str) -> tuple[grpc.aio.Server, int]:
    """Create a configured server and return its resolved listening port."""

    server = grpc.aio.server()
    gnmi_pb2_grpc.add_gNMIServicer_to_server(GnmiService(), server)
    bound_port = server.add_insecure_port(listen_address)
    if bound_port == 0:
        raise RuntimeError(f"failed to bind gNMI server to {listen_address}")
    return server, bound_port


async def serve(
    listen_address: str = DEFAULT_LISTEN_ADDRESS,
    shutdown_grace_seconds: float = DEFAULT_SHUTDOWN_GRACE_SECONDS,
) -> None:
    """Serve until SIGINT or SIGTERM, then perform a bounded graceful stop."""

    logger = configure_logging("gnmi-server")
    server, bound_port = create_server(listen_address)
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_requested.set)
            installed_signals.append(signum)
        except NotImplementedError:
            pass

    await server.start()
    logger.info(
        "gNMI server started",
        extra={"operation": "START", "result": "READY", "resource_id": str(bound_port)},
    )
    try:
        await stop_requested.wait()
    finally:
        await server.stop(shutdown_grace_seconds)
        for signum in installed_signals:
            loop.remove_signal_handler(signum)
        logger.info("gNMI server stopped", extra={"operation": "STOP", "result": "OK"})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="mini-ocs-nos gNMI management service")
    parser.add_argument("--listen", default=DEFAULT_LISTEN_ADDRESS)
    parser.add_argument(
        "--shutdown-grace-seconds",
        type=float,
        default=DEFAULT_SHUTDOWN_GRACE_SECONDS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run the gNMI service command-line entry point."""

    args = _parser().parse_args(argv)
    if args.shutdown_grace_seconds < 0:
        raise SystemExit("--shutdown-grace-seconds must be non-negative")
    asyncio.run(serve(args.listen, args.shutdown_grace_seconds))


if __name__ == "__main__":
    main()
