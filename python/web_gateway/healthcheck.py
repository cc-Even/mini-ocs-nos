"""Container health probe for the gateway and its gNMI dependency."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="probe the mini-ocs web gateway")
    parser.add_argument("--url", default="http://127.0.0.1:8080/healthz")
    parser.add_argument("--timeout-seconds", type=float, default=2.0)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be positive")
    try:
        with urllib.request.urlopen(args.url, timeout=args.timeout_seconds) as response:
            payload = json.load(response)
            if response.status != 200 or payload.get("status") != "ready":
                raise SystemExit("gateway is not ready")
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise SystemExit(f"gateway health check failed: {error}") from error


if __name__ == "__main__":
    main()
