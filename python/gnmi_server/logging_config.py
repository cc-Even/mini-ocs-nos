"""Shared structured logging primitives for Python control-plane services."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Final

LOG_CONTEXT_FIELDS: Final = (
    "service",
    "request_id",
    "event_id",
    "command_id",
    "device",
    "resource_id",
    "desired_version",
    "operation",
    "duration_ms",
    "result",
    "error_code",
)


class JsonFormatter(logging.Formatter):
    """Render the stable MVP log envelope as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "severity": record.levelname,
            "message": record.getMessage(),
        }
        payload.update(
            (field, value)
            for field in LOG_CONTEXT_FIELDS
            if (value := getattr(record, field, None)) is not None
        )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def configure_logging(service: str, level: int = logging.INFO) -> logging.LoggerAdapter:
    """Configure and return a non-propagating service logger."""

    logger = logging.getLogger(service)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logging.LoggerAdapter(logger, {"service": service})
