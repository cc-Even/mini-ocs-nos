"""Central Redis database, key, and stream names for the Python management plane."""

from typing import Final

CONFIG_DB: Final = 4
CONFIG_EVENTS: Final = "OCS_CONFIG_EVENTS"


def device_config_key(device: str) -> str:
    return f"OCS_DEVICE|{device}"


def connection_config_key(device: str, connection_id: str) -> str:
    return f"OCS_CONNECTION|{device}|{connection_id}"


def connection_config_pattern(device: str) -> str:
    return f"OCS_CONNECTION|{device}|*"


def config_revision_key(device: str) -> str:
    return f"OCS_CONFIG_REVISION|{device}"
