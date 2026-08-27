"""Central Redis database, key, and stream names for the Python management plane."""

from typing import Final

APPL_DB: Final = 0
DEVICE_DB: Final = 1
COUNTERS_DB: Final = 2
CONFIG_DB: Final = 4
STATE_DB: Final = 6
ALARM_DB: Final = 8
CONFIG_EVENTS: Final = "OCS_CONFIG_EVENTS"
DEVICE_COMMANDS: Final = "OCS_DEVICE_COMMANDS"
DEVICE_RESULTS: Final = "OCS_DEVICE_RESULTS"
STATE_EVENTS: Final = "OCS_STATE_EVENTS"
ALARM_EVENTS: Final = "OCS_ALARM_EVENTS"


def device_config_key(device: str) -> str:
    return f"OCS_DEVICE|{device}"


def device_config_pattern() -> str:
    return "OCS_DEVICE|*"


def connection_config_key(device: str, connection_id: str) -> str:
    return f"OCS_CONNECTION|{device}|{connection_id}"


def connection_config_pattern(device: str) -> str:
    return f"OCS_CONNECTION|{device}|*"


def config_revision_key(device: str) -> str:
    return f"OCS_CONFIG_REVISION|{device}"


def device_state_key(device: str) -> str:
    return f"OCS_DEVICE_STATE|{device}"


def device_state_pattern() -> str:
    return "OCS_DEVICE_STATE|*"


def input_port_state_key(device: str, port_id: int) -> str:
    return f"OCS_INPUT_PORT_STATE|{device}|{port_id}"


def input_port_state_pattern(device: str) -> str:
    return f"OCS_INPUT_PORT_STATE|{device}|*"


def output_port_state_key(device: str, port_id: int) -> str:
    return f"OCS_OUTPUT_PORT_STATE|{device}|{port_id}"


def output_port_state_pattern(device: str) -> str:
    return f"OCS_OUTPUT_PORT_STATE|{device}|*"


def connection_state_key(device: str, connection_id: str) -> str:
    return f"OCS_CONNECTION_STATE|{device}|{connection_id}"


def connection_state_pattern(device: str) -> str:
    return f"OCS_CONNECTION_STATE|{device}|*"


def device_counters_key(device: str) -> str:
    return f"OCS_DEVICE_COUNTERS|{device}"


def device_counters_pattern() -> str:
    return "OCS_DEVICE_COUNTERS|*"


def active_alarm_key(device: str, alarm_id: str) -> str:
    return f"OCS_ACTIVE_ALARM|{device}|{alarm_id}"


def active_alarm_pattern(device: str) -> str:
    return f"OCS_ACTIVE_ALARM|{device}|*"
