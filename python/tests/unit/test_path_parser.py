import pytest
from gnmi_server.path_parser import (
    NATIVE_ORIGIN,
    PathKind,
    PathParseError,
    parse_path,
    parse_path_string,
    protobuf_path,
)
from gnmi_server.proto import gnmi_pb2


@pytest.mark.parametrize(
    ("path", "kind"),
    [
        ("/ocs", PathKind.ROOT),
        ("/ocs/devices", PathKind.DEVICES),
        ("/ocs/devices/device[name=ocs0]", PathKind.DEVICE),
        ("/ocs/devices/device[name=ocs0]/state", PathKind.DEVICE_STATE),
        ("/ocs/devices/device[name=ocs0]/ports", PathKind.PORTS),
        (
            "/ocs/devices/device[name=ocs0]/ports/input-port[id=3]/state",
            PathKind.INPUT_PORT_STATE,
        ),
        (
            "/ocs/devices/device[name=ocs0]/ports/output-port[id=11]/state",
            PathKind.OUTPUT_PORT_STATE,
        ),
        ("/ocs/devices/device[name=ocs0]/connections", PathKind.CONNECTIONS),
        (
            "/ocs/devices/device[name=ocs0]/connections/connection[id=conn-001]/config",
            PathKind.CONNECTION_CONFIG,
        ),
        (
            "/ocs/devices/device[name=ocs0]/connections/connection[id=conn-001]/state",
            PathKind.CONNECTION_STATE,
        ),
        ("/ocs/devices/device[name=ocs0]/alarms", PathKind.ALARMS),
        (
            "/ocs/devices/device[name=ocs0]/alarms/alarm[id=alarm-001]",
            PathKind.ALARM,
        ),
        ("/ocs/devices/device[name=ocs0]/counters", PathKind.COUNTERS),
        ("/ocs/devices/device[name=ocs0]/diagnostics", PathKind.DIAGNOSTICS),
        ("/ocs/devices/device[name=ocs0]/faults", PathKind.FAULTS),
        (
            "/ocs/devices/device[name=ocs0]/faults/fault[id=next-apply-timeout]/config",
            PathKind.FAULT_CONFIG,
        ),
    ],
)
def test_parse_supported_native_paths(path: str, kind: PathKind) -> None:
    parsed = parse_path_string(path)

    assert parsed.kind is kind
    if "device[name=ocs0]" in path:
        assert parsed.device == "ocs0"


def test_parse_path_combines_prefix_and_relative_path() -> None:
    prefix = protobuf_path("/ocs/devices/device[name=ocs0]")
    prefix.origin = NATIVE_ORIGIN
    prefix.target = "ocs0"
    relative = gnmi_pb2.Path(
        elem=[
            gnmi_pb2.PathElem(name="connections"),
            gnmi_pb2.PathElem(name="connection", key={"id": "conn-001"}),
            gnmi_pb2.PathElem(name="config"),
        ]
    )

    parsed = parse_path(relative, prefix=prefix)

    assert parsed.kind is PathKind.CONNECTION_CONFIG
    assert parsed.device == "ocs0"
    assert parsed.connection_id == "conn-001"
    assert parsed.origin == NATIVE_ORIGIN
    assert parsed.target == "ocs0"
    assert parsed.is_config


@pytest.mark.parametrize(
    "path",
    [
        "ocs/devices",
        "/ocs//devices",
        "/interfaces/interface[name=eth0]",
        "/ocs/devices/device",
        "/ocs/devices/device[name=bad/id]",
        "/ocs/devices/device[name=ocs0]/ports/input-port[id=0]/state",
        "/ocs/devices/device[name=ocs0]/ports/input-port[id=-1]/state",
        "/ocs/devices/device[name=ocs0]/connections/connection[id=x]/unknown",
        "/ocs/devices/device[name=ocs0]/state/extra",
    ],
)
def test_invalid_native_paths_are_rejected(path: str) -> None:
    with pytest.raises(PathParseError, match="."):
        parse_path_string(path)


def test_deprecated_element_paths_are_rejected() -> None:
    with pytest.raises(PathParseError, match="deprecated element"):
        parse_path(gnmi_pb2.Path(element=["ocs", "devices"]))


def test_prefix_and_path_metadata_must_be_consistent() -> None:
    prefix = protobuf_path("/ocs/devices")
    prefix.origin = NATIVE_ORIGIN
    path = gnmi_pb2.Path(
        origin="other-origin",
        elem=[gnmi_pb2.PathElem(name="device", key={"name": "ocs0"})],
    )

    with pytest.raises(PathParseError, match="origins differ"):
        parse_path(path, prefix=prefix)


def test_target_must_match_device_key() -> None:
    path = protobuf_path("/ocs/devices/device[name=ocs0]/state")
    path.target = "ocs1"

    with pytest.raises(PathParseError, match="target does not match"):
        parse_path(path)
