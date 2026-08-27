"""Parser and validator for the project-native gNMI path schema."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from gnmi_server.errors import InvalidArgumentError
from gnmi_server.proto import gnmi_pb2

NATIVE_ORIGIN: Final = "mini-ocs-native"
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_SEGMENT = re.compile(
    r"^(?P<name>[A-Za-z][A-Za-z0-9-]*)"
    r"(?:\[(?P<key>[A-Za-z][A-Za-z0-9-]*)=(?P<value>[A-Za-z0-9_.-]{1,128})\])?$"
)


class PathParseError(InvalidArgumentError):
    """A path is not part of the supported mini-ocs-native schema."""

    detail_prefix = "invalid path"


class PathKind(StrEnum):
    """Supported native resources, independent of protobuf representation."""

    ROOT = "root"
    DEVICES = "devices"
    DEVICE = "device"
    DEVICE_STATE = "device-state"
    PORTS = "ports"
    INPUT_PORT = "input-port"
    INPUT_PORT_STATE = "input-port-state"
    OUTPUT_PORT = "output-port"
    OUTPUT_PORT_STATE = "output-port-state"
    CONNECTIONS = "connections"
    CONNECTION = "connection"
    CONNECTION_CONFIG = "connection-config"
    CONNECTION_STATE = "connection-state"
    ALARMS = "alarms"
    ALARM = "alarm"
    COUNTERS = "counters"
    DIAGNOSTICS = "diagnostics"


@dataclass(frozen=True)
class NativePath:
    """Canonical interpretation of one validated native path."""

    kind: PathKind
    device: str | None = None
    connection_id: str | None = None
    port_id: int | None = None
    alarm_id: str | None = None
    origin: str = ""
    target: str = ""

    @property
    def is_config(self) -> bool:
        return self.kind is PathKind.CONNECTION_CONFIG


def _path_elements(path: gnmi_pb2.Path, label: str) -> list[gnmi_pb2.PathElem]:
    if path.element:
        raise PathParseError(f"{label} uses the deprecated element field")
    return list(path.elem)


def _combine_paths(
    prefix: gnmi_pb2.Path | None, path: gnmi_pb2.Path
) -> tuple[list[gnmi_pb2.PathElem], str, str]:
    prefix_elements = _path_elements(prefix, "prefix") if prefix is not None else []
    path_elements = _path_elements(path, "path")
    prefix_origin = prefix.origin if prefix is not None else ""
    prefix_target = prefix.target if prefix is not None else ""

    if prefix_origin and path.origin and prefix_origin != path.origin:
        raise PathParseError("prefix and path origins differ")
    if prefix_target and path.target and prefix_target != path.target:
        raise PathParseError("prefix and path targets differ")

    origin = path.origin or prefix_origin
    target = path.target or prefix_target
    if origin not in {"", NATIVE_ORIGIN}:
        raise PathParseError(f"unsupported origin {origin!r}")
    if target and _SAFE_ID.fullmatch(target) is None:
        raise PathParseError("target contains unsafe characters")
    return prefix_elements + path_elements, origin, target


def _expect_element(
    element: gnmi_pb2.PathElem, name: str, key_name: str | None = None
) -> str | None:
    if element.name != name:
        raise PathParseError(f"expected element {name!r}, got {element.name!r}")
    expected_keys = {key_name} if key_name is not None else set()
    if set(element.key) != expected_keys:
        keys = ",".join(sorted(element.key)) or "none"
        expected = key_name or "none"
        raise PathParseError(f"element {name!r} requires key {expected!r}, got {keys!r}")
    if key_name is None:
        return None
    value = element.key[key_name]
    if _SAFE_ID.fullmatch(value) is None:
        raise PathParseError(f"key {key_name!r} contains unsafe characters")
    return value


def _parse_port_id(value: str) -> int:
    if not value.isascii() or not value.isdecimal():
        raise PathParseError("port id must be a positive decimal integer")
    port_id = int(value)
    if port_id <= 0 or port_id > 2**31 - 1:
        raise PathParseError("port id is outside the supported range")
    return port_id


def _canonical_path(
    elements: list[gnmi_pb2.PathElem], origin: str, target: str
) -> NativePath:
    if not elements:
        raise PathParseError("empty path")
    _expect_element(elements[0], "ocs")
    if len(elements) == 1:
        return NativePath(PathKind.ROOT, origin=origin, target=target)

    _expect_element(elements[1], "devices")
    if len(elements) == 2:
        return NativePath(PathKind.DEVICES, origin=origin, target=target)
    device = _expect_element(elements[2], "device", "name")
    assert device is not None
    if target and target != device:
        raise PathParseError("target does not match the device path key")
    base = {"device": device, "origin": origin, "target": target}
    if len(elements) == 3:
        return NativePath(PathKind.DEVICE, **base)

    branch = elements[3].name
    if branch == "state" and len(elements) == 4:
        _expect_element(elements[3], "state")
        return NativePath(PathKind.DEVICE_STATE, **base)
    if branch == "counters" and len(elements) == 4:
        _expect_element(elements[3], "counters")
        return NativePath(PathKind.COUNTERS, **base)
    if branch == "diagnostics" and len(elements) == 4:
        _expect_element(elements[3], "diagnostics")
        return NativePath(PathKind.DIAGNOSTICS, **base)
    if branch == "ports":
        _expect_element(elements[3], "ports")
        if len(elements) == 4:
            return NativePath(PathKind.PORTS, **base)
        port_element = elements[4]
        if port_element.name not in {"input-port", "output-port"}:
            raise PathParseError(f"unsupported ports element {port_element.name!r}")
        port_value = _expect_element(port_element, port_element.name, "id")
        assert port_value is not None
        port_id = _parse_port_id(port_value)
        port_base = {**base, "port_id": port_id}
        if len(elements) == 5:
            kind = (
                PathKind.INPUT_PORT
                if port_element.name == "input-port"
                else PathKind.OUTPUT_PORT
            )
            return NativePath(kind, **port_base)
        if len(elements) == 6:
            _expect_element(elements[5], "state")
            kind = (
                PathKind.INPUT_PORT_STATE
                if port_element.name == "input-port"
                else PathKind.OUTPUT_PORT_STATE
            )
            return NativePath(kind, **port_base)
        raise PathParseError("port paths may only end at the port or its state")
    if branch == "connections":
        _expect_element(elements[3], "connections")
        if len(elements) == 4:
            return NativePath(PathKind.CONNECTIONS, **base)
        connection_id = _expect_element(elements[4], "connection", "id")
        assert connection_id is not None
        connection_base = {**base, "connection_id": connection_id}
        if len(elements) == 5:
            return NativePath(PathKind.CONNECTION, **connection_base)
        if len(elements) == 6 and elements[5].name in {"config", "state"}:
            leaf = elements[5].name
            _expect_element(elements[5], leaf)
            kind = (
                PathKind.CONNECTION_CONFIG
                if leaf == "config"
                else PathKind.CONNECTION_STATE
            )
            return NativePath(kind, **connection_base)
        raise PathParseError("connection paths may only end at connection, config, or state")
    if branch == "alarms":
        _expect_element(elements[3], "alarms")
        if len(elements) == 4:
            return NativePath(PathKind.ALARMS, **base)
        if len(elements) == 5:
            alarm_id = _expect_element(elements[4], "alarm", "id")
            assert alarm_id is not None
            return NativePath(PathKind.ALARM, alarm_id=alarm_id, **base)
        raise PathParseError("alarm paths may only end at alarms or a keyed alarm")
    raise PathParseError(f"unsupported device branch {branch!r}")


def parse_path(
    path: gnmi_pb2.Path, *, prefix: gnmi_pb2.Path | None = None
) -> NativePath:
    """Parse a protobuf path and optional prefix into a canonical native path."""

    elements, origin, target = _combine_paths(prefix, path)
    return _canonical_path(elements, origin, target)


def protobuf_path(path: str) -> gnmi_pb2.Path:
    """Convert a strict absolute native path string into a protobuf Path."""

    if not path.startswith("/") or path == "/":
        raise PathParseError("path must be an absolute non-root path")
    raw_segments = path[1:].split("/")
    if any(not segment for segment in raw_segments):
        raise PathParseError("path contains an empty element")
    elements: list[gnmi_pb2.PathElem] = []
    for segment in raw_segments:
        match = _SEGMENT.fullmatch(segment)
        if match is None:
            raise PathParseError(f"malformed element {segment!r}")
        element = gnmi_pb2.PathElem(name=match.group("name"))
        if key := match.group("key"):
            element.key[key] = match.group("value")
        elements.append(element)
    return gnmi_pb2.Path(elem=elements)


def parse_path_string(path: str) -> NativePath:
    """Parse the human-readable form used by documentation and the CLI."""

    return parse_path(protobuf_path(path))


def parse_paths(
    paths: Iterable[gnmi_pb2.Path], *, prefix: gnmi_pb2.Path | None = None
) -> tuple[NativePath, ...]:
    """Parse several paths with the same prefix without partial results."""

    return tuple(parse_path(path, prefix=prefix) for path in paths)
