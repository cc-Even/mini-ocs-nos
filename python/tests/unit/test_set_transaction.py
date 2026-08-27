from __future__ import annotations

import json
import uuid

import pytest
from gnmi_server.errors import ConflictError, InvalidArgumentError
from gnmi_server.path_parser import protobuf_path
from gnmi_server.proto import gnmi_pb2
from gnmi_server.set_transaction import (
    ConnectionConfig,
    SetOperationKind,
    build_candidate,
    decode_json_ietf,
    parse_set_request,
)


def _config_path(connection_id: str) -> gnmi_pb2.Path:
    return protobuf_path(
        f"/ocs/devices/device[name=ocs0]/connections/connection[id={connection_id}]/config"
    )


def _value(**fields) -> gnmi_pb2.TypedValue:
    return gnmi_pb2.TypedValue(
        json_ietf_val=json.dumps(fields, separators=(",", ":")).encode()
    )


def _update(connection_id: str, **fields) -> gnmi_pb2.Update:
    return gnmi_pb2.Update(path=_config_path(connection_id), val=_value(**fields))


def _plan(request: gnmi_pb2.SetRequest):
    return parse_set_request(
        request,
        request_id_factory=lambda: uuid.UUID("00000000-0000-0000-0000-000000000031"),
    )


def test_set_operations_follow_delete_replace_update_order() -> None:
    request = gnmi_pb2.SetRequest(
        delete=[_config_path("delete-me")],
        replace=[_update("replace-me", **{"input-port": 2, "output-port": 10})],
        update=[_update("update-me", **{"input-port": 3, "output-port": 11})],
    )

    plan = _plan(request)

    assert [operation.kind for operation in plan.operations] == [
        SetOperationKind.DELETE,
        SetOperationKind.REPLACE,
        SetOperationKind.UPDATE,
    ]
    assert plan.device == "ocs0"
    assert plan.request_id == "00000000-0000-0000-0000-000000000031"


def test_final_candidate_allows_order_independent_output_swap() -> None:
    current = {
        "a": ConnectionConfig("a", 1, 9, desired_version=1),
        "b": ConnectionConfig("b", 2, 10, desired_version=1),
    }
    plan = _plan(
        gnmi_pb2.SetRequest(
            update=[
                _update("a", **{"output-port": 10}),
                _update("b", **{"output-port": 9}),
            ]
        )
    )

    mutation = build_candidate(current, plan, revision=2, timestamp_ns=200)

    configs = {change.connection_id: change.config for change in mutation.changes}
    assert configs["a"].output_port == 10
    assert configs["b"].output_port == 9
    assert all(config.desired_version == 2 for config in configs.values())


def test_partial_update_preserves_fields_and_replace_preserves_creation_time() -> None:
    current = {
        "a": ConnectionConfig(
            "a",
            1,
            9,
            desired_version=1,
            created_at_ns=50,
            updated_at_ns=100,
        )
    }
    update_plan = _plan(
        gnmi_pb2.SetRequest(update=[_update("a", **{"output-port": 10})])
    )

    updated = build_candidate(current, update_plan, revision=2, timestamp_ns=200)

    updated_config = updated.changes[0].config
    assert updated_config.input_port == 1
    assert updated_config.output_port == 10
    assert updated_config.desired_version == 2
    assert updated_config.created_at_ns == 50
    assert updated_config.updated_at_ns == 200

    replace_plan = _plan(
        gnmi_pb2.SetRequest(
            replace=[_update("a", **{"input-port": 3, "output-port": 11})]
        )
    )
    replaced = build_candidate(current, replace_plan, revision=3, timestamp_ns=300)

    replaced_config = replaced.changes[0].config
    assert replaced_config.input_port == 3
    assert replaced_config.output_port == 11
    assert replaced_config.created_at_ns == 50
    assert replaced_config.updated_at_ns == 300


def test_delete_then_update_uses_final_candidate_semantics() -> None:
    current = {"a": ConnectionConfig("a", 1, 9, desired_version=1)}
    plan = _plan(
        gnmi_pb2.SetRequest(
            delete=[_config_path("a")],
            update=[_update("a", **{"input-port": 4, "output-port": 12})],
        )
    )

    mutation = build_candidate(current, plan, revision=2, timestamp_ns=200)

    assert len(mutation.changes) == 1
    assert mutation.changes[0].operation == "UPSERT"
    assert mutation.changes[0].config.input_port == 4
    assert mutation.changes[0].config.output_port == 12


def test_delete_produces_versioned_remove_without_mutating_input() -> None:
    original = ConnectionConfig("a", 1, 9, desired_version=4)
    current = {"a": original}
    plan = _plan(gnmi_pb2.SetRequest(delete=[_config_path("a")]))

    mutation = build_candidate(current, plan, revision=7, timestamp_ns=700)

    assert mutation.revision == 7
    assert mutation.changes[0].operation == "REMOVE"
    assert mutation.changes[0].config is None
    assert current == {"a": original}


def test_deleting_missing_connection_is_an_idempotent_noop() -> None:
    plan = _plan(gnmi_pb2.SetRequest(delete=[_config_path("missing")]))

    mutation = build_candidate({}, plan, revision=7, timestamp_ns=700)

    assert mutation.changes == ()


def test_conflicting_batch_fails_without_mutating_current() -> None:
    original = ConnectionConfig("existing", 1, 9, desired_version=1)
    current = {"existing": original}
    plan = _plan(
        gnmi_pb2.SetRequest(
            update=[
                _update("a", **{"input-port": 2, "output-port": 12}),
                _update("b", **{"input-port": 3, "output-port": 12}),
            ]
        )
    )

    with pytest.raises(ConflictError, match="output port 12"):
        build_candidate(current, plan, revision=2, timestamp_ns=200)

    assert current == {"existing": original}


@pytest.mark.parametrize(
    "value",
    [
        gnmi_pb2.TypedValue(json_val=b"{}"),
        gnmi_pb2.TypedValue(json_ietf_val=b"[]"),
        gnmi_pb2.TypedValue(json_ietf_val=b'{"input-port":true}'),
        gnmi_pb2.TypedValue(json_ietf_val=b'{"unknown":1}'),
        gnmi_pb2.TypedValue(json_ietf_val=b'{"input-port":1,"input-port":2}'),
    ],
)
def test_invalid_json_ietf_payloads_are_rejected(value: gnmi_pb2.TypedValue) -> None:
    with pytest.raises(InvalidArgumentError):
        decode_json_ietf(value)


def test_replace_requires_complete_ports() -> None:
    request = gnmi_pb2.SetRequest(
        replace=[_update("a", **{"input-port": 1})]
    )

    with pytest.raises(InvalidArgumentError, match="replace requires"):
        _plan(request)


def test_new_partial_update_is_rejected_after_all_operations() -> None:
    plan = _plan(
        gnmi_pb2.SetRequest(update=[_update("a", **{"input-port": 1})])
    )

    with pytest.raises(InvalidArgumentError, match="requires input-port and output-port"):
        build_candidate({}, plan, revision=1, timestamp_ns=100)


def test_set_rejects_state_paths_and_multiple_devices() -> None:
    state_path = protobuf_path(
        "/ocs/devices/device[name=ocs0]/connections/connection[id=a]/state"
    )
    other_device = protobuf_path(
        "/ocs/devices/device[name=ocs1]/connections/connection[id=b]/config"
    )

    with pytest.raises(InvalidArgumentError, match="config paths"):
        _plan(gnmi_pb2.SetRequest(delete=[state_path]))
    with pytest.raises(InvalidArgumentError, match="exactly one device"):
        _plan(
            gnmi_pb2.SetRequest(
                update=[
                    _update("a", **{"input-port": 1, "output-port": 9}),
                    gnmi_pb2.Update(
                        path=other_device,
                        val=_value(**{"input-port": 2, "output-port": 10}),
                    ),
                ]
            )
        )
