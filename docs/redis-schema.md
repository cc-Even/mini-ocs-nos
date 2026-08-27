# Redis State and Event Contract

Redis snapshots represent current facts; Streams carry commands and events that
must survive consumer restarts. Reliable flows must not use Pub/Sub.

| Logical database | Number | Snapshot or stream ownership |
|---|---:|---|
| APPL_DB | 0 | Orchestrated connection application state |
| DEVICE_DB | 1 | Device objects, commands, results, simulator-only fault commands |
| COUNTERS_DB | 2 | Device and connection counters |
| CONFIG_DB | 4 | Desired configuration and `OCS_CONFIG_EVENTS` |
| STATE_DB | 6 | Confirmed operational state and `OCS_STATE_EVENTS` |
| ALARM_DB | 8 | Active alarms and `OCS_ALARM_EVENTS` |

All key and stream literals are centralized in `ocs/redis_keys.hpp`. Connection
keys use `TABLE|device|resource-id`; device-level keys omit the resource suffix.
DEVICE_DB also keeps `OCS_SYNCD_CONNECTION_VERSION|device|id` as the last
successfully applied resource version and
`OCS_PROCESSED_DEVICE_COMMAND|command-id` as the durable exact-command marker.

Every stream message uses this v1 envelope:

```json
{
  "event_schema_version": "1",
  "event_id": "uuid",
  "request_id": "uuid",
  "timestamp_ns": "1780000000000000000",
  "device": "ocs0",
  "resource_type": "connection",
  "resource_id": "conn-001",
  "operation": "UPSERT",
  "desired_version": "7",
  "payload": "{...json...}"
}
```

Redis stores Stream fields as strings; readers parse numeric fields with bounded
C++ types. Unsupported schema versions and missing identity fields are rejected
before append. Consumer groups are created idempotently, delivery creates a
pending entry, and consumers ACK only after successful processing.

## gNMI configuration transaction

The Python management service reads the complete desired connection snapshot
for one device and applies gNMI operations in delete, replace, then update order.
It validates the final candidate, so an order-independent swap is valid while
any final input/output conflict rejects the entire request.

`OCS_CONFIG_REVISION|device` is a device-wide monotonic integer and is also used
as the new `desired_version` for every connection changed by one SetRequest.
This preserves monotonic resource versions across delete and later recreation.
The writer watches the revision and uses bounded retries to prevent concurrent
requests from losing updates.

One CONFIG_DB `MULTI/EXEC` replaces or deletes all affected
`OCS_CONNECTION|device|id` hashes, advances the revision, and appends a
compatible `UPSERT` or `REMOVE` event for each final resource change to
`OCS_CONFIG_EVENTS`. All events from one request share its request ID, revision,
and timestamp. A validation failure executes no Redis write, and a successful
SetResponse confirms only this atomic desired-state persistence—not hardware
application.

Integration tests keep Redis on an internal Docker network with protected mode
enabled and no published TCP port. A random temporary Unix socket is mounted for
the test process and removed together with all Compose resources after the run.

## gNMI operational reads

Get reads desired configuration from CONFIG_DB, confirmed device and port state
from STATE_DB, counters from COUNTERS_DB, and active alarms from ALARM_DB. One
bounded request deadline covers all Redis operations, and Redis clients do not
perform implicit retries. Dependency timeouts and failures map to
`DEADLINE_EXCEEDED` and `UNAVAILABLE` respectively.

Responses use `TypedValue.json_ietf_val`. Redis field names are exposed with
kebab-case JSON names, known numeric fields are JSON numbers, lower-case Redis
booleans are JSON booleans, and collection entries are ordered by device or
resource ID. A connection container has `connection`, a port container has
`input-port` and `output-port`, an alarm container has `alarm`, and a keyed
connection combines its available `config` and `state` objects. CONFIG and
STATE/OPERATIONAL Get types filter those combined objects.

The unkeyed `/ocs/devices` collection is valid when empty. Keyed device,
connection, port, counter, and alarm reads return `NOT_FOUND` when the selected
resource snapshot is absent. A container below an existing device is valid when
empty, while the same container below a missing device returns `NOT_FOUND`.
Malformed paths, incompatible Get types, unsupported models, and encodings other
than JSON_IETF return `INVALID_ARGUMENT` before Redis is read.

## Device synchronization contract

`ocs-syncd` consumes `OCS_DEVICE_COMMANDS` in DEVICE_DB through the
`ocs-syncd` consumer group. The v1 event payload is a non-empty atomic command
batch:

```json
{
  "commands": [
    {
      "operation": "UPSERT",
      "id": "conn-001",
      "input_port": 3,
      "output_port": 11,
      "desired_version": 7
    }
  ],
  "atomic": true,
  "timeout_ms": 1000
}
```

Successful UPSERT commands replace `OCS_CONNECTION_STATE|device|id` in
STATE_DB with the confirmed ports, desired/applied versions, and `ACTIVE`
status. Successful REMOVE commands delete that state key; removing an already
absent device connection is an idempotent success so a delete can converge
after an older create fails. Every attempt
increments `device_apply_total` and one of `device_apply_success_total` or
`device_apply_failure_total` in `OCS_DEVICE_COUNTERS|device` in COUNTERS_DB.
If an update or delete fails, syncd retains the last confirmed actual ports and
applied version while advancing the desired version and recording `FAILED` plus
the stable device error. A failed create without prior actual state uses applied
version zero. Hash snapshots are replaced with a Redis transaction so readers
cannot observe the intermediate delete used to remove obsolete fields.

Each processed command appends an `APPLY_RESULT` event to
`OCS_DEVICE_RESULTS`. Its payload records `success`, `error_code`, and
`error_message`, while its envelope preserves the command request and desired
version. syncd acknowledges the command only after the state, counters, and
result publication steps have all completed.

Before applying a command, syncd checks the durable command marker and the last
successful resource version. An already processed command ID, or a command no
newer than the recorded successful version, is acknowledged without touching
the device or incrementing apply counters. A completed attempt records its
command ID before ACK; a successful device apply also advances the resource
version. These records survive an ordinary syncd restart. Pending-entry claim
after a crash during processing is intentionally deferred to Iteration 40.

## Orchestration contract

`ocs-orch` consumes `OCS_CONFIG_EVENTS` from CONFIG_DB through the `ocs-orch`
consumer group. A connection `UPSERT` payload contains `input_port` and
`output_port`; `REMOVE` identifies the connection in the event envelope. The
orchestrator validates the event, advances the centralized connection state
machine, writes `OCS_CONNECTION_APP|device|id` in APPL_DB, and emits one atomic
batch to `OCS_DEVICE_COMMANDS` in DEVICE_DB. The command event ID is derived
from the configuration event ID so the relationship remains traceable.

The application state is `APPLYING` or `REMOVING` while the device command is
outstanding. `ocs-orch` separately consumes `OCS_DEVICE_RESULTS`, moves a
successful apply to `ACTIVE`, records a successful delete as an `ABSENT`
tombstone, and records failures as `FAILED`. Configuration and result events
are acknowledged only after their corresponding snapshots and downstream
events have been published.

APPL_DB's `desired_version` is the orchestrator's durable per-resource version.
Events at or below that version are stale and are acknowledged without emitting
a command. A one-step increment uses the event delta; a larger gap reloads the
full `OCS_CONNECTION|device|id` snapshot from CONFIG_DB and emits that snapshot's
version. Late device results cannot overwrite a newer application state.
Successful deletes retain an `ABSENT` APPL_DB tombstone with the applied desired
version so a late pre-delete event cannot recreate the connection. These
snapshots let an ordinary orch restart resume without an in-memory cache.
When a newer event arrives while the prior device command is still outstanding,
orch publishes the newer command in stream order and keeps the resource in the
appropriate executing state. The older result is then treated as stale, so
rapid consecutive updates do not terminate the service or overwrite the latest
application version.
