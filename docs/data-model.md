# Data model

The management model is project-native and advertised by gNMI Capabilities as
`mini-ocs-native`. The implementation uses the official gNMI protobuf service
and JSON_IETF encoding, but it does not ship an OpenConfig YANG module.

## Native gNMI paths

Identifiers accept only `[A-Za-z0-9_.-]` and are limited to 128 characters.
Port IDs are positive decimal integers. The parser rejects deprecated protobuf
`element`, unknown keys, mismatched target/device names, unsupported encodings,
and paths outside this schema.

| Resource | Representative path | Access |
|---|---|---|
| Device collection | `/ocs/devices` | Get |
| Device | `/ocs/devices/device[name=ocs0]` | Get |
| Device state | `/ocs/devices/device[name=ocs0]/state` | Get |
| Ports | `/ocs/devices/device[name=ocs0]/ports` | Get |
| Input/output port state | `.../ports/input-port[id=3]/state` | Get, Subscribe |
| Connections | `/ocs/devices/device[name=ocs0]/connections` | Get, Subscribe |
| Connection | `.../connections/connection[id=conn-001]` | Get |
| Connection config | `.../connection[id=conn-001]/config` | Get, Set |
| Connection state | `.../connection[id=conn-001]/state` | Get, Subscribe |
| Active alarms | `/ocs/devices/device[name=ocs0]/alarms` | Get, Subscribe |
| Counters | `/ocs/devices/device[name=ocs0]/counters` | Get |
| Diagnostics | `/ocs/devices/device[name=ocs0]/diagnostics` | Get |
| Development fault | `.../faults/fault[id=next-apply-timeout]/config` | Set when explicitly enabled |

A connection config contains `id`, `input-port`, and `output-port`. Reads return
kebab-case JSON fields and numeric/boolean JSON types rather than Redis strings.
Get supports ALL, CONFIG, STATE, and OPERATIONAL filtering where meaningful.
Subscribe supports STREAM with ON_CHANGE or TARGET_DEFINED for connection,
port, and alarm state; SAMPLE, ONCE, POLL, heartbeat, QoS, and aggregation are
outside the MVP.

The fault subtree is not readable state and is disabled by default. When
`OCS_ENABLE_FAULT_API=1`, one update with JSON_IETF
`{"operation":"INJECT"}` injects a supported keyed fault; deleting a keyed
fault clears it, and deleting `/faults` clears all simulator faults. A fault Set
cannot be mixed with connection operations. Supported IDs are
`next-apply-timeout`, `next-apply-error`, `input-port-down-N`, and
`output-port-down-N`.

## Redis logical databases

| DB | Number | Source of truth / role |
|---|---:|---|
| APPL_DB | 0 | Orchestrated connection application state and tombstones |
| DEVICE_DB | 1 | Device commands/results, prepared batches, apply/recovery markers |
| COUNTERS_DB | 2 | Device and connection counters |
| CONFIG_DB | 4 | Desired inventory/configuration and `OCS_CONFIG_EVENTS` |
| STATE_DB | 6 | Confirmed device/port/connection state, service heartbeats, `OCS_STATE_EVENTS` |
| ALARM_DB | 8 | Active alarm snapshots and `OCS_ALARM_EVENTS` |

Snapshots answer current-state reads. Redis Streams carry durable work and
change signals; reliable flows do not use Pub/Sub. Key and stream names are
centralized in `cpp/include/ocs/redis_keys.hpp` and mirrored by the Python
management repository. The exhaustive key, ownership, and recovery-marker
contract is in [Redis state and event contract](redis-schema.md).

## Version and event identity

Each successful non-no-op Set increments `OCS_CONFIG_REVISION|device`. Every
resource changed by that Set receives the same monotonically increasing
`desired_version`, including later recreation after delete. One stream envelope
carries `event_schema_version`, event/request identity, timestamp, device,
resource identity, operation, desired version, and a JSON payload.

orch derives the device command relationship from the configuration event.
syncd uses the command event ID as the device operation ID. The simulator's
bounded replay cache returns the first result for an exact operation replay.
Successful per-resource version fences prevent older work from rolling a newer
matrix backward. Device generation is stored with successful versions so a
replacement simulator cannot inherit stale suppression decisions.

## Desired, application, and actual state

- CONFIG_DB answers what the operator wants.
- APPL_DB answers where orchestration is in the apply lifecycle.
- DEVICE_DB answers what device work has been prepared, attempted, and
  durably published.
- STATE_DB answers only what syncd confirmed from the device boundary.

`actual_present` is independent of the latest apply status. If an update or
delete fails, STATE_DB retains the last confirmed ports and applied version
while recording the newer desired version and failure. A failed create has no
actual connection and applied version zero. This is why CONFIG_DB acceptance
must never be reported as hardware success.

The centralized connection lifecycle is:

```text
ABSENT -> PENDING_CREATE -> APPLYING -> ACTIVE
ACTIVE -> PENDING_UPDATE -> APPLYING -> ACTIVE
ACTIVE -> PENDING_DELETE -> REMOVING -> ABSENT

PENDING_* / APPLYING / REMOVING -> FAILED -> RETRY_WAIT
RETRY_WAIT -> APPLYING or REMOVING
ACTIVE -> DRIFTED -> RECONCILING -> ACTIVE or FAILED
```

`ABSENT` is retained as an application tombstone after delete so late events
cannot recreate a removed resource.

## Atomicity boundaries

Management Set builds and validates the complete final candidate. A CONFIG_DB
`WATCH` plus `MULTI/EXEC` transaction updates every affected snapshot, advances
the revision, and appends one complete batch event. A conflict or dependency
change writes none of those objects. Concurrent writers use bounded optimistic
retries.

orch preserves that batch through its prepared record, application/device
snapshots, and one device command. syncd sends the complete command to an atomic
device apply. The simulator validates every member before replacing the old
matrix, so a failing member leaves the matrix unchanged.

Redis has no transaction spanning logical databases. Cross-DB effects therefore
use durable, per-phase once markers and ordering. A restarted consumer resumes
the missing phase before ACK; markers, event IDs, desired versions, and device
operation IDs make repeated execution idempotent.

## State events and subscriptions

syncd atomically replaces or deletes a STATE_DB snapshot and appends the
matching state event in the same logical database transaction. The event is a
change signal. Subscribe rereads the authoritative snapshot before emitting a
value; a missing snapshot produces a gNMI delete.

For race-free startup, Subscribe captures stream tails before reading initial
snapshots, sends those snapshots, emits `sync_response=true`, then tails later
events while deduplicating the captured overlap.

## Errors, counters, and alarms

Stable device errors include `OCS_INVALID_PORT`, `OCS_INPUT_CONFLICT`,
`OCS_OUTPUT_CONFLICT`, `OCS_PORT_DISABLED`, `OCS_PORT_DOWN`,
`OCS_DEVICE_NOT_READY`, `OCS_APPLY_TIMEOUT`, `OCS_APPLY_FAILED`,
`OCS_VERSION_STALE`, and `OCS_INTERNAL_ERROR`. Tests assert stable codes rather
than human messages.

MVP counters include configuration requests/rejections; device apply attempts,
successes, failures, and timeouts; active connections/alarms; last/max apply
latency; drift and reconciliation; and port-down events. Active alarms cover
apply timeout, desired/actual drift, and port-down lifecycles. Snapshot/event
publication and counter increments are fenced so crash replay cannot count the
same lifecycle twice.
