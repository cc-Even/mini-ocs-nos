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
Per-phase `OCS_DEVICE_APPLY_*` and `OCS_SYNCD_*_PUBLISHED` hashes make recovery
of device execution, state, counters, and result publication independently
idempotent.
`OCS_ORCH_CONFIG_BATCH|event-id` preserves a prepared orchestration batch.
`OCS_ORCH_APPLICATION_PUBLISHED|event-id`,
`OCS_ORCH_DEVICE_STATE_PUBLISHED|event-id`, and
`OCS_ORCH_DEVICE_COMMAND_PUBLISHED|event-id` fence its APPL_DB, DEVICE_DB, and
atomic outbox phases respectively. Result phases use corresponding
`OCS_ORCH_RESULT_*_PUBLISHED|result-event-id` markers.
Apply timeouts additionally use `OCS_ORCH_TIMEOUT_*_PUBLISHED|command-id`
phase markers and the reliable `OCS_DEVICE_RETRIES` stream. Retry application,
device, and command markers fence resumption after an orch restart. Alarm and
counter publication markers make raise/clear side effects idempotent across
ALARM_DB and COUNTERS_DB.

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

At syncd startup, the backend handshake initializes a missing
`OCS_DEVICE|device` inventory hash with the confirmed identity, dimensions, and
model metadata. Existing inventory is preserved and must match the backend.
The Set target must exist with explicit positive input and output port counts,
and its key identity must match the requested device. An
administratively disabled device, input port, or output port rejects the
complete final candidate. Device and every candidate port key are watched
through commit so an administrative-state change cannot race validation.

One CONFIG_DB `MULTI/EXEC` replaces or deletes all affected
`OCS_CONNECTION|device|id` hashes, advances the revision, and appends a
single `connection-batch` event containing every final `UPSERT` and `REMOVE` to
`OCS_CONFIG_EVENTS`. This preserves the Set transaction boundary through the
device plane. Deleting a missing connection, including a repeated delete, is a
successful no-op that does not advance the revision or append an event. A
validation failure executes no CONFIG_DB write, and a successful SetResponse
confirms only this atomic desired-state persistence—not hardware application.
Every final-candidate commit attempt increments `config_requests_total` in
`OCS_DEVICE_COUNTERS|device`; a validation, conflict, deadline, or dependency
failure after planning also increments `config_rejected_total`. The two fields
are initialized together, and a secondary counter-publication failure never
masks the original commit error.

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
  "timeout_ms": 1000,
  "operation_id": "command-id"
}
```

syncd overwrites `operation_id` with the reliable command event ID. The device
returns the first result for a repeated operation ID, including an original
failure, and rejects an uncached resource version older than its last successful
version. A response-lost retry therefore cannot repeat a hardware transition,
turn a one-shot failure into success, or roll a newer matrix backward.
The simulator retains at most 4096 operation results. In the supported
single-active-syncd topology, pending-command draining prevents a live
unacknowledged operation from being displaced by later commands, while bounding
long-running simulator memory use.

Successful UPSERT commands replace `OCS_CONNECTION_STATE|device|id` in
STATE_DB with the confirmed ports, desired/applied versions, and `ACTIVE`
status. `actual_present` records confirmed hardware presence independently of
the last apply status, so a failed update or delete cannot corrupt active
connection accounting. Each state replacement or deletion, its compatible
UPSERT or REMOVE entry in `OCS_STATE_EVENTS`, and its publication marker occur
in one STATE_DB transaction. Successful
REMOVE commands delete that state key; removing an already absent device
connection is an idempotent success so a delete can converge after an older
create fails. Every attempt
increments `device_apply_total` and one of `device_apply_success_total` or
`device_apply_failure_total` in `OCS_DEVICE_COUNTERS|device` in COUNTERS_DB.
If an update or delete fails, syncd retains the last confirmed actual ports and
applied version while advancing the desired version and recording `FAILED` plus
the stable device error. A failed create without prior actual state uses applied
version zero. An `OCS_APPLY_TIMEOUT` result also increments
`device_apply_timeout_total` in the same once-only counter publication. Hash
snapshots are replaced with a Redis transaction so readers cannot observe the
intermediate delete used to remove obsolete fields.
The same publication stores integer `last_apply_latency_ms` and monotonically
nondecreasing `max_apply_latency_ms`, measured around the deadline-bounded
device apply. Its command marker makes counters and latency crash-replay
idempotent. At syncd initialization, every required MVP device counter is added
with zero only when absent, preserving values accumulated before a restart.

Each processed batch appends one `APPLY_RESULT` event per command to
`OCS_DEVICE_RESULTS`. Its payload records `success`, `error_code`,
`error_message`, and `command_id`, while its envelope preserves the command
request and that resource's desired version. syncd rejects an envelope whose
device does not match the connected backend identity. It acknowledges the
command only after the state, counters, and result publication steps have all
completed.

Before applying a command, syncd checks the durable command marker and the last
successful resource version. An already processed command ID, or a command no
newer than the recorded successful version, is acknowledged without touching
the device or incrementing apply counters. A completed attempt records its
command ID before ACK; a successful device apply also advances the resource
version. These records survive an ordinary syncd restart.

Before device execution, syncd writes an attempt record. The device result is
then durably stored before Redis-derived side effects begin. If the process
exits after the device call but before storing that result, recovery repeats the
same operation ID and receives the original cached result. State publication,
counter increments, and each result event use their own atomic once marker. A
superseded phase cannot overwrite a newer state or version. The
processed-command marker and all advancing per-resource version markers are
written in one DEVICE_DB transaction. If a published command payload is
malformed, syncd reloads the durable prepared orch batch and emits a terminal
failure for every member; an external command without a prepared batch receives
an envelope-level fallback result, which orch acknowledges as an orphan if no
application resource exists. Therefore recovery can resume after any persistence
boundary without losing or duplicating those effects.

syncd polls the standalone device with `GET_DEVICE_INFO` and `GET_CONNECTIONS`
every 250 milliseconds by default; `OCS_SYNCD_DEVICE_POLL_MS` configures a
bounded 10 millisecond to one hour interval. A disconnect changes
`OCS_DEVICE_STATE|device` to `FAILED` but does not erase the last connection
snapshot because the hardware result is not yet confirmed. Read-only UDS
requests may reconnect and retry once; mutation requests are never blindly
retried after an unconfirmed response. syncd instead reconnects with bounded
read-only queries and promotes the result to success only when every requested
UPSERT or REMOVE and every referenced port are confirmed. If that immediate
query also fails, a later successful poll detects terminal Redis state that
disagrees with already-converged hardware truth and publishes one idempotent
confirmation recovery. This closes CREATE, UPDATE, and DELETE reply-loss gaps
without manufacturing success from Redis acceptance alone.

Every successful handshake records `device_generation` and the confirmed
`actual_connection_count` in the device state. When the generation changes,
syncd first reads the new actual matrix, atomically refreshes each known
connection's `actual_present` and `applied_version`, and adjusts the active
counter through generation-specific once markers. It then scans at most 4096
current `OCS_CONNECTION_DEVICE|device|*` snapshots and publishes one atomic
`GENERATION_RECOVERY` command if the new matrix differs. The Redis scan is
bounded by both results and pages. `OCS_SYNCD_GENERATION_RECOVERY|device|gen`
fences command publication, while per-connection state and counter markers
make a crash during refresh resumable. Successful resource-version records
also carry the device generation, so a version accepted by an old simulator
cannot suppress necessary work on its replacement. Recovery results may move
same-version failed application state through the existing retry transition
path back to `ACTIVE` (or `ABSENT` for a remove).

After orchestration has reached a terminal snapshot for the complete desired
configuration, the same 250 millisecond device poll compares CONFIG_DB intent
with the confirmed matrix returned by standalone hwsim. A mismatch publishes
`DRIFTED` and `RECONCILING` state events, raises the device-level
`desired-actual-drift` alarm, and appends one atomic `RECONCILE` command. The
command contains every desired UPSERT plus REMOVE operations for unexpected
actual connections; it never repairs only the observed delta. Normal
configuration work and generation recovery take precedence so reconciliation
cannot race an in-flight create, update, delete, or simulator restart.

`OCS_SYNCD_RECONCILIATION|device` records the durable reconciliation cycle and
its full-snapshot signature. Per-state, command, alarm, counter, and success
publication markers make a crash at any boundary resumable without duplicate
events or counter increments. Detection increments `drift_detected_total` and
`reconciliation_total`; a confirmed successful device apply increments
`reconciliation_success_total`. Convergence clears the alarm and returns the
control record to `CONVERGED`. The simulator-only `OUT_OF_BAND_DRIFT` fault
removes a deterministic actual connection without changing its version fence,
providing the supported scenario-G test path without manipulating Redis.

The device poll also reads every input and output port through deadline-bounded
UDS calls. Initial `OCS_INPUT_PORT_STATE|device|id` and
`OCS_OUTPUT_PORT_STATE|device|id` snapshots are installed without replaying
synthetic changes; later changes atomically replace the snapshot and append an
`input-port` or `output-port` event to `OCS_STATE_EVENTS`. A non-UP port is
excluded from the effective connection matrix, so every affected connection
enters the normal DRIFTED/RECONCILING full-snapshot path.

`OCS_SYNCD_PORT_FAULT|device|direction|id` durably records each ACTIVE,
CLEARED, RECOVERY_PUBLISHED, and RECOVERED fault cycle. A DOWN transition raises
`port-down-direction-id`, increments `port_down_total` once, and accounts for
the alarm once across ALARM_DB and COUNTERS_DB. Applying the full snapshot while
the fault remains active fails with `OCS_PORT_DOWN` without changing hardware.
Members already confirmed ACTIVE on healthy ports keep their state and receive
successful per-member results; only affected or otherwise unconfirmed members
become FAILED even though the device apply is atomic.
After the port returns UP, a failed same-version connection is reapplied once;
successful device confirmation restores ACTIVE and clears both the port and
drift alarms. Standalone hwsim accepts concurrent syncd and simulator-only
control sessions so scenario H never mutates Redis to inject or clear a fault.

Each syncd loop first performs a bounded XPENDING scan and XCLAIM for commands
whose idle time exceeds `OCS_SYNCD_PENDING_MIN_IDLE_MS` (five seconds by
default), then reads a new command. The claim threshold is configurable for
tests but bounded to one hour. If syncd exits after its durable completion
marker but before ACK, a restarted consumer claims the entry, observes the
marker, and only acknowledges it. It does not reapply hardware, republish state
or result events, or increment counters. Scenario E launches a process with the
deterministic crash hook at that exact boundary and verifies all four side-effect
counts remain one after recovery.
While any pending command exists but is not old enough to claim, syncd does not
read newer stream entries. One nonblocking Redis script atomically checks the
group's pending count and reads the next entry, so two consumers cannot
simultaneously turn successive new entries into pending work. This prevents a
newer version from overtaking unfinished state or counter phases; device and
Redis version fences add rollback protection for duplicate recovery consumers.
Redis optimistic once operations retry WATCH conflicts at most eight times and
retain the repository socket deadline.

## Orchestration contract

`ocs-orch` consumes `OCS_CONFIG_EVENTS` from CONFIG_DB through the `ocs-orch`
consumer group. New management writes use one `connection-batch` payload with
the complete Set change set; legacy single-connection `UPSERT` and `REMOVE`
events remain readable for recovery compatibility. The orchestrator validates
the event, advances each affected centralized connection state machine, writes
`OCS_CONNECTION_APP|device|id` in APPL_DB, and emits the complete change set as
one atomic batch to `OCS_DEVICE_COMMANDS` in DEVICE_DB. The command event ID is
derived from the configuration event ID so the relationship remains traceable.

Before changing APPL_DB, orch durably prepares the complete batch in DEVICE_DB.
All affected APPL_DB hashes and their phase marker are replaced in one
conditional transaction; all DEVICE_DB application hashes and their phase
marker are replaced in another; only then is the device command and its outbox
marker appended atomically. Competing consumers can resume a claimed event, but
only one wins each phase marker, so a late consumer cannot regress an already
terminal application to `APPLYING`. Each phase transaction also watches the
target hashes and skips a target whose desired version is newer, fencing races
between different events. On restart, orch claims pending config and
result events before reading newer entries, reloads the prepared batch, and
resumes the whole batch without filtering already staged members. Production
claims require five seconds of idle time; tests can lower that bounded setting.
The same atomic pending-check/read script prevents a second consumer from taking
the next configuration event while the prior one is pending. A crash can
therefore leave a prepared batch waiting, but cannot emit a partial hardware
swap, let a later batch overtake it, or duplicate the device command.

The application state is `APPLYING` or `REMOVING` while the device command is
outstanding. `ocs-orch` separately consumes `OCS_DEVICE_RESULTS`, moves a
successful apply to `ACTIVE`, records a successful delete as an `ABSENT`
tombstone, and records failures as `FAILED`. Configuration and result events
are acknowledged only after their corresponding snapshots and downstream
events have been published.
Result handling conditionally persists the DEVICE_DB terminal snapshot or
deletion first and the APPL_DB terminal snapshot second, with one per-event
marker in each database. Recovery completes whichever phase is missing before
ACK, while duplicate consumers cannot repeat either mutation.

An `OCS_APPLY_TIMEOUT` result preserves desired configuration and confirmed
actual state. orch transitions every member of the atomic command batch through
`FAILED` to `RETRY_WAIT`, or leaves it `FAILED` after the configured retry
limit, and appends one durable retry event for the whole batch. The retry loop
does not ACK that event before its `not_before_ns` deadline. When due, it moves
all still-current members back to `APPLYING`/`REMOVING` and publishes one new
device command with a new operation ID. Delays double from
`OCS_ORCH_APPLY_RETRY_BASE_MS` up to `OCS_ORCH_APPLY_RETRY_MAX_MS`; the number
of retries is bounded by `OCS_ORCH_APPLY_MAX_RETRIES` (defaults: 100 ms, 5000
ms, and 3 retries).

Each timed-out connection owns the stable
`OCS_ACTIVE_ALARM|device|apply-timeout-connection-id` alarm. Raising it appends
an UPSERT to `OCS_ALARM_EVENTS` and increments `active_alarms` only for a new
active lifecycle. A confirmed successful retry deletes the active snapshot,
appends a REMOVE event, and decrements the counter once. Retry and alarm
markers allow either orch process to resume these phases without duplicate
commands, counters, or alarm events.

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

## Diagnostics and service liveness

`ocsctl diagnostics show device` performs one state-only gNMI Get on
`/ocs/devices/device[name=device]/diagnostics`; the CLI never reads Redis
directly. The server reports device health, desired and confirmed-present
connection counts, drift, active alarms, the pending count for each reliable
consumer group, and the complete device counter hash.

`ocs-orch` and `ocs-syncd` refresh `OCS_SERVICE_STATE|service` in STATE_DB with
`status=ONLINE` and `last_seen_ns`. syncd owns a dedicated heartbeat thread and
Redis connection, so blocked device RPCs and full polling cannot make the live
process appear stale. Independently of the configurable full device-poll
cadence, its main loop also schedules a bounded identity probe and records
standalone hwsim as ONLINE or OFFLINE from that result. A diagnostics request is
itself proof that the gNMI service is online; the other service heartbeats must
be no older than two seconds. Missing, malformed, stale, or explicitly offline
heartbeats make `core-services-online` false. Missing streams or groups have
zero pending work, while other Redis errors retain the request's bounded
deadline and normal gNMI error mapping.

## gNMI ON_CHANGE subscriptions

The MVP subscription surface accepts STREAM lists using JSON_IETF and
ON_CHANGE (or TARGET_DEFINED, resolved to ON_CHANGE) for connection-state,
input/output-port-state, and active-alarm paths and their containers. SAMPLE,
ONCE, POLL, heartbeat, suppression, aggregation, and QoS options are rejected
before Redis is read.

For each client, the server captures the relevant stream tail before reading
the baseline snapshots. It sends existing snapshots in request order unless
`updates_only` is set, emits `sync_response=true`, and then tails
`OCS_STATE_EVENTS` and/or `OCS_ALARM_EVENTS`. Capturing the cursor first and
deduplicating authoritative snapshot payloads prevents a snapshot-to-stream
race from losing a change or emitting a duplicate. An event is only a change
signal: the notification value is reread from the authoritative snapshot DB.
A missing snapshot after a matching event produces a gNMI delete. Malformed or
unsupported stream envelopes are skipped after advancing the client cursor.
Client cancellation cancels both request-monitor and bounded Redis-read tasks;
the shared service clients are closed during server shutdown.
