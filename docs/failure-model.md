# Failure and recovery model

The control plane treats desired-state persistence, device execution, and
confirmed operational state as separate facts. Recovery is built around four
invariants:

1. Redis acceptance is never hardware confirmation.
2. A failed atomic batch does not change the previous device matrix.
3. An unconfirmed device mutation is never reported as successful.
4. Replayed work cannot duplicate a completed device effect, state event,
   counter, or alarm transition.

All gRPC, Redis, and UDS calls have deadlines. Retries, scans, payloads, replay
caches, and optimistic transaction attempts are bounded.

## Failure table

| Failure | Observable state | Recovery |
|---|---|---|
| Candidate conflict or invalid port | gNMI Set fails; rejection counter increases | No desired snapshot/event or device command is committed |
| Device apply error | Last confirmed actual state retained; application becomes `FAILED` | A later valid config or reconciliation can converge |
| Device apply timeout | `FAILED` then `RETRY_WAIT`, timeout counter and alarm | Whole atomic batch retries with exponential bounded delay; success clears alarm |
| UDS disconnect or lost reply | Device becomes unavailable; no false ACTIVE | Reconnect, query identity/matrix/ports, accept success only if the entire mutation is confirmed |
| hwsim restart | New generation and initially empty simulated matrix | Refresh actual state, fence old-generation versions, publish one full-snapshot recovery command |
| orch crash | Stream entry remains pending; durable phase may be partial | New consumer claims the entry and resumes only missing marked phases |
| syncd crash before ACK | Device result/effects may already exist; command stays pending | New consumer claims it; operation replay and once markers prevent duplicate effects |
| Out-of-band matrix change | `DRIFTED`/`RECONCILING`, drift alarm and counters | Compare confirmed matrix with CONFIG_DB and apply one complete desired snapshot |
| Input/output port DOWN | Port state and alarm change; affected connection fails/reconciles | Full snapshot is rejected while DOWN; after clear, same-version recovery restores ACTIVE |
| Redis unavailable | RPC/service work fails or waits only to its deadline | Process retries are bounded; durable streams/snapshots resume after dependency recovery |

## Stream crash recovery

orch and syncd use Redis consumer groups. They inspect and claim old pending
entries before reading new work. While a pending entry is too young to claim,
an atomic pending-check/read operation prevents a later command from overtaking
it. Production pending age defaults to five seconds and is bounded when
configured for tests.

Each multi-database workflow records a durable prepared object plus separate
once markers for application state, device state, command publication, result
publication, counters, alarms, and ACK eligibility. A process can exit between
any two phases. The replacement consumer reloads the prepared data and performs
only the missing phase. Newer desired versions fence late consumers.

If syncd exits after calling the device but before storing the result, it sends
the same operation ID after recovery. hwsim returns the original cached result,
including an original failure. If it exits after durable completion but before
ACK, the replacement only ACKs the pending command.

## Timeout and bounded retry

`OCS_APPLY_TIMEOUT` preserves desired config and last confirmed actual state.
orch retries the complete original atomic batch, not individual members. Default
delays double from 100 ms up to 5000 ms, with at most three retries. The retry
stream, batch record, command identity, and alarm/counter markers survive an
orch restart. Exhaustion leaves a visible failure rather than retrying forever.

## Disconnect and false-success prevention

UDS mutations are not automatically retried after timeout, EOF, request-ID
mismatch, or an unreadable reply because the device may already have applied
them. syncd reconnects and issues bounded read-only identity, connection, and
port queries. It repairs the result to success only if every UPSERT/REMOVE and
referenced port matches the requested batch. Otherwise state remains failed or
unavailable and later polling/reconciliation handles convergence.

A blocked hwsim RPC cannot make a live syncd heartbeat stale: syncd owns a
separate heartbeat thread and Redis connection. Device readiness is still
reported independently from process liveness.

## Generation recovery

Every hwsim process has a nonzero generation. On change, syncd refreshes known
connection presence and active counts from the new device before publishing a
recovery operation. A generation-specific marker and per-resource markers make
the refresh resumable. The recovery command contains the current DEVICE_DB
intent as an atomic snapshot; old-generation success fences cannot suppress it.

## Drift and port reconciliation

After ordinary work is terminal, syncd periodically compares CONFIG_DB desired
connections with confirmed device connections. A mismatch begins a durable
cycle, publishes `DRIFTED` then `RECONCILING`, raises
`desired-actual-drift`, increments drift/reconciliation counters, and sends all
desired UPSERTs plus REMOVE operations for unexpected actual entries. Normal
configuration and generation recovery take precedence.

Port polling publishes ON_CHANGE state. A DOWN port is removed from the
effective actual matrix and raises `port-down-<direction>-<id>`. An atomic
full-snapshot apply can fail with `OCS_PORT_DOWN`; connections already confirmed
on healthy ports retain their truthful ACTIVE result, while affected or
unconfirmed members fail. Clearing the port triggers one same-version recovery,
then clears port/drift alarms after confirmation.

## Acceptance scenarios C–H

| Scenario | Automated evidence |
|---|---|
| C: device timeout | Desired state persists, actual is absent/old, retry/failure state, timeout counter, active alarm |
| D: recovery | Clear fault, bounded reconciliation reaches ACTIVE, versions match, alarm clears |
| E: syncd crash | Kill after durable completion before ACK, claim pending, side effects remain once-only |
| F: UDS/hwsim restart | Reject unconfirmed update, detect new generation, refresh actual, recover current intent |
| G: drift | Change simulator matrix only, observe drift lifecycle/alarm/counters, converge full snapshot |
| H: port DOWN | Inject input/output faults through the enabled gNMI development API, observe state/alarm/Subscribe DOWN/UP, converge after clear |

These tests use a real Redis container, actual service processes where the
scenario requires them, `UdsDeviceBackend`, and standalone hwsim. They do not
edit production-path Redis state to manufacture recovery.

## Fault injection boundary

Fault injection is development/test-only. Its gNMI subtree is default-closed
and returns `PERMISSION_DENIED` unless the server has
`OCS_ENABLE_FAULT_API=1`. With that explicit flag, `ocsctl` supports bounded,
confirmed commands through the same gNMI, reliable stream, syncd, and UDS
boundaries used by the demo:

```bash
ocsctl fault inject ocs0 next-apply-timeout
ocsctl fault inject ocs0 input-port-down --port 3
ocsctl fault clear ocs0 --all
```

Managed injection covers one-shot apply timeout/error and input/output port
DOWN. Clear-all also restores every simulated port to UP. The non-idempotent
out-of-band drift control remains inside automated C++ fixtures. Scenario E
uses the test-only `OCS_SYNCD_CRASH_BEFORE_ACK_ONCE` process hook, and scenario
F controls the standalone process lifecycle. The local `ocs-hwsimctl` UDS
helper remains available only to low-level tests; see
[Known limitations](limitations.md).
