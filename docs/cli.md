# ocsctl gNMI client

`ocsctl` is the management client for the project-native `mini-ocs-native`
model. It never imports a Redis client or reads a Redis key. Use global options
before the command:

```bash
ocsctl --target 127.0.0.1:50051 --timeout-seconds 5 --json capabilities
```

Every unary RPC has a positive deadline. `connection watch` is also bounded;
set its explicit `--duration-seconds` according to the observation window.

## Read commands

```bash
ocsctl capabilities
ocsctl device show ocs0
ocsctl port list ocs0
ocsctl connection list ocs0
ocsctl alarm list ocs0
ocsctl counters show ocs0
ocsctl diagnostics show ocs0
ocsctl get '/ocs/devices/device[name=ocs0]/connections/connection[id=conn-001]/state'
```

Human output is the default. Add `--json` as a global option for stable JSON.

`diagnostics show` returns device health, desired and confirmed-active counts,
drift, active alarms, pending entries for each reliable stream, and freshness-
bounded online status for gNMI, orch, syncd, and standalone hwsim. It reaches
Redis only through the gNMI server; `ocsctl` retains no Redis dependency.

## Connection changes

```bash
ocsctl connection create ocs0 conn-001 --input 3 --output 11
ocsctl connection replace ocs0 conn-001 --input 4 --output 12
ocsctl connection delete ocs0 conn-001
```

Create and replace wait for ACTIVE by default. `--no-wait-active` returns after
atomic desired-state acceptance. The Set response alone never means that the
device applied the operation.

Submit one atomic candidate with repeated `--connection ID:INPUT:OUTPUT`:

```bash
ocsctl connection batch ocs0 \
  --connection conn-001:1:9 \
  --connection conn-002:2:10 \
  --connection conn-003:3:11
```

A conflicting batch, such as two connections using output 12, returns
`INVALID_ARGUMENT`; none of its desired snapshots or events are committed.

## Watch

```bash
ocsctl --json connection watch ocs0 --duration-seconds 30
```

The stream emits existing connection state, `sync-response=true`, subsequent
ON_CHANGE values, and delete notifications. The client cancels the RPC and the
server releases its per-subscription tasks when the duration expires or the
user interrupts it.

## Simulator-only fault control

The gNMI fault subtree is development-only and disabled by default. A server
started with `OCS_ENABLE_FAULT_API=1` accepts these `ocsctl` commands:

```bash
ocsctl fault inject ocs0 next-apply-timeout
ocsctl fault inject ocs0 next-apply-error
ocsctl fault inject ocs0 input-port-down --port 3
ocsctl fault inject ocs0 output-port-down --port 11
ocsctl fault clear ocs0 --fault input-port-down --port 3
ocsctl fault clear ocs0 --all
```

The CLI writes only gNMI Set requests. The server publishes one reliable fault
command, syncd calls its normal `UdsDeviceBackend`, and the RPC returns only
after a confirmed simulator result or its bounded deadline. With the feature
flag absent, the same Set returns `PERMISSION_DENIED`. The flag must never be
enabled on an endpoint exposed to an untrusted network.

`ocs-hwsimctl` remains an internal test helper for direct local UDS testing.
Out-of-band drift is intentionally excluded from `ocsctl`: injecting it is not
an idempotent management operation. Process crash and restart controls also
remain in automated fixtures.

## Management and reliability acceptance

`make redis-integration-test` launches standalone hwsim, orch, syncd, and gNMI
components and executes the CLI over a localhost gNMI endpoint. Scenario A
creates 1→9, 2→10, and 3→11, verifies all are ACTIVE with matching versions and
`active-connections=3`, then exercises watch, replace, and delete. Scenario B
submits a conflicting batch and verifies through gNMI that configuration,
operational state, and counters remain unchanged.
Scenario H enables the development fault API and injects an input-port fault
through `ocsctl`, verifies gNMI Subscribe reports DOWN and UP, observes the
related connection and alarm, then waits for same-version full-snapshot
recovery and zero active alarms.

The reproducible `make demo` flow also uses `ocsctl fault` for a one-shot apply
timeout. Out-of-band drift, process crash, and hwsim restart remain automated
fixture controls.
