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
ocsctl get '/ocs/devices/device[name=ocs0]/connections/connection[id=conn-001]/state'
```

Human output is the default. Add `--json` as a global option for stable JSON.

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

## Phase 3 acceptance

`make redis-integration-test` launches standalone hwsim, orch, syncd, and gNMI
components and executes the CLI over a localhost gNMI endpoint. Scenario A
creates 1→9, 2→10, and 3→11, verifies all are ACTIVE with matching versions and
`active-connections=3`, then exercises watch, replace, and delete. Scenario B
submits a conflicting batch and verifies through gNMI that configuration,
operational state, and counters remain unchanged.
