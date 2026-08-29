# Architecture

mini-ocs-nos is a SONiC-inspired control-plane prototype for one simulated 16×16
optical circuit switch. It uses SONiC-like separation of desired, application,
device, and operational state, but every public model and internal contract is
project-native.

## Component map

```text
                              management network
  browser <-- HTTP/WebSocket --> web-gateway
                                      | gNMI
                                      v
  ocsctl  <-------- gNMI/JSON_IETF --> gnmi-server (Python)
                                         |
                   +---------------------+---------------------+
                   | Redis snapshots and reliable Streams     |
                   v                                           v
             CONFIG_DB (4)                                STATE_DB (6)
          OCS_CONFIG_EVENTS                       COUNTERS_DB (2), ALARM_DB (8)
                   |                                           ^
                   v                                           |
            ocs-orch (C++20) <--- OCS_DEVICE_RESULTS ----------+
                   |                                           |
            APPL_DB (0) + DEVICE_DB (1)                        |
                   | OCS_DEVICE_COMMANDS                        |
                   v                                           |
            ocs-syncd (C++20) ---------------------------------+
                   |
          AF_UNIX SOCK_SEQPACKET, protocol v1
                   |
                   v
       standalone ocs-hwsim (C++20) -- simulated matrix/ports
```

Compose exposes gNMI and the bounded web gateway, both bound to `127.0.0.1` by
default. Redis is on an internal network. The gateway is exclusively a gNMI
client and does not mount the UDS volume. syncd and hwsim share the named volume
containing that path; they do not communicate through an in-process shortcut.

## Responsibilities

| Component | Owns | Does not own |
|---|---|---|
| `ocsctl` | Human/JSON workflows, RPC deadlines, bounded watches | Redis keys, hardware state |
| `web-gateway` | Bounded browser REST/WebSocket, gNMI translation and reconnect | Redis keys, UDS, hardware state |
| `gnmi-server` | Capabilities/Get/Set/Subscribe, path and payload validation, atomic desired-state commit | Hardware apply success |
| `ocs-orch` | Connection state machine, durable config/result consumption, retry policy, application state and timeout alarms | UDS or simulator calls |
| `ocs-syncd` | Device command consumption, UDS calls, confirmed state/counters, generation refresh, drift/port reconciliation | Desired-state admission |
| `ocs-hwsim` | Atomic 16×16 matrix, port state, generation, deterministic test faults | Redis or gNMI |
| Redis | Durable snapshots, streams, pending entries, recovery markers | Device truth by itself |

## Configuration flow

1. A client sends gNMI Set using the `mini-ocs-native` origin and JSON_IETF.
2. The server loads the complete desired snapshot for the target device, applies
   deletes, replaces, then updates to a candidate, and validates device, ports,
   IDs, dimensions, and input/output uniqueness.
3. One CONFIG_DB optimistic transaction replaces affected snapshots, increments
   the device revision, and appends one atomic batch to `OCS_CONFIG_EVENTS`.
4. orch consumes the event, durably prepares the batch, advances every member
   through the centralized state machine, and emits one device command batch.
5. syncd persists an attempt, sends `APPLY_CONNECTIONS` through
   `UdsDeviceBackend`, stores the device result, then publishes state, counters,
   and one result per member through independently recoverable phases.
6. orch consumes results and publishes terminal application state. gNMI Get and
   Subscribe expose the confirmed snapshots and ON_CHANGE events.

The SetResponse is returned after step 3, not step 6. This separates admission
and durable desired-state persistence from asynchronous hardware confirmation.

## Process and language boundaries

Python 3.12 handles protobuf generation, asynchronous gRPC, path/payload
validation, Redis-backed management transactions, the CLI, and the separately
packaged gNMI-only browser gateway. C++20 owns the state machine, orchestration,
synchronization, device abstraction, UDS transport, and simulation. Redis event
schemas, gNMI, and the UDS protocol are explicit process boundaries.

This split is recorded in
[ADR 0002](decisions/0002-python-gnmi-cpp-device-plane.md). Moving gNMI to C++
would not improve the hardware abstraction and is outside the MVP.

## Device abstraction and UDS

`OcsDeviceApi` is implemented by:

- `InProcessSimBackend`, used for deterministic unit tests;
- `UdsDeviceBackend`, used by formal integration and Compose paths.

The UDS protocol uses `AF_UNIX`/`SOCK_SEQPACKET`, a 32-byte big-endian header,
magic `OCS1`, protocol version 1, message type, flags, request ID, device
generation, and payload length. Payloads are limited to 1 MiB. Client requests
have deadlines and response request IDs must match. The server safely handles a
stale socket path and removes its own path on shutdown.

Read-only requests may reconnect and retry once. A mutation is never blindly
retried after an unknown outcome: syncd reconnects and queries the actual matrix
and referenced ports before it can convert the outcome to success. The
simulator caches bounded operation results by operation ID, so an exact replay
after a crash returns the original success or failure without repeating the
transition.

UDS here is IPC. It is not a character device, kernel driver, MMIO mapping, or
sysfs contract.

## Startup and health

Compose gates dependencies on real readiness:

- Redis must answer `PING`;
- hwsim must answer a UDS identity/health probe with a nonzero generation;
- orch and syncd must publish fresh heartbeats;
- syncd diagnostics must show device readiness and bounded stream pending work;
- gNMI health performs Capabilities plus dependency diagnostics.

At first syncd handshake, missing device inventory is initialized from confirmed
backend identity. The gNMI server rejects an unknown device instead of creating
inventory from an untrusted request.

## Real and simulated boundaries

Real implementation behavior includes the official gNMI service, protobuf
wire format, JSON_IETF values, gRPC streaming, Redis transactions and Streams,
multiple OS processes, UDS framing, deadlines, restarts, idempotency, alarms,
counters, and reconciliation.

Simulated behavior includes optical switching, the 16×16 matrix, port signal
state, switching delay, device firmware/serial identity, and injected hardware
faults. No packet data plane or optical signal is transported.

## Replacing the simulator with an FPGA

Keep the gNMI server, native model, Redis contracts, orch, state machine, and
most syncd convergence logic. Implement another `OcsDeviceApi` backend that
maps atomic full-snapshot operations, identity, health, matrix reads, and port
reads to a real driver or vendor SDK. The backend must retain deadlines,
operation identity/idempotency, generation or reset detection, stable errors,
and confirmed reads. Hardware-specific initialization, register access,
interrupts, firmware lifecycle, and privilege boundaries belong below that
interface.

See [ADR 0001](decisions/0001-sonic-inspired-not-sonic-compatible.md) for the
scope decision, [Data model](data-model.md) for persistence, and
[Failure model](failure-model.md) for recovery behavior.
