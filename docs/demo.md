# Reproducible operator demo

`make demo` provides a guided, self-checking view of the complete control loop.
It uses `ocsctl` and gNMI at the operator boundary; it never edits Redis or
connects the CLI directly to the simulator socket.

## Run it

Prerequisites are the same as the main quick start: Linux or WSL2, a working
Docker Engine with Compose, GNU Make, and the pinned Python environment created
by `make bootstrap`.

```bash
make demo
```

The command builds and starts an isolated Compose project named
`mini-ocs-nos-demo` on gNMI port `127.0.0.1:50053` and dashboard port
`127.0.0.1:8083`. It enables the default-closed development fault API only for
that project. A cleanup trap removes its containers, networks, and volumes
whether the run passes or fails. The normal `make up` stack and its data are not
reused.

## What the output proves

The numbered output performs these checks in order:

1. every packaged service reaches its dependency-aware health check, and gNMI
   Capabilities plus diagnostics respond;
2. a bounded connection-state ON_CHANGE subscription reaches its sync marker;
3. circuits 1→9, 2→10, and 3→11 reach confirmed `ACTIVE` state;
4. two proposed circuits sharing output 12 are atomically rejected;
5. `ocsctl fault inject` sends one apply timeout through gNMI, DEVICE_DB, syncd,
   UDS, and standalone hwsim; the new circuit reports `OCS_APPLY_TIMEOUT`, with
   an active alarm and incremented counters;
6. `ocsctl fault clear --all` clears simulator faults, bounded orchestration
   retry restores the circuit to `ACTIVE`, and diagnostics report convergence;
7. the captured ON_CHANGE notifications are printed; and
8. the isolated packaged E2E suite runs its gNMI/HTTP pytest checks and real
   Chromium dashboard workflow.

Set acceptance and device confirmation remain visibly separate throughout the
run. A fault command also returns only after syncd records its confirmed device
result; Redis acceptance alone is never presented as simulator success.

## Logs and overrides

Demo logs are retained below the ignored `artifacts/demo/` directory. In
particular, `connection-watch.log` contains the bounded subscription,
`services.log` contains the Compose service logs, and `compose-ps.log` captures
the final container state before cleanup.

For concurrent or customized local runs:

```bash
OCS_DEMO_PROJECT_NAME=my-ocs-demo \
OCS_DEMO_GNMI_PORT=55053 \
OCS_DEMO_WEB_PORT=58083 \
OCS_DEMO_LOG_DIR=artifacts/my-demo \
make demo
```

`OCS_DEMO_RETRY_MS` changes the demo's bounded retry delay; its default is 3000
milliseconds so the FAILED and alarm state remains observable before recovery.
The nested E2E defaults to gNMI port 50052 and web port 8082 and can use the
documented `OCS_E2E_*` overrides if either port is occupied.

## Security boundary

`OCS_ENABLE_FAULT_API=1` is for local simulation and tests only. Do not enable
it on an endpoint exposed to an untrusted network. The normal Compose default
is disabled. The interface intentionally excludes out-of-band drift, service
crash, and process restart controls; automated reliability fixtures own those
non-operator scenarios.
