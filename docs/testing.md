# Testing

The test strategy separates fast domain checks from formal Redis/UDS/process
integration and a packaged gNMI-only E2E. Time-sensitive tests use bounded
deadlines and polling rather than relying on one short fixed sleep.

## Local commands

| Command | Coverage | External requirements |
|---|---|---|
| `make test` | C++/Python/dashboard unit tests, dashboard build, Ruff | Compiler, CMake, Python, Node.js 22.12+; Redis tests skip |
| `make redis-integration-test` | Redis contracts, orch/syncd, reliability C–H, real gNMI/CLI process tests | Docker daemon and Unix sockets |
| `make sanitizer-test` | Non-Redis C++ suite with ASan/UBSan/leak detection | GCC or Clang |
| `make sanitizer-integration-test` | Redis-dependent C++ suite with ASan/UBSan | Docker daemon |
| `make image` | All pinned, non-root service image targets | Docker daemon |
| `make up` | Complete dependency-gated runtime health | Docker daemon; localhost ports 50051 and 8080 |
| `make e2e` | Fresh isolated Compose stack through gNMI and its web adapter | Docker daemon; ports 50052 and 8082 by default |
| `npm run --prefix web test` | SVG matrix and browser API unit contracts | Node.js 22.12+ |
| `make demo` | Guided operator flow, managed fault/recovery, diagnostics, and packaged E2E | Docker daemon; localhost ports 50053, 8083, 50052, and 8082 |

Run `make down` after manual Compose use. The integration and E2E harnesses have
their own cleanup traps. The E2E project name and port can be overridden with
`OCS_E2E_PROJECT_NAME` and `OCS_E2E_GNMI_PORT`.

`make demo` uses its own Compose project, enables the otherwise closed simulator
fault API only inside that stack, stores logs below `artifacts/demo/`, and
removes its containers, networks, and volumes on exit. Override its project and
ports with `OCS_DEMO_PROJECT_NAME`, `OCS_DEMO_GNMI_PORT`, and
`OCS_DEMO_WEB_PORT`.

## Test layers

### C++ unit and contract tests

- atomic create/update/delete and matrix conflict behavior;
- state-machine transitions, stable error codes, event schemas, and Redis keys;
- UDS codec, malformed/oversize frames, request correlation, timeout,
  disconnect, reconnect, generation change, stale path cleanup, and shutdown;
- in-process and standalone simulator behavior, operation replay, faults, and
  atomic old-matrix preservation;
- orchestrator/syncd idempotency, version fences, recovery phases, retry,
  reconciliation, ports, counters, alarms, and heartbeat independence.

The default test run intentionally skips tests whose fixture requires a real
Redis container. The integration command selects those tests explicitly.

### Python unit tests

- native path parsing and unsafe-input rejection;
- JSON_IETF payload and complete-candidate validation;
- update/replace/delete and atomic swap/conflict semantics;
- Redis transaction retries and gRPC status mapping;
- Get typing/filtering and missing-resource behavior;
- Subscribe snapshot/sync/change/delete ordering and cancellation;
- `ocsctl` deadlines, batch validation, ACTIVE waiting, malformed replies, and
  absence of a Redis dependency;
- default-closed fault authorization, strict fault payload/path validation, and
  confirmed reliable command results.

Real-Redis and Compose-marked pytest cases skip in the default run and execute
under their dedicated harnesses.

### Redis and process integration

`make redis-integration-test` starts pinned Redis 7.4.5 with a random temporary
Unix socket, builds the C++ programs, and runs the Redis-dependent C++ and Python
suites. Formal device calls use `UdsDeviceBackend` and standalone hwsim. The
suite covers transaction visibility, stream delivery/pending recovery,
multi-phase crash points, exact result replay, concurrent consumers, gNMI
management, CLI behavior, service restarts, diagnostics, and scenarios A–H.

### Compose E2E

`make e2e` creates an isolated full stack, waits for all health checks, and uses
the published gNMI and localhost web endpoints. It creates three connections,
waits for ACTIVE with matching versions/counters, then verifies that a
conflicting two-member batch is atomically rejected and absent from both
configuration and operational reads.

The harness also enables the otherwise closed simulator fault API only inside
its isolated project and runs a pinned official Playwright/Chromium container.
The browser loads the packaged dashboard, observes WebSocket synchronization,
creates an ACTIVE circuit, verifies the SVG crosspoint and counters, injects and
clears a supported demo fault, deletes the circuit, and makes no request outside
the gateway origin. The browser-test profile is absent from normal startup.
Cleanup removes the test project's containers, networks, and volumes.

### Reproducible operator demo

`make demo` starts an isolated full stack and uses only health commands and
`ocsctl` at its management boundary. It captures a bounded ON_CHANGE stream,
creates three ACTIVE circuits, proves a conflicting batch is rejected, injects
a one-shot timeout, shows FAILED state plus alarm/counter evidence, clears the
fault, waits for automatic ACTIVE recovery, prints diagnostics, and finishes
with the packaged E2E suite. See [Demo](demo.md) and the current
[test report](test-report.md).

## Scenario matrix

| Scenario | Expected result |
|---|---|
| A: normal config | 1→9, 2→10, 3→11 become ACTIVE; desired/applied versions match |
| B: atomic conflict | Two connections sharing output 12 are rejected; neither is committed or applied |
| C: timeout | Desired remains; actual does not falsely advance; retry state/counter/alarm appear |
| D: recovery | Clearing failure converges to ACTIVE and clears alarm |
| E: syncd crash | Pending command is claimed after restart with no duplicate effect |
| F: hwsim restart | New generation refreshes actual state and restores current intent |
| G: drift | Out-of-band simulator change is detected and full-snapshot reconciliation succeeds |
| H: port DOWN | Port/connection/alarm/Subscribe changes are correct and recover after clear |

## CI

`.github/workflows/ci.yml` runs on pushes and pull requests with read-only
repository permissions:

1. Python dependency sync, lint, and pytest;
2. C++ configure, build, and CTest;
3. pinned TypeScript build and SVG/API unit tests;
4. ASan/UBSan unit and Redis integration runs;
5. real Redis and service-process integration;
6. full Compose build, health, reproducible demo, and browser E2E.

Failed jobs upload relevant logs for 14 days. Compose cleanup runs even when a
job fails.

## Logs and reproducibility

Integration output, Redis logs, CTest/pytest logs, and service logs are written
below the ignored `artifacts/test-logs/` tree. CI uploads that tree on failure.
Simulator identity and random seed are deterministic, dependencies and base
images are pinned/locked, and each E2E harness cleans its isolated resources.

For the latest exact test counts and commands, use the current iteration handoff
in `docs/iteration-state.md` when working inside the development repository.
That file is intentionally local/ignored and is not a stable public test report.
