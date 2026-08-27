# MVP test report

This report records the latest validated Phase 5 baseline for Iteration 53. It
was produced on 2026-08-28 from the repository worktree immediately before the
iteration commit. It is evidence for the implemented simulator MVP, not a
performance, scale, security, or real-hardware certification.

## Environment

| Component | Validated version |
|---|---|
| Host | Linux 6.18.33.2-microsoft-standard-WSL2, x86-64 |
| CMake | 4.2.3 |
| C++ compiler | Ubuntu GCC 15.2.0 |
| Python | 3.12.13 |
| uv | 0.12.1 |
| Docker Engine | 29.7.2 |
| Docker Compose | 5.4.0 |

Project dependencies and images remain pinned by the lockfile, CMake fetch
declarations, Dockerfiles, and Compose configuration.

## Command results

| Command | Result |
|---|---|
| `make test` | Passed: 49 active C++ tests; 43 Redis-dependent tests skipped; 87 Python tests passed and 9 integration/Compose tests skipped; Ruff passed |
| `make redis-integration-test` | Passed: all 44 real-Redis C++ tests and all 8 Python integration/management tests, using standalone hwsim and `UdsDeviceBackend` |
| `make sanitizer-test` | Passed: all 49 active C++ tests under ASan/UBSan with leak detection; Redis-dependent cases skipped for their dedicated run |
| `make sanitizer-integration-test` | Passed: all 44 Redis-dependent C++ tests under ASan/UBSan with leak detection |
| `make demo` | Passed: isolated full-stack operator flow, fault/recovery evidence, diagnostics, ON_CHANGE capture, and nested packaged E2E (`1 passed`) |
| `docker compose config --quiet` | Passed for the packaged stack |
| `bash -n scripts/demo.sh` | Passed |
| `uv run --frozen ruff check python` | Passed |

All Redis, integration, E2E, and demo containers, networks, and volumes created
by these harnesses were removed after their runs. Failure and observation logs
are retained only below the ignored `artifacts/` tree.

## Scenario evidence

| Scenario | Validated outcome |
|---|---|
| A — normal configuration | Three circuits became ACTIVE with equal desired/applied versions and correct active count; watch, replace, and delete behavior passed |
| B — atomic conflict | A two-member output conflict returned `INVALID_ARGUMENT`; neither desired nor operational member appeared |
| C — timeout | One-shot UDS apply timeout preserved desired state, avoided false ACTIVE, raised stable error/alarm/counter evidence, and entered bounded retry |
| D — recovery | Clearing faults allowed retry/reconciliation to restore ACTIVE and clear the timeout alarm |
| E — syncd crash | Pending work was claimed after crash-before-ACK without repeating device, state, event, result, or counter effects |
| F — hwsim restart | A generation change refreshed hardware truth and restored current intent without accepting an unconfirmed mutation |
| G — drift | Simulator-only out-of-band change produced DRIFTED/RECONCILING, alarm/counters, and full-snapshot convergence |
| H — port DOWN | gNMI-fronted development fault commands traversed syncd/UDS; port, connection, alarm, and ON_CHANGE DOWN/UP behavior recovered after clear |

The Iteration 53 demo independently makes scenarios A, B, C, and D visible to
an operator through supported interfaces. The integration suites supply the
deeper crash, restart, drift, port, concurrency, and idempotency assertions for
E–H.

## Iteration 53 additions

The new fault workflow was verified at each boundary:

- strict path, payload, supported-type, and single-operation validation;
- `PERMISSION_DENIED` when `OCS_ENABLE_FAULT_API` is not enabled;
- `ocsctl` gNMI requests with no Redis dependency;
- reliable DEVICE_DB delivery and confirmed syncd result handling;
- `ALL` clear restoring one-shot flags and every simulated port to UP;
- Scenario H using the gNMI/CLI path rather than direct Redis or CLI-to-UDS
  access; and
- a complete packaged demo using a standalone hwsim process.

## Claims not made

The tests do not establish optical signal fidelity, packet forwarding,
throughput, latency targets, long-duration stability, hostile-input security,
high availability, multi-device scale, or real FPGA behavior. See
[Known limitations](limitations.md) for the exact product boundary and
[Testing](testing.md) for how to reproduce each suite.
