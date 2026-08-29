# MVP test report

This report records the final Phase 5 baseline for Iteration 56. It was produced
on 2026-08-29 from committed Iteration 55 revision `4d1b054` in Linux/WSL2. It
is evidence for the implemented simulator MVP, not a performance, scale,
security, optical-fidelity, or real-hardware certification.

## Environment

| Component | Validated version |
|---|---|
| Host | Linux 6.18.33.2-microsoft-standard-WSL2, x86-64 |
| CMake | 4.2.3 |
| C++ compiler | Ubuntu GCC 15.2.0 |
| Python | 3.12.13 |
| uv | 0.12.1 |
| Node.js / npm | 24.19.0 / 11.17.0 locally; production builder pinned to Node.js 22.22.3 by digest |
| Dashboard | TypeScript 7.0.2, Vite 8.2.2, Vitest 4.1.11, Playwright 1.62.1 |
| Docker Engine | 29.7.2 |
| Docker Compose | 5.4.0 |

Application dependencies and images are pinned by `uv.lock`,
`web/package-lock.json`, CMake fetch declarations, Dockerfiles, and Compose
configuration. `npm ci` reported zero known vulnerabilities during every final
dashboard build and browser test.

## Final command results

| Command | Result |
|---|---|
| `make bootstrap` | Passed: pinned uv/Python discovery plus Git, CMake, C++, Make, Node, npm, Docker, Compose, and daemon preflight |
| `make build` | Passed: C++ targets, frozen Python environment, TypeScript type-check, and production Vite bundle |
| `make test` | Passed: 49 active C++ tests; 43 Redis-dependent tests skipped for their dedicated run; 97 Python tests passed and 10 integration/Compose tests skipped; 4 dashboard tests passed; Ruff passed |
| `make redis-integration-test` | Passed: all 44 real-Redis C++ tests and all 8 Python integration/management tests using standalone hwsim and `UdsDeviceBackend` |
| `make sanitizer-test` | Passed: all 49 active C++ tests under ASan/UBSan with leak detection; Redis-dependent cases skipped for their dedicated run |
| `make sanitizer-integration-test` | Passed: all 44 Redis-dependent C++ tests under ASan/UBSan with leak detection |
| `make up` | Passed: Redis, standalone hwsim, orch, syncd, gNMI, and web gateway reached dependency-aware health; Redis returned PONG; `ocs0` reported READY with 16 inputs and 16 outputs |
| `make e2e` | Passed: 2 packaged gNMI/HTTP tests and 1 real Chromium dashboard workflow; isolated resources removed |
| `make demo` | Passed: health, capabilities, ON_CHANGE, valid circuits, atomic conflict, timeout/alarm/counters, clear-to-ACTIVE recovery, diagnostics, and nested packaged/browser E2E; both isolated stacks removed |
| `docker compose config --quiet` | Passed for the default stack and profile definitions |
| Shell, formatting, and documentation checks | Passed: script syntax, `git diff --check`, Markdown relative links, and clean Compose cleanup |

Failure and observation logs are retained only below the ignored `artifacts/`
tree. The final acceptance commands removed every container, network, and volume
that their isolated harnesses created.

## Definition of Done audit

| Specification requirement | Evidence | Result |
|---|---|---|
| `docker compose up` starts all core services | Final `make up`; all six services healthy | PASS |
| One ready 16x16 OCS exists by default | Gateway snapshot: `ocs0`, READY, 16 input and 16 output ports | PASS |
| gNMI Capabilities/Get/Set work | CLI, unit, Redis integration, demo, and packaged E2E | PASS |
| gNMI ON_CHANGE Subscribe works | Initial/sync/change/delete tests and captured demo stream | PASS |
| Connection create, replace, and delete work | CLI management scenarios and browser operation E2E | PASS |
| Input/output conflicts are rejected atomically | Candidate validation, Redis transaction tests, and scenario B | PASS |
| C++ simulator batch apply is atomic | Mixed apply, conflict, output-swap, retry, and failure tests | PASS |
| Formal demo uses standalone `ocs-hwsim` over UDS | Compose topology, demo, and process-boundary tests | PASS |
| `UdsDeviceBackend` has framing, deadlines, correlation, disconnect detection, and reconnect | Codec, transport, failure, restart, and reply-loss suites | PASS |
| hwsim generation changes refresh actual state | Restart/generation integration and scenario F | PASS |
| Redis streams recover pending work | XPENDING/XCLAIM and crash-before-ACK scenario E | PASS |
| orch and syncd are idempotent | Duplicate, stale, crash-boundary, replay, and concurrency suites | PASS |
| Desired/applied versions are observable | gNMI Get, CLI, dashboard, demo, and diagnostics assertions | PASS |
| Timeout, drift, and port DOWN are injectable | Managed fault API plus integration scenarios C, G, and H | PASS |
| Failures produce counter, state, and alarm evidence | Timeout, apply-error, drift, and port-fault assertions | PASS |
| Clearing faults reconciles desired and actual state | Retry/reconciliation suites and scenario D | PASS |
| orch, syncd, and hwsim restarts recover | Service restart, pending recovery, generation, and phase-crash suites | PASS |
| C++ and Python unit tests pass | Final `make test` and both sanitizer commands | PASS |
| Integration/E2E scenarios A-H pass | Final Redis integration plus isolated Compose and demo runs | PASS |
| CI automatically runs tests | GitHub Actions defines Python, C++, web, sanitizer, integration, and Compose jobs; each payload command passed locally | PASS |
| README, architecture, model, failure, and test report are complete | Final documentation/link audit and dedicated documents | PASS |
| Demo never requires manual Redis editing | `make demo` uses only gNMI/CLI/web public workflows and managed faults | PASS |
| Claims remain within the simulator boundary | README and limitations explicitly reject SONiC, SAI OCS, driver, and real-hardware claims | PASS |

All 23 MVP Definition of Done requirements pass. No Phase 6 optional feature was
started. The GitHub Actions workflow was not triggered remotely because this
delivery intentionally creates local commits only and does not push.

## Scenario evidence

| Scenario | Validated outcome |
|---|---|
| A - normal configuration | Three circuits became ACTIVE with equal desired/applied versions and correct active count; watch, replace, and delete behavior passed |
| B - atomic conflict | A two-member output conflict returned `INVALID_ARGUMENT`; neither desired nor operational member appeared |
| C - timeout | One-shot UDS apply timeout preserved desired state, avoided false ACTIVE, raised stable error/alarm/counter evidence, and entered bounded retry |
| D - recovery | Clearing faults allowed retry/reconciliation to restore ACTIVE and clear the timeout alarm |
| E - syncd crash | Pending work was claimed after crash-before-ACK without repeating device, state, event, result, or counter effects |
| F - hwsim restart | A generation change refreshed hardware truth and restored current intent without accepting an unconfirmed mutation |
| G - drift | Simulator-only out-of-band change produced DRIFTED/RECONCILING, alarm/counters, and full-snapshot convergence |
| H - port DOWN | gNMI-fronted development fault commands traversed syncd/UDS; port, connection, alarm, and ON_CHANGE DOWN/UP behavior recovered after clear |

The final demo independently exposes scenarios A-D through supported operator
interfaces. The integration suites supply deeper crash, restart, drift, port,
concurrency, and idempotency assertions for E-H.

## Dashboard evidence

Vitest/jsdom covers the 256 SVG crosspoints, desired/actual/converged and port
styles, deadline-bound API behavior, and stable errors. The pinned Chromium E2E
loads the production bundle, observes WebSocket synchronization, verifies all 32
ports, creates an ACTIVE circuit, checks confirmed matrix/counter state, uses the
supported fault and clear workflow, deletes the circuit, and verifies that the
browser contacts only the web gateway origin.

## Claims not made

The tests do not establish optical signal fidelity, packet forwarding,
throughput, latency targets, long-duration stability, hostile-input security,
high availability, multi-device scale, or real FPGA behavior. See
[Known limitations](limitations.md) for the exact product boundary and
[Testing](testing.md) for how to reproduce each suite.
