# ADR 0003: Interactive dashboard replaces video script

- Status: Accepted
- Date: 2026-08-28

## Context

The specification includes a two-minute demonstration video script and defers a
Web UI until after the original MVP. An interactive view provides stronger,
repeatable evidence of the same running-system behavior: the optical matrix,
desired versus confirmed state, asynchronous apply transitions, port faults,
alarms, counters, diagnostics, and recovery can be observed directly instead of
being described in a fixed recording script.

The browser cannot use the current native gNMI endpoint directly, and allowing
it to read Redis or call the simulator UDS would bypass the project's formal
management and device boundaries.

## Decision

Remove the video-script deliverable and add the visualization to Phase 5 before
final MVP acceptance. Deliver it in two test-gated iterations:

1. a separately packaged Python gateway translating bounded REST/WebSocket
   operations to gNMI Get/Set/Subscribe;
2. a pinned TypeScript/SVG single-page dashboard showing the 16×16 matrix,
   desired/actual state, ports, alarms, counters, diagnostics, live changes,
   connection operations, and supported demo faults.

The gateway is a gNMI client. It must not import Redis repositories, expose
Redis data structures, or call hwsim UDS. Public browser mutation paths retain
the same validation, atomic Set semantics, deadlines, and confirmed-state
distinction as `ocsctl`. Development endpoints remain localhost-only until a
future authentication and TLS design exists.

## Consequences

Final MVP acceptance moves from Iteration 54 to Iteration 56 and includes the
packaged dashboard and browser-facing E2E coverage. The project gains pinned
frontend build dependencies and another Compose service boundary. The scripted
operator demo and test report remain required, while production UI security,
multi-user authorization, and remote exposure remain outside the MVP.

This intentionally deviates from the original specification's delivery order
and final-deliverable list. The specification remains unchanged as the record of
the original requirements; this ADR and the development plan record the approved
scope change.
