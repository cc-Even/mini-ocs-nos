# Interactive OCS dashboard

The dashboard is a small, framework-free TypeScript application packaged into
the `web-gateway` image and served at `http://127.0.0.1:8080`. It visualizes the
same native state available to `ocsctl`; browser code uses only the versioned
REST and WebSocket contract documented in [Web gateway](web-gateway.md).

## What it shows

- an SVG 16×16 input/output crosspoint matrix;
- separate desired, confirmed actual, converged, and unavailable-port styles;
- connection ID, desired/actual endpoints, and apply status;
- all input and output port operational states and optical power;
- active alarms, device counters, and diagnostics;
- device health, active circuit count, desired/actual count, and revision;
- live connection, port, and alarm changes from gNMI ON_CHANGE Subscribe.

The connection form supports update and replace, and every row has a delete
operation. HTTP acceptance is shown as desired-state admission; the matrix only
uses the confirmed state to mark an actual circuit. The fault panel supports
the same timeout/error and port-DOWN commands as `ocsctl`, plus clear-all. It
returns a permission error when the default-closed development fault API is not
enabled.

## Build and test

The repository pins Node 22.22.3 in the image builder and locks exact direct and
transitive npm dependencies in `web/package-lock.json`. The production bundle
uses TypeScript and Vite without a runtime framework.

```bash
npm ci --prefix web
npm run --prefix web build
npm run --prefix web test
make e2e
```

Vitest/jsdom tests cover the 256-cell SVG, desired/actual/port styling, bounded
API requests, and stable gateway errors. `make e2e` also starts a short-lived
official Playwright container pinned by version and image digest. Its real
Chromium test loads the packaged dashboard, synchronizes the WebSocket, verifies
all ports/crosspoints, creates and confirms a circuit, exercises the supported
fault/clear workflow, deletes the circuit, and proves the browser only contacts
the gateway origin. The test profile is not part of normal `make up`.

## Operational boundary

The UI and gateway are insecure localhost development interfaces. There is no
multi-user authorization, CSRF design, TLS termination, or production content
security policy. Dynamic device data is rendered as text rather than injected
HTML. Request bodies, identifier formats, batches, RPCs, WebSocket lifetime,
queues, messages, and reconnect attempts are bounded by the gateway contract.

Replacing the frontend does not change any device-facing boundary: the browser
cannot address Redis, orch, syncd, the UDS socket, or hwsim. It can only ask the
gateway to perform public gNMI operations.
