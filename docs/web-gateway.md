# Web visualization gateway

The separately packaged `web-gateway` service is the browser-facing management
boundary. It translates a deliberately small HTTP/WebSocket contract into gNMI
Get, Set, and ON_CHANGE Subscribe calls. It does not import a Redis client, know
Redis keys, mount the hwsim runtime volume, or open the simulator UDS.

Compose exposes the gateway at `http://127.0.0.1:8080` by default and serves the
packaged [interactive dashboard](dashboard.md) at `/`. The interface is insecure
and unauthenticated for local development only. Do not bind it to an untrusted
network.

## REST contract

All routes are under `/api/v1` except the dependency-aware health probe. Device
and connection identifiers match `[A-Za-z0-9_.-]{1,128}`. Request bodies are
limited to 16 KiB by default, unknown fields are rejected, port IDs must be
positive 32-bit integers, and connection batches contain at most 16 unique
members. Every downstream gNMI unary RPC has a configurable deadline that
defaults to three seconds.

| Method and path | gNMI operation | Result |
|---|---|---|
| `GET /healthz` | Capabilities | Gateway and gNMI readiness |
| `GET /api/v1/devices/{device}/snapshot` | Six bounded Get calls | Device, ports, connections, alarms, counters, and diagnostics |
| `PUT /api/v1/devices/{device}/connections/{id}` | Set update or replace | `202` after desired-state admission |
| `POST /api/v1/devices/{device}/connections:batch` | One atomic Set batch | `202` after complete candidate admission |
| `DELETE /api/v1/devices/{device}/connections/{id}` | Set delete | `202` after desired-state admission |
| `POST /api/v1/devices/{device}/faults` | Development Set fault command | Confirmed fault command result |
| `DELETE /api/v1/devices/{device}/faults` | Development Set clear-all | Confirmed clear result |

Connection bodies use JSON field names `input-port`, `output-port`, and an
optional `operation` of `UPDATE` or `REPLACE`. Batch bodies add a `connections`
array whose members also have `id`. Fault bodies support the same bounded demo
surface as `ocsctl`: `NEXT_APPLY_TIMEOUT`, `NEXT_APPLY_ERROR`,
`INPUT_PORT_DOWN`, and `OUTPUT_PORT_DOWN`; port faults require `port` and the
other faults reject it. The gNMI server must still have its default-closed fault
API enabled.

The gateway preserves the management-plane meaning of Set. HTTP `202` means the
candidate was validated and desired state was durably accepted; it does not
claim that hardware is ACTIVE. The snapshot and WebSocket stream expose later
confirmed state.

gRPC failures have a stable JSON envelope:

```json
{
  "error": {
    "code": "UNAVAILABLE",
    "message": "dependency unavailable",
    "retryable": true
  }
}
```

Invalid arguments map to HTTP 422, missing resources to 404, disabled fault
access to 403, dependency unavailability to 503, and deadline expiry to 504.
Unexpected dependency responses are returned as 502 without implementation
details.

## WebSocket contract

Connect to:

```text
ws://127.0.0.1:8080/api/v1/devices/ocs0/events?duration_seconds=300
```

One gNMI Subscribe request contains the device's connections, ports, and alarms
paths. The gateway sends a `ready` envelope, `sync` for the gNMI sync response,
and `update` envelopes for initial and subsequent JSON_IETF notifications.
Deletes retain the native path and set `deleted: true`.

The stream lifetime is capped at five minutes by default and never exceeds one
hour. Its outbound queue defaults to 64 messages, inbound WebSocket frames are
limited to 4 KiB, and client data messages are rejected because this is a
server-streaming contract. A transient gNMI failure produces `reconnecting` and
uses at most three bounded reconnect attempts. Exhaustion produces an `error`
envelope and WebSocket close code 1013. Browser disconnect immediately cancels
the gNMI call and closes its channel.

Configuration bounds are available through the `OCS_WEB_*` environment
variables defined in `web_gateway.config`. The Compose service only joins the
management and control-plane networks; it does not join or mount a device
transport boundary.
