# Known limitations

mini-ocs-nos demonstrates a recoverable control-plane loop; it is not a
production network operating system or hardware implementation.

## Product scope

- It controls one simulated 16×16 OCS named `ocs0`; it has no packet data plane,
  optical signal model, traffic generator, or throughput measurement.
- It is SONiC-inspired, not SONiC-compatible. It does not build a SONiC image,
  implement standard SAI objects, or reuse SONiC schemas/daemons.
- The `mini-ocs-native` gNMI data model is implemented directly in code and is
  not backed by an OpenConfig or project YANG module.
- `ocs-hwsim` models an atomic matrix, ports, delay, generation, and faults in
  memory. Restarting it creates a new generation and loses its matrix; this is
  deliberate and exercises control-plane recovery.

## Security

- Development gNMI is insecure: there is no TLS/mTLS, authentication,
  authorization, role separation, certificate rotation, or audit log.
- Compose binds gNMI to localhost and keeps Redis internal. Changing those
  bindings without adding security is unsupported.
- Redis persistence is a local named volume and is not configured for HA,
  encryption, authenticated remote access, backup, or disaster recovery.
- Fault control is local test infrastructure and must not be exposed to an
  untrusted network. The public gNMI API has no fault/reset endpoint.

## Management and operations

- `ocsctl` supports Capabilities, device/port/connection reads, connection
  create/replace/batch/delete/watch, alarms, counters, diagnostics, and raw Get.
  It does not yet support `fault inject`, `fault clear`, or a diagnostics bundle.
- `ocs-hwsimctl` is an internal UDS helper and currently exposes only input and
  output port DOWN/clear plus health. Other deterministic faults are invoked by
  test fixtures through the device abstraction.
- The automated operator demo, consolidated test report, and two-minute video
  script are separate Phase 5 follow-up deliverables. The current Makefile has
  no `demo` target.
- Logs are structured and collected on test failure, but there is no centralized
  log service, metrics exporter, dashboard, tracing system, or alert receiver.

## Availability and scale

- The supported topology has one active orch, one active syncd, one gNMI
  server, one Redis instance, and one hwsim. Consumer fencing protects recovery
  races, but active/active service HA is not a deployment claim.
- Resource scans and replay caches are deliberately bounded for the MVP. The
  system has not been load-, soak-, chaos-, or performance-tested beyond the
  functional concurrency and failure cases.
- Compose health and restart policies target a single development host. There
  is no Kubernetes packaging, rolling upgrade, schema migration, or multi-node
  quorum.
- Subscribe implements only the required STREAM ON_CHANGE/TARGET_DEFINED
  subset for connection, port, and alarm state. SAMPLE, POLL, ONCE, QoS,
  heartbeat, aggregation, and history are unsupported.

## Hardware boundary

- UDS is an IPC protocol, not a Linux character device, sysfs interface,
  kernel driver, MMIO/register map, or vendor SDK.
- No real FPGA timing, interrupts, DMA, reset sequencing, firmware upgrade,
  optical power, link training, or hardware access permissions are modeled.
- Replacing hwsim requires a new `OcsDeviceApi` backend and real driver/SDK
  integration that preserves atomic apply, confirmed reads, deadlines,
  operation idempotency, error mapping, and reset/generation detection.

## Deferred work

mTLS/RBAC, Prometheus metrics, complete diagnostics bundles, a character-device
lab, register-map simulation, gNOI, SONiC VS exploration, traffic-aware topology
optimization, and an experimental C++ gNMI server are Phase 6 candidates. They
must not be described as implemented MVP behavior.
