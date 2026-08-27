# ADR 0001: SONiC-inspired, not SONiC-compatible

- Status: Accepted
- Date: 2026-08-27

## Context

The project demonstrates control-plane patterns associated with network device
software, but it does not build a SONiC image, implement a standard SAI OCS
object, or control packet-switch hardware.

## Decision

Describe the system as "A SONiC-inspired control-plane prototype for a simulated
optical circuit switch." Use separate Redis-backed desired, application, device,
and operational state, while keeping all names and interfaces project-native.

## Consequences

The architecture can explain SONiC concepts without claiming compatibility.
Documentation, demonstrations, and resume material must distinguish real gNMI,
Redis, and Unix socket behavior from simulated optical hardware.
