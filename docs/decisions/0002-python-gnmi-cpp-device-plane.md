# ADR 0002: Python gNMI and C++ device-facing services

- Status: Accepted
- Date: 2026-08-27

## Context

The management plane needs protobuf handling, path validation, streaming RPCs,
and test automation. Device-facing services need explicit state machines,
concurrency, transport deadlines, and a replaceable hardware abstraction.

## Decision

Implement the gNMI server and CLI in Python 3.12. Implement orchestration,
synchronization, device abstraction, UDS transport, and hardware simulation in
C++20. Redis and versioned UDS contracts form the language boundaries.

## Consequences

The management implementation can evolve independently of device backends.
Rewriting gNMI in C++ is optional Phase 6 work and cannot block or substitute for
MVP reliability and recovery behavior.
