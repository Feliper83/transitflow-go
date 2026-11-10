# ADR-0004: Transactional outbox for reliable event publishing

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

When `booking-service` changes a booking's state it must also publish an event.
Doing these as two independent operations — commit to PostgreSQL, then produce
to Kafka — is a **dual-write**: a crash between them either loses the event or
publishes an event for a change that rolled back.

## Decision

Adopt the **transactional outbox pattern**. The state change and the outbound
event are written to the `bookings` and `outbox_events` tables **inside the same
pgx transaction**. A background **relay** goroutine polls unsent rows, publishes
them to Kafka, and marks them sent.

The database transaction is the single source of truth; Kafka is brought into
agreement asynchronously, at-least-once (duplicates are handled by idempotent
consumers — ADR-0005).

## Consequences

- **Positive:** no lost or phantom events; state and its event are atomic.
- **Positive:** survives Kafka being briefly unavailable — events accumulate and
  drain when it recovers.
- **Negative:** adds a polling relay and a little publish latency. A production
  system might tail the outbox with CDC (Debezium) instead of polling.
- **Negative:** the relay can publish a row twice if it crashes after producing
  but before marking it sent — which is exactly why consumers must be idempotent.
