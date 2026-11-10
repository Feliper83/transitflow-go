# ADR-0005: Idempotent consumers

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

Kafka delivers **at-least-once**: a consumer may process the same record more
than once (a rebalance before the offset is committed, or the outbox relay
re-publishing a row — ADR-0004). For side-effecting consumers this is dangerous:
processing `ProcessPaymentCommand` twice must not charge the passenger twice.

## Decision

Every consumer that causes a side effect derives an **idempotency key** from the
message — the **booking id** — and records processed keys in its own database.

- `payment-service` stores payments with a `UNIQUE (booking_id)` constraint. A
  duplicate command finds the existing row and returns the stored outcome
  instead of charging again.
- `booking-service` treats a booking already in a terminal state (`CONFIRMED` /
  `CANCELLED`) as done, so a redelivered `payment.events` message is ignored.

Consumers commit the Kafka offset only **after** the work (and any resulting
publish) succeeds, so a crash mid-processing re-delivers rather than loses.

## Consequences

- **Positive:** correctness under redelivery — no double charges, no duplicate
  confirmations.
- **Negative:** idempotency state must be stored and eventually pruned; we keep
  it in each service's own database keyed by the natural business id.
- **Non-goal (named):** a malformed "poison" message is logged and skipped here.
  A production system would route it to a dead-letter topic with bounded retry
  and an operational replay process; this reference stubs that boundary.
