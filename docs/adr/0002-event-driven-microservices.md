# ADR-0002: Event-driven microservices with a database per service

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

A booking touches three bounded contexts — trips, bookings, payments. Sharing
one database would couple their schemas and deployments and recreate a
distributed monolith. Payment is slow and can fail, so holding a synchronous
request open across the whole flow is poor design.

## Decision

Split into independent services, each owning its **own PostgreSQL database**.
Integrate through **well-defined HTTP APIs** (where an immediate answer is
required) and **asynchronous Kafka events** (for the payment flow). No service
reads another service's tables.

## Consequences

- **Positive:** services evolve, deploy, and scale independently; a slow payment
  never blocks the passenger's request.
- **Negative:** no cross-service transaction — consistency must be achieved with
  a SAGA (ADR-0003), reliable publishing (ADR-0004), and idempotency (ADR-0005).
