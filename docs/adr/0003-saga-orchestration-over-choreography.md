# ADR-0003: Orchestrated SAGA over choreography

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

A booking requires a seat reservation **and** a payment, across two services,
with no distributed transaction available (ADR-0002). The system must stay
consistent when a later step fails — e.g. the seat is held but the payment is
declined. The options are **choreography** (each service reacts to events) and
**orchestration** (one component drives the steps and compensations).

## Decision

Use an **orchestrated SAGA** with `booking-service` as the orchestrator. It owns
the state machine `PENDING → SEAT_RESERVED → CONFIRMED | CANCELLED` and issues
the compensating action (release the seat) when payment fails.

## Consequences

- **Positive:** the business process lives in one place — explicit, testable, and
  easy to extend. Debugging a stuck booking means reading one state machine.
- **Positive:** compensation logic is centralized and obvious.
- **Negative:** the orchestrator is a focal point; we keep it thin — it
  coordinates, it does not do other services' work.
- **Rejected — choreography:** with only 2–3 steps it offered no benefit and
  would scatter "what happens next" across services.
