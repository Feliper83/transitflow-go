# ADR-0001: Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

This is a portfolio-grade distributed-systems project. The *reasoning* behind
each pattern matters as much as the code. Decisions made implicitly are easy to
forget and hard to challenge later.

## Decision

Capture significant architectural decisions as short, immutable ADRs in
`docs/adr/`, numbered sequentially. Each records the context, the decision, and
its consequences.

## Consequences

- A newcomer can read the design rationale in minutes.
- Superseding a decision means adding a new ADR, not editing history.
