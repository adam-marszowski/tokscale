# ADR-002: Centralize hourly rollups instead of raw events or full sessions

## Status

Accepted

## Context

The local parser can observe finer-grained events than the central platform needs. Centralizing raw events or full sessions would increase:

- privacy risk,
- data volume,
- dedup complexity,
- reporting surface complexity.

## Decision

The collector will upload **finalized hourly rollups** to Open Mercato.

## Rationale

- hourly rollups are sufficient for management reporting,
- they materially reduce privacy exposure,
- they simplify idempotency and retry behavior,
- they keep the central schema stable and compact.

## Consequences

- per-message central analytics are unavailable in v1,
- the collector still parses finer-grained local messages internally,
- the currently open hour is not uploaded as final.
