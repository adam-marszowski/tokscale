# ADR-001: Add an Open Mercato uploader in a company fork of Tokscale

## Status

Accepted

## Context

The company needs a local collector that already understands Codex and Claude local artifacts, but must also:

- enroll devices into the internal reporting system,
- upload privacy-safe rollups to Open Mercato,
- support internal auth, retry, and operational support.

Three options were considered:

1. wrap upstream `tokscale` externally,
2. fork upstream `tokscale`,
3. build a new collector from scratch.

## Decision

Fork `tokscale` and implement the Open Mercato uploader, local state, and enrollment behavior in the fork.

## Rationale

- upstream already solves the hard local parsing problem,
- wrapper approach would create awkward ownership boundaries around parser state and upload state,
- greenfield collector would recreate a lot of parser work for little product value,
- a fork gives release control and internal supportability.

## Consequences

- parser logic should remain close to upstream where feasible,
- company-specific logic must stay isolated to uploader/export/enrollment concerns,
- the fork needs an internal release cadence and merge strategy for upstream updates.
