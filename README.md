# tokscale-om

Company-maintained fork of [`junhoyeo/tokscale`](https://github.com/junhoyeo/tokscale) used as the foundation for the employee-side collector that reports AI coding usage into Open Mercato.

## Scope

This repository exists to produce a **local collector** that:

- parses supported local AI coding tool artifacts on employee laptops,
- builds privacy-safe hourly usage aggregates,
- authenticates with the internal Open Mercato application,
- uploads finalized usage buckets for centralized reporting.

V1 source scope:

- Codex CLI
- Claude Code

## Non-goals

This fork does **not** aim to:

- become the source of billing truth for vendors,
- export prompts, responses, or transcripts,
- act as a remote control agent,
- centralize raw session content,
- replace the Open Mercato application.

## Why a fork

The upstream project already solves the hard local parsing problem for the sources we need. The company-specific work is a thin but opinionated layer on top:

- enrollment
- device identity
- upload/auth
- privacy-safe aggregation
- internal release and support workflow

We keep parser logic as close to upstream as possible and isolate company behavior to uploader/export/enrollment concerns.

## Upstream reference

- Upstream repository: `junhoyeo/tokscale`
- Local baseline commit used for preparation: `6d0880a`
- Upstream original README preserved at [README.upstream.md](./README.upstream.md)

## Primary documents

- [Collector Architecture](./docs/collector-architecture.md)
- [ADR-001: Open Mercato uploader](./docs/adr/ADR-001-open-mercato-uploader.md)
- [ADR-002: Hourly rollups](./docs/adr/ADR-002-hourly-rollups.md)
- [ADR-003: Device token auth](./docs/adr/ADR-003-device-token-auth.md)
- [Release Plan](./docs/release-plan.md)

## Current repository state

This repository now includes an initial Open Mercato collector implementation for the V1 source scope:

- privacy-safe hourly bucket generation for Codex CLI and Claude Code,
- local collector config and state under `~/.config/tokscale-om`,
- manual collector commands: `tokscale-om om configure`, `tokscale-om om status`, `tokscale-om om sync`, and `tokscale-om om retry-blocked`,
- optional foreground daemon mode via `tokscale-om om daemon`,
- per-user scheduled install/uninstall for `launchd` on macOS and `systemd --user` on Linux,
- default daily local schedule at `13:00` on weekdays, configurable in `tokscale-om om configure`,
- upload batching with deterministic sub-batches, local ack tracking, retry/backoff, and blocked-batch handling,
- device token storage outside `config.json` with keyring-first storage and a secure local fallback,
- ingest payloads aligned to the Open Mercato V1 collector contract at `/api/ai-usage/collector/v1/ingest`,
- workspace labels omitted from central upload by default unless explicitly enabled in collector config.

Enrollment still remains a placeholder around a manually supplied device token until the server-side Open Mercato flow is available.
