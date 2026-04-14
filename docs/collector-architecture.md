# Collector Architecture

## TLDR

`tokscale-om` is the employee-side collector. It parses local Codex and Claude Code usage artifacts, keeps local parser and delivery state, aggregates data into privacy-safe hourly buckets, and uploads those buckets to Open Mercato once per workday on a local scheduled run.

The collector is intentionally **local-first** and **privacy-constrained**:

- parsing stays on the laptop,
- central upload carries hourly rollups only,
- transcript content never leaves the device.

## Supported sources in V1

- `codex` from local Codex session and headless artifacts
- `claude` from local Claude Code artifacts

Out of scope for V1:

- Cursor
- OpenCode
- vendor API sync
- billing imports

## High-level architecture

### Layers

1. **Upstream parser layer**
   - existing `tokscale` scanners and parsers
   - reused with minimal change

2. **Company aggregation layer**
   - transforms parsed local messages into finalized hourly rollups
   - enforces privacy-safe field allowlist

3. **State layer**
   - tracks offsets, parser state, finalized buckets, and upload acknowledgements

4. **Uploader layer**
   - signs requests with device token
   - sends batches to Open Mercato
   - handles retry and ack semantics

## Local parser state model

The collector maintains a local state file under its own config directory.

### State sections

#### `device`

- `deviceId`
- `deviceFingerprint`
- `userBindingId`
- `collectorVersion`
- `channel`

#### `sources`

Per source file / source root state:

- source type
- canonical path hash
- last consumed offset
- parser-specific state snapshot if needed
- last successful scan time

#### `finalizedBuckets`

Per finalized hourly bucket:

- unique bucket key
- batch membership
- finalized at
- payload hash
- ack status

#### `uploadLedger`

Per upload batch:

- `collectorBatchId`
- created at
- sent at
- server ack status
- server batch id if returned
- retry count

## Hourly aggregation model

### Bucket close rule

Only **closed** hours are eligible for upload.

Examples:

- at `09:15 UTC`, the collector may upload `08:00 UTC` and earlier
- the currently open `09:00 UTC` hour is not uploaded as final

### Bucket key

One hourly bucket is uniquely identified by:

- `hourStartUtc`
- `sourceClient`
- `providerId`
- `modelId`
- `agentName`
- `workspaceFingerprint`

### Metrics included

- `inputTokens`
- `outputTokens`
- `cacheReadTokens`
- `cacheWriteTokens`
- `reasoningTokens`
- `messageCount`
- `turnCount`
- `sourceSessionCount`
- `estimatedUsd`

### Workspace handling

- default central identity: `workspaceFingerprint`
- optional human-friendly `workspaceLabel`
- privacy default: omit `workspaceLabel` from upload unless the collector explicitly enables it
- no raw local path is uploaded

## Upload cadence

- default schedule: **13:00 local time on weekdays**, configurable via `tokscale om configure`
- optional diagnostic mode: foreground daemon loop
- every run:
  1. scan local sources incrementally
  2. update local hourly buckets
  3. finalize closed hours
  4. build one or more upload batches from unacked finalized buckets
  5. send to Open Mercato
  6. mark acknowledgements

## Offline buffering

The collector must tolerate:

- laptop offline
- VPN disconnected
- server unavailable
- transient TLS/DNS failures

### Rules

- finalized buckets are retained locally until acknowledged
- failed uploads remain queued for retry
- collector never drops acknowledged state on restart
- collector must survive process restarts without replay inflation

## Ack and retry logic

### Batch idempotency

Each upload batch has a stable `collectorBatchId`.

If the same batch is retried:

- the server may return `accepted` on first receipt,
- the server may return `duplicate` on replay,
- either outcome is treated as successful acknowledgement.

### Retry policy

- retry on network and `5xx` failures
- do not retry on `4xx` schema/auth failures without operator action
- exponential backoff with capped retry interval

### Local handling

- successful `accepted` or `duplicate` marks the batch acked
- acked buckets are retained only as long as needed for local dedup safety

## Device identity model

### Device identity inputs

- stable collector-generated `deviceFingerprint`
- admin-assigned device enrollment in Open Mercato
- per-device token

### Device auth

- `Authorization: Bearer <device token>`
- one active token per device in v1
- token rotation handled centrally in Open Mercato

## Privacy exclusions

The collector must not send:

- prompts
- responses
- transcripts
- source file contents
- raw local paths
- repository paths
- vendor credentials

The collector may keep transient parsed raw messages in memory locally, but not persist them as a company-specific telemetry log unless explicitly required in a later ADR.

## Implementation boundary versus upstream

Keep upstream parser behavior intact wherever possible.

Company-owned logic should live in clearly separated modules:

- enrollment/config
- batch builder
- uploader client
- local state store
- privacy filter

The fork should avoid invasive parser rewrites unless upstream behavior makes the product requirements impossible.
