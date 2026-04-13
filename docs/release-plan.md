# Release Plan

## TLDR

The collector will be released as an internal tool with predictable install and update steps, explicit config, and a conservative rollback model.

## Packaging and distribution

### V1 distribution method

- internal GitHub repository
- tagged releases
- installable package distributed as an internal npm package and release archive

### Supported platforms in V1

- macOS arm64
- macOS x64
- Linux x64 best-effort if needed by the company fleet
- Windows supports manual foreground daemon mode, but not service installation in V1

## Install path

Primary install path:

```bash
npm install -g @company/tokscale-om
```

Fallback install path:

- download release archive from internal release page
- unpack and place binary/script according to the release instructions

## Update path

V1 uses **manual controlled updates**:

```bash
npm install -g @company/tokscale-om@<version>
```

No auto-update agent is introduced in V1.

## Config surface

Local config file:

- `~/.config/tokscale-om/config.json`

Required config values:

- `serverUrl`
- `deviceFingerprint`
- `channel`

Optional config values:

- `workspaceLabelStrategy`
- `scan.extraPaths`
- `schedule.dailyHourLocal`
- `schedule.dailyMinuteLocal`
- `schedule.weekdaysOnly`
- `log.level`

Credential storage:

- `deviceToken` is stored outside `config.json`
- primary storage is OS keyring
- secure local fallback is used when keyring integration is unavailable

## Versioning policy

- semantic versioning for company releases
- example: `0.1.0-company.1`
- upstream merge points documented in release notes

## Rollback policy

If a release breaks parsing or upload:

1. stop the collector process
2. reinstall previous known-good version
3. keep local state directory intact
4. rerun upload loop

The local state store is designed so rollback does not require re-enrollment or data loss.

## Scheduled execution

Default V1 runtime:

- one local scheduled run per workday
- default time: `13:00` in the user's local timezone
- configurable through `tokscale om configure`
- macOS uses `launchd`
- Linux uses `systemd --user` timers
- Windows remains manual in V1

## Release gates

Before publishing a collector release:

- Codex parser smoke tests pass
- Claude parser smoke tests pass
- hourly aggregation tests pass
- idempotent uploader tests pass
- forbidden field/privacy tests pass
- service install/uninstall smoke tests pass on supported platforms
- install and rollback procedure verified on a clean laptop
