# ADR-003: Use per-device token authentication

## Status

Accepted

## Context

The collector needs a practical authentication mechanism for unattended uploads from employee laptops.

Options considered:

1. shared organization API key,
2. per-device token,
3. full SSO/OIDC machine login.

## Decision

Use a **per-device token** issued by Open Mercato and bound to a specific enrolled device and user.

## Rationale

- stronger control than a shared org key,
- simple to operate in V1,
- supports rotation and revoke,
- does not require shipping a heavy SSO machine-auth stack in the first release.

## Consequences

- device enrollment is an explicit admin flow,
- token rotation and revoke must be first-class operations in Open Mercato,
- collector config must securely store one token locally.
