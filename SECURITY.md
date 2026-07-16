# Security policy

## Supported versions

`aikit` is currently an unpublished v1 implementation candidate. Until a release line exists,
security fixes are made on the default branch only. Published support windows will be listed here
before the first registry release.

## Report a vulnerability

Please do **not** open a public issue for a suspected vulnerability.

This checkout has no verified public repository or security mailbox yet. The security contact is
therefore **TBD before public release**. Until a private reporting channel is published, contact
the project owner through the private channel by which you received the source and ask for a
secure reporting route **before** sending exploit details, credentials, private data, or logs.
Do not open a public issue containing vulnerability details.

Include, when safe:

- affected commit/version and platform;
- attack prerequisites and security boundary crossed;
- minimal reproduction or proof of concept;
- impact and suggested mitigation;
- whether the issue is already public.

Once a public reporting route exists, the target acknowledgement time will be seven days.
Timelines for validation, remediation, and coordinated disclosure depend on severity and
provider/dependency involvement. Please allow a reasonable remediation window before publication.

## Scope notes

High-value areas include permission bypass, tool execution before approval, path-jail escapes,
containment failures, secret leakage, cross-provider reasoning replay, budget bypass, audit
tampering, unsafe deserialization, and session/memory tenant isolation.

The documented security boundary matters when assessing a report. Built-in Bash can use Seatbelt
or hardened Docker containment; arbitrary Rust executors and Python/Node callbacks run in their
host unless the application isolates them. See [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md).

Never send a real provider key as part of a report. Revoke any credential that may have been
exposed.
