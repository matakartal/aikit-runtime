# Security policy

## Supported versions

`aikit-runtime` is currently a source-first v0.2.0 development preview. Security fixes land on the
default branch; no public package-registry support line is claimed.

Source-distribution checks include dependency and CodeQL review. Source users should review the
repository's current security state before deploying a commit.

| Version | Supported |
|---|---|
| `main` (pre-release) | Yes |
| Public registry packages | Not distributed |

## Report a vulnerability

Please do **not** open a public issue for a suspected vulnerability.

Use GitHub's private vulnerability-reporting form:

**[Open a private security advisory](https://github.com/matakartal/aikit-runtime/security/advisories/new)**

Only the repository owner and explicitly added security managers can read a submitted report.
If GitHub does not offer the private form, open a public issue containing only a request for a
private contact route; do not include vulnerability details, credentials, private data, or logs.

### What to include (when safe)

- affected commit/version and platform;
- attack prerequisites and the security boundary crossed;
- minimal reproduction or proof of concept;
- impact and suggested mitigation;
- whether the issue is already public.

### Response targets

| Stage | Target |
|---|---|
| Acknowledgement | Within 7 days |
| Initial severity triage | As soon as practical after acknowledgement |
| Fix / coordinated disclosure | Depends on severity and provider/dependency involvement |

Please allow a reasonable remediation window before public disclosure.

## Scope notes

High-value areas include:

- permission bypass and tool execution before approval;
- path-jail escapes and containment failures;
- secret leakage through audit, sessions, memory, or provider metadata;
- cross-provider reasoning replay;
- budget bypass and audit tampering;
- unsafe deserialization;
- session/memory tenant isolation failures.

The documented security boundary matters when assessing a report. Built-in Bash can use Seatbelt,
Linux namespaces+seccomp, Windows Job Objects, or hardened Docker containment; arbitrary Rust
executors and Python/Node callbacks run in their host process unless the application isolates them.
See [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md).

Never send a real provider key as part of a report. Revoke any credential that may have been
exposed.

## Safe disclosure checklist for reporters

- [ ] No real API keys or tokens in the report body or attachments
- [ ] No private customer prompts or production data
- [ ] Reproduction prefers a local mock provider when possible
- [ ] Boundary claimed matches [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md)
