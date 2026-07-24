# Security policy

## Supported versions

`aikit-runtime` is currently a source-first `v0.3.0-alpha.1` development preview. Security fixes land on the
default branch; no public package-registry support line is claimed.

CI security checks include dependency advisory/license/source review (`cargo-deny`), an independent
RustSec audit (`cargo-audit`), repository-level CodeQL default setup, complete-history
committed-secret scanning (Gitleaks), deterministic CycloneDX SBOM generation, and
release-provenance contract checks. Source users should review the repository's current security
state before deploying a commit.

Run the same supply-chain checks locally with:

```bash
./scripts/security-check.sh --all
```

The local command requires `cargo-deny`, `cargo-audit`, and Gitleaks. Individual modes are available
for dependency checks, secret scanning, SBOM generation, and provenance validation; run the script
with an unknown argument to see the complete usage. The SBOM is written to
`dist/security/aikit-runtime.cdx.json` and binds its evidence to the current commit and
`Cargo.lock` digest. Release provenance uses GitHub's OIDC-backed attestation action and therefore
does not require a repository signing secret.

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
- MCP discovery/call-filter bypasses, unbounded pagination, or transport framing attacks;
- structured-output validator decisions that fail open, leak candidate values, or create
  uncontrolled retry loops;
- expired-session lease replay that can duplicate an external side effect.
- A2A owner/tenant isolation, idempotency-key confusion, artifact/media validation, SSE replay,
  protected-cancellation admission, and snapshot/journal corruption;

The documented security boundary matters when assessing a report. Built-in Bash can use Seatbelt,
Linux namespaces+seccomp, Windows Job Objects, or hardened Docker containment; arbitrary Rust
executors and Python/Node callbacks run in their host process unless the application isolates them.
Remote MCP servers are untrusted inputs: discovery, cursors, JSON-RPC responses, names, and tool
results must remain bounded and governed. Semantic validators also execute as host callbacks; they
must be pure/idempotent and use a caller-owned timeout when their latency is not inherently bounded.
The protected A2A cancellation listener assumes a private/mTLS or equivalent transport boundary;
exposing that TCP port directly to untrusted peers requires pre-HTTP transport identity and quotas.
See [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md).

Never send a real provider key as part of a report. Revoke any credential that may have been
exposed.

## Safe disclosure checklist for reporters

- [ ] No real API keys or tokens in the report body or attachments
- [ ] No private customer prompts or production data
- [ ] Reproduction prefers a local mock provider when possible
- [ ] Boundary claimed matches [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md)
