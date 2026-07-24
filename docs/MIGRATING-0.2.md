# Migrating from the 0.1 source preview to 0.2

> **Historical guide:** this page describes the 0.2 milestone, not current `main`. Users moving to
> the 0.3 alpha should continue with [MIGRATING-0.3.md](MIGRATING-0.3.md).

This guide is for source-checkout users moving code written against the `0.1.0` preview to the
`0.2.0` tree at that milestone. No public registry upgrade is implied because npm, PyPI, and crates.io
packages are not published by this project.

## Before changing application code

1. Pin the exact aikit commit you are migrating to.
2. Run your existing keyless tests and save the typed error/outcome fixtures.
3. Identify custom `SessionStore`, `OffPromptStore`, browser, MCP, and structured-output code.
4. Update Rust first, then Python/Node consumers, then regenerate any serialized test fixtures.
5. Run the verification checklist at the end of this document.

## Workspace version

Every workspace package and local Python/Node artifact now reports `0.2.0`. Exact-version Node
platform dependencies also moved to `0.2.0`; do not combine a 0.2 wrapper with 0.1 native addons.

## Rust `ObjectOptions`

`ObjectOptions` gained `semantic_validator`. Existing struct literals must set it or use the
default update syntax:

```rust
let options = ObjectOptions {
    max_retries: 2,
    semantic_validator: None,
    ..ObjectOptions::default()
};
```

When a validator is present, it runs only after JSON parsing and JSON-Schema validation. It may
accept, request a bounded repair, or reject. Keep it pure/idempotent, return secret-free retry
reasons, and apply a host timeout if callback latency is not inherently bounded.

## `RunOutcome` and evaluation boundaries

`RunOutcome` gained `invocation_start_message_index`. Prefer `RunOutcome::default()` plus field
updates for manually constructed outcomes. The runtime records the boundary automatically.

Legacy serialized outcomes still deserialize, but message-derived eval gates fail closed when the
boundary is absent. Terminal-status and usage gates remain usable. This prevents old conversation
history from satisfying a gate intended for the current invocation.

## Browser registration

Browser tools now require an explicit assertion that a caller-owned proxy, BiDi interceptor, or
equivalent boundary enforces the exact hostname allowlist and rejects private/local/non-routable
addresses before every browser request.

- Rust: pass `BrowserEgressPolicy::ExternallyEnforced` to `BrowserTools::new`.
- Python: pass the required keyword `external_egress_enforced=True`.
- Node: pass `{ externalEgressEnforced: true }` in `BrowserToolsOptions`.

The value is an assertion, not a switch that installs or verifies network enforcement. Do not set
it unless that external boundary actually exists.

## Off-prompt storage

`OffPromptStore::store` is now fallible and returns a `Result`. Propagate or handle failures caused
by OS randomness, retention limits, or oversized values instead of assuming storage always
succeeds. Handles are scoped to one executor and are not durable application identifiers.

## Session execution leases

Expired execution leases are no longer acquired automatically. `SessionStore` acquire/recovery
operations return an opaque `SessionExecutionLease`; commit and release consume the exact claim.
Custom stores must persist the store-generated fencing token and validate it on commit/release.

Safe recovery sequence:

1. prove the prior worker has stopped;
2. reconcile every potentially completed external effect;
3. call `recover_expired_execution_lease` (or the assertion-gated Python/Node helper);
4. decide separately whether a new run/resume is safe.

Expiry alone is never replay permission.

## MCP visibility and limits

MCP connections may now use exact, case-sensitive allow/deny filters. Omitted `allow` means
allow-all, an empty allow list exposes nothing, and deny always wins. Invalid, duplicated, control/
bidirectional-formatting names and unknown binding fields are rejected.

Discovery and transport now fail closed at these limits:

| Limit | Value |
|---|---:|
| Pages per discovery operation | 128 |
| Incoming items per discovery operation | 10,000 |
| Serialized discovery-item bytes | 8 MiB |
| One cursor | 4 KiB |
| Cumulative cursor bytes | 64 KiB |
| One transport response / stdio line | 4 MiB |
| Filter entries | 1,024 |
| Tool-name length | 128 characters |

If a server exceeds a limit, fix or proxy the server rather than weakening the client boundary.

## CLI and evaluation

The source CLI now includes `eval`. Non-mock datasets require `--allow-live`; live suites also
have aggregate case, input-byte, requested-output-token, and wall-time ceilings. Exit code `4`
means the dataset ran and at least one gate failed; infrastructure failures remain exit code `3`.

```bash
cargo +1.97.1 run -p aikit-cli --locked -- eval evals/smoke.json
```

## Runtime and distribution changes

- Blank credentials and unknown top-level/nested run-option fields now fail closed.
- Provider streams and retained outputs have stricter size/lifetime/terminal invariants.
- JSON/SQLite paths verify descriptor identity; identity lookup failure is an error.
- Linux artifacts target glibc 2.28+; musl remains unsupported.
- Primary CI uses Rust 1.97.1 while Rust 1.88 remains the MSRV.
- GitHub Actions/build inputs are pinned, and version/tag/evidence collisions are rejected.

Review the full [`CHANGELOG`](../CHANGELOG.md) and [threat model](THREAT-MODEL.md) before deploying.

## Verification checklist

```bash
cargo +1.97.1 fmt --all --check
cargo +1.97.1 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.97.1 test --workspace --all-features --locked
cargo +1.88.0 check --workspace --all-targets --all-features --locked
./scripts/build-node.sh
./scripts/parity-check.sh
cargo +1.97.1 run -p aikit-cli --locked -- eval evals/smoke.json
./scripts/release-check.sh --candidate
```

Run the billable live-provider contract only with explicit maintainer approval and dedicated
low-limit credentials; see [LIVE-SMOKE.md](LIVE-SMOKE.md).
