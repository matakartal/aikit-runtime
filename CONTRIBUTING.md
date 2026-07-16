# Contributing to aikit

Thanks for helping improve aikit. Changes should preserve its central contract: one canonical Rust
implementation, thin language bindings, explicit fidelity grades, and governance before side
effects.

## Before opening a change

1. Search [existing issues](https://github.com/matakartal/aikit-runtime/issues) and keep the proposal scoped.
2. For security-sensitive behavior, read [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md).
3. Never use or commit a real API key in a test, fixture, log, screenshot, or issue.
4. Add the smallest regression test that proves the behavior at its real boundary.
5. Prefer implementing public behavior in Rust first, then project it to Python and Node.js.

Security vulnerabilities must **not** be filed as public issues — follow [`SECURITY.md`](SECURITY.md).

## Local setup

| Requirement | Version |
|---|---|
| Rust | 1.88+ (MSVR declared in workspace) |
| Python | 3.9+ |
| Node.js | 18.17+ |
| C/C++ toolchain | Required for native bindings |

```bash
# Pure-Rust default members (fast)
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all --check
```

To build and verify the language bindings:

```bash
python3 -m venv .venv
.venv/bin/pip install "maturin>=1.5,<2"
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml
./scripts/build-node.sh
./scripts/parity-check.sh
```

Optional end-to-end release-candidate invariants (still keyless):

```bash
./scripts/release-check.sh --candidate
```

Normal tests must remain keyless. The live smoke path is an explicit, billable maintainer action;
see [`docs/LIVE-SMOKE.md`](docs/LIVE-SMOKE.md).

### Useful examples

```bash
cargo run -p aikit-runtime --example quickstart
cargo run -p aikit-runtime-core --example policy
cargo run -p aikit-runtime-core --example plan_mode
cargo run -p aikit-runtime-core --example smart_approval
cargo run -p aikit-runtime-core --example reliability
python examples/python/agent_governance.py
node examples/node/agent_governance.cjs
```

## Design rules

- Preserve canonical messages and provider-owned reasoning state; do not flatten to final text.
- Do not translate opaque reasoning/signatures across provider boundaries.
- Report structured-output fidelity instead of silently degrading it.
- Apply permissions and hooks immediately before a tool side effect.
- Child agents may narrow a parent's tools, permissions, and budget, never broaden them.
- Retry providers only before the first delta; never replay tool side effects.
- Keep prices/model rankings caller-supplied so the core does not ship stale facts.
- Add public behavior to Rust first, then keep Python and Node.js projections aligned.
- Ship cross-language surface changes with a parity/conformance update in the same change when possible.
- Document capability limits honestly (especially containment guarantees and fidelity grades).

## Where code lives

| Path | Role |
|---|---|
| `crates/aikit-core` | Canonical runtime, providers, governance, tools |
| `crates/aikit` | Ergonomic Rust facade (`aikit-runtime` package) |
| `crates/aikit-py` | PyO3 binding (`import aikit`) |
| `crates/aikit-node` | napi binding (`aikit-runtime` npm package) |
| `examples/` | Cross-language demos and conformance drivers |
| `docs/` | Feature reference, threat model, release evidence |
| `scripts/` | Build, parity, live-smoke, and release gates |

See [`docs/README.md`](docs/README.md) for the full documentation map.

## Pull requests

Use the repository PR template. Keep commits reviewable and explain:

- the user-visible outcome;
- the safety/fidelity boundary affected;
- tests run, including anything skipped and why;
- whether package or public API compatibility changes;
- whether Python/Node bindings need a follow-up if the change is core-only for now.

Suggested local checklist before request:

- [ ] `cargo fmt --all --check`
- [ ] strict Clippy (`-D warnings`)
- [ ] relevant Rust tests
- [ ] binding/parity checks when a public schema or binding surface changed
- [ ] no credentials, private prompts, or generated native artifacts committed
- [ ] docs updated when behavior or limits change

Documentation-only changes should still pass formatting and link review. By contributing, you agree
that your contribution is licensed under MIT OR Apache-2.0, at the recipient's option.

## Issue triage

| Label intent | Use for |
|---|---|
| Bug | Reproducible defects (prefer keyless mock-provider repros) |
| Enhancement | Scoped capability or API improvements |
| Documentation | Docs-only corrections and expansions |

Feature proposals should describe the safety and fidelity boundary, not only the happy path.
Deferred post-v1 items are listed in [`docs/FEATURES.md`](docs/FEATURES.md) and the
[competitive roadmap](docs/COMPETITIVE-ROADMAP.md).

## Code of conduct

Participation is governed by [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
