# Contributing to aikit

Thanks for helping improve aikit. Changes should preserve its central contract: one canonical Rust
implementation, thin language bindings, explicit fidelity grades, and governance before side
effects.

## Before opening a change

1. Search existing issues and keep the proposal scoped.
2. For security-sensitive behavior, read [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md).
3. Never use or commit a real API key in a test, fixture, log, screenshot, or issue.
4. Add the smallest regression test that proves the behavior at its real boundary.

## Local setup

The workspace declares Rust 1.88 as its MSRV. Stable Rust, Python 3.9+, Node.js 18+, and a C/C++
toolchain are needed for all three surfaces.

```bash
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

To build and verify the bindings:

```bash
python3 -m venv .venv
.venv/bin/pip install maturin
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml
./scripts/build-node.sh
./scripts/parity-check.sh
```

Normal tests must remain keyless. The live smoke path is an explicit, billable maintainer action;
see [`docs/LIVE-SMOKE.md`](docs/LIVE-SMOKE.md).

## Design rules

- Preserve canonical messages and provider-owned reasoning state; do not flatten to final text.
- Do not translate opaque reasoning/signatures across provider boundaries.
- Report structured-output fidelity instead of silently degrading it.
- Apply permissions and hooks immediately before a tool side effect.
- Child agents may narrow a parent's tools, permissions, and budget, never broaden them.
- Retry providers only before the first delta; never replay tool side effects.
- Keep prices/model rankings caller-supplied so the core does not ship stale facts.
- Add public behavior to Rust first, then keep Python and Node.js projections aligned.

## Pull requests

Keep commits reviewable and explain:

- the user-visible outcome;
- the safety/fidelity boundary affected;
- tests run, including anything skipped and why;
- whether package or public API compatibility changes.

Documentation-only changes should still pass formatting/link review. By contributing, you agree
that your contribution is licensed under MIT OR Apache-2.0, at the recipient's option.
