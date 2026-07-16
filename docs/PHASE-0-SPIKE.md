# Phase 0 retrospective — FFI risk retired

Phase 0 answered one architectural question: can the Rust core stream deltas to a host language,
await an async host tool callback, and resume the same multi-turn run?

The answer was yes. The Python spike proved both FFI directions; the Node.js binding later proved
the same design through napi. These seams now sit behind the normal binding and parity tests, so
this document is historical rather than a current roadmap.

The lasting implementation rules are:

- Never hold Python's GIL across `.await`.
- Treat each host stream as single-consumer and reject concurrent polling.
- Keep permission decisions and tool-loop state in Rust, on both sides of the callback boundary.
- Test observable parity at the binding boundary, not only Rust helper functions.

Reproduce the current proof with:

```bash
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml
./scripts/build-node.sh
python examples/python/spike.py
./scripts/parity-check.sh
```

For current status and limitations, use the repository [`README`](../README.md) and
[`feature reference`](FEATURES.md).
