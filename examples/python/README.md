# Python examples

Runnable examples for the `aikit` Python binding (PyO3). They all run **keyless and offline** by
default against the deterministic in-memory mock model (`mock-1`), so they give the same output
on every machine and never bill a provider. Only `quickstart.py` has an opt-in live mode.

> aikit is `v0.3.0-alpha.1` and source-first: there is no published PyPI package for it yet, so
> you build the binding from this checkout. The future PyPI distribution name is
> `aikit-runtime`; the import name is `aikit`. The unrelated `aikit` package on PyPI is not this
> project.

## 1. Build the binding once

Prerequisites: Rust 1.88+ (`cargo`), a C/C++ toolchain, and Python 3.9+. From the repository
root:

```bash
python3 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements/maturin.txt
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml --locked
```

`maturin develop` compiles the Rust core and installs `aikit` into `.venv`. Re-run it after
pulling or editing Rust code; an `AttributeError` for a function shown in
[`aikit.pyi`](../../crates/aikit-py/aikit.pyi) usually means the installed build is stale.
More detail: [Python binding README](../../crates/aikit-py/README.md) and
[Getting started, step 6](../../docs/GETTING-STARTED.md#6-use-python-or-node-optional).

## 2. Run the examples

Run every command from the repository root. Suggested order:

| Example | What it shows | Run |
|---|---|---|
| [`quickstart.py`](quickstart.py) | Start here. `generate_text`, `stream_text`, an `@aikit.tool` behind an "ask" approval callback, `run` with RunOptions (turn and token budget), `evaluate_outcome` gates, and the typed `AikitError` for a missing key. | `.venv/bin/python examples/python/quickstart.py` |
| [`agent_governance.py`](agent_governance.py) | The broad tour: key-driven capability discovery with redacted `repr`, deny/allow permissions, every async lifecycle hook, memory, routing, bounded orchestration (`run_subagent`, `fan_out`, `council`), structured output (`generate_object` / `stream_object`, optional Pydantic), and approval interruption. | `.venv/bin/python examples/python/agent_governance.py` |
| [`run_options.py`](run_options.py) | RunOptions in depth: `Client`, fallback models, retry, `max_turns` and budget terminal statuses, misspelled options failing closed with `ValueError`, typed error envelopes, and cancellation (before the first pull, mid-hook, and `async with`). | `.venv/bin/python examples/python/run_options.py` |
| [`spike.py`](spike.py) | The historical Phase 0 proof of the two FFI seams (Rust stream into `async for`, Rust awaiting a Python `async def` tool) using the low-level `aikit.query` function and a hand-written tool class. | `.venv/bin/python examples/python/spike.py` |
| [`conformance.py`](conformance.py) | A cross-language parity fixture, not a tutorial: prints one `CONFORMANCE_<MODULE>_JSON=` line per surface (governance, structured output, RunOptions, state, orchestration, built-ins, input, A2A, external agents). | `.venv/bin/python examples/python/conformance.py` |

What to expect from the mock model: it replies with fixed Turkish sentences (for example
`Araç sonucunu aldım; görevi tamamladım.`, "I got the tool result; task done"), and when tools
are registered it calls the first one with `{"q": "merhaba"}` by default. That is why example
tools take a `q` field. Each example exits non-zero if an assertion fails, so a clean exit is the pass signal.

## 3. Optional: a live provider (billable)

Only `quickstart.py` supports this. Export the provider's key and name a model it serves:

```bash
export ANTHROPIC_API_KEY=...            # or OPENAI_API_KEY, GEMINI_API_KEY, ...
AIKIT_EXAMPLE_MODEL=<a claude-* model id> .venv/bin/python examples/python/quickstart.py
```

`aikit.Agent()` reads provider keys from the process environment; the examples otherwise use
`aikit.Agent.from_env({})` so a key in your shell cannot change their output. If the named model's
provider has no key, the quickstart exits with a message naming the variable to set; in code,
catch `aikit.AikitError` and branch on `error.code == "provider_auth"`. Mock-only assertions are
skipped in live mode because a real model picks its own wording and tool calls.

`aikit providers` in the CLI lists every supported provider with its key variable and model
prefix.

## 4. Parity contracts (for contributors)

`agent_governance.py`, `run_options.py`, and `conformance.py` double as CI fixtures. Their
`PARITY_JSON=`, `GOVERNANCE_JSON=`, `BINDING_STREAM_JSON=`, `RUN_OPTIONS_JSON=`, and
`CONFORMANCE_*_JSON=` lines are compared byte-for-byte with the Node examples in
[`examples/node/`](../node/) (and, for `PARITY_JSON` and `CONFORMANCE_*`, with Rust) by:

```bash
./scripts/build-node.sh      # once, for the Node side
./scripts/parity-check.sh
```

Comments and prose can change freely; the emitted facts must change in all languages together.

## Going further

- More focused scenarios (MCP tool filters, built-in tool containment, production state,
  multimodal input, semantic validators, A2A mapper, external agents) live as runnable scripts
  in [`crates/aikit-py/tests/`](../../crates/aikit-py/tests/).
- Full type information for every call is in
  [`aikit.pyi`](../../crates/aikit-py/aikit.pyi).
- Capability and governance reference: [`docs/FEATURES.md`](../../docs/FEATURES.md); trust
  boundaries: [`docs/THREAT-MODEL.md`](../../docs/THREAT-MODEL.md).
