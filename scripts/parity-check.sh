#!/usr/bin/env bash
# Cross-language public-surface conformance.
#
# Runs the SAME agent-native scenario through Rust, Python, and Node, then asserts all three emit a
# BYTE-IDENTICAL canonical transcript. Focused modules cover governance, structured streams,
# RunOptions, state/audit surfaces, subagent session context, and built-in tool containment. Add a
# future surface by appending its module name below and emitting the same prefixed line in each
# example.
#
# Prereqs: `maturin develop` for aikit-py + `./scripts/build-node.sh` for aikit-node.
set -euo pipefail

cd "$(dirname "$0")/.."

# Honour an explicit $PYTHON (CI passes `python`); otherwise prefer the local venv, then python3.
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  if [ -x ".venv/bin/python" ]; then PY=".venv/bin/python"; else PY="python3"; fi
fi

echo "→ running Python demo ($PY)"
py_output="$("$PY" examples/python/agent_governance.py)"
py_line="$(grep '^PARITY_JSON=' <<<"$py_output")"
py_governance="$(grep '^GOVERNANCE_JSON=' <<<"$py_output")"
py_binding_stream="$(grep '^BINDING_STREAM_JSON=' <<<"$py_output")"
py_run_options="$("$PY" examples/python/run_options.py | grep '^RUN_OPTIONS_JSON=')"
py_conformance="$("$PY" examples/python/conformance.py)"
py_production_state="$("$PY" crates/aikit-py/tests/production_state.py | grep '^PRODUCTION_STATE_JSON=')"

echo "→ running Node demo (node)"
node_output="$(node examples/node/agent_governance.cjs)"
node_line="$(grep '^PARITY_JSON=' <<<"$node_output")"
node_governance="$(grep '^GOVERNANCE_JSON=' <<<"$node_output")"
node_binding_stream="$(grep '^BINDING_STREAM_JSON=' <<<"$node_output")"
node_run_options="$(node examples/node/run_options.cjs | grep '^RUN_OPTIONS_JSON=')"
node_conformance="$(node examples/node/conformance.cjs)"
node_production_state="$(node crates/aikit-node/tests/production-state.cjs | grep '^PRODUCTION_STATE_JSON=')"

echo "→ running Rust demo (cargo)"
rust_output="$(cargo run -q -p aikit-runtime --example parity --locked)"
rust_line="$(grep '^PARITY_JSON=' <<<"$rust_output")"
rust_conformance="$(cargo run -q -p aikit-runtime --example conformance --locked)"

echo
echo "rust:   $rust_line"
echo "python: $py_line"
echo "node:   $node_line"
echo

if [ "$rust_line" != "$py_line" ] || [ "$rust_line" != "$node_line" ]; then
  echo "❌ PARITY DRIFT: Rust, Python, and Node diverged. Diff:"
  diff <(printf '%s\n' "$rust_line") <(printf '%s\n' "$py_line") || true
  diff <(printf '%s\n' "$rust_line") <(printf '%s\n' "$node_line") || true
  exit 1
fi

if [ "$py_governance" != "$node_governance" ]; then
  echo "❌ GOVERNANCE DRIFT: Python and Node host callbacks diverged. Diff:"
  diff <(printf '%s\n' "$py_governance") <(printf '%s\n' "$node_governance") || true
  exit 1
fi

if [ "$py_binding_stream" != "$node_binding_stream" ]; then
  echo "❌ OBJECT STREAM DRIFT: Python and Node structured streams diverged. Diff:"
  diff <(printf '%s\n' "$py_binding_stream") <(printf '%s\n' "$node_binding_stream") || true
  exit 1
fi

if [ "$py_run_options" != "$node_run_options" ]; then
  echo "❌ RUN OPTIONS DRIFT: Python and Node client/cancellation outcomes diverged. Diff:"
  diff <(printf '%s\n' "$py_run_options") <(printf '%s\n' "$node_run_options") || true
  exit 1
fi

if [ "$py_production_state" != "$node_production_state" ]; then
  echo "❌ PRODUCTION STATE DRIFT: Python and Node persistence/audit outcomes diverged. Diff:"
  diff <(printf '%s\n' "$py_production_state") <(printf '%s\n' "$node_production_state") || true
  exit 1
fi

# This array is the conformance registry. It deliberately stays independent of language-specific
# build logic, so BUILTINS (or any later surface) can be added as one isolated module.
conformance_modules=(GOVERNANCE STRUCTURED RUN_OPTIONS STATE ORCHESTRATION BUILTINS INPUT)
if [ -n "${AIKIT_CONFORMANCE_EXTRA_MODULES:-}" ]; then
  read -r -a extra_modules <<<"$AIKIT_CONFORMANCE_EXTRA_MODULES"
  conformance_modules+=("${extra_modules[@]}")
fi

for module in "${conformance_modules[@]}"; do
  prefix="CONFORMANCE_${module}_JSON="
  rust_module="$(grep -m 1 "^${prefix}" <<<"$rust_conformance" || true)"
  py_module="$(grep -m 1 "^${prefix}" <<<"$py_conformance" || true)"
  node_module="$(grep -m 1 "^${prefix}" <<<"$node_conformance" || true)"
  if [ -z "$rust_module" ] || [ -z "$py_module" ] || [ -z "$node_module" ]; then
    echo "❌ ${module} CONFORMANCE MISSING: every language must emit ${prefix}<json>"
    exit 1
  fi
  if [ "$rust_module" != "$py_module" ] || [ "$rust_module" != "$node_module" ]; then
    echo "❌ ${module} CONFORMANCE DRIFT: Rust, Python, and Node diverged. Diff:"
    diff <(printf '%s\n' "$rust_module") <(printf '%s\n' "$py_module") || true
    diff <(printf '%s\n' "$rust_module") <(printf '%s\n' "$node_module") || true
    exit 1
  fi
  echo "✅ ${module}: byte-identical across Rust, Python, and Node."
done

echo "✅ PARITY: Rust, Python, and Node produce a byte-identical canonical transcript."
echo "✅ GOVERNANCE: Python and Node produce byte-identical async host-callback outcomes."
echo "✅ OBJECT STREAM: Python and Node expose byte-identical delta/repair/completion outcomes."
echo "✅ RUN OPTIONS: Python and Node expose byte-identical client/budget/turn/cancel outcomes."
echo "✅ PRODUCTION STATE: Python and Node expose byte-identical audit/memory/session outcomes."
echo "✅ CONFORMANCE: ${#conformance_modules[@]} canonical modules match all three public surfaces."
echo "   One Rust core → identical observable behaviour across languages."
