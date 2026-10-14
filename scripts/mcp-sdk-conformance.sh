#!/usr/bin/env bash
# Official MCP SDK conformance: build and supervise the deterministic `mcp_sdk_sut` fixture, then
# drive it with the pinned `@modelcontextprotocol/sdk` client. No provider credentials are used.
#
# Prereqs: Rust toolchain, Node, and `npm ci --prefix .github/node-test-deps --ignore-scripts`
# (the same pinned dependency set the general CI workflow installs).
set -euo pipefail

cd "$(dirname "$0")/.."

readonly SUT_URL="http://127.0.0.1:9998/mcp"
readonly SUT_CARD_PROBE="http://127.0.0.1:9998/mcp"

fail() {
  printf 'mcp-sdk-conformance: %s\n' "$1" >&2
  exit 2
}

command -v node >/dev/null 2>&1 || fail "node is required"
test -d .github/node-test-deps/node_modules/@modelcontextprotocol/sdk \
  || fail "run: npm ci --prefix .github/node-test-deps --ignore-scripts"

cargo build -p aikit-runtime-core --example mcp_sdk_sut --locked

cargo run -q -p aikit-runtime-core --example mcp_sdk_sut --locked &
sut_pid=$!
cleanup() {
  kill "$sut_pid" 2>/dev/null || true
  wait "$sut_pid" 2>/dev/null || true
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 60); do
  # Any HTTP status proves the listener is accepting; unauthenticated requests return 401.
  if curl -s -o /dev/null "$SUT_CARD_PROBE" 2>/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "$sut_pid" 2>/dev/null; then
    fail "SUT exited before becoming ready"
  fi
  sleep 0.5
done
test "$ready" = "1" || fail "SUT did not become ready"

AIKIT_MCP_SUT_URL="$SUT_URL" \
NODE_PATH="$PWD/.github/node-test-deps/node_modules" \
  node scripts/mcp-sdk-conformance.cjs

printf 'MCP official SDK conformance passed.\n'
