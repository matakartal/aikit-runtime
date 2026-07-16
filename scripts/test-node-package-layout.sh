#!/usr/bin/env bash
# Prove that the published root wrapper loads its sibling optional native package without a local
# addon fallback. This mirrors an npm install while remaining registry-free.
set -euo pipefail

cd "$(dirname "$0")/.."

TARGET="${1:-}"
NATIVE_FILE="${2:-}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

./scripts/stage-node-platform.sh "$TARGET" "$NATIVE_FILE" "$TMP/staged"
mkdir -p "$TMP/packs" "$TMP/install/node_modules/aikit-runtime"

root_tgz="$(npm pack --silent --pack-destination "$TMP/packs" ./crates/aikit-node)"
platform_tgz="$(npm pack --silent --pack-destination "$TMP/packs" "$TMP/staged/$TARGET")"
platform_name="$(node -p "require('./crates/aikit-node/npm/$TARGET/package.json').name")"
mkdir -p "$TMP/install/node_modules/$platform_name"

tar -xzf "$TMP/packs/$root_tgz" --strip-components=1 -C "$TMP/install/node_modules/aikit-runtime"
tar -xzf "$TMP/packs/$platform_tgz" --strip-components=1 -C "$TMP/install/node_modules/$platform_name"

NODE_PATH="$TMP/install/node_modules" node -e '
const { Agent } = require("aikit-runtime");
const agent = Agent.fromEnv({});
if (!Array.isArray(agent.activeProviders())) throw new Error("packaged native addon did not load");
console.log("PACKAGED_NODE_LOAD=PASS");
'
