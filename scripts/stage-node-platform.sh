#!/usr/bin/env bash
# Build a locally testable optional native package from one CI-produced addon.
set -euo pipefail

cd "$(dirname "$0")/.."

TARGET="${1:-}"
NATIVE_FILE="${2:-}"
OUTPUT_ROOT="${3:-dist/npm}"

case "$TARGET" in
  darwin-arm64|darwin-x64|linux-arm64-gnu|linux-x64-gnu|win32-x64-msvc) ;;
  *)
    echo "usage: $0 <platform-target> <native-addon-path> [output-root]" >&2
    exit 2
    ;;
esac

if [ ! -f "$NATIVE_FILE" ]; then
  echo "native addon not found: $NATIVE_FILE" >&2
  exit 1
fi

TEMPLATE="crates/aikit-node/npm/$TARGET/package.json"
DEST="$OUTPUT_ROOT/$TARGET"
rm -rf "$DEST"
mkdir -p "$DEST"
cp "$TEMPLATE" "$DEST/package.json"
cp LICENSE-APACHE LICENSE-MIT "$DEST/"
cp "$NATIVE_FILE" "$DEST/aikit_node.node"

node -e '
const fs = require("fs");
const path = require("path");
const root = require("./crates/aikit-node/package.json");
const platformDir = path.resolve(process.argv[1]);
const platform = require(path.join(platformDir, "package.json"));
if (root.version !== platform.version) {
  throw new Error(`version drift: root=${root.version} platform=${platform.version}`);
}
if (root.optionalDependencies[platform.name] !== platform.version) {
  throw new Error(`${platform.name} is not pinned exactly by the root package`);
}
if (fs.statSync(path.join(platformDir, "aikit_node.node")).size === 0) {
  throw new Error("native addon is empty");
}
' "$DEST"

echo "staged $DEST"
