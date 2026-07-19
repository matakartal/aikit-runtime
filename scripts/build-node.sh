#!/usr/bin/env bash
# Build the napi addon without the @napi-rs/cli (no network needed): compile the cdylib with
# cargo, then copy it next to index.js as `aikit_node.node`. Requiring that file runs napi's
# module init, which self-registers the exports. `napi_build::setup()` (build.rs) already set the
# `-undefined dynamic_lookup` linker flag so the Node-provided `napi_*` symbols resolve at load.
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILE="${1:-debug}"
case "$PROFILE" in
  release)
    cargo build -p aikit-node --release --locked
    ;;
  debug)
    cargo build -p aikit-node --locked
    ;;
  *)
    echo "usage: $0 [debug|release]" >&2
    exit 2
    ;;
esac

# Resolve the built cdylib name across platforms (macOS .dylib, Linux .so, Windows .dll).
OUT_DIR="target/$PROFILE"
DEST="crates/aikit-node/aikit_node.node"
if   [ -f "$OUT_DIR/libaikit_node.dylib" ]; then cp "$OUT_DIR/libaikit_node.dylib" "$DEST"
elif [ -f "$OUT_DIR/libaikit_node.so"    ]; then cp "$OUT_DIR/libaikit_node.so"    "$DEST"
elif [ -f "$OUT_DIR/aikit_node.dll"      ]; then cp "$OUT_DIR/aikit_node.dll"      "$DEST"
else
  echo "error: could not find the built aikit-node cdylib in $OUT_DIR" >&2
  exit 1
fi

# Rust's linker-created ad-hoc signature can become unusable after the Mach-O is copied to its
# final `.node` path. Re-sign that exact staged file so hardened Node builds can load it.
if [ "$(uname -s)" = "Darwin" ]; then
  codesign --force --sign - "$DEST"
fi

echo "wrote $DEST"
