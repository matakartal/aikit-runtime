#!/usr/bin/env bash
# Generate the shared external-agent declaration blocks from the Rust schemas and splice them into
# the checked-in Python/TypeScript declaration files.
#
#   ./scripts/generate-declarations.sh            # write the regions
#   ./scripts/generate-declarations.sh --check    # fail when the checked-in region is stale (CI)
#
# Only the marked region is owned by the generator; everything else in `aikit.pyi` /`index.d.ts`
# stays hand-written. This is the real generator half of the v1 "declarations generated from Rust
# schemas" gate; `scripts/check-binding-declarations.sh` remains the runtime-export inventory gate.
set -euo pipefail

cd "$(dirname "$0")/.."

readonly PY_FILE="crates/aikit-py/aikit.pyi"
readonly TS_FILE="crates/aikit-node/index.d.ts"
readonly PY_BEGIN="# --- BEGIN GENERATED external-agent declarations (scripts/generate-declarations.sh) ---"
readonly PY_END="# --- END GENERATED external-agent declarations ---"
readonly TS_BEGIN="// --- BEGIN GENERATED external-agent declarations (scripts/generate-declarations.sh) ---"
readonly TS_END="// --- END GENERATED external-agent declarations ---"

mode="write"
if test "${1:-}" = "--check"; then
  mode="check"
elif test -n "${1:-}"; then
  printf 'generate-declarations: unknown argument %s\n' "$1" >&2
  exit 2
fi

python_block="$(cargo run -q -p aikit-runtime-core --example generate_declarations --locked -- python)"
typescript_block="$(cargo run -q -p aikit-runtime-core --example generate_declarations --locked -- typescript)"

AIKIT_MODE="$mode" \
AIKIT_PY_FILE="$PY_FILE" AIKIT_TS_FILE="$TS_FILE" \
AIKIT_PY_BEGIN="$PY_BEGIN" AIKIT_PY_END="$PY_END" \
AIKIT_TS_BEGIN="$TS_BEGIN" AIKIT_TS_END="$TS_END" \
AIKIT_PY_BLOCK="$python_block" AIKIT_TS_BLOCK="$typescript_block" \
python3 - <<'SPLICE_PY'
import os
import sys

mode = os.environ["AIKIT_MODE"]
stale = []

def splice(path, begin, end, block):
    with open(path, encoding="utf-8") as handle:
        source = handle.read()
    if begin not in source or end not in source:
        print(f"❌ {path} is missing the generated-region markers")
        sys.exit(1)
    head, rest = source.split(begin, 1)
    _, tail = rest.split(end, 1)
    desired = f"{begin}\n{block.rstrip()}\n{end}"
    current = f"{begin}{rest.split(end, 1)[0]}{end}"
    if current == desired:
        return
    if mode == "check":
        stale.append(path)
        return
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"{head}{desired}{tail}")
    print(f"✅ regenerated the declaration region in {path}")

splice(
    os.environ["AIKIT_PY_FILE"],
    os.environ["AIKIT_PY_BEGIN"],
    os.environ["AIKIT_PY_END"],
    os.environ["AIKIT_PY_BLOCK"],
)
splice(
    os.environ["AIKIT_TS_FILE"],
    os.environ["AIKIT_TS_BEGIN"],
    os.environ["AIKIT_TS_END"],
    os.environ["AIKIT_TS_BLOCK"],
)

if stale:
    print("❌ generated declarations are stale; run ./scripts/generate-declarations.sh:")
    for path in stale:
        print("   -", path)
    sys.exit(1)
print("✅ GENERATED DECLARATIONS: the checked-in regions match the Rust schemas.")
SPLICE_PY
