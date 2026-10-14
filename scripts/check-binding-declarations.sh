#!/usr/bin/env bash
# Runtime ↔ declaration inventory gate for the Python and Node bindings.
#
# Every name the built extension actually exports must exist in the checked-in typed
# declaration file (`aikit.pyi` / `index.d.ts`). This catches the dangerous drift direction —
# shipping a public runtime surface with no typed declaration — mechanically instead of by
# review. Declaration-only names (TypedDicts, type aliases) are legitimate and not flagged.
#
# Prereqs: `maturin develop` for aikit-py + `./scripts/build-node.sh` for aikit-node.
set -euo pipefail

cd "$(dirname "$0")/.."

PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  if [ -x ".venv/bin/python" ]; then PY=".venv/bin/python"; else PY="python3"; fi
fi

echo "→ checking Python runtime exports against aikit.pyi"
"$PY" - <<'CHECK_PY'
import re
import sys

import aikit

# `aikit` appears in dir() as the native submodule artifact of the PyO3 module name; it is not a
# public API surface.
ALLOWED_UNDECLARED = {"aikit"}

runtime = {name for name in dir(aikit) if not name.startswith("_")}
with open("crates/aikit-py/aikit.pyi", encoding="utf-8") as handle:
    pyi = handle.read()
declared = set()
declared.update(re.findall(r"^(?:async )?def (\w+)", pyi, re.M))
declared.update(re.findall(r"^class (\w+)", pyi, re.M))
declared.update(re.findall(r"^(\w+)(?:\s*[:=])", pyi, re.M))
missing = sorted(runtime - declared - ALLOWED_UNDECLARED)
if missing:
    print("❌ runtime exports missing from aikit.pyi:", ", ".join(missing))
    sys.exit(1)
print(f"✅ {len(runtime - ALLOWED_UNDECLARED)} Python runtime exports are declared in aikit.pyi")
CHECK_PY

echo "→ checking Node runtime exports against index.d.ts"
node - <<'CHECK_NODE'
const fs = require("node:fs");
const path = require("node:path");

const root = process.cwd();
const runtime = Object.keys(require(path.join(root, "crates/aikit-node"))).filter(
  (name) => !name.startsWith("_"),
);
const dts = fs.readFileSync(path.join(root, "crates/aikit-node/index.d.ts"), "utf8");
const declared = new Set();
for (const match of dts.matchAll(
  /^export (?:declare )?(?:async )?(?:function|class|const|let|interface|type|enum) (\w+)/gm,
)) {
  declared.add(match[1]);
}
const missing = runtime.filter((name) => !declared.has(name)).sort();
if (missing.length > 0) {
  console.error("❌ runtime exports missing from index.d.ts:", missing.join(", "));
  process.exit(1);
}
console.log(`✅ ${runtime.length} Node runtime exports are declared in index.d.ts`);
CHECK_NODE

echo "→ checking declared field sets against the Rust schema inventory"
schema_inventory="$(cargo run -q -p aikit-runtime-core --example declaration_schemas --locked)"
AIKIT_SCHEMA_INVENTORY="$schema_inventory" "$PY" - <<'CHECK_FIELDS_PY'
import json
import os
import re
import sys

inventory = json.loads(os.environ["AIKIT_SCHEMA_INVENTORY"])
pyi = open("crates/aikit-py/aikit.pyi", encoding="utf-8").read()
dts = open("crates/aikit-node/index.d.ts", encoding="utf-8").read()

def pyi_block(name: str):
    match = re.search(rf"^class {re.escape(name)}\(([^)]*)\):\n((?:    .*\n|\n)*?)(?=^\S)", pyi, re.M)
    if match is None:
        return None
    bases, body = match.group(1), match.group(2)
    fields = dict.fromkeys(re.findall(r"^    (\w+):", body, re.M))
    required = set()
    if "total=False" not in bases:
        required.update(fields)
    for base in re.findall(r"(_\w+Required)", bases):
        base_block = pyi_block(base)
        if base_block is None:
            print(f"❌ {name}: base {base} not found in aikit.pyi")
            sys.exit(1)
        base_fields, base_required = base_block
        for field in base_fields:
            fields.setdefault(field, None)
        required.update(base_required)
    return list(fields), required

def dts_block(name: str):
    match = re.search(rf"^export interface {re.escape(name)} \{{\n(.*?)^\}}", dts, re.M | re.S)
    if match is None:
        return None
    body = match.group(1)
    fields, required = [], set()
    for field, optional in re.findall(r"^  (\w+)(\??):", body, re.M):
        fields.append(field)
        if optional != "?":
            required.add(field)
    return fields, required

failures = []
for type_name, expected in inventory.items():
    expected_fields = set(expected["fields"])
    expected_required = set(expected["required"])
    for language, block in (("aikit.pyi", pyi_block(type_name)), ("index.d.ts", dts_block(type_name))):
        if block is None:
            failures.append(f"{type_name}: no declaration block found in {language}")
            continue
        declared_fields, declared_required = block
        declared_fields = set(declared_fields)
        if declared_fields != expected_fields:
            failures.append(
                f"{type_name} ({language}): fields differ — missing {sorted(expected_fields - declared_fields)}, extra {sorted(declared_fields - expected_fields)}"
            )
        elif declared_required != expected_required:
            failures.append(
                f"{type_name} ({language}): required set differs — should-be-required {sorted(expected_required - declared_required)}, over-required {sorted(declared_required - expected_required)}"
            )
if failures:
    print("❌ declaration field drift against the Rust schema inventory:")
    for failure in failures:
        print("   -", failure)
    sys.exit(1)
print(f"✅ {len(inventory)} declared types match the Rust schema inventory field-for-field")
CHECK_FIELDS_PY

echo "✅ DECLARATIONS: every binding runtime export carries a typed declaration."
