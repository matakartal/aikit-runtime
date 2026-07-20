#!/usr/bin/env bash
# Reproducible local/CI supply-chain gates. No provider, registry, or GitHub secrets are required.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: ./scripts/security-check.sh [--all] [--dependencies] [--secrets]
                                   [--sbom [OUTPUT_DIR]] [--provenance]

  --all           Run every gate and write the SBOM to dist/security (default).
  --dependencies  Run cargo-deny and cargo-audit against Cargo.lock.
  --secrets       Scan the complete committed Git history with Gitleaks.
  --sbom [DIR]    Generate and validate DIR/aikit-runtime.cdx.json.
  --provenance    Validate immutable CI actions and the release-attestation contract.
EOF
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'ERROR required command is not installed: %s\n' "$1" >&2
    exit 127
  }
}

pass() { printf 'PASS  %s\n' "$1"; }

check_dependencies() {
  require_command cargo-deny
  require_command cargo-audit
  cargo deny --all-features --locked check
  cargo audit --deny warnings
  pass "RustSec advisories, licenses, dependency bans, and sources"
}

check_secrets() {
  require_command gitleaks
  git rev-parse --is-inside-work-tree >/dev/null
  if [ "$(git rev-parse --is-shallow-repository)" = "true" ]; then
    printf 'ERROR complete Git history is required for the committed-secret scan\n' >&2
    exit 1
  fi
  gitleaks git --redact --no-banner --verbose .
  pass "complete committed Git history contains no unallowlisted secrets"
}

generate_sbom() {
  local output_dir="$1"
  local metadata_file
  local output_file
  local commit_sha

  require_command cargo
  require_command python3
  metadata_file="$(mktemp)"
  trap 'rm -f "$metadata_file"' RETURN
  output_file="$output_dir/aikit-runtime.cdx.json"
  commit_sha="$(git rev-parse HEAD 2>/dev/null || printf unknown)"
  mkdir -p "$output_dir"
  cargo metadata --format-version 1 --locked >"$metadata_file"

  python3 - "$metadata_file" "$output_file" "$commit_sha" <<'PY'
import hashlib
import json
import pathlib
import sys
import urllib.parse

metadata_path, output_path, commit_sha = sys.argv[1:]
metadata = json.loads(pathlib.Path(metadata_path).read_text(encoding="utf-8"))
workspace = set(metadata["workspace_members"])


def ref(package):
    source = package.get("source") or "workspace"
    return (
        f"pkg:cargo/{urllib.parse.quote(package['name'], safe='-_.*')}"
        f"@{urllib.parse.quote(package['version'], safe='-_.*')}"
        f"?source={urllib.parse.quote(source, safe='-_.*')}"
    )


packages = sorted(metadata["packages"], key=lambda item: item["id"])
package_by_id = {package["id"]: package for package in packages}
ref_by_id = {package["id"]: ref(package) for package in packages}
if len(set(ref_by_id.values())) != len(ref_by_id):
    raise SystemExit("SBOM component references are not unique")

components = []
for package in packages:
    component = {
        "type": "application" if package["id"] in workspace else "library",
        "bom-ref": ref_by_id[package["id"]],
        "name": package["name"],
        "version": package["version"],
        "purl": f"pkg:cargo/{urllib.parse.quote(package['name'])}@{package['version']}",
        "properties": [
            {"name": "aikit:cargo-package-id", "value": package["id"]},
            {"name": "aikit:source", "value": package.get("source") or "workspace"},
        ],
    }
    if package.get("license"):
        component["licenses"] = [{"expression": package["license"]}]
    components.append(component)

nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
dependencies = []
for package in packages:
    node = nodes.get(package["id"], {"dependencies": []})
    dependencies.append(
        {
            "ref": ref_by_id[package["id"]],
            "dependsOn": sorted(
                ref_by_id[dependency]
                for dependency in node["dependencies"]
                if dependency in ref_by_id
            ),
        }
    )

workspace_packages = [package for package in packages if package["id"] in workspace]
workspace_versions = sorted({package["version"] for package in workspace_packages})
workspace_version = workspace_versions[0] if len(workspace_versions) == 1 else "mixed"
lock_digest = hashlib.sha256(pathlib.Path("Cargo.lock").read_bytes()).hexdigest()
bom = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "version": 1,
    "metadata": {
        "component": {
            "type": "application",
            "bom-ref": "aikit-runtime-workspace",
            "name": "aikit-runtime-workspace",
            "version": workspace_version,
        },
        "properties": [
            {"name": "aikit:git-commit", "value": commit_sha},
            {"name": "aikit:cargo-lock-sha256", "value": lock_digest},
            {"name": "aikit:generator", "value": "scripts/security-check.sh"},
        ],
    },
    "components": components,
    "dependencies": [
        {
            "ref": "aikit-runtime-workspace",
            "dependsOn": sorted(ref_by_id[package["id"]] for package in workspace_packages),
        },
        *dependencies,
    ],
}
path = pathlib.Path(output_path)
path.write_text(json.dumps(bom, indent=2, sort_keys=True) + "\n", encoding="utf-8")

# Fail closed on a malformed or internally dangling SBOM before publishing it as evidence.
known = {"aikit-runtime-workspace", *(component["bom-ref"] for component in components)}
for dependency in bom["dependencies"]:
    if dependency["ref"] not in known or not set(dependency["dependsOn"]).issubset(known):
        raise SystemExit(f"SBOM contains a dangling dependency reference: {dependency}")
if bom["bomFormat"] != "CycloneDX" or bom["specVersion"] != "1.5" or not components:
    raise SystemExit("SBOM contract validation failed")
PY

  rm -f "$metadata_file"
  trap - RETURN
  pass "CycloneDX SBOM generated and validated at $output_file"
}

check_provenance() {
  local unpinned_actions

  unpinned_actions="$(
    grep -RhE '^[[:space:]]*-[[:space:]]*uses:' .github/workflows \
      | grep -Ev '@[0-9a-f]{40}([[:space:]]|$)' \
      || true
  )"
  if [ -n "$unpinned_actions" ]; then
    printf 'ERROR workflow actions must use immutable commit SHAs:\n%s\n' "$unpinned_actions" >&2
    exit 1
  fi
  if grep -RqsE '^[[:space:]]*pull_request_target:' .github/workflows; then
    printf 'ERROR pull_request_target is forbidden for this repository security model\n' >&2
    exit 1
  fi
  if grep -RqsE '^[[:space:]]*permissions:[[:space:]]*write-all' .github/workflows; then
    printf 'ERROR workflow-level write-all permissions are forbidden\n' >&2
    exit 1
  fi

  grep -Eq 'actions/attest-build-provenance@[0-9a-f]{40}' .github/workflows/release.yml
  grep -Fq 'id-token: write' .github/workflows/release.yml
  grep -Fq 'attestations: write' .github/workflows/release.yml
  grep -Fq 'subject-path:' .github/workflows/release.yml
  grep -Fq 'dist/release/SHA256SUMS' .github/workflows/release.yml
  grep -Fq 'sha256sum -c SHA256SUMS' .github/workflows/release.yml
  pass "release provenance, checksum, least-privilege, and immutable-action contracts"
}

if [ "$#" -eq 0 ]; then
  set -- --all
fi

case "$1" in
  --all)
    [ "$#" -eq 1 ] || usage
    check_dependencies
    check_secrets
    generate_sbom "dist/security"
    check_provenance
    ;;
  --dependencies)
    [ "$#" -eq 1 ] || usage
    check_dependencies
    ;;
  --secrets)
    [ "$#" -eq 1 ] || usage
    check_secrets
    ;;
  --sbom)
    [ "$#" -le 2 ] || usage
    generate_sbom "${2:-dist/security}"
    ;;
  --provenance)
    [ "$#" -eq 1 ] || usage
    check_provenance
    ;;
  *) usage ;;
esac
