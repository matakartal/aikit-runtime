#!/usr/bin/env bash
# npm trusted-publisher settings exist only after a package has an owner and first release.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <package-name> [<package-name> ...]" >&2
  exit 2
fi

for package_name in "$@"; do
  set +e
  status="$(./scripts/registry-package-status.py npm-package-exists "$package_name" 2>&1)"
  result=$?
  set -e
  case "$result" in
    0)
      printf 'PASS  npm owner bootstrap exists: %s\n' "$package_name"
      ;;
    3)
      printf '%s\n' \
        "ERROR npm has no owner bootstrap for '$package_name'." \
        "Trusted publishing cannot be configured until the package exists." \
        "Follow the one-time manual npm ownership bootstrap in docs/RELEASE.md, then configure the trusted publisher." >&2
      exit 1
      ;;
    *)
      printf 'ERROR cannot verify npm owner bootstrap for %s: %s\n' \
        "$package_name" "$status" >&2
      exit "$result"
      ;;
  esac
done
