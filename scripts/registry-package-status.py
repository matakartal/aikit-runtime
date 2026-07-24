#!/usr/bin/env python3
"""Fail-closed registry integrity checks used by the prepared publish workflow."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import pathlib
import re
import shutil
import sys
import tarfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from email.parser import BytesParser


MISSING = 3
ADVANCE = 4
TRANSIENT = 5
CONFLICT = 6
SEMVER = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


class IntegrityError(RuntimeError):
    pass


class RegistryTransientError(RuntimeError):
    pass


def user_agent(version: str) -> str:
    repository = os.environ.get("GITHUB_REPOSITORY", "local/aikit-runtime")
    return f"aikit-release-integrity/{version} (+https://github.com/{repository})"


def endpoint(base: str, *components: str) -> str:
    quoted = "/".join(urllib.parse.quote(component, safe="") for component in components)
    return f"{base.rstrip('/')}/{quoted}"


def fetch_json(url: str, version: str) -> dict[str, object] | None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "file" and pathlib.Path(urllib.request.url2pathname(parsed.path)).is_dir():
        url = url.rstrip("/") + "/index.json"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": user_agent(version)},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        if error.code == 429 or 500 <= error.code <= 599:
            raise RegistryTransientError(
                f"registry request is temporarily unavailable (HTTP {error.code}): {url}"
            ) from error
        raise IntegrityError(f"registry request failed with HTTP {error.code}: {url}") from error
    except urllib.error.URLError as error:
        if urllib.parse.urlparse(url).scheme == "file" and isinstance(
            error.reason, FileNotFoundError
        ):
            return None
        raise RegistryTransientError(
            f"registry request is temporarily unavailable: {url}: {error.reason}"
        ) from error
    except TimeoutError as error:
        raise RegistryTransientError(f"registry request timed out: {url}") from error
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise IntegrityError(f"registry returned invalid JSON: {url}") from error
    if not isinstance(decoded, dict):
        raise IntegrityError(f"registry JSON root must be an object: {url}")
    return decoded


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def npm_info(path: pathlib.Path) -> tuple[str, str, str]:
    if not path.is_file():
        raise IntegrityError(f"npm tarball not found: {path}")
    try:
        with tarfile.open(path, "r:gz") as archive:
            members = [
                member
                for member in archive.getmembers()
                if member.name == "package/package.json" and member.isfile()
            ]
            if len(members) != 1:
                raise IntegrityError(
                    f"npm tarball must contain exactly one package/package.json: {path}"
                )
            extracted = archive.extractfile(members[0])
            if extracted is None:
                raise IntegrityError(f"cannot read npm package metadata: {path}")
            metadata = json.load(extracted)
    except (tarfile.TarError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise IntegrityError(f"invalid npm tarball: {path}") from error
    name = metadata.get("name")
    version = metadata.get("version")
    if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
        raise IntegrityError(f"npm package name/version is missing: {path}")
    digest = hashlib.sha512(path.read_bytes()).digest()
    integrity = "sha512-" + base64.b64encode(digest).decode("ascii")
    return name, version, integrity


def pypi_info(path: pathlib.Path) -> tuple[str, str]:
    if not path.is_file():
        raise IntegrityError(f"wheel not found: {path}")
    try:
        with zipfile.ZipFile(path) as archive:
            metadata_names = [
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
            if len(metadata_names) != 1:
                raise IntegrityError(f"wheel must contain exactly one METADATA file: {path}")
            metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))
    except (zipfile.BadZipFile, KeyError) as error:
        raise IntegrityError(f"invalid wheel: {path}") from error
    name = metadata.get("Name")
    version = metadata.get("Version")
    if not name or not version:
        raise IntegrityError(f"wheel Name/Version metadata is missing: {path}")
    return name, version


def canonical_python_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def check_crate(args: argparse.Namespace) -> int:
    artifact = pathlib.Path(args.artifact)
    if not artifact.is_file():
        raise IntegrityError(f"crate artifact not found: {artifact}")
    local_checksum = sha256(artifact)
    base = os.environ.get("AIKIT_CRATES_API_BASE", "https://crates.io/api/v1/crates")
    payload = fetch_json(endpoint(base, args.name, args.version), args.version)
    if payload is None:
        print("missing")
        return MISSING
    version = payload.get("version")
    if not isinstance(version, dict):
        raise IntegrityError("crates.io response is missing version metadata")
    if version.get("crate") != args.name or version.get("num") != args.version:
        raise IntegrityError("crates.io returned metadata for a different crate/version")
    remote_checksum = version.get("checksum")
    if remote_checksum != local_checksum:
        raise IntegrityError(
            f"crate checksum conflict for {args.name} {args.version}: "
            f"local={local_checksum} registry={remote_checksum or 'missing'}"
        )
    print("exact")
    return 0


def check_crate_exists(args: argparse.Namespace) -> int:
    base = os.environ.get("AIKIT_CRATES_API_BASE", "https://crates.io/api/v1/crates")
    payload = fetch_json(endpoint(base, args.name), "bootstrap")
    if payload is None:
        print("missing")
        return MISSING
    crate = payload.get("crate")
    if not isinstance(crate, dict) or crate.get("id") != args.name:
        raise IntegrityError("crates.io returned metadata for a different crate")
    print("exists")
    return 0


def check_npm(args: argparse.Namespace) -> int:
    artifact = pathlib.Path(args.artifact)
    name, version, integrity = npm_info(artifact)
    base = os.environ.get("AIKIT_NPM_REGISTRY_BASE", "https://registry.npmjs.org")
    payload = fetch_json(endpoint(base, name, version), version)
    if payload is None:
        print("missing")
        return MISSING
    if payload.get("name") != name or payload.get("version") != version:
        raise IntegrityError("npm registry returned metadata for a different package/version")
    distribution = payload.get("dist")
    remote_integrity = distribution.get("integrity") if isinstance(distribution, dict) else None
    if remote_integrity != integrity:
        raise IntegrityError(
            f"npm integrity conflict for {name}@{version}: "
            f"local={integrity} registry={remote_integrity or 'missing'}"
        )
    print("exact")
    return 0


def semver_precedence(version: str) -> tuple[tuple[int, int, int], tuple[tuple[int, object], ...]]:
    match = SEMVER.fullmatch(version)
    if match is None:
        raise IntegrityError(f"version is not npm-compatible SemVer: {version}")
    core = tuple(int(match.group(index)) for index in range(1, 4))
    prerelease = match.group(4)
    if prerelease is None:
        return core, ((2, ""),)
    identifiers: list[tuple[int, object]] = []
    for identifier in prerelease.split("."):
        if identifier.isdigit():
            if len(identifier) > 1 and identifier.startswith("0"):
                raise IntegrityError(f"numeric prerelease identifier has a leading zero: {version}")
            identifiers.append((0, int(identifier)))
        else:
            identifiers.append((1, identifier))
    return core, tuple(identifiers)


def compare_semver(left: str, right: str) -> int:
    left_core, left_pre = semver_precedence(left)
    right_core, right_pre = semver_precedence(right)
    if left_core != right_core:
        return -1 if left_core < right_core else 1
    if left_pre == right_pre:
        return 0
    if left_pre == ((2, ""),):
        return 1
    if right_pre == ((2, ""),):
        return -1
    for left_id, right_id in zip(left_pre, right_pre):
        if left_id == right_id:
            continue
        if left_id[0] != right_id[0]:
            return -1 if left_id[0] < right_id[0] else 1
        return -1 if left_id[1] < right_id[1] else 1
    return -1 if len(left_pre) < len(right_pre) else 1


def check_npm_tag(args: argparse.Namespace) -> int:
    artifact = pathlib.Path(args.artifact)
    name, version, _ = npm_info(artifact)
    base = os.environ.get("AIKIT_NPM_REGISTRY_BASE", "https://registry.npmjs.org")
    payload = fetch_json(endpoint(base, name), version)
    if payload is None:
        print("missing")
        return MISSING
    tags = payload.get("dist-tags")
    remote_version = tags.get(args.tag) if isinstance(tags, dict) else None
    if remote_version is None:
        print("missing")
        return MISSING
    if remote_version == version:
        print("exact")
        return 0
    if not isinstance(remote_version, str):
        raise IntegrityError(f"npm dist-tag {args.tag} has an invalid registry value")
    if compare_semver(remote_version, version) < 0:
        print("advance")
        return ADVANCE
    raise IntegrityError(
        f"refusing to move npm dist-tag {args.tag} backward: "
        f"registry={remote_version} requested={version}"
    )


def npm_metadata(args: argparse.Namespace) -> int:
    name, version, _ = npm_info(pathlib.Path(args.artifact))
    print(f"{name}\t{version}")
    return 0


def check_npm_package_exists(args: argparse.Namespace) -> int:
    base = os.environ.get("AIKIT_NPM_REGISTRY_BASE", "https://registry.npmjs.org")
    payload = fetch_json(endpoint(base, args.name), "bootstrap")
    if payload is None:
        print("missing")
        return MISSING
    if payload.get("name") != args.name:
        raise IntegrityError("npm returned metadata for a different package")
    print("exists")
    return 0


def check_pypi_wheel(path: pathlib.Path) -> int:
    name, version = pypi_info(path)
    base = os.environ.get("AIKIT_PYPI_API_BASE", "https://pypi.org/pypi")
    payload = fetch_json(endpoint(base, name, version, "json"), version)
    if payload is None:
        return MISSING
    info = payload.get("info")
    if not isinstance(info, dict):
        raise IntegrityError("PyPI response is missing info metadata")
    if canonical_python_name(str(info.get("name", ""))) != canonical_python_name(name):
        raise IntegrityError("PyPI returned metadata for a different distribution")
    if info.get("version") != version:
        raise IntegrityError("PyPI returned metadata for a different version")
    urls = payload.get("urls")
    if not isinstance(urls, list):
        raise IntegrityError("PyPI response is missing file metadata")
    matches = [entry for entry in urls if isinstance(entry, dict) and entry.get("filename") == path.name]
    if not matches:
        return MISSING
    if len(matches) != 1:
        raise IntegrityError(f"PyPI returned duplicate metadata for {path.name}")
    digests = matches[0].get("digests")
    remote_checksum = digests.get("sha256") if isinstance(digests, dict) else None
    local_checksum = sha256(path)
    if remote_checksum != local_checksum:
        raise IntegrityError(
            f"PyPI checksum conflict for {path.name}: "
            f"local={local_checksum} registry={remote_checksum or 'missing'}"
        )
    return 0


def plan_pypi(args: argparse.Namespace) -> int:
    source = pathlib.Path(args.input)
    output = pathlib.Path(args.output)
    if not source.is_dir():
        raise IntegrityError(f"wheel input directory not found: {source}")
    if output.exists():
        raise IntegrityError(f"refusing to replace existing PyPI plan: {output}")
    wheels = sorted(source.glob("*.whl"))
    if not wheels:
        raise IntegrityError(f"no wheels found in {source}")
    missing: list[pathlib.Path] = []
    for wheel in wheels:
        status = check_pypi_wheel(wheel)
        if status == MISSING:
            missing.append(wheel)
        else:
            print(f"PASS  PyPI already has the exact artifact: {wheel.name}")
    output.mkdir(parents=True)
    for wheel in missing:
        shutil.copy2(wheel, output / wheel.name)
        print(f"PLAN  PyPI upload required: {wheel.name}")
    print(f"HAS_PACKAGES={'true' if missing else 'false'}")
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    subcommands = result.add_subparsers(dest="command", required=True)
    crate = subcommands.add_parser("crate")
    crate.add_argument("name")
    crate.add_argument("version")
    crate.add_argument("artifact")
    crate.set_defaults(handler=check_crate)
    crate_exists = subcommands.add_parser("crate-exists")
    crate_exists.add_argument("name")
    crate_exists.set_defaults(handler=check_crate_exists)
    npm = subcommands.add_parser("npm")
    npm.add_argument("artifact")
    npm.set_defaults(handler=check_npm)
    npm_tag = subcommands.add_parser("npm-tag")
    npm_tag.add_argument("artifact")
    npm_tag.add_argument("tag")
    npm_tag.set_defaults(handler=check_npm_tag)
    metadata = subcommands.add_parser("npm-metadata")
    metadata.add_argument("artifact")
    metadata.set_defaults(handler=npm_metadata)
    npm_exists = subcommands.add_parser("npm-package-exists")
    npm_exists.add_argument("name")
    npm_exists.set_defaults(handler=check_npm_package_exists)
    pypi = subcommands.add_parser("pypi-plan")
    pypi.add_argument("input")
    pypi.add_argument("output")
    pypi.set_defaults(handler=plan_pypi)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        return args.handler(args)
    except RegistryTransientError as error:
        print(f"TRANSIENT {error}", file=sys.stderr)
        return TRANSIENT
    except IntegrityError as error:
        print(f"CONFLICT {error}", file=sys.stderr)
        return CONFLICT
    except OSError as error:
        print(f"ERROR {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
