#!/usr/bin/env python3
"""Verify strict SHA256SUMS syntax, path safety, exact coverage, and file digests."""

from __future__ import annotations

import hashlib
import pathlib
import re
import sys


LINE = re.compile(r"^([0-9a-f]{64})  (\./[^\r\n]+)$")


def fail(message: str) -> None:
    raise SystemExit(f"ERROR {message}")


def main() -> None:
    if len(sys.argv) != 2:
        fail(f"usage: {sys.argv[0]} <assembled-bundle>")
    root = pathlib.Path(sys.argv[1])
    manifest = root / "SHA256SUMS"
    if not root.is_dir() or not manifest.is_file() or manifest.is_symlink():
        fail(f"regular checksum manifest not found: {manifest}")
    try:
        lines = manifest.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError as error:
        fail(f"checksum manifest is not UTF-8: {error}")
    if not lines:
        fail("checksum manifest is empty")

    expected: dict[str, str] = {}
    for line_number, line in enumerate(lines, 1):
        match = LINE.fullmatch(line)
        if match is None:
            fail(f"malformed checksum line {line_number}")
        checksum, raw_path = match.groups()
        relative = raw_path[2:]
        pure = pathlib.PurePosixPath(relative)
        if (
            pure.is_absolute()
            or not pure.parts
            or any(part in ("", ".", "..") for part in pure.parts)
            or "\\" in relative
            or relative != pure.as_posix()
            or relative == "SHA256SUMS"
        ):
            fail(f"unsafe checksum path on line {line_number}: {raw_path}")
        normalized = pure.as_posix()
        if normalized in expected:
            fail(f"duplicate checksum path on line {line_number}: {raw_path}")
        expected[normalized] = checksum

    actual: dict[str, pathlib.Path] = {}
    for path in sorted(root.rglob("*")):
        if path == manifest:
            continue
        if path.is_symlink():
            fail(f"bundle contains a symlink: {path.relative_to(root).as_posix()}")
        if path.is_file():
            actual[path.relative_to(root).as_posix()] = path
    omitted = sorted(set(actual) - set(expected))
    extra = sorted(set(expected) - set(actual))
    if omitted:
        fail(f"checksum manifest omits files: {', '.join(omitted)}")
    if extra:
        fail(f"checksum manifest lists missing files: {', '.join(extra)}")

    for relative, path in actual.items():
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected[relative]:
            fail(
                f"checksum mismatch for {relative}: "
                f"expected={expected[relative]} actual={digest}"
            )
    print(f"PASS  checksum manifest exactly covers {len(actual)} bundle files")


if __name__ == "__main__":
    main()
