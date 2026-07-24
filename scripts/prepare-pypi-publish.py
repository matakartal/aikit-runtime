#!/usr/bin/env python3
"""Stage only missing PyPI wheels after exact remote checksum verification."""

from __future__ import annotations

import pathlib
import runpy
import sys


SCRIPT = pathlib.Path(__file__).with_name("registry-package-status.py")
namespace = runpy.run_path(str(SCRIPT))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(f"usage: {sys.argv[0]} <verified-wheels> <new-output-directory>")
    sys.argv = [str(SCRIPT), "pypi-plan", sys.argv[1], sys.argv[2]]
    raise SystemExit(namespace["main"]())
