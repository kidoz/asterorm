#!/usr/bin/env python3
"""Run clang-format over the project tree."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

EXTENSIONS = (".cpp", ".hpp", ".h", ".cc", ".cxx")
SKIP_DIRS = {"subprojects", ".agents"}


def collect(root: Path) -> list[Path]:
    collected_files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(
            part in SKIP_DIRS or part.startswith(".") or part.startswith("build")
            for part in path.relative_to(root).parts[:-1]
        ):
            continue
        if path.suffix in EXTENSIONS:
            collected_files.append(path)
    return collected_files


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--check", action="store_true")
    parsed_arguments = parser.parse_args()

    source_files = collect(parsed_arguments.source_root)
    if not source_files:
        return 0

    command = ["clang-format"]
    command += ["--dry-run", "--Werror"] if parsed_arguments.check else ["-i"]
    command += [str(source_file) for source_file in source_files]

    try:
        return subprocess.call(command)
    except FileNotFoundError:
        print("clang-format not found on PATH; skipping", file=sys.stderr)
        return 0


if __name__ == "__main__":
    sys.exit(main())
