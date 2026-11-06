#!/usr/bin/env python3
"""Compile typed DSL controls and invalid queries with the configured toolchain."""
from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
from pathlib import Path


def compile_command(build_root: Path) -> tuple[list[str], Path]:
    fixture = Path(__file__).with_name("test_typed_dsl.cpp").resolve()
    entries = json.loads((build_root / "compile_commands.json").read_text())
    entry = next(
        entry for entry in entries
        if (Path(entry["directory"]) / entry["file"]).resolve() == fixture
    )
    arguments = entry.get("arguments") or shlex.split(entry["command"])
    command = []
    argument_iterator = iter(arguments)
    for argument in argument_iterator:
        if argument in {"-o", "-MF", "-MQ", "-MT"}:
            next(argument_iterator)
        elif argument not in {"-c", "-MD", "-MMD"}:
            command.append(argument)
    return command + ["-fsyntax-only", "-fdiagnostics-color=never"], Path(entry["directory"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", required=True, type=Path)
    arguments = parser.parse_args()
    command, working_directory = compile_command(arguments.build_root.resolve())
    comparison_diagnostic = r"invalid operands|no match for .*operator|no matching function"
    cases = [
        (0, "valid queries", None),
        (1, "boolean/string literal", comparison_diagnostic),
        (2, "boolean/character pointer", comparison_diagnostic),
        (3, "optional boolean/string literal", comparison_diagnostic),
        (4, "boolean/std::string", comparison_diagnostic),
        (5, "boolean/integer", comparison_diagnostic),
        (6, "integer/boolean", comparison_diagnostic),
        (7, "integer/std::string", comparison_diagnostic),
        (8, "boolean/text column", comparison_diagnostic),
        (9, "unmapped member", r"not declared in.*entity_traits"),
        (10, "unmapped entity", r"entity_traits<.*unknown_entity"),
        (11, "cross-entity predicate", r"typed_predicate"),
        (12, "incompatible DTO", r"DTO field types do not match"),
        (13, "NULL equality", r"Use sql::typed::is_null"),
        (14, "mixed-entity projection", r"every entity-bound item to share the same entity"),
        (15, "mixed-entity inferred projection",
         r"every entity-bound item to share the same entity"),
        (16, "inferred projection without an entity",
         r"requires at least one entity-bound column"),
        (17, "empty inferred projection", r"select_cols requires at least one item"),
    ]
    failures = 0
    for case_number, description, diagnostic_pattern in cases:
        process = subprocess.run(
            command + [f"-DASTERORM_COMPILE_CASE={case_number}"],
            cwd=working_directory, capture_output=True, text=True, timeout=30,
        )
        diagnostics = process.stdout + process.stderr
        if diagnostic_pattern is None:
            passed = process.returncode == 0
        else:
            passed = (
                process.returncode > 0
                and re.search(diagnostic_pattern, diagnostics) is not None
                and "fatal error:" not in diagnostics
            )
        print(f'{"PASS" if passed else "FAIL"}: {description}', flush=True)
        if not passed:
            failures += 1
            print(diagnostics or "Compiler accepted an invalid query.", flush=True)
            # Without a valid control, rejections could just be a broken
            # compiler, include path or fixture. Never count those as passes.
            if case_number == 0:
                break
    print(f"{failures} failed compile checks", flush=True)
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
