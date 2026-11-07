"""Syntax-check the native PostgreSQL single-row guide with configured flags."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", type=Path, required=True)
    parser.add_argument("--guide", type=Path,
                        default=Path("docs/how-to/postgresql-single-row-reader.md"))
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    entries = json.loads((arguments.build_root / "compile_commands.json").read_text())
    entry = next(item for item in entries if item["file"].endswith("/integration/test_single_row.cpp"))
    command = []
    skip = False
    for argument in shlex.split(entry["command"]):
        if skip:
            skip = False
            continue
        if argument in ("-o", "-MF", "-MQ", "-MT"):
            skip = True
            continue
        if argument not in ("-c", "-MD", "-MMD", entry["file"]):
            command.append(argument)
    guide = (root / arguments.guide).read_text()
    snippets = [part.split("```", 1)[0] for part in guide.split("```cpp\n")[1:]]
    if not snippets:
        raise RuntimeError("Expected at least one native single-row guide example")
    source = "\n".join(snippets)
    checked = subprocess.run([*command, "-x", "c++", "-fsyntax-only", "-"],
                             cwd=entry["directory"], input=source, text=True)
    return checked.returncode


if __name__ == "__main__":
    raise SystemExit(main())
