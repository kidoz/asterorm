"""Compile the pool cache guide with configured SQLite C++23 flags."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--build-root", type=Path, required=True)
arguments = parser.parse_args()
root = Path(__file__).resolve().parents[1]
entries = json.loads((arguments.build_root / "compile_commands.json").read_text())
entry = next(item for item in entries if item["file"].endswith("/integration/test_sqlite_crud.cpp"))
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
guide = (root / "docs/how-to/pool-cache-and-profiles.md").read_text()
snippets = [part.split("```", 1)[0] for part in guide.split("```cpp\n")[1:]]
if len(snippets) != 1:
    raise RuntimeError("Expected one self-contained pool cache guide")
checked = subprocess.run([*command, "-x", "c++", "-fsyntax-only", "-"],
                         cwd=entry["directory"], input='#include "asterorm/sqlite/driver.hpp"\n' + snippets[0] + '\ntemplate asterorm::result<int> pool_cache_example(asterorm::sqlite::driver, std::string, bool);\n', text=True)
raise SystemExit(checked.returncode)
