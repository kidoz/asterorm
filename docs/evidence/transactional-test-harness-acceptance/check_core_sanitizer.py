"""Instrument the generic testing-scope unit TU and run its ownership/failure cases."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import tempfile


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
arguments = parser.parse_args()
build = arguments.build_root.resolve()
workspace = Path(tempfile.mkdtemp(prefix="asterorm-harness-core-asan-"))
entries = json.loads((build / "compile_commands.json").read_text())
entry = next(item for item in entries if item["file"].endswith("/unit/test_transactional_session.cpp"))
compile_command = shlex.split(entry["command"])
object_path = workspace / "scope.o"
compile_command[compile_command.index("-o") + 1] = str(object_path)
filtered = []
skip = False
for argument in compile_command:
    if skip:
        skip = False
    elif argument in ("-MF", "-MQ", "-MT"):
        skip = True
    elif argument not in ("-MD", "-MMD"):
        filtered.append(argument)
subprocess.run([*filtered, "-fsanitize=address", "-fno-omit-frame-pointer"],
               cwd=entry["directory"], check=True)
commands = subprocess.check_output(["ninja", "-C", str(build), "-t", "commands", "tests/core_tests"],
                                   text=True).splitlines()
link = shlex.split(commands[-1])
executable = workspace / "scope_tests"
link[link.index("-o") + 1] = str(executable)
filtered = []
replaced = False
for argument in link:
    if argument.startswith("tests/core_tests.p/") and argument.endswith(".o"):
        if not replaced:
            filtered.append(str(object_path))
            replaced = True
    else:
        filtered.append(argument)
assert replaced
subprocess.run([*filtered, "-fsanitize=address", "-Wl,-rpath," + str(build / "src")],
               cwd=build, check=True)
subprocess.run([str(executable), "[transactional-testing]"], check=True)
print("Generic scope unit translation unit passed AddressSanitizer; existing core/Catch2 libraries are not instrumented")
print("Temporary instrumented unit:", workspace)
