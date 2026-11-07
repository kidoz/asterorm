"""Instrument both adapter test translation units and run the subprocess matrix."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
arguments = parser.parse_args()
root = Path(__file__).resolve().parents[3]
build = arguments.build_root.resolve()
workspace = Path(tempfile.mkdtemp(prefix="asterorm-catch2-asan-"))
print("Instrumented adapter files:", workspace, flush=True)
entries = json.loads((build / "compile_commands.json").read_text())
sources = ("/support/catch2_harness_main.cpp", "/integration/test_catch2_harness.cpp")
replacements = {}
for index, source in enumerate(sources):
    entry = next(item for item in entries if item["file"].endswith(source))
    command = shlex.split(entry["command"])
    output_index = command.index("-o") + 1
    previous = command[output_index]
    instrumented = workspace / f"adapter-{index}.o"
    command[output_index] = str(instrumented)
    # Keep dependency-file writes inside the original build untouched.
    filtered = []
    skip = False
    for argument in command:
        if skip:
            skip = False
        elif argument in ("-MF", "-MQ", "-MT"):
            skip = True
        elif argument not in ("-MD", "-MMD"):
            filtered.append(argument)
    command = filtered
    command += ["-fsanitize=address", "-fno-omit-frame-pointer"]
    subprocess.run(command, cwd=entry["directory"], check=True)
    replacements[previous] = str(instrumented)
commands = subprocess.check_output(["ninja", "-C", str(build), "-t", "commands", "tests/catch2_harness"],
                                   text=True).splitlines()
link = shlex.split(commands[-1])
executable = workspace / "catch2_harness"
link[link.index("-o") + 1] = str(executable)
link = [replacements.get(argument, argument) for argument in link]
assert all(value in link for value in replacements.values())
link += ["-fsanitize=address", "-Wl,-rpath," + str(build / "src"),
         "-Wl,-rpath," + str(build / "src/database")]
subprocess.run(link, cwd=build, check=True)
subprocess.run([sys.executable, str(root / "tests/check_catch2_harness.py"), str(executable)], check=True)
print("Both adapter translation units passed AddressSanitizer; dependency binaries are not instrumented")
