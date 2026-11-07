"""Instrument worked fixture, native-loss and adapter paths with configured flags."""
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
workspace = Path(tempfile.mkdtemp(prefix="asterorm-reference-asan-"))
print("Instrumented fixture files:", workspace, flush=True)
entries = json.loads((build / "compile_commands.json").read_text())
targets = {
    "pg_integration": ("support/pg_integration_main.cpp", "integration/test_crud.cpp"),
    "pg_reference_probe": ("support/pg_integration_main.cpp", "integration/test_pg_reference_fixture.cpp"),
    "sqlite_integration": ("support/sqlite_integration_main.cpp", "integration/test_sqlite_reference_fixture.cpp"),
    "sqlite_reference_probe": ("support/sqlite_integration_main.cpp", "integration/test_sqlite_reference_probe.cpp"),
    "catch2_harness": ("support/catch2_harness_main.cpp", "integration/test_catch2_harness.cpp"),
}

for target, sources in targets.items():
    replacements = {}
    for index, source in enumerate(sources):
        candidates = [entry for entry in entries if entry["file"].endswith("/" + source)]
        entry = next(entry for entry in candidates if f"tests/{target}.p/" in entry["command"])
        command = shlex.split(entry["command"])
        output_index = command.index("-o") + 1
        previous = command[output_index]
        instrumented = workspace / f"{target}-{index}.o"
        command[output_index] = str(instrumented)
        filtered = []
        skip = False
        for argument in command:
            if skip:
                skip = False
            elif argument in ("-MF", "-MQ", "-MT"):
                skip = True
            elif argument not in ("-MD", "-MMD"):
                filtered.append(argument)
        filtered += ["-fsanitize=address", "-fno-omit-frame-pointer"]
        subprocess.run(filtered, cwd=entry["directory"], check=True)
        replacements[previous] = str(instrumented)
    commands = subprocess.check_output(["ninja", "-C", str(build), "-t", "commands", "tests/" + target],
                                       text=True).splitlines()
    link = shlex.split(commands[-1])
    link[link.index("-o") + 1] = str(workspace / target)
    link = [replacements.get(argument, argument) for argument in link]
    assert all(value in link for value in replacements.values())
    link += ["-fsanitize=address", "-Wl,-rpath," + str(build / "src"),
             "-Wl,-rpath," + str(build / "src/database")]
    subprocess.run(link, cwd=build, check=True)

for backend, prefix in (("pg", "pg"), ("sqlite", "sqlite")):
    subprocess.run([sys.executable, str(root / "tests/check_reference_fixtures.py"),
                    "--backend", backend, "--integration", str(workspace / (prefix + "_integration")),
                    "--probe", str(workspace / (prefix + "_reference_probe"))], check=True)
subprocess.run([sys.executable, str(root / "tests/check_catch2_harness.py"),
                str(workspace / "catch2_harness")], check=True)
print("Selected worked/loss/adapter translation units passed AddressSanitizer; remaining objects/libraries are not instrumented")
