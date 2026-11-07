"""Probe coroutines and optional std::generator using the configured C++23 flags."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess


SOURCES = {
    "coroutine": '''#include <coroutine>
struct Task { struct promise_type {
Task get_return_object() { return {}; }
std::suspend_never initial_suspend() noexcept { return {}; }
std::suspend_never final_suspend() noexcept { return {}; }
void return_void() noexcept {} void unhandled_exception() noexcept {}
}; }; Task example() { co_return; }
''',
    "generator": '''#include <generator>
#ifndef __cpp_lib_generator
#error generator feature macro absent
#endif
std::generator<int> example() { co_yield 1; }
''',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
    parser.add_argument("--json-out", type=Path, required=True)
    arguments = parser.parse_args()
    entries = json.loads((arguments.build_root / "compile_commands.json").read_text())
    entry = next(item for item in entries
                 if item["file"].endswith("/integration/test_repository_stream.cpp"))
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
    records = {}
    for name, source in SOURCES.items():
        checked = subprocess.run([*command, "-x", "c++", "-fsyntax-only", "-"],
                                 cwd=entry["directory"], input=source, capture_output=True,
                                 text=True, timeout=30)
        records[name] = {"source": source, "exit_code": checked.returncode,
                         "stderr": checked.stderr}
        print(f"Configured {name} compile exit: {checked.returncode}")
    arguments.json_out.write_text(json.dumps(records, indent=2) + "\n")
    if records["coroutine"]["exit_code"]:
        raise RuntimeError("Configured compiler failed the baseline coroutine probe")
    # Optional generator failure is a recorded capability result, not a failed gate.


if __name__ == "__main__":
    main()
