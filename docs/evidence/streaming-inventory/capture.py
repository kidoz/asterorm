"""Capture native PostgreSQL streaming and configured-toolchain inventory evidence."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import tempfile


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
    arguments = parser.parse_args()
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    build = arguments.build_root.resolve()
    entries = json.loads((build / "compile_commands.json").read_text())
    entry = next(item for item in entries if item["file"].endswith("/integration/test_crud.cpp"))
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
    toolchain = {}
    for name, source in {
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
    }.items():
        checked = subprocess.run([*command, "-x", "c++", "-fsyntax-only", "-"],
                                 cwd=entry["directory"], input=source, text=True,
                                 capture_output=True, timeout=30)
        toolchain[name] = {"source": source, "exit_code": checked.returncode,
                           "stderr": checked.stderr}
    if toolchain["coroutine"]["exit_code"]:
        raise RuntimeError("Configured compiler cannot compile the baseline coroutine probe")
    targets = json.loads((build / "meson-info/intro-targets.json").read_text())
    libraries = [Path(next(item for item in targets if item["name"] == name)["filename"][0])
                 for name in ("asterorm_core", "asterorm_pg")]
    environment = os.environ.copy()
    pkg_paths = [path for path in (Path("/opt/homebrew/opt/libpq/lib/pkgconfig"),
                                  Path("/usr/local/opt/libpq/lib/pkgconfig")) if path.exists()]
    environment["PKG_CONFIG_PATH"] = os.pathsep.join(map(str, pkg_paths))
    if os.environ.get("PKG_CONFIG_PATH"):
        environment["PKG_CONFIG_PATH"] += os.pathsep + os.environ["PKG_CONFIG_PATH"]
    libpq_flags = shlex.split(subprocess.check_output(
        ["pkg-config", "--libs", "libpq"], env=environment, text=True))
    command += [str(directory / "probe.cpp"), *map(str, libraries), *libpq_flags]
    command += ["-Wl,-rpath," + str(parent) for parent in sorted({path.parent for path in libraries})]
    results = {}
    with tempfile.TemporaryDirectory(prefix="asterorm-streaming-inventory-") as temporary:
        executable = Path(temporary) / "probe"
        compiled = subprocess.run([*command, "-o", str(executable)], cwd=entry["directory"],
                                  capture_output=True, text=True, timeout=120)
        if compiled.returncode:
            print(compiled.stderr)
            return compiled.returncode
        for mode in ("protocol", "buffered", "single-row", "cursor"):
            executed = subprocess.run([str(executable), *([] if mode == "protocol" else [mode])],
                                      capture_output=True, text=True, timeout=30)
            if executed.returncode:
                print(executed.stderr)
                return executed.returncode
            results[mode] = dict(line.split("=", 1) for line in executed.stdout.splitlines())
            print(executed.stdout, end="")
    for mode, max_rows in (("single-row", 1), ("cursor", 256)):
        metrics = results[mode]
        growth = int(metrics["peak_rss_after_bytes"]) - int(metrics["peak_rss_before_bytes"])
        if int(metrics["max_batch_rows"]) > max_rows or growth > 32 * 1024 * 1024:
            raise RuntimeError("Native probe exceeded the proposed batch/RSS bound")
    (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (directory / "toolchain.json").write_text(json.dumps(toolchain, indent=2) + "\n")
    inspected = [
        "include/asterorm/core/repository.hpp", "include/asterorm/core/session.hpp",
        "include/asterorm/core/hydration.hpp", "include/asterorm/core/observer.hpp",
        "include/asterorm/pool/connection_pool.hpp", "include/asterorm/postgres/connection.hpp",
        "src/database/postgres/connection.cpp", "src/database/postgres/result.cpp",
        "src/database/sqlite/connection.cpp", "src/database/mysql/connection.cpp",
        "src/database/clickhouse/connection.cpp", "meson.build", ".github/workflows/ci.yml",
    ]
    metadata = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "Pre-implementation native/toolchain inventory; no AsterORM stream acceptance",
        "git_base_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "compiler": json.loads((build / "meson-info/intro-compilers.json").read_text())["host"]["cpp"],
        "compile_command": [*command, "-o", "<temporary executable>"],
        "compile_working_directory": entry["directory"],
        "compile_exit_code": 0,
        "native_probe_exit_codes": {mode: 0 for mode in results},
        "connection_configuration": "ASTERORM_TEST_CONNINFO or existing Compose defaults; values omitted",
        "evidence_sha256": {name: digest(directory / name)
                            for name in ("capture.py", "probe.cpp", "results.json", "toolchain.json")},
        "inspected_source_sha256": {name: digest(root / name) for name in inspected},
        "limits": ["Local unsigned inventory, not CI/release evidence",
                   "Read-only native queries and transaction-local cursors; no persistent schema changes",
                   "Each scan uses a fresh process; getrusage peak RSS is normalized to bytes",
                   "100000 rows of 1024-byte payload; proposed growth bound is 32 MiB",
                   "Client result batching only; server plan memory and caller retention are not bounded",
                   "No production streaming, session-busy gate, decoder or destructor cancellation exercised",
                   "Generator failure is recorded as a toolchain finding, not a capture failure"],
    }
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print("Configured coroutine compile exit:", toolchain["coroutine"]["exit_code"])
    print("Configured std::generator compile exit:", toolchain["generator"]["exit_code"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
