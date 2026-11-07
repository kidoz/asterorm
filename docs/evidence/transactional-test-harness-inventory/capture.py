"""Capture session/savepoint and configured Catch2 lifecycle inventory; no harness implementation."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import subprocess
import tempfile


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
    arguments = parser.parse_args()
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    build = arguments.build_root.resolve()
    entries = json.loads((build / "compile_commands.json").read_text())
    entry = next(item for item in entries if item["file"].endswith("/integration/test_crud.cpp"))
    compile_flags = []
    skip = False
    for argument in shlex.split(entry["command"]):
        if skip:
            skip = False
            continue
        if argument in ("-o", "-MF", "-MQ", "-MT"):
            skip = True
            continue
        if argument not in ("-c", "-MD", "-MMD", entry["file"]):
            compile_flags.append(argument)
    targets = json.loads((build / "meson-info/intro-targets.json").read_text())

    def library(name):
        found = next((item for item in targets if item["name"] == name), None)
        if not found:
            raise RuntimeError(f"Inventory requires the configured {name} target")
        return Path(found["filename"][0])

    libraries = [library(name) for name in ("asterorm_pg", "asterorm_sqlite", "asterorm_core")]
    catch_libraries = [library(name) for name in ("Catch2Main", "Catch2")]
    environment = os.environ.copy()
    pkg_paths = [path for path in (Path("/opt/homebrew/opt/libpq/lib/pkgconfig"),
                                  Path("/opt/homebrew/opt/sqlite/lib/pkgconfig")) if path.exists()]
    environment["PKG_CONFIG_PATH"] = os.pathsep.join(map(str, pkg_paths))
    if os.environ.get("PKG_CONFIG_PATH"):
        environment["PKG_CONFIG_PATH"] += os.pathsep + os.environ["PKG_CONFIG_PATH"]
    native_flags = shlex.split(subprocess.check_output(
        ["pkg-config", "--libs", "libpq", "sqlite3"], env=environment, text=True))
    rpaths = ["-Wl,-rpath," + str(parent) for parent in sorted({path.parent for path in libraries})]
    results = {"sessions": {}, "catch2": {}}
    with tempfile.TemporaryDirectory(prefix="asterorm-test-harness-inventory-") as temporary:
        session_executable = Path(temporary) / "session-probe"
        catch_executable = Path(temporary) / "catch-probe"
        for source, executable, links in (
            ("session_probe.cpp", session_executable, [*map(str, libraries), *native_flags, *rpaths]),
            ("catch2_probe.cpp", catch_executable, list(map(str, catch_libraries))),
        ):
            subprocess.run([*compile_flags, str(directory / source), *links, "-o", str(executable)],
                           cwd=entry["directory"], check=True, timeout=120)
        for backend in ("sqlite", "postgres"):
            checked = subprocess.run([str(session_executable), backend], env=environment,
                                     capture_output=True, text=True, timeout=30)
            if checked.returncode:
                raise RuntimeError(f"{backend} session probe failed; settings omitted")
            results["sessions"][backend] = dict(line.split("=", 1)
                                                   for line in checked.stdout.splitlines())
            print(f"{backend} session/savepoint inventory: passed")
        for name, filters, parts, expected_code in (
            ("flow", ["[inventory-flow]"], 4, 0),
            ("fatal", ["[inventory-fatal]"], 1, 42),
            ("skip", ["[inventory-skip]"], 1, 4),
            ("list", ["[inventory-flow]", "--list-tests"], 0, 0),
        ):
            checked = subprocess.run([str(catch_executable), *filters], capture_output=True,
                                     text=True, timeout=30)
            events = re.findall(r"^event=(\w+)$", checked.stdout, flags=re.MULTILINE)
            counts = Counter(events)
            if checked.returncode != expected_code:
                raise RuntimeError(f"Unexpected {name} Catch2 probe status")
            expected = {"case_start": 1, "case_end": 1, "partial_start": parts,
                        "partial_end": parts, "body": parts} if parts else {}
            if dict(counts) != expected:
                raise RuntimeError(f"Unexpected {name} callback counts: {dict(counts)}")
            if parts and events != ["case_start", *[event for _ in range(parts)
                    for event in ("partial_start", "body", "partial_end")], "case_end"]:
                raise RuntimeError(f"Unexpected {name} callback order")
            results["catch2"][name] = {"exit_code": checked.returncode,
                                      "expected_exit_code": expected_code,
                                      "events": events, "counts": dict(counts)}
            print(f"Catch2 {name}: {parts} partial runs, exit {checked.returncode}" +
                  (" (intentional assertion failure)" if name == "fatal" else ""))
    (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    inspected = [
        "include/asterorm/core/session.hpp", "include/asterorm/pool/connection_pool.hpp",
        "include/asterorm/core/row_stream.hpp", "src/database/postgres/connection.cpp",
        "src/database/sqlite/connection.cpp", "tests/integration/test_crud.cpp",
        "tests/integration/test_sqlite_crud.cpp", "tests/integration/test_transactions.cpp",
        "tests/unit/test_transactions.cpp", "tests/meson.build", "meson.build",
        "subprojects/Catch2.wrap",
        "subprojects/Catch2-3.14.0/src/catch2/interfaces/catch_interfaces_reporter.hpp",
        "subprojects/Catch2-3.14.0/src/catch2/internal/catch_run_context.cpp",
        "subprojects/Catch2-3.14.0/src/catch2/catch_session.cpp",
        "subprojects/Catch2-3.14.0/docs/event-listeners.md",
    ]
    metadata = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "Transactional test harness inventory/contract input; no delivered harness API",
        "git_base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "snapshot_note": "Current working tree, including pre-existing staged streaming acceptance work. Git base is not a clean-tree claim; inspected hashes identify the reviewed files. This inventory changes no runtime implementation.",
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "compiler": json.loads((build / "meson-info/intro-compilers.json").read_text())["host"]["cpp"],
        "compile_flags": compile_flags,
        "compile_working_directory": entry["directory"],
        "compile_exit_codes": {"session_probe": 0, "catch2_probe": 0},
        "probe_sha256": {path.name: digest(path) for path in
                          (directory / "session_probe.cpp", directory / "catch2_probe.cpp", Path(__file__))},
        "inspected_source_sha256": {relative: digest(root / relative) for relative in inspected},
        "results_sha256": digest(directory / "results.json"),
        "limits": ["Local configured PG/SQLite/Catch2 observations only",
                   "Temporary tables only; no shared application tables changed",
                   "Intentional Catch2 fatal assertion is expected to exit nonzero",
                   "No harness implementation, fixture migration or speedup claim",
                   "Connection settings and credentials omitted"],
    }
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
