"""Capture the existing DML baseline; this does not implement the proposed APIs."""
import argparse
import datetime
import hashlib
import json
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
    args = parser.parse_args()
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    build = args.build_root.resolve()
    commands = json.loads((build / "compile_commands.json").read_text())
    postgres = next(entry for entry in commands if entry["file"].endswith("/integration/test_crud.cpp"))
    command = []
    skip = False
    for argument in shlex.split(postgres["command"]):
        if skip:
            skip = False
            continue
        if argument in ("-o", "-MF", "-MQ", "-MT"):
            skip = True
            continue
        if argument in ("-c", "-MD", "-MMD", postgres["file"]):
            continue
        command.append(argument)
    for suffix in ("/integration/test_sqlite_crud.cpp", "/integration/test_mysql_crud.cpp"):
        entry = next(item for item in commands if item["file"].endswith(suffix))
        if entry["directory"] != postgres["directory"]:
            raise RuntimeError("Expected Meson compilation entries to share a build directory")
        for argument in shlex.split(entry["command"]):
            if argument.startswith("-I") and argument not in command:
                command.append(argument)
    targets = json.loads((build / "meson-info/intro-targets.json").read_text())
    libraries = []
    for name in ("asterorm_core", "asterorm_pg", "asterorm_sqlite", "asterorm_mysql"):
        target = next(item for item in targets if item["name"] == name)
        libraries.append(Path(target["filename"][0]))
    command += [str(directory / "probe.cpp"), *map(str, libraries)]
    command += ["-Wl,-rpath," + str(parent) for parent in sorted({path.parent for path in libraries})]
    with tempfile.TemporaryDirectory(prefix="asterorm-bulk-dml-inventory-") as temporary:
        executable = Path(temporary) / "probe"
        compiled = subprocess.run([*command, "-o", str(executable)], cwd=postgres["directory"],
                                  capture_output=True, text=True, timeout=120)
        if compiled.returncode:
            print(compiled.stderr)
            return compiled.returncode
        executed = subprocess.run([str(executable)], capture_output=True, text=True, timeout=30)
    (directory / "output.txt").write_text(executed.stdout)
    (directory / "stderr.txt").write_text(executed.stderr)
    paths = [directory / name for name in ("probe.cpp", "capture.py", "output.txt", "stderr.txt")]
    inspected = [
        "include/asterorm/core/repository.hpp", "include/asterorm/core/session.hpp",
        "include/asterorm/sql/ast.hpp", "include/asterorm/sql/typed.hpp", "src/sql/compiler.cpp",
        "src/database/postgres/result.cpp", "src/database/sqlite/connection.cpp",
        "src/database/mysql/connection.cpp", "src/database/mysql/driver.cpp",
        "src/database/clickhouse/result.cpp",
    ]
    metadata = {
        "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "purpose": "Existing compiler/adapter/observer baseline, not future API acceptance",
        "git_base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "snapshot_note": "Hashes describe the inspected working tree, including any uncommitted changes; the unrelated .gitignore edit is outside review scope",
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "compiler": json.loads((build / "meson-info/intro-compilers.json").read_text())["host"]["cpp"],
        "compile_command": [*command, "-o", "<temporary executable>"],
        "compile_working_directory": postgres["directory"],
        "compile_exit_code": compiled.returncode,
        "probe_exit_code": executed.returncode,
        "connection_configuration": "Existing ASTERORM_TEST_CONNINFO / ASTERORM_MYSQL_TEST_CONNINFO or Compose defaults; values omitted; SQLite :memory:",
        "evidence_sha256": {str(path.relative_to(root)): digest(path) for path in paths},
        "inspected_source_sha256": {name: digest(root / name) for name in inspected},
        "limits": ["Temporary tables only; no persistent schema changes", "No large-row-count workload or new repository API exercised", "ClickHouse inspected only; no live runtime test", "Local unsigned snapshot, not CI/release evidence"],
    }
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(executed.stdout, end="")
    if executed.stderr:
        print(executed.stderr, end="")
    return executed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
