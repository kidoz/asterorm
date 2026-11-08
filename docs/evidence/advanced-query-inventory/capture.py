"""Compile inventory probes with configured flags and capture local SQL semantics."""
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
    parser.add_argument("--lint", action="store_true")
    arguments = parser.parse_args()
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    build = arguments.build_root.resolve()
    entries = json.loads((build / "compile_commands.json").read_text())
    results = {}
    flags_record = {}
    with tempfile.TemporaryDirectory(prefix="asterorm-advanced-query-inventory-") as workspace:
        for backend, target, source, macro in (
            ("ast", "core_tests", "unit/test_ast.cpp", None),
            ("postgres", "pg_integration", "integration/test_sql.cpp", "ASTERORM_INVENTORY_POSTGRES"),
            ("sqlite", "sqlite_integration", "integration/test_sqlite_crud.cpp", "ASTERORM_INVENTORY_SQLITE"),
            ("mysql", "mysql_integration", "integration/test_mysql_crud.cpp", "ASTERORM_INVENTORY_MYSQL"),
        ):
            entry = next(item for item in entries if item["file"].endswith("/" + source))
            original = shlex.split(entry["command"])
            flags = []
            skip = False
            for argument in original:
                if skip:
                    skip = False
                elif argument in ("-o", "-MF", "-MQ", "-MT"):
                    skip = True
                elif argument not in ("-c", "-MD", "-MMD", entry["file"]):
                    flags.append(argument)
            flags_record[backend] = {"arguments": flags, "directory": entry["directory"]}
            executable = Path(workspace) / backend
            object_path = Path(workspace) / (backend + ".o")
            probe = directory / ("ast_probe.cpp" if backend == "ast" else "native_probe.cpp")
            if arguments.lint:
                lint_flags = flags[1:] + (["-D" + macro] if macro else [])
                if platform.system() == "Darwin":
                    lint_flags += ["-isysroot", subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()]
                subprocess.run(["clang-tidy", "--warnings-as-errors=*", str(probe), "--", *lint_flags],
                               cwd=entry["directory"], check=True)
            subprocess.run([*flags, *(["-D" + macro] if macro else []), "-c", str(probe),
                            "-o", str(object_path)], cwd=entry["directory"], check=True)
            commands = subprocess.check_output(["ninja", "-C", str(build), "-t", "commands", "tests/" + target],
                                               text=True).splitlines()
            link = shlex.split(commands[-1])
            link[link.index("-o") + 1] = str(executable)
            filtered = []
            replaced = False
            for argument in link:
                if argument.startswith("tests/" + target + ".p/") and argument.endswith(".o"):
                    if not replaced:
                        filtered.append(str(object_path))
                        replaced = True
                elif "Catch2" not in argument:
                    filtered.append(argument)
            assert replaced
            filtered += ["-Wl,-rpath," + str(build / "src"), "-Wl,-rpath," + str(build / "src/database")]
            subprocess.run(filtered, cwd=build, check=True)
            checked = subprocess.run([str(executable)], capture_output=True, text=True, timeout=30,
                                     env=os.environ.copy())
            if checked.returncode:
                raise RuntimeError(f"{backend} probe failed: {checked.stdout}{checked.stderr}")
            results[backend] = dict(line.split("=",1) for line in checked.stdout.splitlines())
            print(backend, "inventory compiled/executed: exit 0")
    (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    metadata = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_base": subprocess.check_output(["git","rev-parse","HEAD"],cwd=root,text=True).strip(),
        "purpose": "Current AST inventory/native feasibility; no structured advanced-query implementation",
        "platform": {"system":platform.system(),"machine":platform.machine()},
        "configured_flags":flags_record,
        "strict_probe_lint":arguments.lint,
        "probe_sha256":{name:hashlib.sha256((directory/name).read_bytes()).hexdigest()
                        for name in ("ast_probe.cpp","native_probe.cpp","capture.py")},
        "results_sha256":hashlib.sha256((directory/"results.json").read_bytes()).hexdigest(),
        "limits":["TEMP tables only; connection settings/credentials omitted",
                  "Configured PG/SQLite/MySQL observations; no ClickHouse or older-server execution",
                  "Native syntax success does not establish AST support or a performance claim",
                  "Dangerous cyclic/null legacy function graphs are inspected in source, not executed"],
    }
    (directory/"capture.json").write_text(json.dumps(metadata,indent=2)+"\n")


if __name__ == "__main__":
    main()
