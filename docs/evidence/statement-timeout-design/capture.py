"""Capture native PostgreSQL semantics; this does not test a future ORM API."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    directory = Path(__file__).resolve().parent
    psql = os.environ.get("ASTERORM_PSQL") or shutil.which("psql")
    if not psql:
        for prefix in ("/opt/homebrew/opt/libpq", "/usr/local/opt/libpq"):
            candidate = Path(prefix) / "bin/psql"
            if candidate.exists():
                psql = str(candidate)
                break
    if not psql:
        raise RuntimeError("Set ASTERORM_PSQL to the PostgreSQL psql executable")
    conninfo = os.environ.get("ASTERORM_TEST_CONNINFO", (
        "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
        "password=orm_test sslmode=disable"
    ))
    # Bound all other statements; the probe deliberately changes statement_timeout.
    environment = os.environ.copy()
    environment["PGOPTIONS"] = environment.get("PGOPTIONS", "") + " -c lock_timeout=3s"
    command = [psql, "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--file=" + str(directory / "probe.sql")]
    result = subprocess.run(
        [*command, "--dbname=" + conninfo], env=environment,
        text=True, capture_output=True, timeout=20,
    )
    (directory / "output.txt").write_text(result.stdout)
    (directory / "stderr.txt").write_text(result.stderr)
    metadata = {
        "captured_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "purpose": "Native semantics for timeout design; future API acceptance is pending",
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "connection_configuration": "ASTERORM_TEST_CONNINFO or compose defaults; value not captured",
        "command": [*command, "--dbname=<configured connection>"],
        "exit_code": result.returncode,
        "sha256": {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                   for name in ("probe.sql", "capture.py", "output.txt", "stderr.txt")},
    }
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(result.stdout)
    if result.returncode:
        print(result.stderr)
    print("Probe exit:", result.returncode)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
