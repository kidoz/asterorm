"""Verify worked reference fixtures, process isolation, lazy startup and native loss."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import re
import subprocess


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--backend", choices=("pg", "sqlite"), required=True)
parser.add_argument("--integration", type=Path, required=True)
parser.add_argument("--probe", type=Path, required=True)
parser.add_argument("--json-out", type=Path)
arguments = parser.parse_args()
integration = str(arguments.integration.resolve())
probe = str(arguments.probe.resolve())
backend = arguments.backend
fixture_tag = "[transactional:" + ("pg-crud" if backend == "pg" else "sqlite-crud") + "]"
paths = 7 if backend == "pg" else 4
schema = 7 if backend == "pg" else 1
receipts = []


def child(executable, selection, status=0, damage=None, invalid_settings=False, options=()):
    environment = os.environ.copy()
    environment["ASTERORM_REFERENCE_RECEIPT"] = "1"
    environment.pop("ASTERORM_REFERENCE_DAMAGE", None)
    if damage:
        environment["ASTERORM_REFERENCE_DAMAGE"] = damage
    if invalid_settings:
        environment["ASTERORM_TEST_CONNINFO"] = "invalid-reference-test-setting"
    result = subprocess.run([executable, selection, "--order", "lex", *options],
                            env=environment, capture_output=True, text=True, timeout=20)
    output = result.stdout + result.stderr
    assert result.returncode == status, (selection, damage, result.returncode, status, output)
    found = re.search(r"^reference backend=\w+ (.+)$", result.stdout, re.MULTILINE)
    assert found, output
    counts = {name: int(value) for name, value in re.findall(r"(\w+)=(\d+)", found[1])}
    if damage:
        assert "AsterORM transactional harness:" in result.stderr, output
    else:
        assert "AsterORM transactional harness:" not in result.stderr, output
    return {"selection": selection, "damage": damage, "exit_code": result.returncode,
            "counts": counts, "diagnostic": result.stderr if damage else ""}


def check_normal(receipt):
    assert receipt["counts"] == {"preparations": 1, "schema": schema, "identity": 1,
                                 "validations": paths, "bodies": paths, "connect_attempts": 1,
                                 "native_connects": 1}, receipt


normal = child(integration, fixture_tag)
check_normal(normal)
receipts.append(normal)
# Simultaneous processes use identical table names without sharing schema/data.
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as workers:
    futures = [workers.submit(child, integration, fixture_tag) for _ in range(2)]
    for future in futures:
        receipt = future.result()
        check_normal(receipt)
        receipts.append(receipt)
for option in ("--list-tests", "--list-tags"):
    receipt = child(integration, fixture_tag, invalid_settings=True, options=(option,))
    assert all(count == 0 for count in receipt["counts"].values()), receipt
    receipts.append(receipt)
receipt = child(probe, "[reference-untagged]", invalid_settings=True)
assert all(count == 0 for count in receipt["counts"].values()), receipt
receipts.append(receipt)
for damage in (("close", "marker", "baseline", "terminate") if backend == "pg" else ("close", "marker", "baseline")):
    receipt = child(probe, "[reference-loss]", status=1, damage=damage)
    counts = receipt["counts"]
    assert counts["preparations"] == counts["native_connects"] == counts["identity"] == 1, receipt
    assert counts["schema"] == schema and counts["bodies"] == 0, receipt
    if damage == "close":
        assert counts["connect_attempts"] == 2 and counts["validations"] == 1, receipt
        assert "reconnect disabled" in receipt["diagnostic"], receipt
    elif damage == "marker":
        assert counts["connect_attempts"] == 1 and counts["validations"] == 2, receipt
        # The failed validation must preserve the native error and block typed access.
        assert "asterorm_reference_identity" in receipt["diagnostic"], receipt
    elif damage == "baseline":
        assert counts["connect_attempts"] == 1 and counts["validations"] == 2, receipt
        assert "baseline changed" in receipt["diagnostic"], receipt
    else:
        assert counts["connect_attempts"] in (1, 2), receipt
        assert counts["validations"] in (1, 2), receipt
    receipts.append(receipt)
print(f"Passed {len(receipts)} {backend} reference scenarios; {paths} worked paths, one preparation/native connection")
print(json.dumps(normal["counts"], sort_keys=True))
if arguments.json_out:
    arguments.json_out.write_text(json.dumps({"backend": backend, "scenarios": receipts}, indent=2) + "\n")
