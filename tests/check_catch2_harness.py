"""Exercise the optional adapter in fresh Catch2 processes, including expected failures."""
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET


executable = str(Path(sys.argv[1]).resolve())
scenarios = 0


def run_case(tag, status=0, fault=None, arguments=(), **expected):
    global scenarios
    environment = os.environ.copy()
    environment.pop("ASTERORM_HARNESS_FAULT", None)
    if fault:
        environment["ASTERORM_HARNESS_FAULT"] = fault
    child = subprocess.run([executable, tag, "--order", "lex", *arguments],
                           capture_output=True, text=True, env=environment, timeout=15)
    output = child.stdout + child.stderr
    assert child.returncode == status, (tag, fault, child.returncode, status, output)
    match = re.search(r"^summary (.+)$", child.stdout, re.MULTILINE)
    assert match, output
    counts = {key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", match[1])}
    for key, value in expected.items():
        assert counts[key] == value, (tag, fault, key, counts, output)
    if fault or tag in ("[unknown]", "[multiple]", "[empty]", "[malformed]", "[mismatch]",
                        "[outside]", "[late]", "[unsupported]"):
        assert "AsterORM transactional harness:" in child.stderr, output
    else:
        assert "AsterORM transactional harness:" not in child.stderr, output
    scenarios += 1
    print("Passed:", tag, fault or "normal", "exit", status)
    return child, counts


child, _ = run_case("[flow]", factories=1, constructed=1, prepares=1, connects=1, begins=4, savepoints=4,
                    rollback_to=4, releases=4, rollbacks=4, bodies=4, unwinds=4, validations=4)
events = re.findall(r"^event=(\w+)$", child.stdout, re.MULTILINE)
assert events == ["begin", "body_unwind", "rollback_to", "rollback"] * 4, events
for tag, status in [("[fatal]", 42), ("[skip]", 4), ("[exception]", 42)]:
    run_case(tag, status, prepares=1, begins=1, rollback_to=1, releases=1, rollbacks=1,
             bodies=1, unwinds=1)
run_case("[noaccess]", prepares=1, begins=1, rollbacks=1)
run_case("[untagged]", constructed=0, prepares=0, connects=0, begins=0)
for option in ("--list-tests", "--list-tags", "--list-listeners"):
    run_case("[flow]", arguments=(option,), constructed=0, prepares=0, connects=0, begins=0)
run_case("[does-not-exist]", 2, constructed=0, prepares=0, connects=0, begins=0)
run_case("[fatal],[flow]", 42, arguments=("--abort",), prepares=1, begins=1, rollbacks=1,
         bodies=1, unwinds=1)
for tag in ("[unknown]", "[multiple]", "[empty]", "[malformed]"):
    run_case(tag, 1, constructed=0, prepares=0, connects=0, begins=0)
run_case("[unsupported]", 1, factories=0, constructed=0, prepares=0, connects=0, begins=0)
run_case("[mismatch]", 1, prepares=1, begins=1, rollbacks=1)
run_case("[outside]", 1, constructed=0, begins=0)
run_case("[thread]", prepares=1, begins=1, rollbacks=1)
run_case("[casefold]", prepares=1, begins=1, rollbacks=1)
run_case("[late]", 1, prepares=1, begins=1, rollbacks=1)
for fault in ("duplicate_name", "duplicate_type", "invalid_name"):
    run_case("[untagged]", 1, fault, constructed=0, prepares=0, begins=0)
for fault in ("factory_throw", "factory_nonstd"):
    run_case("[repeat]", 1, fault, factories=1, constructed=0, prepares=0, begins=0, bodies=2)
for fault in ("prepare_error", "prepare_throw"):
    child, _ = run_case("[repeat]", 1, fault, constructed=1, prepares=1, connects=0,
                        begins=0, bodies=2)
    assert ("XX901" if fault == "prepare_error" else "injected preparation exception") in child.stderr
child, _ = run_case("[repeat]", 1, "entry_error", prepares=1, connects=1, begins=1,
                    savepoints=1, closes=1, rollbacks=1, bodies=2)
assert "XX901" in child.stderr and "original diagnostic retained" in child.stderr
for fault in ("cleanup_error", "cleanup_throw", "cleanup_alloc"):
    child, counts = run_case("[repeat]", 1, fault, prepares=1, connects=1, begins=1,
                             rollback_to=1, closes=1, bodies=2)
    assert counts["rollbacks"] == (1 if fault == "cleanup_error" else 0)
    diagnostic = {"cleanup_error": "XX901", "cleanup_throw": "injected cleanup exception",
                  "cleanup_alloc": "bad_alloc"}[fault]
    assert diagnostic in child.stderr
run_case("[untagged]", 1, "run_twice", constructed=0, prepares=0, begins=0, bodies=1)
run_case("[fatal]", 42, "cleanup_error", prepares=1, begins=1, closes=1, rollbacks=1)
run_case("[skip]", 4, "cleanup_error", prepares=1, begins=1, closes=1, rollbacks=1)
run_case("[flow]", 42, "prepare_error", prepares=1, begins=0, connects=0)
for fault in ("validate_error", "validate_throw", "validate_alloc"):
    child, _ = run_case("[repeat]", 1, fault, prepares=1, validations=1, connects=1, begins=1,
                        rollback_to=1, releases=1, rollbacks=1, bodies=2)
    diagnostic = {"validate_error": "XX901", "validate_throw": "injected validation exception",
                  "validate_alloc": "bad_alloc"}[fault]
    assert diagnostic in child.stderr
run_case("[flow]", 42, "validate_error", prepares=1, validations=1, begins=1, connects=1)
run_case("[noaccess],[healthy]", 1, "cleanup_error", constructed=2, prepares=2, connects=2,
         begins=2, rollback_to=2, rollbacks=2, bodies=2, unwinds=2)
run_case("[noaccess],[untagged]", 1, "cleanup_error", constructed=1, prepares=1, connects=1,
         begins=1, bodies=2, unwinds=2)
with tempfile.TemporaryDirectory(prefix="asterorm-catch2-report-") as directory:
    report = Path(directory) / "result.xml"
    run_case("[noaccess]", 1, "cleanup_error", arguments=("--reporter", "junit", "--out", str(report)),
             prepares=1, begins=1, closes=1)
    suites = ET.parse(report).getroot()
    assert sum(int(suite.get("failures", "0")) for suite in suites.iter("testsuite")) == 0
    assert sum(int(suite.get("tests", "0")) for suite in suites.iter("testsuite")) > 0
print(f"Passed all {scenarios} subprocess scenarios")
