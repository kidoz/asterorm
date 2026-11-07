"""Run repository RSS scenarios in fresh processes."""
import re
import argparse
import json
from pathlib import Path
import subprocess
import sys

if sys.platform not in ("darwin", "linux"):
    raise SystemExit(77)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("executable")
parser.add_argument("fixture_filter", nargs="?", default="[.repository-stream-memory]")
parser.add_argument("--controls", action="store_true")
parser.add_argument("--matrix", action="store_true")
parser.add_argument("--json-out")
arguments = parser.parse_args()
filters = [arguments.fixture_filter]
if arguments.controls or arguments.matrix:
    filters = ["[.repository-buffered-memory]", "[.repository-single-large-memory]",
               "[.repository-cursor-large-memory]"]
if arguments.matrix:
    filters = ["[.repository-stream-memory]", "[.repository-cursor-memory]", *filters]
measurements = []
for fixture_filter in filters:
    checked = subprocess.run([arguments.executable, fixture_filter],
                             capture_output=True, text=True, timeout=55)
    print(checked.stdout, end="")
    print(checked.stderr, end="", file=sys.stderr)
    if checked.returncode:
        raise SystemExit(checked.returncode)
    records = re.findall(r"^repository_stream_memory=(.+)$", checked.stdout, flags=re.MULTILINE)
    if len(records) != 1:
        raise RuntimeError("The fresh process must report exactly one RSS measurement")
    measurements.append(json.loads(records[0]))
if arguments.json_out:
    Path(arguments.json_out).write_text(json.dumps(measurements, indent=2) + "\n")
