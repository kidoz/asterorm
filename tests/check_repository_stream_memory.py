"""Run the fixed-width repository RSS fixture in a fresh process."""
import re
import subprocess
import sys

if sys.platform not in ("darwin", "linux"):
    raise SystemExit(77)

fixture_filter = sys.argv[2] if len(sys.argv) > 2 else "[.repository-stream-memory]"
checked = subprocess.run([sys.argv[1], fixture_filter],
                         capture_output=True, text=True, timeout=55)
print(checked.stdout, end="")
print(checked.stderr, end="", file=sys.stderr)
if checked.returncode:
    raise SystemExit(checked.returncode)
if not re.search(r"repository_stream_rss_growth_bytes=\d+", checked.stdout):
    raise RuntimeError("The fresh process did not report the repository RSS measurement")
