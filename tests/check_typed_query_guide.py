#!/usr/bin/env python3
"""Keep the guide's C++ snippets identical to its compiled, exercised example."""

from pathlib import Path
import re
import textwrap


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "examples/typed_query_migration.cpp").read_text()
    guide = (root / "docs/typed-query-migration.md").read_text()
    regions = re.findall(
        r"^[ \t]*// guide:begin (\w+)\n(.*?)^[ \t]*// guide:end \1$",
        source,
        re.MULTILINE | re.DOTALL,
    )
    snippets = re.findall(
        r"<!-- guide:(\w+) -->\n```cpp\n(.*?)\n```", guide, re.DOTALL
    )
    if not regions or len(dict(regions)) != len(regions):
        raise SystemExit("Missing or duplicate source regions")
    if len(dict(snippets)) != len(snippets):
        raise SystemExit("Duplicate guide snippets")
    if dict(snippets).keys() != dict(regions).keys():
        raise SystemExit("Guide/source snippet identifiers differ")
    for name, code in regions:
        if textwrap.dedent(code).strip() != dict(snippets)[name].strip():
            raise SystemExit(f"Guide snippet '{name}' differs from the executable example")
    print(f"Verified {len(regions)} guide snippets against the executable example")


if __name__ == "__main__":
    main()
