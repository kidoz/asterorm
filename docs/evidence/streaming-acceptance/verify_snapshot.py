"""Verify the reviewed file hashes; this does not rerun acceptance checks."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-canonical-contexts", action="store_true")
    arguments = parser.parse_args()
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    metadata = json.loads((directory / "metadata.json").read_text())
    groups = ["source_sha256", "document_sha256", "evidence_sha256"]
    if arguments.include_canonical_contexts:
        groups.append("canonical_context_sha256")
    checked = 0
    mismatches = []
    for group in groups:
        for relative, expected in metadata[group].items():
            path = root / relative
            actual = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
            checked += 1
            if actual != expected:
                mismatches.append(relative)
    if mismatches:
        raise SystemExit("Reviewed snapshot differs: " + ", ".join(mismatches))
    print(f"Reviewed snapshot hashes match: {checked} files")


if __name__ == "__main__":
    main()
