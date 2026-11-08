"""Verify pool ownership snapshot bytes; optionally check still-available local logs."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--logs', action='store_true')
    arguments = parser.parse_args()
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    receipt = json.loads((directory / 'verification.json').read_text())
    expected = {root / path: digest
                for group in ('source_build_test_document_sha256',
                              'ignored_canonical_context_sha256')
                for path, digest in receipt[group].items()}
    if arguments.logs:
        expected.update({Path(check['local_log']): check['log_sha256']
                         for check in receipt['verification']})
    failures = []
    for path, digest in expected.items():
        if not path.is_file():
            failures.append('missing: ' + str(path))
        elif hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            failures.append('changed: ' + str(path))
    for failure in failures:
        print(failure)
    print(f'Checked {len(expected)} snapshot files; {len(failures)} mismatches')
    return int(bool(failures))


if __name__ == '__main__':
    raise SystemExit(main())
