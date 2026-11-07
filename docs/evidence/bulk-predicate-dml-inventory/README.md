# Bulk predicate DML inventory evidence

This is the starting-point probe for the
[implementation contract](../../bulk-predicate-dml-design.md), not acceptance of
the future repository methods.

`probe.cpp` checks three compiler dialects, confirms missing repository entry
points for the proposed arguments, and exercises current adapters through the
session's observed prepared execution path. Each backend uses one connection,
connection-local temporary source/archive tables, and a caller-owned transaction.
It checks no-change/zero-match counts, constraint errors, rollback, and native
INSERT-from-SELECT support. It deliberately observes the current empty-SET and
schema-independent compiler behavior without executing those invalid statements.

From the repository root, with PostgreSQL, SQLite, and MySQL enabled and built in
`buildDir`, and the existing test services running:

```sh
python3 docs/evidence/bulk-predicate-dml-inventory/capture.py --build-root buildDir
```

The Python runner takes compiler flags/library paths from Meson, builds a
temporary executable, and captures `output.txt`, `stderr.txt`, and `metadata.json`.
It needs a Unix compiler/linker supporting `-Wl,-rpath`. The snapshot records
compiler/platform and inspected source hashes. Connection strings are taken from
the existing test environment or Compose defaults and are not recorded. No
persistent tables are created or dropped.

Counts above `INT_MAX` are a source-review finding, not measured by this fixture.
The compile-time absence checks intentionally stop passing once those API forms
are implemented: freeze this capture as historical evidence and add normal API
tests then. Existing unit/integration suites remain the regression baseline.
