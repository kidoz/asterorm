# Locking and timeout acceptance evidence

This directory supports the [local acceptance review](../../locking-timeout-acceptance.md).
It does not replace the earlier native semantic snapshots.

- `check_guides.py` syntax-checks the first C++ examples in the SELECT locking,
  advisory locking, timeout scope, pooling, and migration guides. It instantiates
  them with a PostgreSQL session and a mapped `Job`, using the configured compiler
  and flags from the Meson compilation database. It also checks ClickHouse's
  unsupported dialect/capabilities and scoped-call fallback through its headers.
  It executes no SQL.
- `metadata.json` records the reviewed source/test hashes, selected build options,
  compiler/platform, redacted environment description, and fresh check summaries.
  Receipt hashes bind the local logs; full logs remain in the build/tmp directories.
  This is an unsigned local review record, not release provenance.

From the repository root with PostgreSQL-enabled `buildDir` configured and built:

```sh
python3 docs/evidence/locking-timeout-acceptance/check_guides.py --build-root buildDir
```

For current runtime verification, start the enabled database services and run
`just test-pg`, `just test-sqlite`, and `just test`, then the normal build, format,
and lint checks. The process-based migration and no-server PostgreSQL adapter tests are included
in `just test-pg` and the full suite. Connection settings use the existing test environment or
Compose defaults; connection strings and credentials are not captured.

The reviewed snapshot includes staged/working-tree changes beyond the recorded
Git base. Compare source hashes before reusing the acceptance result. Re-run the
checks and create a new review record after material source or contract changes.
