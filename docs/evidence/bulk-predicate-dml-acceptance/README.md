# Bulk predicate DML acceptance evidence

This directory supports the [local acceptance review](../../bulk-predicate-dml-acceptance.md).
It is separate from the frozen pre-implementation inventory capture.

- `metadata.json` records the Git base, SHA-256 hashes of reviewed source/tests and
  documents, compiler/platform, selected build options, and fresh command results.
  Receipt hashes bind local logs retained under `/tmp`; full logs and connection
  settings are not published. This is unsigned local evidence, not release provenance.
- `check_consumers.py` configures a fresh Meson build with PostgreSQL/SQLite/MySQL
  enabled and ClickHouse disabled, installs into a temporary prefix, then builds
  and runs CMake and shared pkg-config consumers. Both execute literal and nested
  arithmetic UPDATE, INSERT-from-SELECT and predicate DELETE against SQLite
  `:memory:`. The copied price is checked through the DELETE result. The probe
  also checks the installed SQLite 3.37+ private pkg-config requirement. Temporary
  files remain at the printed path for inspection; no system installation occurs.

From the repository root, with Meson, Ninja, a C++23 compiler, CMake, pkg-config and
the three relational client development packages available:

```sh
python3 docs/evidence/bulk-predicate-dml-acceptance/check_consumers.py
```

The script honors `PKG_CONFIG_PATH`/`CXX` and adds existing `/opt/homebrew/opt`
dependency paths on the reviewed macOS host. Other layouts may require the normal
dependency-discovery environment. The existing CMake SQLite imported-target
deprecation warning remains; static linking is not tested.

For runtime verification, use the existing test environment/services and run
`just test-pg`, `just test-sqlite`, and `just test`, followed by the normal build,
format and lint checks. `bulk mutation guide` in the full suite uses
[the guide checker](../../../tests/check_bulk_predicate_guide.py) to compile all
three public C++ snippets. Unsupported capabilities are exercised by no-server
unit cases; this review does not connect to ClickHouse.

The metadata hashes exclude `.gitignore`, local connection settings and build
outputs. Ignored canonical context hashes are recorded separately; those files
are maintained locally under `.agents/` and are not force-added to Git. Compare
the reviewed hashes before reusing this result and repeat relevant checks after
material source or contract changes. Earlier evidence snapshots are historical.
