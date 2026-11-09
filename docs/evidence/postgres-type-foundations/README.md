# PostgreSQL value/text-codec foundations: local verification

Reviewed 2026-11-09 against PostgreSQL 16.14. This delivery implements
`interval`, `network` (inet), distinct `cidr`, and six-byte `macaddr`, strict
numeric/date/timestamp prerequisites, atomic flat arrays, and explicit nested/
dimensioned-array rejection. See the [guide](../../how-to/postgresql-value-codecs.md)
and [contract](../../postgresql-type-design.md). Ranges, hstore/composites, checked
JSONB nodes and combined type acceptance remain planned.

## Observable checks

- Five no-server cases pass 480 assertions: four interval styles, mixed signs,
  component limits/overflow, NULL and unchanged output, numeric special values,
  invalid calendars/offsets, temporal bounds, IPv4/IPv6/mapped families, CIDR host
  bits, MAC, array quoting/malformed/nesting.
- Three PostgreSQL cases pass 69 assertions: nine interval values in all four
  styles (including INT32/INT64 limits), calendar month/day differences, native
  scalar canonicalization, exact numeric/special values, JSONB/nullable interval
  arrays, offset seconds, and repository writes/read/NULL/rejection.
  GUC changes use SET LOCAL on an owned connection; the only table is TEMP inside
  a rolled-back transaction. No caller schema, extension or persistent GUC changes.
- `meson test -C buildDir --no-rebuild --print-errorlogs`: 35 targets passed,
  zero failures, with PostgreSQL, SQLite and MySQL enabled. This includes existing
  custom/enum codecs, both native streaming paths and three-backend regressions.
- AddressSanitizer + UndefinedBehaviorSanitizer: the same five unit and three PG
  cases pass in a separately instrumented Meson core/adapter/test build. Leak
  detection is disabled; system/vendor libraries and the database server are not
  instrumented. This is scoped sanitizer coverage, not a whole-suite sanitizer run.
- Strict clang-tidy passes the new compiled parser. Strict checks also pass both
  new test translation units with only `clang-analyzer-optin.core.EnumCastOutOfRange`
  disabled there to avoid the existing Catch2 Flags diagnostic. The ordinary full
  project lint covers 86 registered translation units; retained project/vendor/SDK
  diagnostics mean this is not a warning-free project claim. Formatting passes.
- [run_consumers.py](run_consumers.py) builds/installs a fresh tests-disabled
  PG/SQLite library in a temporary directory. [consumer.cpp](consumer.cpp) compiles
  and runs through CMake `AsterORM::core`, CMake `AsterORM::asterorm`, and shared
  pkg-config linkage. These exercise new/old compiled codecs, arrays, NULL/error
  distinction and custom codecs using installed headers. Homebrew's libpq
  pkg-config path is supplied when applicable. No source include/library fallback.
- Same-date standards findings match the pre-context-edit baseline: 53 errors,
  12 warnings, zero infos. Strict governance remains nonzero for those existing
  findings; this delivery adds none.

## Reproduce

```sh
just db-up
meson compile -C buildDir -j4
buildDir/tests/core_tests '[type-foundations]'
buildDir/tests/pg_integration '[type-foundations]'
meson test -C buildDir --no-rebuild --print-errorlogs
python3 docs/evidence/postgres-type-foundations/run_consumers.py
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy

meson setup buildTypeSanitized -Dtests=true -Dexamples=false \
  -Dpostgres=enabled -Dsqlite=enabled -Dmysql=disabled \
  -Db_sanitize=address,undefined -Db_lundef=false
meson compile -C buildTypeSanitized -j4 core_tests pg_integration
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 \
  buildTypeSanitized/tests/core_tests '[type-foundations]'
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 \
  buildTypeSanitized/tests/pg_integration '[type-foundations]'
```

Native tests use `ASTERORM_TEST_CONNINFO` or the existing local test default.
The broad suite also needs the configured MySQL service. Reconfigure existing
Meson directories rather than recreating them. The receipt records strict lint,
standards and other commands/log hashes for this run.

## Provenance and limits

[verification.json](verification.json) binds source/build/test/docs and ignored
canonical contexts to commands, exit codes and local log hashes. Run
`python3 docs/evidence/postgres-type-foundations/verify_snapshot.py --logs` while
those temporary logs remain. Future source/context changes intentionally invalidate
this snapshot. The [inventory capture](../postgres-type-inventory/README.md)
remains historical; its missing-API compilations are not future acceptance tests.

Compiled parsing helpers require rebuilding core and consumers together, including
existing temporal/numeric/array users. Existing value layouts are unchanged. No
binary protocol, OID registry, runtime schema discovery, extension installation,
PG type support on other engines, range/JSONB AST support, index/performance claim,
remote CI, cross-platform or release-readiness acceptance is added here.
