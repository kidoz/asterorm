# Worked transactional reference fixtures

This records the 2026-11-08 delivery of the
[worked fixture guide](../../how-to/worked-reference-fixtures.md): checked
PostgreSQL/SQLite reference providers, the seven-path main PostgreSQL CRUD
migration, four SQLite generator/section paths, and pinned validation in the
optional Catch2 adapter. Earlier inventory, generic scope and adapter captures
remain historical and are not refreshed here.

| Check | Local result |
| --- | --- |
| Main PostgreSQL CRUD case | 118 assertions; 7 body/validation paths |
| SQLite worked reference case | 34 assertions; 4 body/validation paths |
| Reference process checks | 9 PostgreSQL + 8 SQLite scenarios passed |
| Adapter subprocess checks | 43 scenarios passed |
| Full PostgreSQL/SQLite/MySQL suite | 23 targets passed |
| Separate SQLite-only suite | 9 targets passed |
| Build and format | Passed |
| clang-tidy | 65 distinct translation units; exit 0, existing diagnostics retained |
| Focused AddressSanitizer | Worked paths, loss probes and adapter matrix passed |
| Fresh installed guide consumers | CMake/shared pkg-config passed with separate Catch2 |
| Strict workspace standards | Existing 53 errors, 12 warnings; findings unchanged from a quiescent same-date baseline |

Normal PostgreSQL dispatch counts are one preparation, seven schema CREATEs,
one identity CREATE, seven validations/bodies and one native fixture connection.
SQLite has one preparation, one schema CREATE, one identity CREATE, four
validations/bodies and one native fixture connection. These counters cover successful
native runs. Connection counters measure fixture driver calls; the independent
PostgreSQL killer connection is outside those counters. Failed first attempts
would still consume the single permitted native attempt.

The reference parent checks run identical table names in two simultaneous
processes, verify lazy listing/untagged behavior with invalid PostgreSQL settings,
then damage idle fixtures between tagged invocations. A closed connection cannot
be replaced, missing TEMP identity fails validation, and a killed PostgreSQL backend
fails scope entry/validation. The body accessor rejects, preparation/native setup
are never replayed, and the parent requires exit 1 plus the harness diagnostic.
The deliberately failing probes are separate from ordinary integration executables.

Validation runs on the root/private-savepoint connection before publication of
typed access. Its structured errors and standard/allocation exceptions retain the
primary diagnostic, poison the provider and clean the opened scope. Four additional
adapter subprocess scenarios exercise that hook. Existing fixtures without a hook
retain their behavior. Normal library dependencies, layout and pool reconnection
policy are unchanged.

To reproduce with the normal local development dependencies/test services:

```sh
just test
meson compile -C buildDir
just format-check
just lint
python3 tests/check_reference_fixtures.py --backend pg --integration buildDir/tests/pg_integration --probe buildDir/tests/pg_reference_probe
python3 tests/check_reference_fixtures.py --backend sqlite --integration buildDir/tests/sqlite_integration --probe buildDir/tests/sqlite_reference_probe
python3 tests/check_catch2_harness.py buildDir/tests/catch2_harness
python3 docs/evidence/worked-transactional-fixtures/check_sanitizer.py --build-root buildDir
python3 docs/evidence/catch2-transactional-adapter/check_consumers.py --build-root buildDir
```

`check_sanitizer.py` instruments selected main/worked-case/probe translation units
for both backends plus the adapter's two translation units, links the other
existing objects/libraries, then runs the native and adapter matrices. It does not
constitute a full-project sanitizer, leak/thread sanitizer or cross-platform result.
The consumer probe freshly installs AsterORM with PostgreSQL/SQLite/MySQL enabled
and tests disabled, separately builds/installs Catch2, then runs the complete public
guide with its validation hook using CMake/shared pkg-config. No system installation
occurs; static AsterORM linking is not tested. The existing CMake SQLite
imported-target warning remains.

`scenarios.json` records sanitized process receipts/counts. `verification.json`
binds selected source/test/build/docs and ignored canonical contexts to SHA-256,
along with local command/log receipts. Logs under `/tmp` can expire; no connection
settings or credentials are published. The unrelated `.gitignore` edit and build
outputs are excluded. This is unsigned local evidence, not independent attestation,
remote CI, release provenance or a waiver of standards findings.

The old PostgreSQL case's repeated DDL is a source comparison rather than a fresh
old-executable measurement. No timing/speedup or complete suite migration is
claimed. The separate shared bulk-DML case and transaction/migration/locking/
streaming/multi-session cases stay untagged. Combined harness acceptance is next.
