# Transactional test harness acceptance evidence

This directory supports the [combined local review](../../transactional-test-harness-acceptance.md).
It records the reviewed generic scope, session gates, optional Catch2 lifecycle,
reference providers and worked migration together. Earlier inventory and delivery
captures remain historical and are not rewritten.

- `metadata.json` records the Git base, compiler/platform/build configuration,
  SHA-256 source/test/build/document/context hashes and fresh command/log receipts.
- `scenarios.json` contains sanitized reference-process results and successful
  preparation/DDL/validation/connection counts. Current parents pass 10 PG and
  9 SQLite scenarios, including a changed baseline on a healthy connection.
- `check_core_sanitizer.py` instruments the generic scope unit TU with configured
  C++23 flags, links existing core/Catch2 libraries and runs its 19 ownership/failure
  cases. The separate worked-fixture sanitizer runner covers selected main/worked/
  loss/adapter TUs and their native/subprocess matrices. Other objects/libraries
  are not instrumented; neither is a full-project leak/thread-sanitizer claim.
- `verify_snapshot.py` checks the reviewed file hashes, optionally including
  ignored canonical contexts. It does not rerun tests or verify expired logs.

From the repository root, with normal local test services and development dependencies:

```sh
just test
meson compile -C buildDir
just format-check
just lint
buildDir/tests/core_tests '[transactional-testing]'
buildDir/tests/pg_integration '[transactional-testing]'
buildDir/tests/sqlite_integration '[transactional-testing]'
python3 tests/check_catch2_harness.py buildDir/tests/catch2_harness
python3 tests/check_reference_fixtures.py --backend pg --integration buildDir/tests/pg_integration --probe buildDir/tests/pg_reference_probe
python3 tests/check_reference_fixtures.py --backend sqlite --integration buildDir/tests/sqlite_integration --probe buildDir/tests/sqlite_reference_probe
python3 docs/evidence/transactional-test-harness-acceptance/check_core_sanitizer.py --build-root buildDir
python3 docs/evidence/worked-transactional-fixtures/check_sanitizer.py --build-root buildDir
python3 docs/evidence/transactional-test-scope/check_consumers.py
python3 docs/evidence/catch2-transactional-adapter/check_consumers.py --build-root buildDir
python3 docs/evidence/transactional-test-harness-acceptance/verify_snapshot.py --include-canonical-contexts
```

The fresh independent SQLite-only configuration disables PostgreSQL/MySQL/
ClickHouse, enables SQLite/tests and disables examples; all nine targets pass.
The main configured PG/SQLite/MySQL suite has 23 passing targets, including the
43-scenario Catch2 parent and native process checks. Intentional assertion/skip/
harness-only child statuses are observations checked by their parent, not failed
acceptance commands. Lint covers 65 distinct translation units and retains existing
diagnostics. Strict workspace standards still fails with 53 errors/12 warnings;
its finding lists match the same-date quiescent baseline. Nothing is waived.

Both consumer probes freshly build/install AsterORM under temporary prefixes with
tests disabled and PG/SQLite/MySQL enabled. The generic guide runs on PG/SQLite
without Catch2. The adapter guide uses separately installed Catch2 and runs four
SQLite paths. Both CMake/shared pkg-config consumers pass. These scripts honor
their documented discovery/test environment, add existing Homebrew paths, and
do not install system-wide. Static AsterORM linking is not tested; the existing
CMake SQLite imported-target warning remains.

The new baseline-corruption case changes a reference row between invocations
without closing its connection or removing TEMP identity. The next invocation's
pinned validation rejects typed access, retains the failure, and leaves preparation
and native setup counts at one. Close, missing identity and PG termination cases
remain in the matrix. The simultaneous process checks use identical TEMP names;
fixture counters exclude the independent PG killer connection.

Normal top-level CREATE dispatches are 7 schema + 1 identity for PG's seven paths,
and 1 schema + 1 identity for SQLite's four paths. The legacy repeated DDL counts
come from the recorded pre-migration Git source, not a fresh old-executable run.
There is no elapsed-time or speedup result.

Metadata excludes build outputs, credentials and the unrelated `.gitignore` edit.
Full logs remain under `/tmp` and can expire. Canonical `.agents/` contexts remain
ignored and are never force-added; omit `--include-canonical-contexts` in checkouts
without them. This unsigned snapshot is a drift check, not independent execution
attestation or release provenance. Local acceptance does not establish remote CI,
all platforms/framework versions or release readiness.
