# Generic transactional test scope verification

This records local verification of the generic scope implemented after the
[design and native inventory](../../transactional-test-harness-design.md).
The optional Catch2 adapter/runner, process-owned reference fixtures, worked CRUD
migration and combined harness acceptance remain planned.

The [usage guide](../../how-to/transactional-test-scopes.md) describes the public
API and isolation limits. Verified behavior includes owned root/private-savepoint
cleanup, observer restoration, session admission gates, stale-generation safety,
deferred discard during callbacks and persistent failure diagnostics. Tests inject
SQL errors, native C++ exceptions and pool-return allocation failure. Native cases
cover PostgreSQL backend termination and SQLite automatic transaction rollback.
Ordinary guard abandonment is also hardened for connections with nonthrowing close.

| Check | Local result |
| --- | --- |
| Full PostgreSQL/SQLite/MySQL suite | 20 targets passed |
| Separate SQLite-only suite | 7 targets passed |
| Focused generic cases | 19 cases, 304 assertions passed |
| Focused PostgreSQL cases | 3 cases, 79 assertions passed |
| Focused SQLite cases | 2 cases, 71 assertions passed |
| Build and format | Passed |
| clang-tidy | 58 translation units; exit 0, existing diagnostics retained |
| Focused AddressSanitizer | 19 cases, 304 assertions passed |
| Fresh installed CMake/shared pkg-config consumers | Guide passed on PostgreSQL and SQLite |
| Strict workspace standards | Exit 1: existing 53 errors and 12 warnings, findings unchanged |

The sanitizer check instruments the generic scope unit translation unit and links
the existing core/Catch2 binaries. It is not a full-project sanitizer build or a
cross-platform acceptance claim. The standards result is not waived by these
checks. No harness speedup or release readiness is established here.

With the normal local test services and development dependencies available:

```sh
just test
meson compile -C buildDir
just format-check
just lint
buildDir/tests/core_tests '[transactional-testing]'
buildDir/tests/pg_integration '[transactional-testing]'
buildDir/tests/sqlite_integration '[transactional-testing]'
python3 docs/evidence/transactional-test-scope/check_consumers.py
```

`check_consumers.py` freshly builds/installs PostgreSQL, SQLite and MySQL under a
temporary prefix with tests and examples disabled, then compiles and runs the
literal guide using CMake and shared pkg-config. Each path verifies that only the
committed reference row remains after finish. The helper has no Catch2 dependency;
no system installation occurs. The script honors `ASTERORM_TEST_CONNINFO`, `CXX`
and `PKG_CONFIG_PATH`, uses the suite's local PostgreSQL defaults otherwise, and
adds existing Homebrew dependency paths. Other layouts may need their normal
discovery environment. Static linking is not tested; the existing CMake SQLite
imported-target warning is retained.

`verification.json` binds the reviewed sources, tests, build definitions and docs
to SHA-256 hashes and records local command/log receipts. Canonical context hashes
are separate because `.agents/` is ignored and maintained locally. Build outputs,
credentials and the unrelated `.gitignore` edit are excluded. Logs under `/tmp`
are local and may expire. This unsigned record is not independent execution
attestation; compare hashes before reusing it. Previous inventory and acceptance
captures remain historical and are not refreshed by this work.
