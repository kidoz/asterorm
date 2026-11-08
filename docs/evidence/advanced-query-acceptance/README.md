# Advanced-query acceptance evidence

Reviewed locally on 2026-11-08. The [acceptance review](../../advanced-query-acceptance.md)
maps the revised v1 contract to production code, fresh regression tests and new
cross-feature cases. [composition.hpp](composition.hpp) is a portable acceptance
fixture rather than an installed library header. Native/core tests include it;
the consumer probe copies it into a temporary external project.

The new query combines ordinary CTEs, set seeds, constrained recursion, typed or
runtime windows, outer computed predicates, duplicate/NULL rows and branch/final
pagination. PG/SQLite add FILTER, and PG adds explicit DISTINCT ON. Native tests
also check one event per statement, lease state, rollback/testing scopes, empty
metadata, native errors and throwing-observer recovery. Core tests cover typed/
runtime SQL/bind/label equality, compiler reuse, invalid later CTEs/branches and
query-wide budgets before acquisition.

| Fresh check | Result |
| --- | --- |
| Configured regression | 28 targets passed, zero failures |
| Fresh SQLite-only ASan/UBSan | 14 targets passed, zero failures |
| Focused core composition | 60 assertions across two cases |
| Native composition | PG: 155 assertions; SQLite: 136; MySQL: 88, one case each |
| Typed compile controls | 34 cases, zero failures |
| Installed consumers | Six programs through each of CMake/shared pkg-config; all pass |
| Formatting/project lint | Exit 0; lint covers 72 translation units, with retained diagnostics |
| Standards comparison | Same 53 errors/12 warnings; full finding lists identical |

Run from the repository root with the normal PostgreSQL/MySQL test services:

```sh
meson compile -C buildDir
meson test -C buildDir --print-errorlogs
buildDir/tests/core_tests '[advanced-query]'
buildDir/tests/pg_integration '[advanced-query]'
buildDir/tests/sqlite_integration '[advanced-query]'
buildDir/tests/mysql_integration '[advanced-query]'
python3 tests/compile/check_typed_dsl.py --build-root buildDir
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy
python3 docs/evidence/advanced-query-acceptance/check_consumers.py
```

`check_consumers.py` makes one fresh tests/examples-disabled install. It extracts
the current single complete C++ block from each of the five public advanced-query
guides and reuses their existing expected-result mains. Six independent consumers
(five guides plus composition) build and run through both CMake and shared
pkg-config on PG/SQLite/MySQL. Earlier consumer scripts/receipts stay unchanged;
the new receipt hashes the inputs actually used by this run. CMake's existing
SQLite target deprecation notices remain dependency diagnostics.

A separate fresh SQLite-only build uses `-Dpostgres=disabled -Dsqlite=enabled
-Dmysql=disabled -Dclickhouse=disabled -Dexamples=false
-Db_sanitize=address,undefined` and runs all 14 targets. No pre-existing build is
reused for sanitizer coverage.

Project lint retains existing style/negative-enum/Catch2 diagnostics. The new
typed composition also makes the analyzer report an uninitialized `cols` reference
in `typed.hpp::column_name_for`. The referenced traits tuple is a `static constexpr`
initialized mapping; compile controls, native typed/runtime equality and sanitizers
exercise it successfully. This analyzer report is retained, not suppressed or
treated as proof of an uninitialized read. The receipt records the successful
full lint run; a preliminary targeted run read a header during formatting and
failed parsing, so it is not a completion claim.

Standards validation uses the same date and quiescent workspace as its baseline:
53 errors, 12 warnings and 0 infos, with identical finding lists. These failures
remain separate from scoped functional acceptance.

`verification.json` records source/test/build/document hashes, ignored canonical
context hashes, command exit codes and local log hashes. Temporary logs may expire.
This is unsigned local evidence, without remote CI/release, older-server,
ClickHouse, static-linking or performance claims. Credentials/build outputs and
the unrelated `.gitignore` edit are excluded. Ignored contexts are not force-added.
No production API/layout change is introduced by this review; prior rebuild
requirements remain applicable. Next inventory the production connection pool.

Verify the recorded snapshot with
`python3 docs/evidence/advanced-query-acceptance/verify_snapshot.py`; add `--logs` while the
temporary local logs still exist. This checks file bytes rather than rerunning or
attesting the commands.
