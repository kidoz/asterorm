# Combined production-pool evidence

Reviewed locally on 2026-11-09. The [combined review](../../production-pool-acceptance.md)
closes scoped local acceptance of the revised v1 contract. No production code
change was needed. New composed tests and a fresh four-guide installed-consumer
probe supplement the earlier delivery checks; older captures stay frozen.

Fresh verification covers:

- All **35 configured regression targets** pass. The focused core pool run passes
  **1,142 assertions in 52 cases**, including **76 assertions** in a new generated
  combined health/age/profile/shutdown case. It verifies reserved capacity,
  metadata-only maintenance and native failure/exception versus close/age ordering.
- Focused native PG pool checks pass **447 assertions in 15 cases**; native PG
  no-server policy/advisory/timeout checks pass **78 assertions in three cases**.
  SQLite and MySQL each pass **209 assertions in five pool cases**. The new
  cross-feature checks add **50 assertions per adapter**, plus **60 PG assertions**
  across the two managed repository stream modes. Overdue ownership, close,
  subsequent commit, dirty discard, idle retirement, observer silence and
  slot/histogram/outcome conservation are checked together.
- The allocation-denial executable passes, including late lease return, allocation-
  free overdue warning delivery and preserved queued records after polling failure.
  The health checker passes **14 real-libpq scenarios**; the cache checker passes
  **three unnamed packet scenarios and one named-Parse negative control**.
- The opt-in pinned/configuration-checked PgBouncer **1.24.1** fixture passes
  **208 assertions in two cases**. It retains transaction mode, two server slots,
  round-robin reuse and max_prepared_statements=0. Alongside six rounds of supported
  bound CRUD/local-timeout/cursor work, a new forced backend switch keeps the
  original client connection count at one. Warning delivery and close with a live
  transaction preserve usable ownership and release capacity on commit.
- A fresh SQLite-only address/undefined sanitizer build passes **19 targets**.
  A fresh PG-only sanitizer build passes the focused **447 native / 78 no-server
  / 208 proxy assertions** and both packet matrices. A fresh adapter-disabled
  ThreadSanitizer build passes **921 assertions in 35 cases**, with no reported
  races. Native SDK internals are not instrumented; native MySQL sanitizer
  execution is not claimed.
- A fresh tests/examples-disabled install compiles and executes all four literal
  public guide programs and composed health/off-cache/aging/warning/close/metrics
  checks through both CMake and shared pkg-config on PG/SQLite/MySQL. Off/profile
  and unsupported-backend outcomes are checked. No Catch2 dependency is required;
  pkg-config flags are explicitly checked for it. Required thread linkage is
  consumed through the public package targets.
- Formatting, full project lint (**83 translation units**) and strict production
  pool-header lint pass. Existing project/Catch2 and SDK diagnostics remain in the
  full log; the strict pool-header run treats emitted warnings as errors.
- Strict standards findings match the **same-date pre-change baseline** byte for
  byte: **53 errors, 12 warnings, 0 infos**. No waiver is introduced. This is
  functional local acceptance, not release readiness.

The host compiler reports Apple Clang 21.0.0. Recorded server queries report
PostgreSQL 16.14 and MySQL 8.4.13. A freshly installed AsterORM SQLite adapter
reports runtime 3.54.1; the local pkg-config/build metadata reports 3.54.0. Both
observations are retained rather than equating metadata with the loaded runtime.
The SQLite CLI independently reports 3.54.1. This version discrepancy is an
explicit local environment limit, not a changed adapter requirement.

With the normal PG/MySQL test services:

```sh
meson compile -C buildDir -j4
meson test -C buildDir --no-rebuild --print-errorlogs
buildDir/tests/core_tests '[pool],[pool-timeout],[pool-foundation],[pool-health],[pool-aging],[pool-policy],[pool-acceptance]'
buildDir/tests/pg_integration '[pool],[pool-foundation],[pool-health],[pool-timeout],[pool-aging],[pool-policy],[pool-acceptance]~[pgbouncer]'
buildDir/tests/postgres_unit_tests '[pool-policy],[pg-timeout-unit],[pg-advisory-unit]'
buildDir/tests/sqlite_integration '[pool-foundation],[pool-health],[pool-aging],[pool-policy],[pool-acceptance]'
buildDir/tests/mysql_integration '[pool-foundation],[pool-health],[pool-aging],[pool-policy],[pool-acceptance]'
buildDir/tests/pool_allocation_probe
python3 tests/check_pool_health_wire.py buildDir/tests/pool_health_wire
python3 tests/check_pool_policy_wire.py buildDir/tests/pool_policy_wire
just test-pgbouncer
python3 docs/evidence/pool-acceptance/check_consumers.py
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy
python3 docs/evidence/pool-acceptance/verify_snapshot.py --logs
```

The fresh SQLite sanitizer setup enables only SQLite and uses
`-Db_sanitize=address,undefined`; its full configured suite runs. The PG sanitizer
setup enables only PG and runs the focused groups above, both wire probes and the
proxy runner. The race setup disables every adapter and uses `-Db_sanitize=thread`,
builds core_tests and runs foundation/health/aging/policy/acceptance tags. These
are separate fresh build directories; no earlier acceptance build is reused.

The proxy was stopped/removed after verification; existing PostgreSQL/MySQL
services were retained. The [receipt](verification.json) binds current source,
build/test/docs, commands and temporary logs, plus ignored canonical contexts.
[Snapshot verification](verify_snapshot.py) checks those bytes; it does not
independently attest execution. Logs and temporary metadata probes may expire.

This unsigned local record does not establish other host platforms, older servers,
other proxy versions/configurations, arbitrary session SQL, ClickHouse execution,
static package linking, remote CI/release provenance, performance or hard native
cleanup/scheduling deadlines. Per-session caching remains deferred. Rebuild for
previous pool/config/stats/entry/PG changes, and retain the documented session/
lease/stream lifetime rules. The unrelated .gitignore edit stays unstaged; ignored
contexts are not force-added. Next prepare PostgreSQL type coverage inventory and
its v1 contract.
