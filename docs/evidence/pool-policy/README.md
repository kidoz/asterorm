# Pool cache and transaction-profile evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/pool-cache-and-profiles.md)
and [design follow-up](../../production-pool-design.md#cache-and-profile-implementation-follow-up-2026-11-08)
cover implemented explicit off/per-connection caching and a restricted PostgreSQL
transaction-pool profile. Combined production-pool acceptance remains next;
per-session caching stays deferred.

Off configures zero retained cache capacity on PG/SQLite/MySQL. Zero capacity
also remains effective with the default per-connection policy. PG bound calls use
unnamed parameter execution; SQLite/MySQL may prepare ephemeral native statements
for binding. Unknown enum values and profile conflicts use connection_failed;
unsupported explicit capabilities use query_failed. All reject before warmup or
lazy connect. The transaction profile requires effective cache off and absent
pool session timeout. Pool metadata gates advisory scopes and serialized
migration operations before lease, callback, observation or SQL. Native PG session
advisory/timeouts also reject; profile state follows connection moves. Managed
transaction-local timeout and WITHOUT HOLD cursor work remain supported.

Local verification covers:

- **1,066 assertions in 51 focused core pool cases**, including **134 assertions
  in four new policy cases** with generated warm/lazy, default/off/zero-capacity,
  unknown-enum/conflict, unsupported capability and pre-lease advisory controls.
- **11 no-server native PG assertions** verify profile moves and early rejection,
  then restoration of the existing missing-connection diagnostic when disabled.
- Direct native cache-policy matrices exercise all four policy/capacity pairings,
  repeated observed parameters, cached-entry counts, repository CRUD and typed
  queries. PG passes **89 assertions in three cases**, including early rejection
  of all serialized migration entry points and live native moves. SQLite/MySQL
  each pass **78 assertions**, including unsupported profile rejection.
- A controlled loopback peer checks real libpq packets from execute_prepared:
  off, zero capacity and profile each send an unnamed Parse for SELECT 41 and
  retain no cache entry. A cached positive-capacity negative control fails on
  its named Parse; the checker requires that precise rejection. This distinguishes
  packet behavior from a cache-count-only check. The existing health peer helper
  is reused without changing its frozen source.
- A new opt-in [Compose overlay](../../../compose.pgbouncer.yaml) uses
  edoburu/pgbouncer:v1.24.1-p1 pinned to image index digest
  `sha256:3db3d7223e93af52b4116f642951a1a5fa44702a88c2a59cf7562cac19320c9e`.
  Its runner verifies **PgBouncer 1.24.1**, transaction mode, two backing slots,
  round-robin reuse and max_prepared_statements=0 before running **169 assertions**.
  Six rounds of simultaneous explicit transactions observe two distinct backend
  PIDs and check bound repository/typed insert/read/update/delete, commit visibility,
  transaction-local timeout/cursor use, zero named server plans and rejected
  session APIs/migrations. This is scoped local acceptance of that fixture.
- All **35 configured regression targets** pass, including the compiled literal
  guide and policy packet checker. The proxy case is hidden and separately invoked;
  the ordinary suite does not require Docker/PgBouncer.
- A fresh SQLite-only address/undefined sanitizer build passes **19 targets**.
  A fresh PG-only sanitizer build passes the **11 no-server**, **89 direct native**,
  **169 proxy assertions**, plus the packet controls. The PG sanitizer log retains
  an initial incorrect no-server assertion followed by its corrected final-source
  rerun; the SQLite log includes successful initial/final-source runs.
- A fresh adapter-disabled ThreadSanitizer build passes **845 assertions in
  34 foundation/health/aging/policy cases**, with no reported races. Vendor SDK
  internals are not instrumented; native MySQL sanitizer execution is not claimed.
- Fresh installed CMake/shared pkg-config consumers compile the literal guide,
  with tests/examples disabled and no Catch2 dependency. Direct off returns 42 on
  all three adapters; PG profile returns 42, and SQLite/MySQL profile rejects.
- Formatting, full project lint (**81 translation units**) and strict production
  pool-header lint pass. Existing project/Catch2 and SDK diagnostics remain recorded;
  the strict pool-header check treats every emitted warning as an error.
- Strict standards findings remain byte-identical to the same-date aging baseline:
  **53 errors, 12 warnings, 0 infos**. No waiver is introduced.

With the normal PostgreSQL/MySQL test services:

```sh
meson compile -C buildDir -j4
meson test -C buildDir --no-rebuild --print-errorlogs
buildDir/tests/core_tests '[pool],[pool-timeout],[pool-foundation],[pool-health],[pool-aging],[pool-policy]'
buildDir/tests/postgres_unit_tests '[pool-policy]'
buildDir/tests/pg_integration '[pool-policy]'
buildDir/tests/sqlite_integration '[pool-policy]'
buildDir/tests/mysql_integration '[pool-policy]'
python3 tests/check_pool_policy_wire.py buildDir/tests/pool_policy_wire
just test-pgbouncer
python3 docs/evidence/pool-policy/check_consumers.py
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy
python3 docs/evidence/pool-policy/verify_snapshot.py --logs
```

The proxy fixture was stopped/removed after verification; existing PostgreSQL and
MySQL services were retained. The recipe intentionally leaves services running for
repeated development runs. Test credentials and loopback listeners are fixture
settings. Current source hashes bind the overlay, runner and configuration.

The [receipt](verification.json) binds final production/build/test/docs, ignored
canonical contexts and temporary logs. Earlier captures remain frozen historical
evidence. This unsigned local record verifies bytes rather than independently
attest execution, and temporary logs may expire. No remote CI/release, other proxy
versions/configurations, arbitrary session SQL, older-server, Windows/Linux host
build, ClickHouse, static-linking or performance acceptance follows. The proxy
container runs Linux, separately from the macOS arm64 library build.

Rebuild library and consumers for appended config/pool and native PG layout/API
changes. The unrelated .gitignore edit remains unstaged; ignored canonical
contexts are not force-added. Next complete combined production-pool acceptance.
