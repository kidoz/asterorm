# Pool ownership foundation evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/pool-ownership-and-shutdown.md)
and [design follow-up](../../production-pool-design.md#ownership-implementation-follow-up-2026-11-08)
cover the implemented ownership/configuration/shutdown foundation. Bounded health
and dirty-return handling remain next; this does not accept the whole production
pool plan or establish PgBouncer compatibility.

Shared state and owned intrusive entries replace raw pool/alive pointers. Return
bookkeeping is allocation-free and nonthrowing; retirement retains capacity until
native cleanup outside the mutex completes. Close wakes waiters and prevents new
handoff without draining borrowers/setup. Leases can outlive the facade; sessions
and facade member calls keep their caller lifetime requirement. No health/reaper/
histogram/warning/profile options are introduced as successful no-ops.

Size/acquire-timeout validation happens before dialing, including lazy pools and
duration extremes. `pool_closed` appends to the enum without renumbering old values;
capacity expiry uses `timeout`. Successful late setup is discarded, while a failed
native operation or C++ exception retains precedence. Gauges distinguish leased,
connecting, checking, retiring and waiting work. `leaked_at_close` snapshots the
first close transition's non-idle slots rather than a ten-second wait expiry.

Local verification covers:

- Focused core pool/timeout/foundation checks: **495 assertions in 27 cases**.
  Ten new foundation cases contain **274 assertions**, including 64 synchronized
  return/facade-destruction races. They cover validation without dialing, zero/huge/
  minimum durations, move assignment with nonassignable connections, late leases,
  repeated close, waiters, blocked native cleanup, slot invariants, readiness/cache/
  connect exceptions and close/failure precedence.
- A separate [allocation probe](../../../tests/support/pool_allocation.cpp) denies
  C++ allocations after construction. Failed lazy entry allocation releases its
  slot/closes the native object; reusable return, close and late return/destruction
  allocate no pool bookkeeping. Global replacement allocation functions stay in
  this isolated executable. This is not fault injection into vendor C allocators.
- Native PG/SQLite/MySQL each pass **24 ownership assertions**: held bound queries
  after close, usable leases after facade destruction, typed closed failure,
  no transaction retry callback on closure, and managed rollback after pool close.
  The combined PG ownership/single-row run passes **100,221 assertions in 14 cases**.
  The old single-row test's access through an already-released lease was removed;
  it still verifies escaped-reader invalidation and capacity reuse, in accordance
  with the new explicit no-access-after-release contract.
- All **30 configured regression targets** pass. A fresh SQLite-only
  address/undefined sanitizer build passes all **16 targets**. A separate fresh
  adapter-disabled ThreadSanitizer build runs the ten new foundation cases:
  **274 assertions**, no reported races.
- Formatting and full project lint pass. The strict production pool-header pass
  includes its header explicitly and treats all emitted warnings as errors.
  Existing project/Catch2 diagnostics and SDK declaration-name diagnostics for
  the isolated replacement new/delete functions remain in the full lint log.
- Fresh installed CMake and shared pkg-config consumers extract the literal guide
  and execute close/late-lease/bound-query/release behavior on all three native
  adapters, with tests/examples disabled and no Catch2 dependency.
- Strict standards findings are identical to the same-date inventory baseline:
  **53 errors, 12 warnings, 0 infos**. No waiver is introduced.

Run with the normal PostgreSQL/MySQL test services:

```sh
meson compile -C buildDir -j4
meson test -C buildDir --no-rebuild --print-errorlogs
buildDir/tests/core_tests '[pool],[pool-timeout],[pool-foundation]'
buildDir/tests/pg_integration '[pool-foundation],[single-row]'
buildDir/tests/sqlite_integration '[pool-foundation]'
buildDir/tests/mysql_integration '[pool-foundation]'
python3 docs/evidence/pool-ownership/check_consumers.py
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy
```

Fresh sanitizer setup uses `-Dpostgres=disabled -Dsqlite=enabled -Dmysql=disabled
-Dclickhouse=disabled -Dexamples=false -Db_sanitize=address,undefined`. The race
build disables every adapter and uses `-Db_sanitize=thread`, compiles `core_tests`
and executes only `[pool-foundation]`. Other core code/Catch2 are instrumented in
that build, but the run makes no race claim about unexecuted cases or native SDKs.

The receipt hashes final production/test/build/docs, ignored canonical contexts,
commands and temporary logs. Earlier inventory captures stay frozen and their gap
probes are not current feature acceptance. Source snapshots verify bytes rather
than independently attesting execution. Logs may expire; this is unsigned local
evidence, without remote CI/release, older-server, ClickHouse, static linking,
proxy or performance claims.

Rebuild library and consumers for pool/lease/stats layouts and update exhaustive
error switches. Direct lease construction and raw pool return are removed; a
moved/released handle grants no connection access. No production configuration
fields are added in this slice. The unrelated `.gitignore` edit remains unstaged;
ignored contexts are not force-added. Next implement bounded health and dirty
return handling.
