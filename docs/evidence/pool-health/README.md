# Pool health and dirty-return evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/pool-health-and-reuse.md)
and [design follow-up](../../production-pool-design.md#health-and-dirty-return-implementation-follow-up-2026-11-08)
describe implemented optional PostgreSQL bounded pre-ping and local native
dirty-return checks. Aging, diagnostics, cache/profile policy and combined pool
acceptance remain planned.

Pre-ping validates configuration before dialing, reserves its checking slot and
runs outside the pool mutex. PostgreSQL uses nonblocking uncached SELECT 1,
flush/read/socket waits and terminal drain against a steady deadline. Success
restores native blocking mode; failure closes the candidate and returns once.
Timeout has empty SQLSTATE; completed server failures retain diagnostic fields.
Health SQL produces no session query events or named prepared statements. The
deadline does not bound connect, baseline setup or total acquisition.

PG, SQLite and MySQL reject known dirty transaction/protocol state on return,
including when pre-ping is disabled. They discard without rollback/reset SQL;
native close performs its normal abort cleanup. SQLite busy statements and PG
pipeline/reader state also prevent reuse. MySQL retains lazy SQL-mode restoration
before bound execution. Enabled pre-ping is rejected by unsupported adapters
before dialing; custom adapters keep the documented local capability fallback.

Local verification covers:

- All **32 configured regression targets** pass. Focused pool checks pass
  **579 assertions in 32 cases**; the five new fake-adapter health cases cover
  capability/configuration rejection, warm/lazy/repeated ping, ordering, failure,
  exceptions and synchronized close while health is blocked.
- Native PG health/timeout/single-row/cursor checks pass **100,863 assertions in
  30 cases**. Health cases discard unfinished and failed raw transactions, verify
  rollback through a separate connection, preserve observation and recover on a
  later explicit acquire. Termination targets only a disposable owned backend.
- Native SQLite and MySQL each pass **38 health assertions**. SQLite also passes
  **10 busy-statement/readiness assertions** against an adopted native handle.
  SQLite rollback coverage uses a shared-memory URI kept alive by its controller.
- A controlled loopback peer exercises **14 real-libpq wire scenarios**: fragmented
  success, no-response timeout, terminal-drain timeout, completed server error,
  fatal error without ReadyForQuery, wrong result and disconnect, each starting in
  blocking and nonblocking mode. Assertions cover mode restoration, exact uncached
  SQL, no hidden reconnect, native idle state, full diagnostics and pipeline
  rejection without revoking caller-owned work. This peer is protocol control,
  separate from the live PostgreSQL tests.
- A fresh SQLite-only address/undefined sanitizer build passes **17 targets**.
  A separate fresh PostgreSQL sanitizer build passes the focused **30 cases**
  and **14 wire scenarios**. The SQLite log includes an initial guide compilation
  failure, followed by the corrected guide and complete successful rerun.
- A fresh adapter-disabled ThreadSanitizer build passes **358 assertions in
  15 foundation/health cases**, without reported races. This does not establish
  native SDK race coverage.
- Formatting and full project lint pass across **77 translation units**. Existing
  project/Catch2 and SDK declaration-name diagnostics remain recorded. A strict
  two-source check explicitly includes the production pool and private PG
  diagnostics headers and treats all emitted warnings as errors; it passes.
- Fresh installed CMake and shared pkg-config consumers compile the literal guide
  with tests/examples disabled: PostgreSQL returns 43 after health/dirty-return
  operations, SQLite/MySQL reject enabled health, and no Catch2 dependency is
  required. The normal suite also compiles the guide.
- Strict standards findings match the same-date ownership baseline byte for byte:
  **53 errors, 12 warnings, 0 infos**. No waiver is introduced.

With the normal PostgreSQL/MySQL test services:

```sh
meson compile -C buildDir -j4
meson test -C buildDir --no-rebuild --print-errorlogs
buildDir/tests/core_tests '[pool],[pool-timeout],[pool-foundation],[pool-health]'
buildDir/tests/pg_integration '[pool-health],[pool-timeout],[single-row],[cursor]'
buildDir/tests/sqlite_integration '[pool-health]'
buildDir/tests/sqlite_unit_tests '[pool-health]'
buildDir/tests/mysql_integration '[pool-health]'
python3 tests/check_pool_health_wire.py buildDir/tests/pool_health_wire
python3 docs/evidence/pool-health/check_consumers.py
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy
python3 docs/evidence/pool-health/verify_snapshot.py --logs
```

The [receipt](verification.json) binds source/build/test/docs, ignored canonical
contexts and local logs. Earlier captures remain frozen historical evidence.
Snapshots verify bytes rather than independently attest execution; temporary logs
may expire. This unsigned local evidence makes no remote CI/release, older-server,
Windows, ClickHouse execution, static-linking, proxy, performance or vendor-C-
allocator-fault-injection claim. Windows socket code is not executed here.

Rebuild library and consumers for appended configuration fields and adapter API
changes. Arbitrary session reset is outside this contract; discarding a private
SQLite memory connection loses its contents. The unrelated `.gitignore` edit is
unstaged; ignored canonical contexts are not force-added. Next implement aging
and diagnostics.
