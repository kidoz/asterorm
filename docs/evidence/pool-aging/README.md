# Pool aging and diagnostic evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/pool-aging-and-diagnostics.md)
and [design follow-up](../../production-pool-design.md#aging-and-diagnostic-implementation-follow-up-2026-11-08)
cover implemented idle/lifetime retirement, acquisition metrics and structured
long-held-checkout diagnostics. Cache/profile policy and combined pool acceptance
remain planned; no proxy compatibility is established.

An optional facade-owned stop-aware worker keeps weak state access and inspects
held-entry metadata only. Idle handles are detached under the mutex and disposed
outside it, keeping their slots reserved until cleanup finishes. Checkout and
return enforce inclusive steady-clock age checks independently of scheduling.
Expiry during setup returns once; held transactions/streams remain usable and
retire on return. Close requests stop without joining; destruction joins outside
the mutex and may await already-running native idle cleanup. No background refill,
reconnect or health SQL is introduced.

Preallocated warning storage is capped at max_size. Each checkout generation
warns at most once, including dropped warnings; return can enqueue without
allocation. Polling allocation failure preserves the queue. Warning records
contain only slot, generation, duration and severity. No logger, application
callback or stderr output runs on return, maintenance or destruction. Counters
and eight fixed acquisition-duration buckets saturate; gauges retain explicit
leased/connecting/checking/retiring/waiting distinctions.

Local verification covers:

- **932 assertions in 47 focused pool cases**, including **353 assertions in
  15 aging/diagnostic cases**. A private clock/maintenance seam tests exact idle,
  lifetime and warning thresholds, latest-return versus birth age, warmed/lazy
  entries, disabled/huge/invalid durations, expiry during setup without retry,
  all histogram boundaries, error/exception outcomes, once-per-generation
  warnings, bounded overflow/drop behavior, lease moves and multi-entry unlinking.
  Real-worker controls cover idle retirement, held-lease preservation, stop with
  a saturated wake deadline and blocked native cleanup during close/destruction.
- The separate allocation-denial probe passes with warning detection enabled:
  overdue return and close allocate no pool bookkeeping; failed polling allocation
  leaves its pending warning available. Existing allocation/late-lease controls
  remain in the same executable. Vendor C allocators are not fault-injected.
- Native PG/SQLite/MySQL each pass **19 new aging assertions**, keeping a held raw
  transaction usable past lifetime/warning thresholds, committing it explicitly,
  retiring on return and lazily replacing capacity. The worker then expires an
  idle replacement. SQLite uses a deliberately disposable private memory database.
  Combined PG aging/foundation/health/timeout/single-row/cursor coverage passes
  **100,906 assertions in 32 cases**.
- All **33 configured regression targets** pass, including the compiled literal
  guide. A fresh SQLite-only address/undefined sanitizer build passes **18 targets**.
  A fresh adapter-disabled ThreadSanitizer build passes **711 assertions in
  30 foundation/health/aging cases**, with no reported races. This does not claim
  race or sanitizer coverage of vendor SDK internals.
- Fresh installed CMake and shared pkg-config consumers compile and execute the
  literal guide on all three native adapters, with tests/examples disabled and no
  Catch2 dependency. Meson exports its thread dependency; installed CMake exports
  Threads::Threads and pkg-config receives the platform thread dependency. This
  run is macOS arm64; other platform linker behavior is not execution-tested.
- Formatting and full project lint pass across **78 translation units**. Existing
  project/Catch2 and SDK declaration-name diagnostics remain recorded. A strict
  production pool-header check treats all emitted warnings as errors and passes.
- Strict standards findings match the same-date health baseline byte for byte:
  **53 errors, 12 warnings, 0 infos**, with no waiver.

With the normal PostgreSQL/MySQL test services:

```sh
meson compile -C buildDir -j4
meson test -C buildDir --no-rebuild --print-errorlogs
buildDir/tests/core_tests '[pool],[pool-timeout],[pool-foundation],[pool-health],[pool-aging]'
buildDir/tests/pool_allocation_probe
buildDir/tests/pg_integration '[pool-aging],[pool-foundation],[pool-health],[pool-timeout],[single-row],[cursor]'
buildDir/tests/sqlite_integration '[pool-aging]'
buildDir/tests/mysql_integration '[pool-aging]'
python3 docs/evidence/pool-aging/check_consumers.py
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy
python3 docs/evidence/pool-aging/verify_snapshot.py --logs
```

The fresh SQLite sanitizer build enables only SQLite with
`-Db_sanitize=address,undefined` and runs the full configured suite. The race build
disables every adapter and uses `-Db_sanitize=thread`, builds core_tests and runs
only `[pool-foundation],[pool-health],[pool-aging]`. The [receipt](verification.json)
binds final source/build/test/docs, ignored canonical contexts and local logs.
Earlier captures remain frozen historical evidence. Snapshots verify bytes rather
than independently attest execution; temporary logs may expire.

This unsigned local evidence does not establish remote CI/release, older-server,
Windows/Linux execution, ClickHouse execution, static linking, proxy, performance,
hard real-time scheduling or bounded native cleanup. Public pool/config/stats and
internal entry layouts change; rebuild library and consumers. The unrelated
`.gitignore` edit remains unstaged; ignored canonical contexts are not force-added.
Next implement cache/profile policy, then combined pool acceptance.
