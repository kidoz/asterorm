# Retire aged connections and poll pool diagnostics

Pool aging and long-held-lease diagnostics are opt-in. `idle_timeout`,
`max_lifetime` and `leak_threshold` default to zero (disabled). Negative durations
are invalid. `maintenance_interval` defaults to 1000 milliseconds and must be
positive even when no feature is enabled. Configuration errors are retained by
`warmup_error()` and reject before dialing.

This complete example runs on PostgreSQL, SQLite and MySQL. It enables retirement
and warning delivery, executes one query, and explicitly consumes diagnostic
records. Applications can route the returned records to their own logger:

```cpp
#include <asterorm/session.hpp>

template <typename Driver>
asterorm::result<int> pool_aging_example(Driver driver, std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 2;
    config.idle_timeout = std::chrono::minutes{5};
    config.max_lifetime = std::chrono::minutes{30};
    config.leak_threshold = std::chrono::seconds{30};
    config.maintenance_interval = std::chrono::seconds{1};
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    if (auto failure = pool.warmup_error())
        return std::unexpected(*failure);
    asterorm::session db{pool};
    auto answer = db.template native_scalar<int>("SELECT 42");
    if (!answer)
        return std::unexpected(answer.error());
    auto warnings = pool.take_diagnostics(); // May allocate; no implicit logging.
    auto metrics = pool.stats();
    if (*answer != 42 || metrics.acquires_succeeded != 1 ||
        metrics.acquires_failed != 0 || !warnings.empty()) {
        asterorm::db_error error;
        error.kind = asterorm::db_error_kind::query_failed;
        error.message = "Unexpected pool diagnostics example result";
        return std::unexpected(std::move(error));
    }
    return **answer;
}
```

A physical entry's lifetime starts when native connect succeeds, before cache/
timeout setup. Idle age starts at creation for a warmed entry and at its latest
return thereafter. Expiry is inclusive at the configured duration. Retirement
may reduce the pool below `min_size`; the next borrow lazily creates capacity.
There is no background reconnect, refill or health query.

The optional worker retires only idle entries. Checkout also sweeps expired idle
candidates, so delayed maintenance cannot hand them out. An entry that exceeds
`max_lifetime` during health/setup is discarded and acquisition returns
`connection_failed` with a checkout-expiry diagnostic; it does not retry setup in
a loop. Existing native failures/exceptions keep their primary outcome, and
successful setup after pool closure reports `pool_closed`.

Held leases, transactions and streams retain exclusive native ownership. Aging
never closes their handles; over-age leases retire when returned. A clean return
resets idle age but not birth time. Local dirty-return handling and optional
[PostgreSQL pre-ping](pool-health-and-reuse.md) still apply. Discarding a private
SQLite `:memory:` connection loses its database; keep retirement disabled when
that physical connection must preserve fixture state.

`stats()` adds monotonic saturating unsigned counters:

| Field | Meaning |
| --- | --- |
| `acquires_succeeded`, `acquires_failed` | All completed acquisition calls, including propagated exceptions as failures |
| `acquire_timeouts` | Failures caused by exhausted capacity waiting |
| `connection_failures` | Connect/cache/baseline/local-new-connection errors or exceptions, including expiry during setup |
| `health_failures` | Optional pre-ping errors or exceptions (including health timeout) |
| `retirements` | Physical entries detached for disposal, including dirty/expired/error/closed entries; connecting reservations without an entry do not count |
| `diagnostics_dropped` | Overdue warning records dropped because the queue was full |
| `overdue_leases` | Currently held checkouts at or above the warning threshold; computed at snapshot time |

Closed/retained-warmup-error calls increase `acquires_failed` without a native
failure counter. Each completed call also increments exactly one noncumulative
`acquisition_duration` bucket: **≤1, ≤5, ≤10, ≤50, ≤100, ≤500, ≤1000 milliseconds,
then overflow**. These durations include connect/setup and exception unwinding,
not user query latency. Counters and buckets saturate rather than wrap; beyond
saturation their sums need not equal call totals. Existing gauges continue to
satisfy `total = idle + leased + connecting + checking + retiring`, and
`in_use = total - idle`. Retiring capacity remains reserved until native cleanup
finishes outside the mutex. Snapshots count only calls whose recording completed.

Long-held means overdue, not proven leaked. Each checkout has a slot identifier,
a generation, a timestamp and a warned bit. Maintenance, or a return before the
next maintenance pass, produces at most one warning per checkout. Dropped warnings
are not retried for that generation. `take_diagnostics()` returns and clears the
pending FIFO records; a vector allocation failure propagates and leaves the queue
intact. At most `max_size` records are pending, using storage allocated during
construction only when warning detection is enabled. Records contain only
`slot`, `generation`, `held_for` and `severity` (`warning`), with no SQL, connection
settings, parameters or native handles. Slot/generation identifiers are local to
the pool and saturate at their unsigned limit. No application callback, global
logger or stderr output runs from maintenance, return or destruction.

`close()` stops new maintenance scheduling, wakes capacity waiters and prevents
handoff without waiting for the worker or held leases. Detached native idle
cleanup can still take native-close time. Facade destruction requests stop and
joins its worker outside the state mutex; it may wait for already-running native
idle cleanup. Explicit leases can outlive that facade and close on return. No
worker is created when all three features are disabled. Steady-clock deadlines
and age comparisons handle very large durations without overflowing; OS scheduling
and native cleanup are not hard real-time deadlines.

Rebuild the library and downstream consumers for changed pool/config/stats/entry
layouts. Meson dependencies, installed CMake `Threads::Threads` and pkg-config
metadata carry thread linkage. The [design](../production-pool-design.md) and
[evidence](../evidence/pool-aging/README.md) record deterministic clock tests,
worker shutdown controls, native transactions and verification limits.
Cache/profile policy and combined pool acceptance remain planned.
