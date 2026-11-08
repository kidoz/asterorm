# Own leases and shut down a pool

The pool uses shared internal state and move-only leases. `close()` rejects new
acquisitions and wakes capacity waiters without waiting for held leases or native
connect/setup work. A held lease remains usable; returning it after close destroys
its connection. A lease may outlive the pool facade. Sessions still borrow the
facade, so the facade must outlive sessions and calls made on it.

This complete example runs on PostgreSQL, SQLite and MySQL. It validates the
closed error, destroys the facade while retaining a lease, executes a bound query
on that lease and explicitly releases it:

```cpp
#include <asterorm/session.hpp>

template <typename Driver>
asterorm::result<int> pool_example(Driver driver, std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds{0};
    using pool_type = asterorm::connection_pool<Driver>;
    auto pool = std::make_unique<pool_type>(std::move(driver), config);
    if (auto failure = pool->warmup_error())
        return std::unexpected(*failure);
    auto lease = pool->acquire();
    if (!lease)
        return std::unexpected(lease.error());
    pool->close();
    auto rejected = pool->acquire();
    if (rejected || rejected.error().kind != asterorm::db_error_kind::pool_closed) {
        asterorm::db_error error;
        error.kind = asterorm::db_error_kind::query_failed;
        error.message = "Expected a closed pool";
        return std::unexpected(std::move(error));
    }
    pool.reset();
    auto selected = (*lease)->execute_prepared("SELECT $1 AS value", {"41"});
    if (!selected)
        return std::unexpected(selected.error());
    if (selected->rows() != 1 || selected->get_string(0, 0) != "41") {
        asterorm::db_error error;
        error.kind = asterorm::db_error_kind::parse_failed;
        error.message = "Unexpected bound result";
        return std::unexpected(std::move(error));
    }
    lease->release_to_pool();
    return 41;
}
```

Before connecting, construction rejects a zero maximum, a minimum greater than
the maximum, and a negative capacity timeout. These `connection_failed` diagnostics
remain available through `warmup_error()`. Existing explicit statement-timeout
validation remains unchanged. Correct configuration or warmup failure by replacing
the pool. A closed pool returns `pool_closed` even if warmup previously failed.
Capacity exhaustion returns `timeout` with an empty SQLSTATE. Zero timeout accepts
immediately available idle/new capacity and never waits; huge durations saturate
the monotonic deadline. Native connection/setup time is still not bounded by this
capacity timeout. Native failures/exceptions retain precedence if setup fails
concurrently with shutdown; successful setup after close is discarded.

`release_to_pool()` and lease destruction are nonthrowing. Return uses an existing
intrusive entry and performs no allocation. Moves transfer ownership exactly once,
including move assignment over an existing lease. After move/release, that handle
confers no connection access; do not use `operator*` or `operator->` on it. Direct
lease construction and raw `pool.release(connection)` are no longer supported;
obtain ownership through `acquire()`.

Stats preserve `total == idle + in_use`. `in_use` includes reserved connect/setup
and cleanup work; `leased`, `connecting`, `checking`, `retiring` and `waiters`
explain those states. More precisely,
`in_use == leased + connecting + checking + retiring`. Native close/destruction
finishes before a retiring slot is freed. `leaked_at_close` is the non-idle slot
snapshot at the first close transition, including setup; it does not prove a leak.
Repeated close retains that snapshot. After close, stats may therefore show live
borrowers/work until their owners complete cleanup.

Native close/destruction and setup occur outside the state mutex. Close can still
take the time needed to close detached idle native handles; it is not a hard
wall-clock operation. It does not cancel a synchronous driver connect or revoke
a held transaction/stream. Managed completion remains the caller's responsibility.
Pool closure is terminal for new acquisitions and is not retried as a transaction
serialization/deadlock failure.

Rebuild library and consumers together: pool, lease and stats layouts changed.
`pool_closed` is appended to the error enum, preserving existing numeric values;
update exhaustive switches and capacity-timeout handling. Connection types must
be nothrow move-constructible/destructible. Existing optional readiness/timeout
capabilities remain; custom readiness exceptions on return cause safe discard.
Legacy adapters retain their documented fallback and own native resource cleanup.

This implements the [contract's](../production-pool-design.md) ownership,
configuration and shutdown foundation. [Evidence](../evidence/pool-ownership/README.md)
covers concurrency, allocation failure, native/session/stream regression and
installed consumers. [Bounded health and dirty-return handling](pool-health-and-reuse.md)
are now implemented. Aging, histograms/warnings and cache/proxy policy remain planned.
