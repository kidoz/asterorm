# Check pool health and discard dirty returns

Set `pool_config::pre_ping = true` for an uncached PostgreSQL SELECT 1 before
each lease handoff. `pre_ping_timeout` defaults to 1000 milliseconds and must be
positive when enabled; a negative value is invalid even when disabled. Zero is
accepted only while disabled. Invalid settings and unsupported enabled backends
fail before connecting, including constructor warmup and lazy pools.

Bounded pre-ping is PostgreSQL-only. SQLite, MySQL and ClickHouse reject enabled
configuration; they continue to work with the default `pre_ping = false`.
The generic pool dispatches through an optional bounded `pool_ping(deadline)`
capability and keeps native protocol work in the adapter.

This complete example returns 43 on PostgreSQL and an unsupported-configuration
error on SQLite/MySQL before connecting. It demonstrates that health SQL does not
emit query events, and that returning a raw transaction discards its connection:

```cpp
#include <asterorm/session.hpp>

template <typename Driver>
asterorm::result<int> pool_health_example(Driver driver, std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    config.pre_ping = true;
    config.pre_ping_timeout = std::chrono::milliseconds{500};
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    if (auto failure = pool.warmup_error())
        return std::unexpected(*failure);
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const auto&) { ++events; });
    auto first = db.template native_scalar<int>("SELECT 41");
    if (!first)
        return std::unexpected(first.error());
    auto second = db.template native_scalar<int>("SELECT 42");
    if (!second)
        return std::unexpected(second.error());
    auto lease = pool.acquire();
    if (!lease)
        return std::unexpected(lease.error());
    auto begun = (*lease)->execute("BEGIN");
    if (!begun)
        return std::unexpected(begun.error());
    lease->release_to_pool(); // Discard this unfinished transaction; never reuse it.
    auto fresh = db.template native_scalar<int>("SELECT 43");
    if (!fresh)
        return std::unexpected(fresh.error());
    if (events != 3 || *first != 41 || *second != 42 || *fresh != 43) {
        asterorm::db_error error;
        error.kind = asterorm::db_error_kind::query_failed;
        error.message = "Unexpected observed query results";
        return std::unexpected(std::move(error));
    }
    return **fresh;
}
```

PG uses nonblocking send/flush/read and socket readiness waits against a steady
deadline. Success drains the final null result, verifies transaction-idle state
and restores the previous native blocking mode. It creates no named preparation,
query event, transaction, GUC override or cancellation round trip. Timeout closes
the disposable connection and returns `timeout` with empty SQLSTATE. Completed
server errors retain SQLSTATE, message and diagnostic fields. Dispatch/read/protocol
failure also discards the candidate. A failed check returns once; the pool does
not hide it with a reconnect loop. A later explicit acquire can create a fresh
connection. See [libpq's protocol contract](https://www.postgresql.org/docs/16/libpq-async.html).

The ping deadline covers that health operation, including its terminal drain.
It does not time driver connect, timeout-baseline setup, user queries or total
acquisition. Socket scheduling/native-call overhead is not a real-time guarantee.
Warmup applies existing timeout setup and checks health on first borrow; lazy
new connections are checked before handoff. Health runs outside the pool mutex
while keeping its reserved slot. Close never hands out a late healthy candidate;
an already-failed check retains its primary failure/exception.

PG, SQLite and MySQL now reject known non-idle transactions/unread native work on
return, even without pre-ping or a timeout baseline. They close/discard rather than
issue rollback/reset SQL to reuse the connection. Server/native close performs
its normal abort cleanup. PG also rejects active pipeline mode; SQLite inspects
busy statements; MySQL inspects local transaction/protocol status. Successful
materialized results and managed transaction/stream cleanup remain usable.
MySQL keeps its existing lazy SQL-mode restoration before bound execution.

This is not an arbitrary session reset: TEMP objects, caller GUCs and other
connection-local state have their existing lifetime while a connection is clean.
SQLite private `:memory:` contents disappear when a dirty connection is discarded.
Do not return unfinished raw work when you need that database to survive; use
managed transaction guards or finish the work explicitly.

Custom adapters without the complete nonthrowing local reuse/close capability
retain `is_open()` fallback. A partial ping capability cannot enable pre-ping.
Low-level `pg::connection::pool_ping` rejects a caller-owned active reader,
transaction or pipeline without revoking it; once an idle ping starts, any failure
closes its connection. Leases remain exclusive; raw references must not be used
concurrently or after returning them.

Rebuild library and consumers for the appended `pool_config` fields and adapter
API changes. The [design](../production-pool-design.md) and
[evidence](../evidence/pool-health/README.md) record native rollback/stale-backend
checks, controlled real-libpq wire scenarios and regression/consumer checks.
[Aging and diagnostics](pool-aging-and-diagnostics.md) now add idle retirement,
metrics and structured warnings. [Cache/profile policy](pool-cache-and-profiles.md) is implemented with scoped
PgBouncer checks. The [combined review](../production-pool-acceptance.md) now records scoped local
v1 acceptance.
