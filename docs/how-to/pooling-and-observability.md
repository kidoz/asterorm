# Configure pools and observe queries

## Size and probe a pool

Set `pool_config` before constructing the pool:

```cpp
asterorm::pool_config cfg;
cfg.conninfo = "host=127.0.0.1 dbname=app user=app";
cfg.min_size = 1;
cfg.max_size = 8;
cfg.acquire_timeout = std::chrono::milliseconds{2000};
cfg.prepared_statement_cache_size = 128;
cfg.statement_timeout = std::chrono::milliseconds{1000}; // PostgreSQL only
asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};

if (auto failure = pool.warmup_error())
    return std::unexpected(*failure);
```

Include `<asterorm/postgres/driver.hpp>` and `<asterorm/session.hpp>` and use the
snippet in a function returning `asterorm::result<...>`. Keep `min_size` no
greater than `max_size`, and choose a positive `max_size`.

Invalid size/timeout configuration or a failed constructor warmup is retained:
`acquire()` keeps returning that error until close or replacement. Construction
rejects a zero maximum, `min_size > max_size`, and a negative acquire timeout
before connecting. A closed pool returns `pool_closed`; exhausted capacity returns
`timeout`. The [ownership guide](pool-ownership-and-shutdown.md) covers these errors.
Correct the settings or service availability and construct a replacement pool.
With `min_size = 0`, the first acquire opens the first connection.

`pool.stats()` reports `total`, `idle`, `in_use`, `leaked_at_close`, and `closed`,
plus `leased`, `connecting`, `checking`, `retiring` and `waiters`. Reserved setup/
cleanup slots remain counted until completion. These gauges distinguish held
leases, exhausted capacity and work still finishing after close.

For private SQLite `:memory:` databases, set both sizes to one. For persistence,
set `conninfo` to a file path or supported `file:` URI. Each new SQLite
connection enables foreign keys and a five-second busy timeout.

## Set a PostgreSQL statement timeout baseline

The supported PostgreSQL baseline for this API is 16+, matching the development
service. The adapter checks connection state rather than probing server versions.

`pool_config::statement_timeout` is an optional `std::chrono::milliseconds`.
Leave it absent to preserve the server/role/connection setting. Zero explicitly
disables the timeout. Values from 1 through 2,147,483,647 milliseconds are accepted;
negative or larger values return a persistent `query_failed` configuration error
before connecting, including on a lazy pool. SQLite, MySQL, and ClickHouse reject
explicit configuration, even zero, before connecting.

The pool applies a session-level setting during connection creation and before
every idle checkout. A borrower can change it through raw APIs, but the next
checkout reapplies the configured baseline on the same connection. This covers
explicit leases, native/typed/prepared queries, DDL, COPY, and advisory waits on
that connection. Initialization/reapplication does not emit session observer events.

Setup runs outside the pool mutex. A failed or throwing setup discards its
connection and frees capacity. Warmup failures persist through `warmup_error()`;
lazy/reuse failures affect that acquisition and permit a later retry. Built-in
PG/SQLite/MySQL adapters discard known active/failed transactions on return, even
without a timeout baseline. The next acquisition can create a fresh connection;
no return-time rollback/reset SQL is issued to reuse the dirty work.

The timeout limits individual server statements, not a whole callback, C++ work,
client transfer time, or pool waiting. `acquire_timeout` still bounds capacity
waiting rather than connection/configuration work. Server cancellation retains
`query_failed` and SQLSTATE `57014`; that code can also mean explicit cancellation.

`pool_config` gained a field: rebuild the library and its consumers together.
Use [`session::with_statement_timeout`](statement-timeouts.md) for a temporary
override inside an existing transaction. Low-level PostgreSQL adapter setters
provide no automatic scope restoration.

## Record query timing

```cpp
db.set_observer([](const asterorm::query_event& event) {
    std::clog << event.sql << " params=" << event.param_count
              << " ns=" << event.elapsed.count();
    if (event.error)
        std::clog << " error=" << event.error->message;
    std::clog << '\n';
});
```

Include `<iostream>`. Timing covers dispatch through response, excluding row
decoding. The event carries SQL and a parameter count, not parameter values.
Copy error details during the callback if you need to retain them; `error` is
a borrowed pointer.

The observer covers paths using `session::observed_execute()`, including
repository CRUD, native session queries, and typed `query()` calls. Direct
`connection::execute()` calls and transaction control SQL bypass this hook.
Events describe execution before decoding; copy error details during the callback.
Custom session facades without a compatible `observed_execute` helper retain
direct typed execution and do not gain these events automatically.

Keep callbacks short and non-throwing. They run while the connection lease is
held; do not wait for another lease from that pool. Initiate shutdown outside
callbacks, since detached idle native cleanup can still take time.

## Shut down

`pool.close()` prevents new acquisitions and wakes waiters without waiting for
borrowers or in-flight connect/setup. Held leases and managed transactions remain
usable until their owners finish; returns after close destroy their connections.
Explicit leases may outlive the facade; sessions and facade member calls retain
their normal lifetime requirements. See the [ownership guide](pool-ownership-and-shutdown.md)
and [pool reference](../reference/connections.md).

Capacity waiters are notified when close begins; this does not interrupt a
synchronous native connect. `is_open()` is a local state check, not an on-borrow
health query. Optional [bounded PG pre-ping](pool-health-and-reuse.md) checks before
handoff; SQLite/MySQL reject enabled pre-ping before connecting. Idle/lifetime
retirement, acquisition histograms and lease warnings remain planned.

Finish raw transactions before returning explicit leases when their connection
must remain available. Built-in adapters discard known dirty returns; private
SQLite memory data disappears with its connection. Custom legacy adapters keep
their documented local fallback. Managed sessions retain transaction-guard cleanup.

## Retire connections and consume lease warnings

Set `idle_timeout`/`max_lifetime` to enable age retirement and `leak_threshold`
to enable long-held checkout warnings. Zero disables each option;
`maintenance_interval` must stay positive. The [aging guide](pool-aging-and-diagnostics.md)
explains idle-only background work, held-lease protection, acquisition outcome
counters/histogram and `take_diagnostics()` pull delivery. Native cleanup and OS
scheduling are not hard deadlines. Private SQLite memory contents vanish when
the physical connection is retired.
