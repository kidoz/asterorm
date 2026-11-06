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
asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};

if (auto failure = pool.warmup_error())
    return std::unexpected(*failure);
```

Include `<asterorm/postgres/driver.hpp>` and `<asterorm/session.hpp>` and use the
snippet in a function returning `asterorm::result<...>`. Keep `min_size` no
greater than `max_size`, and choose a positive `max_size`.

A failed constructor warmup is retained: `acquire()` keeps returning that error.
Correct the settings or service availability and construct a replacement pool.
With `min_size = 0`, the first acquire opens the first connection.

`pool.stats()` reports `total`, `idle`, `in_use`, `leaked_at_close`, and `closed`.
Use it to distinguish an exhausted pool from a connection failure.

For private SQLite `:memory:` databases, set both sizes to one. For persistence,
set `conninfo` to a file path or supported `file:` URI. Each new SQLite
connection enables foreign keys and a five-second busy timeout.

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
repository CRUD and native session queries. Typed `query()` currently executes
prepared statements directly, so it does not emit these events. Direct
`connection::execute()` calls and transaction control SQL also bypass this hook.

Keep callbacks short and non-throwing. They run while the connection lease is
held; calling `pool.close()` there waits for outstanding leases and can stall.

## Shut down

Finish transactions and release explicit leases before calling `pool.close()`.
Close prevents new acquisitions and waits for outstanding leases up to its
internal deadline. See the [pool reference](../reference/connections.md) for
defaults and lifetime rules.
