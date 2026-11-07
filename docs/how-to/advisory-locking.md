# Scope an advisory lock

`session::with_advisory_lock(int64_t key, callback)` holds an exclusive advisory
lock on one connection while a synchronous callback runs. The callback receives
the session and returns `asterorm::result<T>` by value, including `result<void>`
or a move-only value. The helper returns that result after cleanup.
The supported PostgreSQL baseline is 16+, matching the supplied development service;
the helper does not probe the server version.

```cpp
constexpr std::int64_t refresh_key = 4217;
auto refreshed = db.with_advisory_lock(refresh_key,
    [](auto& locked) -> asterorm::result<void> {
        return locked.transact([](auto& transaction_session) -> asterorm::result<void> {
            auto value = transaction_session.template native_scalar<int>("SELECT 1");
            if (!value) return std::unexpected(value.error());
            // Perform the coordinated work through transaction_session here.
            return {};
        });
    });
```

All cooperating callers must choose the same key in the same database. Advisory
locks coordinate those callers; ordinary statements do not consult the key.
PostgreSQL is the only built-in adapter that supports this helper. SQLite,
MySQL, and ClickHouse return `query_failed` before acquiring a connection or
invoking the callback.

## Connection and transaction lifetime

Acquisition, callback queries through the session, and release use one pinned
lease. A callback can nest scopes with the same key or different keys. Each
successful acquisition has a matching release; ending an inner same-key scope
leaves the outer lock held.

No transaction is started automatically. An existing transaction is reused, and
`begin()` inside the callback uses the pinned connection. Committing or rolling
back that transaction keeps the advisory lock and lease until the scope ends.
Conversely, a transaction retained beyond the callback keeps its lease after the
advisory lock is released. Its guard must still be committed or rolled back.

This follows PostgreSQL session-lock semantics: acquisition counts stack, and
transaction completion does not release session advisory locks.
[PostgreSQL advisory lock lifetimes](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS)

The callback holds the session mutex. Do not wait for another thread to use the
same session, or for a lease from the same pool when none is available. Work on
separate connections is outside this scope's ownership. Avoid manually unlocking
the key or closing its connection inside the callback.

## Errors and cleanup

A callback error is returned after cleanup; an exception propagates after
cleanup. A callback error alone does not roll back SQL work. Use `transact()`
inside the scope when the coordinated work needs a transaction.

If acquisition fails or throws, the connection is closed and discarded because
ownership may be uncertain. The callback is skipped. If release fails, returns
false, or throws, the connection is also discarded. Closing it aborts any active
transaction and releases its session locks. For example, an aborted PostgreSQL
transaction cannot execute the unlock statement: the helper discards that
connection rather than returning it with an outstanding lock.

When both the callback result and release report errors, the callback's kind,
SQLSTATE, and message are preserved, with cleanup information appended to
`detail`. When only release reports an error, that error becomes the result.
Callback exceptions retain their identity even if cleanup also throws. An
adapter exception during release on a normal callback exit propagates after
connection discard.

A cleanup failure in a nested scope prevents further session queries or new
transactions until all enclosing scopes unwind. The outer scope also fails if
its callback ignores the inner error. Old transaction guards cannot complete a
later transaction after the connection has been discarded. The session can be
used again after the outermost scope exits.

## Adapter and execution details

The PostgreSQL adapter binds the signed 64-bit key to
`pg_catalog.pg_advisory_lock($1::bigint)` and
`pg_catalog.pg_advisory_unlock($1::bigint)`. Acquisition blocks until available;
server timeout settings can bound that statement. This API adds no timeout,
try-lock, shared-lock, or transaction-lock variant.
[PostgreSQL advisory lock functions](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)

Lock-control statements do not emit session observer events. Callback queries
using the normal managed query APIs retain their existing observation behavior.

A custom adapter opts in by providing `result<void> advisory_lock(int64_t)`,
`result<void> advisory_unlock(int64_t)`, and `void close() noexcept`. Unlock must
report an error when the connection no longer owns the key. The generic session
contains no vendor SQL and requires all three capabilities before invoking them.

[Transactions](transactions.md) · [Lock selected rows](select-locking.md) ·
[Connections reference](../reference/connections.md)
