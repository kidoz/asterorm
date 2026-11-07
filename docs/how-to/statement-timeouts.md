# Temporarily limit statements in a transaction

`session::with_statement_timeout(milliseconds, callback)` temporarily changes the
PostgreSQL statement timeout for an existing session-managed transaction. Its
synchronous callback receives the session and returns `asterorm::result<T>` by
value, including `result<void>` or a move-only value. The supported PostgreSQL
baseline is 16+; no runtime server version check is performed.

Use `transact()` when the entire operation should commit on success and roll back
on failure:

```cpp
auto completed = db.transact([](auto& transaction) -> asterorm::result<void> {
    return transaction.with_statement_timeout(std::chrono::milliseconds{250},
        [](auto& bounded) -> asterorm::result<void> {
            auto value = bounded.template native_scalar<int>("SELECT $1::int", 42);
            if (!value)
                return std::unexpected(value.error());
            return {};
        });
});
```

Include `<asterorm/session.hpp>` and the PostgreSQL driver header when constructing
your pool. The scope requires an active transaction; autocommit entry fails before
acquiring a lease or invoking the callback. It starts no transaction or savepoint
and does not automatically commit or roll back a healthy callback error.

## Values and nesting

Acceptable durations are whole milliseconds from 0 through 2,147,483,647. Zero
disables the timeout temporarily; negative or larger values return `query_failed`
before SQL or callback execution. SQLite, MySQL, and ClickHouse reject scoped use
before leasing. Invalid duration validation comes first.

The helper snapshots the actual effective timeout, applies a transaction-local
setting, and restores the snapshot on normal/error/exception exit while that
transaction remains active. Nested scopes restore their parent value. A prior
manual `SET LOCAL` is preserved rather than replaced by the pool default.

The scope owns a lease pin and holds the session mutex through the callback.
Use the supplied session and its connection. Other connections are outside this
scope; do not wait for another thread using the same session or for an unavailable
lease from the same pool.

## Transaction completion and savepoints

Committing or fully rolling back inside the callback is allowed. This ends the
server's local setting, but the scope retains its lease until callback exit.
Further managed queries, `observed_execute`, new transactions, or new timeout/
advisory scopes fail until all timeout frames for that transaction unwind. Void
connection callbacks throw `logic_error`. After unwind, an enclosing advisory
scope can continue using its pin.

The helper never restores a captured transaction-local value into session state
after transaction completion. PostgreSQL ends local settings at transaction end.
[PostgreSQL setting lifetimes](https://www.postgresql.org/docs/16/sql-set.html)

Creating/releasing savepoints is supported. `rollback_to_savepoint()` is rejected
before SQL while any timeout frame is active, without poisoning a healthy
transaction. Roll back to that savepoint after leaving the scope, or fully roll
back inside the callback. Savepoint rollback can otherwise remove an active
override or resurrect a completed inner override. Raw transaction control,
savepoint rollback, and manual timeout mutation bypass the managed contract.

## Failure and cleanup

A snapshot failure skips the callback without installing an override; the caller
still owns transaction cleanup. Failed/throwing application or restoration closes
and discards the connection, invalidates its transaction, and prevents reuse until
all enclosing timeout/advisory scopes unwind. Closing aborts an unfinished
transaction and releases session advisory locks. Stale guards cannot complete a
later transaction.

A callback error retains its kind, SQLSTATE, and message. If restoration also
reports an error, cleanup information is appended to `detail`. A callback success
becomes an error if cleanup fails. Callback exceptions retain their identity even
when cleanup also fails or throws; an adapter cleanup exception on a normal exit
propagates after discard. Ignoring an inner cleanup failure cannot make an outer
scope succeed.

PostgreSQL cancellation retains `query_failed` and SQLSTATE `57014`. It usually
leaves an explicit transaction aborted, so restoration cannot run and the helper
discards it. When the connection remains open, a callback may explicitly roll back before
returning its error to avoid that discard. Cancellation also includes explicit server/client cancellation;
`57014` alone does not identify the cause.

## Coverage and pool defaults

The server timeout applies to individual prepared/native/typed statements, raw
SQL, DDL, COPY, and advisory waits on the scoped connection. It does not create a
shared callback deadline or limit C++ work, decoding, pool waiting, or every part
of client-side data transfer. Snapshot/configuration/restoration statements emit
no observer events; managed body queries retain their existing observation.

For autocommit work or a baseline for every lease, use
[`pool_config::statement_timeout`](pooling-and-observability.md#set-a-postgresql-statement-timeout-baseline).
The [contract](../statement-timeout-design.md) records the full rules and acceptance
checks. Rebuild consumers with the library after the session layout change.
