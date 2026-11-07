# Connections, sessions, and transactions

Declarations: [pool](../../include/asterorm/pool/connection_pool.hpp) and
[session](../../include/asterorm/core/session.hpp).

## Pool configuration

| `pool_config` field | Default | Meaning |
| --- | --- | --- |
| `min_size` | `1` | Connections opened during construction |
| `max_size` | `10` | Maximum pool connections |
| `acquire_timeout` | `5000ms` | Wait for an available lease when exhausted |
| `prepared_statement_cache_size` | `128` | Per-connection prepared-statement limit |
| `conninfo` | empty string | Backend-specific connection settings |
| `statement_timeout` | `nullopt` | PostgreSQL session baseline on creation/checkout; optional milliseconds, 0 disables, maximum 2,147,483,647 |

`connection_pool<Driver>::acquire()` returns `result<lease_type>`. A lease is
move-only and returns its connection on destruction or `release_to_pool()`.
An optional `bool is_pool_reusable() const noexcept` plus `void close() noexcept`
lets an adapter reject idle reuse. Opted-in discarded connections close outside
the pool mutex before their capacity slot is freed, including explicit release.
PostgreSQL uses this to discard connections with unread single-row results or
owned cursor portals.
`warmup_error()` reports retained invalid configuration or a startup/setup failure. `stats()` returns pool
counts and shutdown state. `close()` prevents further acquisition and waits
for outstanding leases up to an internal deadline.

Explicit timeout configuration rejects unsupported adapters before connecting,
including lazy pools. New/idle setup failures discard the connection; lazy/reuse
errors permit retry. See [pool timeout behavior](../how-to/pooling-and-observability.md#set-a-postgresql-statement-timeout-baseline).
The appended `pool_config` field changes its layout; rebuild consumers together.

## Session operations

| Operation | Contract |
| --- | --- |
| `session db{pool}` | Borrows the pool |
| `has_active_transaction()` | Thread-safe snapshot of session-managed transaction state |
| `with_connection(callback)` | Uses the transaction/advisory scope's pinned connection or obtains a temporary lease |
| `with_advisory_lock(key, callback)` | PostgreSQL exclusive lock; callback returns `result<T>`; see [scope and cleanup](../how-to/advisory-locking.md) |
| `with_statement_timeout(milliseconds, callback)` | PostgreSQL transaction-bound override; callback returns `result<T>`; see [scope rules](../how-to/statement-timeouts.md) |
| `native<T>(sql, args...)` | `result<vector<T>>`; strict hydration by mapped column name |
| `native_as<T>(mode, sql, args...)` | Explicit strict or lenient hydration |
| `native_scalar<T>(sql, args...)` | `result<optional<T>>`; first row, first column |
| `native_map(sql, mapper, args...)` | Maps each row through `mapper(result, row_index)` |
| `set_observer(callback)` | Sets the query callback; see [coverage](../how-to/pooling-and-observability.md) |

Lenient native hydration leaves omitted members at their value-initialized
defaults. A scalar's empty optional means no row; SQL NULL still follows the
selected `T` codec. `native_map()` preserves the mapper's return type, so a
mapper returning `result<T>` yields a vector of per-row results inside the
outer query result.

An active [repository stream](../how-to/stream-entities.md) holds an independent
connection pin. Unrelated session work, second streams, transaction completion,
savepoints and advisory/timeout scopes reject while it is active. Clean EOF keeps
a caller transaction, as does successful explicit cursor close. Early single-row
close, failure or unfinished destruction discards and invalidates it. Cursor mode
requires an existing healthy managed transaction before reserving a lease.

Streams snapshot the observer on opening and emit one logical SELECT event at EOF,
error or explicit close. Normal callbacks run before the stream pin is released;
if a parent guard already invalidated it, the next call reports that invalidation
after pin loss. Destruction emits no event. Callback exceptions preserve identity,
discard ownership and are never replayed. Stream elapsed time sums native calls,
excluding hydration and consumer pauses. An event's error pointer is valid only
during the callback; copy fields that must outlive it.

## Transactions

`transaction_options` defaults to `default_level` isolation, `read_only=false`,
and `deferrable=false`. Available isolation values are `default_level`,
`read_uncommitted`, `read_committed`, `repeatable_read`, and `serializable`.
Backend interpretation is listed in [transaction how-to](../how-to/transactions.md).

`begin(options)` returns `result<transaction_guard<Session>>`. The move-only
guard offers `commit()` and `rollback()` and rolls back on destruction while
active. The session must outlive the guard. A session supports one active
transaction; savepoint methods provide named partial rollback.

`transact(callback, options)` commits a successful callback result and rolls
back errors. `retry_transaction(callback, max_attempts=3, options={})` retries
serialization and deadlock failures. Rollback does not restore C++ object state.

The pool must outlive sessions; sessions must outlive repositories and guards.
Use separate sessions for independent concurrent work so transaction state is
not shared between unrelated operations. Timeout scopes keep their own lease pins;
completing a transaction inside one blocks further work until its timeout frames
unwind. Savepoint rollback is rejected while a timeout scope is active. See the
[timeout guide](../how-to/statement-timeouts.md) for restoration and discard rules.
