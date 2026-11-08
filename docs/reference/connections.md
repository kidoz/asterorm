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
| `pre_ping` | `false` | Bounded PG SELECT 1 before handoff; unsupported enabled backends reject before connect |
| `pre_ping_timeout` | `1000ms` | Ping flush/read/drain deadline; positive when enabled, nonnegative otherwise |

`connection_pool<Driver>::acquire()` returns `result<lease_type>`. A lease is
move-only and returns its connection without throwing on destruction or
`release_to_pool()`. Pool bookkeeping allocates no memory on return. A moved or
released lease no longer grants access. Direct lease construction and raw
`pool.release(connection)` are removed; use `acquire()`.
An optional `bool is_pool_reusable() const noexcept` plus `void close() noexcept`
lets an adapter reject idle reuse. Opted-in discarded connections close outside
the pool mutex before their capacity slot is freed, including explicit release.
PostgreSQL uses this to discard connections with unread single-row results or
owned cursor portals.
Built-in PG/SQLite/MySQL also reject known non-idle transactions/unread work on
return without a timeout baseline. PG rejects pipeline mode; SQLite checks busy
statements; MySQL checks local transaction/protocol state. Readiness checks do no
network I/O. Legacy adapters without the complete capability retain their fallback.
`warmup_error()` retains invalid configuration or startup/setup failure. Construction
rejects zero maximum, minimum above maximum and negative acquire timeout before
dialing. Zero acquire timeout permits immediate idle/new capacity but never waits;
huge durations saturate the steady deadline. Connect/setup is not timed by it.

`close() noexcept` wakes waiters and prevents new handoff without draining active
leases or setup. Held leases use shared state and may outlive the facade. Detached
idle native cleanup occurs outside the state mutex and may still take native-close
time. A closed acquisition returns `pool_closed`; capacity timeout returns `timeout`.
Existing enum values retain their numbers. Native setup failure/exception takes
precedence over concurrent shutdown; successful late setup is discarded.

Stats satisfy `total == idle + in_use` and
`in_use == leased + connecting + checking + retiring`; `waiters` counts capacity
waiters. `leaked_at_close` snapshots non-idle slots at the first close transition,
including setup, and no longer means the old ten-second wait expired. The
[ownership guide](../how-to/pool-ownership-and-shutdown.md) covers migration;
[design](../production-pool-design.md) separates the remaining health/age/metric work.

Explicit timeout configuration rejects unsupported adapters before connecting,
including lazy pools. New/idle setup failures discard the connection; lazy/reuse
errors permit retry. See [pool timeout behavior](../how-to/pooling-and-observability.md#set-a-postgresql-statement-timeout-baseline).
Pool, lease and stats layouts changed; rebuild library/consumers together. Connection
types must be nothrow move-constructible/destructible. The earlier appended
`pool_config` timeout field also requires coordinated rebuilds.

`pre_ping` requires the bounded adapter capability and runs before an explicit
timeout baseline while retaining its slot outside the mutex. It emits no query
event/cache entry. Timeout/failure discards without an automatic reconnect loop;
completed native diagnostics are preserved. The deadline does not include connect
or baseline setup. See the [health guide](../how-to/pool-health-and-reuse.md).
The appended health configuration fields require a coordinated rebuild too.

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

On connections with nonthrowing close, guard destruction catches rollback and
pool-return exceptions and discards the connection. If a callback still borrows
it, close/release waits for the callback to unwind. Generation checks preserve
any newer managed transaction. Legacy connections retain their existing cleanup
path.

`transact(callback, options)` commits a successful callback result and rolls
back errors. `retry_transaction(callback, max_attempts=3, options={})` retries
serialization and deadlock failures. Rollback does not restore C++ object state.

The pool must outlive sessions; sessions must outlive repositories and guards.
Use separate sessions for independent concurrent work so transaction state is
not shared between unrelated operations. Timeout scopes keep their own lease pins;
completing a transaction inside one blocks further work until its timeout frames
unwind. Savepoint rollback is rejected while a timeout scope is active. See the
[timeout guide](../how-to/statement-timeouts.md) for restoration and discard rules.

The separately included `testing::transactional_session<Session>` supports an
optional rollback-only test boundary on PostgreSQL/SQLite. It requires an idle
session, starts its own root/private savepoint, permits ordinary queries and user
savepoints, and gates managed completion and stream/advisory/timeout composition.
Explicit `finish()` rolls back, restores the entering observer and releases a
confirmed-idle connection. Failure discards and poisons that borrowed session;
there is no automatic reconnection or baseline recreation. See the
[scope guide](../how-to/transactional-test-scopes.md) for lifetime, callbacks and
exception policy. Session layout changed; rebuild consumers with the library.

The optional `<asterorm/testing/catch2.hpp>` adds explicit fixture registration,
typed session access during a matching tagged partial invocation and a custom
runner for listener diagnostics/process status. It requires a separately supplied
Catch2 dependency without its main. See the [Catch2 guide](../how-to/catch2-transactional-tests.md).
