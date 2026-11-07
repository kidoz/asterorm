# Statement timeout contract

Decided on 2026-11-07. **Pool defaults, PostgreSQL adapter support, and
transaction-bound session scopes are implemented within the v1 limits below.**
This resolves the timeout portion of the [runtime audit](locking-and-timeout-audit.md).
Local acceptance is recorded in the [combined review](locking-timeout-acceptance.md).
Existing query cancellation remains `query_failed` with SQLSTATE `57014`.

## API and limits

```cpp
// Available pool addition:
struct pool_config {
    // Existing fields remain in order; append:
    std::optional<std::chrono::milliseconds> statement_timeout{};
};

// Available scope: callback receives session&, returns result<T> by value.
db.with_statement_timeout(std::chrono::milliseconds duration, callback);
```

| Input | Required behavior |
| --- | --- |
| Pool option absent | Preserve server/role/connection defaults; add no timeout control SQL |
| Zero | Explicitly disable the statement timeout for that baseline or scope |
| 1 through 2,147,483,647 milliseconds | Accept, without rounding or clamping |
| Negative or above that maximum | `query_failed`, before connection creation, lease acquisition, SQL, or callback |
| Explicit pool option on an unsupported adapter, including zero | Persistent configuration error, before connecting, even with `min_size=0` |
| Scoped call on an unsupported adapter | `query_failed` before leasing or invoking the callback |

PostgreSQL is the only built-in implementation in v1. SQLite busy handlers and
MySQL execution/lock timeouts have different meanings; they do not implement this
contract. Only whole milliseconds are accepted by this overload; no floating-point
or lossy sub-millisecond overload is added. Standard exact chrono conversions are
allowed. Validate durations first, backend capability second, then session state.

## Pool baseline

Set a **session-level** value in autocommit when preparing each new physical
connection, and reapply it before handing out an idle connection. Warmed connections
have the baseline before entering the idle list. This covers explicit pool leases,
autocommit queries, transactions, migration setup/DDL, and advisory acquisition.
An unchanged explicit value is still reapplied on checkout; no dirty-state cache
assumes that a previous borrower left it intact.

The PostgreSQL adapter binds `"<count>ms"` to
`SELECT pg_catalog.set_config('statement_timeout', $1, false)`. `SET LOCAL` is not
used for this path. The physical connection must be healthy and transaction-idle.
If a returned connection contains an open/failed transaction, close it and fail
that acquisition rather than implicitly committing or silently rolling back work.
The next acquisition can create a replacement.

Constructor validation uses the existing persistent `warmup_error()` channel,
including invalid configuration on a lazy pool. A warmup connection/setup failure
also retains existing persistent failure behavior. A lazy/reused connection setup
failure discards that connection, releases its reserved pool slot, wakes waiters,
and returns the original diagnostic; it does not poison the whole pool. No failed
setup connection enters the idle list or reaches a borrower.

Do network setup outside the pool mutex, while preserving slot accounting and
close/acquire races. Lease return and destructors perform no timeout SQL. A
configured baseline is guaranteed at checkout, not throughout caller-owned raw
setting changes. With the option absent, existing pool behavior is unchanged.
`acquire_timeout` keeps its existing meaning: pool-capacity waiting, not a deadline
for connection establishment or server configuration.

## Transaction-bound scope

Require an existing session-managed transaction on the pinned connection. Reject
autocommit entry, invalidated transactions, completed/poisoned scopes, and new
transaction entry while an enclosing timeout scope is still unwinding. No implicit
transaction or savepoint is created, committed, or rolled back by this helper.
An aborted transaction cannot admit a scope; a snapshot failure is returned and
the callback is skipped without applying an override.

Read the effective numeric value from `pg_catalog.pg_settings` for
`statement_timeout` (unit `ms`), then bind the override through
`SELECT pg_catalog.set_config('statement_timeout', $1, true)`. Capture the actual
effective value, including an earlier `SET LOCAL`, rather than assuming the pool
baseline. Nesting saves and restores values in stack order; zero is a valid inner
override. Restore with the same local setter while the original transaction lives.

A scope owns a lease pin independently of transaction/advisory pins, retains the
session recursive mutex through its synchronous callback, and records the original
transaction generation. Every frame releases its pin exactly once. The pool and
session must outlive their scopes. Do not wait for another thread to use the same
session or for an unavailable lease from its pool inside a callback.

### Transaction and savepoint actions

| Action during callback | Required behavior |
| --- | --- |
| Commit or full rollback of the original transaction | Allow; PostgreSQL ends its local setting. Retain the scope's lease pin until callback exit |
| Work after that transaction ends but before all its timeout frames exit | Reject managed queries, direct `observed_execute`, new transactions, and new timeout/advisory scope entry before SQL; void connection callbacks throw `logic_error` |
| Unwinding frames after successful transaction completion | Skip restoration SQL; never apply a saved transaction-local value to the session baseline |
| `savepoint` / `release_savepoint` while the transaction is live | Preserve their existing behavior |
| `rollback_to_savepoint` while any timeout frame is live | v1 rejects with `query_failed` before SQL; a healthy transaction and override remain usable |
| Raw transaction control, raw savepoint rollback, or manual timeout mutation | Caller-owned escape hatch; prohibited within the managed scope contract |

Savepoint rollback is deliberately restricted. It can remove an active override or
resurrect an inner override after that inner scope has already restored its parent.
Supporting it safely needs coordinated savepoint/setting history, rather than a
name-only check. Perform partial rollback after leaving the timeout scope. This
restriction does not block full rollback after cancellation.

Normal transaction completion is not a cleanup failure: return the callback's
result after frames unwind. If a commit fails without ending the transaction,
normal restoration/failed-cleanup rules still apply. A commit reported as rollback
keeps the existing session error. Existing advisory scopes may perform their
necessary unlock cleanup, and continue after all completed timeout frames exit.

## Failure and exception rules

| Exit | Required behavior |
| --- | --- |
| Snapshot read fails/throws before mutation | Skip callback; propagate diagnostic/exception. No override needs restoring; caller still owns transaction cleanup |
| Applying the local setting fails/throws | Ownership is uncertain: close/discard the connection, invalidate transaction ownership, skip callback |
| Callback success/error, original transaction healthy | Restore previous effective local value; return callback result |
| Callback throws, transaction healthy | Restore before propagating the same exception |
| Restoration fails, including an aborted transaction | Close/discard; invalidate transaction generation; reject reuse until all enclosing scopes unwind |
| Callback error and restoration error result | Preserve callback kind/SQLSTATE/message, append cleanup information to `detail` |
| Callback success and restoration error result | Return the cleanup error |
| Adapter restoration throws on normal callback exit | Close/discard and propagate the exception |
| Callback exception plus cleanup error/exception | Close/discard and preserve the original callback exception |

A failed restoration cannot return a connection with an unknown setting to the
pool. Closing it aborts its transaction and releases session advisory locks. Old
transaction guards cannot finish later transactions. Share poisoning and ownership
state across advisory/timeout scopes: an inner failure cannot be ignored to make
an outer scope report success. Guards skip SQL on discarded connections. A session
becomes reusable only after all enclosing scopes unwind.

After cancellation, a callback can explicitly roll back the transaction and return
its original error; then no restoration SQL is needed. Without that rollback, a
failed restoration discards the aborted transaction. No implicit recovery savepoint
or retry hides the cancellation.

## Adapter boundary and coverage

The PostgreSQL adapter implements this protocol, with exact result types:

```cpp
result<std::chrono::milliseconds> statement_timeout();
result<void> set_statement_timeout(std::chrono::milliseconds);       // idle/session
result<void> set_local_statement_timeout(std::chrono::milliseconds); // active transaction
bool is_transaction_idle() const noexcept;
void close() noexcept;
```

The session setter requires an idle healthy connection; the local setter requires
a healthy active transaction and rejects autocommit before SQL. Both validate the
range. Missing capabilities reject explicit configuration instead of ignoring it.
Vendor SQL, libpq transaction-status checks, and setting decoding stay in the
PostgreSQL adapter. Core/pool templates contain no PostgreSQL SQL. Optional pool
configuration is appended to `pool_config`; rebuild consumers after its layout
change. Custom adapters/sessions opt in only when they satisfy the same semantics.

The server setting applies to prepared/native/typed queries, raw SQL on that
connection, DDL, advisory waits, and COPY statements while their scope is valid.
It limits each server statement, not decoding, C++ work, pool waiting, an entire
callback, or all client-side COPY transfer time. Configuration/snapshot/restoration
SQL bypasses session observation; managed query events retain their current
coverage and original diagnostics. SQLSTATE `57014` also represents explicit
cancellation, so do not relabel every occurrence as a timeout.

## Acceptance and delivery

| Slice | Required executable checks |
| --- | --- |
| Pool baseline — implemented | Absent option adds no SQL on every backend; zero/max valid; invalid values and unsupported adapters fail before connect, including lazy pools; warmup/new/idle acquisition see baseline; borrower mutation is reset on the same PID; failed setup/active returned transaction discards and frees slots; pool close during setup returns no lease; initialization outside mutex lets unrelated returns progress |
| Scoped success | Admission gates before SQL/callback; explicit transaction and one-connection pool; actual prior local value restored; nested overrides including zero; move-only `result<T>`; no observer/control event changes |
| Scope completion | Commit/rollback inside callback retain scope pin; post-completion work rejected; no restoring a local value into session state; all nested frames unwind; next lease sees baseline; existing advisory owner can continue afterward |
| Scope failures | Callback result/exception, cancellation `57014`, aborted snapshots `25P02`, rejected restoration, uncertain apply, restoration error/throw, ignored inner errors, stale guards, and rollback failure; exactly-once pin release and reuse after unwind |
| Savepoints | Create/release supported; managed rollback rejected before SQL without poisoning; native probes show both undo and resurrection hazards |
| Integration | PostgreSQL server timeout through native/typed/prepared execution and raw DDL/COPY/advisory paths; snapshot/control observation absent; SQLite/MySQL rejection and existing suites remain green |

Pool/adapter support has [unit](../tests/unit/test_pool_timeout.cpp) and
[PostgreSQL integration](../tests/integration/test_statement_timeout.cpp) coverage;
see the audit for commands and observed results. Transaction-bound scopes now have
[unit](../tests/unit/test_timeout_scope.cpp) and live PostgreSQL coverage of the
remaining boundaries, plus unsupported-backend checks. The native semantic probe
is historical server evidence, not API acceptance. A combined locking/timeout
[acceptance review](locking-timeout-acceptance.md) accepts this revised v1 contract
with explicit boundaries and fresh check results.

## Evidence and references

The [native probe](evidence/statement-timeout-design/README.md) passed nine checks
on PostgreSQL 16.14: ineffective autocommit local setting, nested restoration,
transactional session settings, both savepoint hazards, transaction completion,
failed-transaction cleanup, numeric bounds, and restoration of the original value.
It does not exercise the ORM API; the runtime tests above provide that coverage.

PostgreSQL documents the underlying [setting lifetimes](https://www.postgresql.org/docs/16/sql-set.html),
[statement timeout](https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-STATEMENT-TIMEOUT),
and [numeric setting metadata](https://www.postgresql.org/docs/16/view-pg-settings.html).
The restrictions, ownership policy, and delivery order above are AsterORM design
decisions based on those semantics and the recorded probe.
