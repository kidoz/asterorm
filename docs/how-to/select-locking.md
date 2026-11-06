# Lock selected rows

Both SELECT builders provide `for_update()`, `for_share()`, `nowait()`, and
`skip_locked()`. Support is PostgreSQL 16+ and MySQL 8.0.19+ transactional tables;
integration checks use PostgreSQL 16 and MySQL 8.4/InnoDB. SQLite and ClickHouse
reject the locking AST before execution.

Include `<asterorm/sql/typed_query.hpp>`. Given a mapped `Job::id` and session
`db`, use this inside a function returning `asterorm::result<void>`:

```cpp
namespace typed = asterorm::sql::typed;
auto transaction = db.begin();
if (!transaction)
    return std::unexpected(transaction.error());
auto jobs = typed::query(db, typed::select<Job>()
    .where(typed::col<&Job::id> >= typed::val(1))
    .order_by(typed::col<&Job::id>)
    .limit(10).for_update().skip_locked());
if (!jobs)
    return std::unexpected(jobs.error());
// Read/modify these jobs through this session before ending the transaction.
return transaction->commit();
```

The guard rolls back on early returns/exceptions. There is no implicit
transaction. SELECT does not persist a claimed/processed state itself, and
returning selected objects does not retain their locks after commit/rollback.
Lock scope can include index ranges or gaps, not just returned physical records.

| Method | Behavior |
|---|---|
| `for_update()` | Exclusive row lock |
| `for_share()` | Shared row lock |
| Neither wait modifier | Ordinary database blocking behavior |
| `nowait()` | Fail on a conflicting row lock |
| `skip_locked()` | Omit conflicting rows |

`SKIP LOCKED` is useful for queues but does not promise a complete consistent
view. Wait modifiers concern row locks; other locks or database work can still
wait. Use suitable database timeouts for bounded blocking operations.
[PostgreSQL locking](https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE),
[MySQL locking reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html).

Strength and wait policy are independent, with the last call to each winning:
`for_update().nowait().for_share().skip_locked()` produces `FOR SHARE SKIP LOCKED`.
Modifiers may precede the strength call, but a wait modifier alone fails SQL
compilation with `parse_failed`.

Runtime builders use the same methods:

```cpp
namespace sql = asterorm::sql;
auto selection = sql::select({"id"}).from("jobs")
    .where(sql::col("id") >= sql::val(1)).order_by("id")
    .limit(10).for_update().skip_locked().build();
auto compiled = sql::compiler{sql::dialect::postgres}.compile(selection);
```

The clause follows `LIMIT`/`OFFSET`, preserving parameter values/order. Execute
it on the same session's pinned transaction connection. Choose `dialect::mysql`
for MySQL, `dialect::sqlite` for SQLite, or `dialect::clickhouse` for the
experimental adapter; the latter two reject locking. Native SQL remains
caller-owned and does not gain AST validation.

The initial locking API accepts one base table with whole entities, plain
columns, column aliases, or `*`. It rejects joins, all function projections
(scalar functions and aggregates), GROUP BY, and HAVING when locking is requested.
Unlocked queries retain their existing behavior. Use native SQL for more complex
locking, PostgreSQL `OF` lists, or other strengths.

The AST stores `select_lock_clause` with `select_lock_strength` and
`select_lock_wait`. Invalid enum values, missing strengths, unsupported dialects,
and unsupported shapes return `parse_failed`; this is not complete SQL/schema
validation. Typed locking requires `Session::connection_type` and an explicit
`Connection::sql_dialect` of `postgres` or `mysql`. Built-in adapters declare
their dialects. Unknown custom facades retain unlocked queries but must opt in
with this metadata for locking. The legacy PostgreSQL-compatible fallback is not
an opt-in. Rejections precede connection acquisition and observer callbacks.
Dialect checks do not inspect server versions, storage engines, or active
transaction state; callers must meet those requirements.

The new AST member changes its public layout: rebuild the library and consumers
together, and do not mix new headers with older binary libraries.
See the [locking audit](../locking-and-timeout-audit.md) for executed checks.
