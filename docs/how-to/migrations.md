# Apply and roll back schema migrations

Use `schema_migrator` with PostgreSQL or SQLite to apply ordered SQL changes
and record their versions. Start with a session `db` and include
`<asterorm/migration.hpp>`.

## Define and apply changes

Use unique, increasing positive versions and SQL appropriate to the selected
backend. This example works with PostgreSQL and SQLite:

```cpp
std::vector<asterorm::migration> migrations{
    {.version = 1,
     .name = "create contacts",
     .up_sql = "CREATE TABLE contacts (id INT PRIMARY KEY, name TEXT NOT NULL)",
     .down_sql = "DROP TABLE contacts"},
    {.version = 2,
     .name = "add email",
     .up_sql = "ALTER TABLE contacts ADD COLUMN email TEXT",
     .down_sql = "ALTER TABLE contacts DROP COLUMN email"},
};

asterorm::schema_migrator migrator{db};
auto applied = migrator.apply(migrations);
if (!applied)
    return std::unexpected(applied.error());
// applied->current_version == 2; applied->applied_count == 2 on a fresh database.
```

`apply()` sorts by version and skips versions at or below the current maximum.
Each migration's SQL and history entry run in their own transaction. Earlier
successful migrations remain applied if a later migration fails. Keep each
SQLite SQL string to one statement; its adapter rejects trailing statements.

The default history table is `asterorm_schema_migrations`. Supply a second
constructor argument to choose another table. This runner does not verify content
checksums.

## Coordinate PostgreSQL executors

PostgreSQL `apply()`, `rollback_to()`, and `current_version()` automatically take
one exclusive session advisory lock before history-table creation or version
reads. They hold the same connection and lock across all per-migration commits,
then release it on success, error, or exception. A waiting executor reads the
current version after acquiring the lock. Separate processes are coordinated by
the server; no process-local mutex or table-name hash determines lock identity.

The reserved signed-bigint key is `0x41535445524f524d` (ASCII `ASTERORM`). All
AsterORM migration histories in one database share it, including custom schemas,
unqualified names, and differing search paths. This intentionally serializes
unrelated histories too. Reserve this key for migration control. Every executor
must use this locking protocol; older library versions and external schema tools
need their own coordination.

A session-level lock spans the existing per-migration commits. PostgreSQL
transaction advisory locks end when their transaction ends, so they cannot span
this operation without changing commit behavior.
[PostgreSQL advisory lock lifetimes](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS)

Use a dedicated session without an active transaction for these PostgreSQL
operations. A caller-owned transaction is rejected with `query_failed` before
history setup or waiting for the lock; it remains active. Do not invoke migration
operations recursively from a query observer. Migration SQL must leave transaction
control to the migrator. A lock wait can be bounded by the connection's server
timeout settings; the helper adds no separate timeout option.

If a later migration fails, earlier commits remain. The failed migration rolls
back before the advisory scope releases the lock. Observer exceptions also unwind
that transaction. The [advisory scope cleanup rules](advisory-locking.md) apply if
rollback or unlock cannot complete, including connection discard. A cleanup
failure can be reported after earlier migrations have committed; inspect history
before retrying.

SQLite keeps its existing per-migration transaction behavior and does not acquire
an advisory lock. Coordinate SQLite migration executors externally. Custom
sessions with advisory-capable connections must provide `with_advisory_lock` and
`has_active_transaction()` with the standard session contracts.

## Inspect or roll back

```cpp
auto version = migrator.current_version();
auto rolled_back = migrator.rollback_to(0, migrations);
```

Check both results before using them. Rollback executes supplied down SQL in
descending version order, removes the corresponding history entries, and
reports the remaining maximum version. Supply the full migration definitions
needed for the rollback. An empty `down_sql` fails when that migration is reached;
each earlier rollback may already have committed.

The MySQL adapter rejects this migrator before creating the history table
because its DDL can implicitly commit. Apply MySQL schema SQL through
`db.with_connection()` and `conn.execute()`, with migration tracking designed for
that backend's DDL semantics.
