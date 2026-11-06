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
constructor argument to choose another table. Run migrations from one controlled
process; this runner does not coordinate concurrent migration executors or
verify content checksums.

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
