# Backend capabilities and connection settings

SELECT row locking (`FOR UPDATE` / `FOR SHARE`, `NOWAIT`, `SKIP LOCKED`) is
available through both builders for PostgreSQL and MySQL's transactional tables.
SQLite and ClickHouse reject the locking AST. See the
[locking guide](../how-to/select-locking.md) for the restricted query shapes.

## Capability matrix

| Capability | PostgreSQL | SQLite | MySQL | ClickHouse |
| --- | --- | --- | --- | --- |
| Default build | Enabled | Enabled | Disabled | Disabled |
| Client | libpq | sqlite3 | mysqlclient or bundled Connector/C | clickhouse-cpp |
| Value binding | Native parameters | Native parameters | Native prepared statements | Interpolated literals |
| Mutable single-row refresh | `RETURNING` | `RETURNING` | Write plus key lookup | Experimental |
| Bulk insert | Yes, no entity refresh | Yes, no entity refresh | Yes, no entity refresh | Experimental |
| Bulk upsert with refresh | Yes | Yes | Rejected | Experimental |
| Generic schema migrator | Yes | Yes | Rejected | Experimental |
| PostgreSQL COPY helpers | Yes | No | No | No |

Use PostgreSQL 16+ for the supplied development service, SQLite 3.35+ for
`RETURNING`, and MySQL 8.0.19+ for the MySQL adapter. CI uses MySQL 8.4.

## PostgreSQL

Driver: `asterorm::pg::driver`, header `<asterorm/postgres/driver.hpp>`.
`pool_config::conninfo` is passed to libpq, for example
`host=127.0.0.1 port=5432 dbname=app user=app`. Use libpq connection settings
appropriate to your environment, including TLS settings for remote servers.

Parameters and results use text format. Each connection owns a bounded prepared
statement cache. PostgreSQL-specific extras include arrays, JSONB, UUID, bytea,
date/time/numeric codecs, enum labels, and raw or structured COPY IN/OUT through
`postgres/copy.hpp`. There is no binary protocol or pipeline API.

## SQLite

Driver: `asterorm::sqlite::driver`, header `<asterorm/sqlite/driver.hpp>`.
Connection settings accept a file path, `file:` URI, or `:memory:`. Private
in-memory databases belong to individual connections; use one pool connection
when all operations must see the same private database.

The adapter enables foreign keys and a five-second busy timeout. Booleans bind
as integer 1/0; result rows are materialized. Statements reject trailing SQL.
Transaction isolation requests have the mapping described in the
[transaction guide](../how-to/transactions.md). COPY is unsupported.

## MySQL

Driver: `asterorm::mysql::driver`, header `<asterorm/mysql/driver.hpp>`.

`conninfo` accepts whitespace-separated `key=value` pairs:

| Key | Meaning |
| --- | --- |
| `host` | Server host |
| `port` | Server port |
| `user` | Login user |
| `password` | Login password |
| `dbname` | Database |
| `unix_socket` | Socket path |
| `connect_timeout` | Seconds; default 5 |

Values may use single or double quotes; a backslash escapes the next character.
Unknown keys, duplicates, malformed pairs, and invalid numeric settings fail
before connection. TLS behavior follows the selected client library's defaults.

The adapter uses `utf8mb4`, `ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, and UTC session
time. Native parameterized SQL accepts `$1`, `$2`, including repeated or reordered
references; compilation emits positional `?` parameters. Calls accept one
statement. Simple queries restore the required SQL modes and UTC time zone
before the next parameterized call.

Mutable single-row refresh uses a transaction or savepoint around the write
and primary-key read on the same lease. Generated insert keys support one
integral generated primary key. Supply composite keys explicitly. Use InnoDB
for transaction guarantees.

`upsert()` requires explicit primary keys and uses `ON DUPLICATE KEY UPDATE`;
any unique index can trigger duplicate handling. If the row cannot be found
by the supplied key, refresh fails and its transaction/savepoint rolls back.
`upsert_many()` with refresh fails before writing. `RETURNING`, targeted AST
`ON CONFLICT`, `FULL JOIN`, deferrable transactions, and generic atomic schema
migrations are unsupported. PostgreSQL COPY and vendor-specific array/bytea
representations have no MySQL mapping.

## ClickHouse

Driver: `asterorm::ch::driver`, header `<asterorm/clickhouse/driver.hpp>`.
Enable with `-Dclickhouse=enabled`. This adapter is experimental and emulates
parameters by interpolating literals. Do not use it with untrusted input.
Do not infer PostgreSQL-style transaction or CRUD guarantees from the shared
template interface.
