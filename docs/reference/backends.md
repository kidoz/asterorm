# Backend capabilities and connection settings

SELECT row locking (`FOR UPDATE` / `FOR SHARE`, `NOWAIT`, `SKIP LOCKED`) is
available through both builders for PostgreSQL and MySQL's transactional tables.
SQLite and ClickHouse reject the locking AST. See the
[locking guide](../how-to/select-locking.md) for the restricted query shapes.

Scoped advisory locks are PostgreSQL-only. Other built-in adapters reject
`with_advisory_lock` before leasing; see the
[advisory locking guide](../how-to/advisory-locking.md).

## Capability matrix

| Capability | PostgreSQL | SQLite | MySQL | ClickHouse |
| --- | --- | --- | --- | --- |
| Default build | Enabled | Enabled | Disabled | Disabled |
| Client | libpq | sqlite3 | mysqlclient or bundled Connector/C | clickhouse-cpp |
| Value binding | Native parameters | Native parameters | Native prepared statements | Interpolated literals |
| Native single-row reader | Yes | No | No | No |
| Native declared-cursor batches | Yes, transaction required | No | No | No |
| Repository entity stream | Single-row or cursor mode | Rejected | Rejected | Rejected |
| Mutable single-row refresh | `RETURNING` | `RETURNING` | Write plus key lookup | Experimental |
| INSERT-from-SELECT | Yes, restricted v1 | Yes, restricted v1 | Yes, restricted v1 | Rejected |
| Predicate UPDATE/DELETE | Yes, restricted v1 | Yes, restricted v1 | Yes, restricted v1 | Rejected |
| Bulk insert | Yes, no entity refresh | Yes, no entity refresh | Yes, no entity refresh | Experimental |
| Bulk upsert with refresh | Yes | Yes | Rejected | Experimental |
| Transaction-bound statement timeout scopes | Yes | Rejected | Rejected | Rejected |
| Pool statement timeout baseline | Yes | Rejected | Rejected | Rejected |
| Scoped advisory locks | Yes | Rejected | Rejected | Rejected |
| Generic schema migrator | Yes, serialized | Yes, externally coordinated | Rejected | Experimental |
| PostgreSQL COPY helpers | Yes | No | No | No |

Use PostgreSQL 16+ for the supplied development service, SQLite 3.37+ for
64-bit change counts and `RETURNING`, and MySQL 8.0.19+ for the MySQL adapter. CI uses MySQL 8.4.

Relational result `affected_rows()` getters return `std::uint64_t`. Rebuild the
library and consumers together after this public type/layout change. See the
[predicate mutation guide](../how-to/bulk-predicate-mutations.md) for mapping,
versioned-entity, capability, and query-shape limits.

## PostgreSQL

Driver: `asterorm::pg::driver`, header `<asterorm/postgres/driver.hpp>`.
`pool_config::conninfo` is passed to libpq, for example
`host=127.0.0.1 port=5432 dbname=app user=app`. Use libpq connection settings
appropriate to your environment, including TLS settings for remote servers.

Parameters and results use text format. Each connection owns a bounded prepared
statement cache. PostgreSQL-specific extras include arrays, JSONB, UUID, bytea,
date/time/numeric codecs, enum labels, and raw or structured COPY IN/OUT through
`postgres/copy.hpp`. There is no binary protocol or pipeline API.

`open_single_row(sql, parameters)` returns a native `single_row_reader` with
`next()` yielding `result<optional<pg::result>>`. Normal EOF completes the protocol;
early close, destruction or failure discards the physical connection. The adapter
does not hydrate entities or emit session query events. See the
[native reader guide](../how-to/postgresql-single-row-reader.md). The
[repository bridge](../how-to/stream-entities.md) adds mapped entities and terminal
observation. `open_cursor(sql, parameters, chunk_size)` returns a `cursor_reader`
whose `next()` owns at most `chunk_size` native rows. It requires a healthy
transaction, uses a private quoted NO SCROLL / WITHOUT HOLD portal and closes it
on empty fetch. Explicit `close()` retains the transaction when successful;
unfinished destruction and failures discard. Both strategies bypass the prepared
cache and block competing native work. Rebuild consumers after the connection layout change.

PostgreSQL connections additionally expose `statement_timeout()` returning
`result<chrono::milliseconds>`, `set_statement_timeout(milliseconds)` for an idle
connection, and `set_local_statement_timeout(milliseconds)` for a healthy active
transaction. Both setters validate 0 through 2,147,483,647 ms before SQL.
`is_transaction_idle() const noexcept` reports the physical connection state.
The getter uses numeric server metadata rather than parsing human-readable units.
These control operations bypass session observation and provide no automatic
restoration. Use [session timeout scopes](../how-to/statement-timeouts.md) for
automatic restoration within an existing transaction.

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
