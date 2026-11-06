# AsterORM

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B-336791.svg)](https://www.postgresql.org/)
[![SQLite](https://img.shields.io/badge/SQLite-3.35%2B-003B57.svg)](https://www.sqlite.org/)
[![Meson Build](https://img.shields.io/badge/Meson-Build-green.svg)](https://mesonbuild.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A modern C++23 ORM with connection pooling, CRUD, and composable SQL APIs. PostgreSQL is the primary backend; SQLite is supported for embedded use and zero-setup tests.**

AsterORM is a modern, data-mapper style Object Relational Mapper for C++23. By heavily leveraging modern C++ features like `std::expected` and compile-time reflection via pointer-to-members, it keeps your domain entities as plain structs without any intrusive inheritance or persistence logic.

## Documentation

Start with the [documentation guide](docs/README.md), or go directly to
[tutorials](docs/tutorials/README.md), [how-to guides](docs/how-to/README.md),
[reference](docs/reference/README.md), and [explanation](docs/explanation/README.md).

## Features

* **Data Mapper Architecture:** Entities remain plain `C++` structs. No magic base classes.
* **Compile-Time Traits:** Define mappings using simple pointer-to-member templates (`&User::id`). No macros required.
* **PostgreSQL Native:** Built directly on top of `libpq` using text parameters/results, per-connection prepared statement caching, and `RETURNING` for generated values.
* **SQLite Backend:** First-class `sqlite::driver` over the `sqlite3` C API. Same pool/session/repository templates as PostgreSQL; supports file paths, `file:` URIs, and `:memory:` for tests. Bool parameters bind as `INTEGER 1/0`, `BEGIN`/`BEGIN IMMEDIATE` is selected from `transaction_options`, and the prepared-statement cache honors `pool_config::prepared_statement_cache_size`.
* **Connection Pooling:** Built-in connection pool with timeout leasing, explicit close, basic statistics, and open-connection checks.
* **Transactions and Migrations:** Scoped `db.begin()` guards, callback-based transaction helpers, bounded transaction retries, and a small schema migration runner.
* **CRUD:** Insert, find, update, erase, patch, single/bulk insert, single/bulk upsert, single/composite primary keys, generated-column refresh, and optional optimistic locking through version-column traits.
* **SQL AST:** `select({"id"}).from("users").where(col("active") == val(true))` style SQL generation with parameterized values.
* **Native Escape Hatch:** `db.native<User>("SELECT * FROM users WHERE active = $1", true)` hydrates mapped structs from raw SQL. Scalar and row-mapper helpers are also available.
* **PostgreSQL Extras:** Supports `JSONB` text wrappers, simple arrays (`std::vector<T>`), nullable array elements, UUID/bytea/timestamp/date/time/numeric codecs, enum label traits, and raw or structured `COPY IN/OUT` helpers.

> ClickHouse adapter is experimental and disabled by default (`-Dclickhouse=disabled`). It emulates parameterized queries via string interpolation; do not use with untrusted input without additional validation.

## Current Scope

AsterORM v0.1 covers a small but production-shaped surface:

* connection pooling with exclusive leases
* explicit transactions and savepoints
* entity traits for plain structs
* basic CRUD (`insert`, `find`, `update`, `erase`, `patch`, `upsert`, `insert_many`, `upsert_many`)
* single-column and tuple-based composite primary keys for repository CRUD/upsert paths
* generated-value refresh through `RETURNING` on mutable insert/upsert/update paths
* optimistic locking through an optional `version_column` entity trait
* transaction helpers and schema migrations
* PostgreSQL backend (`pg::driver`): codecs for common scalars, optional values, arrays, nullable array elements, JSONB, UUID, bytea, timestamp, date, time, numeric, and enum labels; structured errors with SQLSTATE and libpq diagnostics; raw or structured `COPY IN/OUT`
* SQLite backend (`sqlite::driver`): file/URI/`:memory:` databases via `sqlite3_open_v2`, foreign keys on by default, 5s busy timeout, native bool binding (`INTEGER 1/0`), bounded LRU prepared-statement cache, `transaction_options`-aware `BEGIN`/`BEGIN IMMEDIATE`
* raw SQL mapping through `native<T>()`, `native_scalar<T>()`, and `native_map()`
* a small SQL AST/compiler for parameterized statements
* typed SELECT predicates with compile-time operand checks; boolean columns accept
  boolean operands, and text columns accept string literals as bound text parameters
* inferred tuple projections through `sql::typed::select_cols(...)`, including
  optional columns and aggregate projected types, alongside explicit DTO projections
* session query-observer events for repository, native, and typed query execution
* an [incremental typed-query migration guide](docs/typed-query-migration.md) with
  executable examples for PostgreSQL, SQLite, and MySQL

Not implemented yet: lazy/eager loading, async APIs, SQL parsing, raw SQL type checking, streaming cursors for large result sets, and a production-ready identity map. Relationship rows are loaded explicitly with `load_related_one` / `load_related_many`. The typed DSL covers `select`, filters, order, limit, `group_by`, `having`, and aggregates. PostgreSQL binary protocol and SQLite-side bulk-load helpers (analogous to `COPY`) are also not implemented yet.

## Security

Values are bound as parameters on PostgreSQL, SQLite, and MySQL. Table and column names are quoted for the active dialect, and a name that contains a NUL byte is rejected. `func()` accepts only a bare function name. `raw_expr`, `native()`, and `COPY` SQL are caller-owned statement text.

PostgreSQL simple `execute` can run more than one statement in that caller-owned text. SQLite rejects trailing statements. MySQL does not enable multi-statement execution, and after every simple query it restores `ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, and `time_zone = '+00:00'` before the next parameterized call.

The ClickHouse adapter is experimental, off by default, and interpolates parameter literals. Do not use it with untrusted input. A failed pool warmup is reported by `warmup_error()` and by `acquire()`; the pool does not pretend that `min_size` connections were opened.

## Quick Example

```cpp
#include <asterorm/session.hpp>
#include <asterorm/repository.hpp>
#include <optional>
#include <string>

// 1. Define a plain C++ struct
struct User {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
};

// 2. Define the mapping traits
template <>
struct asterorm::entity_traits<User> {
    static constexpr const char* table = "users";
    static constexpr auto primary_key = asterorm::pk<&User::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&User::id>("id", asterorm::generated::by_default),
        asterorm::column<&User::email>("email"),
        asterorm::column<&User::name>("name"),
        asterorm::column<&User::active>("active")
    );
};

// 3. Start querying!
int main() {
    asterorm::pool_config cfg{.conninfo = "host=127.0.0.1 user=admin password=admin dbname=app"};
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    
    // Insert (automatically populates the generated ID)
    User u{.email = "alice@example.com", .name = "Alice"};
    repo.insert(u);
    
    // Find
    auto loaded = repo.find<User>(*u.id).value();
    
    // Raw SQL fallback with automatic struct hydration
    auto admins = db.native<User>("SELECT * FROM users WHERE active = $1", true).value();
    
    return 0;
}
```

## Typed Projections

To convert existing queries, follow the
[string-to-typed migration guide](docs/typed-query-migration.md).

For a mapped `User` and a session `db`, omit the result type to infer a tuple in
selection order:

```cpp
#include <asterorm/sql/typed_query.hpp>

namespace typed = asterorm::sql::typed;
auto rows = typed::query(db,
    typed::select_cols(typed::col<&User::email>, typed::col<&User::active>)
        .where(typed::col<&User::active> == typed::val(true)));
// result<vector<tuple<string, bool>>> for string email and bool active fields.
```

Optional fields remain optional in the tuple. Aggregate items use their existing
projected types, for example `count_all()` produces `std::int64_t`. Include at
least one mapped column or entity-bound aggregate to determine the table, and
keep all entity-bound items on the same entity. Use `select_cols<DTO>(...)` when
you want a named result struct instead.

Plain built-in column projections lower to `select_ast::columns`. Aggregates,
mixed projections, and custom selectables lower to `select_ast::items`, preserving
custom `to_select_item()` aliases and expressions.

## Getting Started

### Prerequisites

* C++23 compliant compiler (GCC 13+, Clang 16+, MSVC v19.38+)
* [Meson Build System](https://mesonbuild.com/) (>= 1.1.0)
* For the PostgreSQL backend: PostgreSQL development headers (`libpq`)
* For the SQLite backend: SQLite 3.35+ development headers (`sqlite3`)

Both backends are enabled by default and discovered via `pkg-config`. Either can be turned off with `-Dpostgres=disabled` / `-Dsqlite=disabled`.

### Building

```bash
# Configure the build directory (both backends auto-detected)
meson setup buildDir

# Compile the library
meson compile -C buildDir

# Run the full test suite (PostgreSQL integration tests need a live database;
# SQLite integration tests use :memory: and need no external services)
meson test -C buildDir

# Or run only the SQLite suite to skip the live database requirement
meson test -C buildDir --suite asterorm sqlite\ integration
```

### Installing

```bash
meson setup buildDir --prefix=/usr/local
meson install -C buildDir
```

Installation provides headers, `libasterorm_core`, enabled adapter libraries, `pkg-config` metadata (`asterorm.pc`), and a CMake package config:

```cmake
find_package(AsterORM CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE AsterORM::asterorm)
```

## PostgreSQL Type Mapping Notes

Use `asterorm::date`, `asterorm::time_of_day`, and `asterorm::numeric` for PostgreSQL `DATE`, `TIME`, and exact `NUMERIC`/`DECIMAL` values. `numeric` stores the database text representation so exact decimal scale is not rounded through binary floating point.

PostgreSQL enum labels are mapped with `asterorm::enum_traits<T>`:

```cpp
template <> struct asterorm::enum_traits<UserStatus> {
    static std::string_view to_db(UserStatus);
    static std::optional<UserStatus> from_db(std::string_view);
};
```

Arrays map to `std::vector<T>`. Nullable array elements map to `std::vector<std::optional<T>>`; SQL `NULL` elements decode to empty optionals. Multidimensional arrays are still out of scope.

For PostgreSQL text `COPY`, `pg::copy_row` models a row as `std::vector<std::optional<std::string>>`. Use `connection::copy_in_rows()` and `connection::copy_out_rows()` when you want AsterORM to handle tab/newline/backslash escaping and SQL `NULL` fields. The lower-level `copy_in()` and `copy_out()` methods remain available for callers that already own raw COPY lines.

## Composite Primary Keys

Single-column keys use the original `primary_key = pk<&T::id>("id")` mapping. Composite keys use a tuple of key parts:

```cpp
static constexpr auto primary_key = std::make_tuple(
    asterorm::pk<&OrderLine::order_id>("order_id"),
    asterorm::pk<&OrderLine::line_no>("line_no")
);
```

Use a tuple-like key when loading or deleting:

```cpp
auto line = repo.find<OrderLine>(std::make_tuple(100, 1)).value();
repo.erase<OrderLine>(std::make_tuple(100, 1)).value();
```

A `compose.yaml` file is provided to quickly spin up a PostgreSQL test database. The justfile wraps it for one-shot integration runs:
```bash
# Bring the database up (waits for healthcheck), run the PG integration suite, leave it running
just db-up
just test-pg

# Or run everything end-to-end (brings DB up, runs all tests, tears DB down on success)
just test-all
```

The SQLite integration tests run entirely against `:memory:` and require no external services.

## Choosing a Backend

```cpp
// PostgreSQL
asterorm::pool_config cfg{.conninfo = "host=127.0.0.1 user=app dbname=app"};
asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};

// SQLite (file)
asterorm::pool_config cfg{.conninfo = "app.db"};
asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, cfg};

// SQLite (in-memory, ideal for tests)
asterorm::pool_config cfg{.conninfo = ":memory:", .min_size = 1, .max_size = 1};
asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, cfg};
```

The `session<Pool>` and `repository<Session>` templates are identical across backends; only the driver type changes.

## Developer Tooling

This project uses `clang-format` and `clang-tidy` for code quality, configured via [Just](https://just.systems/man/en/).

```bash
# Format all C++ files
just format

# Run static analysis
just lint

# Run format checks and tests
just check
```

## SQL DSL Benchmark

An opt-in, database-free benchmark compares typed and string query construction,
SQL compilation, and allocations in an optimized build. See
[benchmark instructions and recorded results](benchmarks/README.md). The recorded
baseline found extra typed-builder construction allocations. Reserving known
selection sizes removes those growth allocations, and compact storage for plain
typed projections removes the measured excess allocation bytes. All current
benchmark cases are within the timing/allocation investigation screen; the
measurements do not establish a blanket claim of unchanged runtime cost.

A separate [row-decoding benchmark](benchmarks/ROW_DECODING.md) compares the
production typed/string tuple decoders using native PostgreSQL, SQLite and MySQL
result classes populated in memory. The recorded cases have matching allocation
counts/bytes and no material typed slowdown; database execution and networking
remain outside these measurements.

## License

Distributed under the MIT License.

## MySQL

The optional MySQL adapter targets **MySQL 8.0.19 or newer** and uses the MySQL
client C API with native server prepared statements. Enable it explicitly:

```bash
# Debian/Ubuntu: sudo apt install default-libmysqlclient-dev
# macOS: brew install mysql-client
meson setup buildDir --reconfigure -Dmysql=enabled
meson compile -C buildDir
just test-mysql
```

Meson first looks for a system `mysqlclient` through pkg-config or Homebrew.
If it is unavailable, Meson downloads and builds the pinned MariaDB Connector/C
3.4.11 wrap, which provides the MySQL C API. The bundled build needs CMake,
OpenSSL development files and zlib development files; it includes the MySQL
`caching_sha2_password` and `sha256_password` authentication plugins directly.
On Debian/Ubuntu install `cmake libssl-dev zlib1g-dev`; on macOS install
`cmake openssl@3` with Homebrew.

To select the bundled client even when a system client is installed:

```bash
meson setup buildDir --reconfigure -Dmysql=enabled --force-fallback-for=mysqlclient
```

`--wrap-mode=nofallback` requires a system client. `-Dmysql=auto` allows dependency
resolution (including the wrap); `-Dmysql=disabled` skips client discovery and
downloads. The Meson dependency is `asterorm_mysql_dep`; installed CMake consumers
link `AsterORM::mysql`. Installation includes the bundled client's matching
headers, shared library, license, and `asterorm_mysqlclient.pc` when selected.
Add the installation's pkg-config directory to `PKG_CONFIG_PATH` when using a
custom prefix. For a system Homebrew mysql-client, use its `lib/pkgconfig` path.

MariaDB Connector/C can connect to MySQL servers; choosing it does not require a
MariaDB server or change the `asterorm::mysql::driver` API. Percona Server for
MySQL is expected to work through the same protocol, but is not covered by the
current integration tests. Backend implementations live under
`src/database/{postgres,sqlite,mysql,clickhouse}`; public include paths are unchanged.

```cpp
#include <asterorm/mysql/driver.hpp>
#include <asterorm/repository.hpp>

asterorm::pool_config cfg{
    .conninfo = "host=127.0.0.1 port=3306 dbname=app user=app password='with spaces'"};
asterorm::connection_pool<asterorm::mysql::driver> pool{asterorm::mysql::driver{}, cfg};
asterorm::session db{pool};
asterorm::repository repo{db};
```

Connection options are whitespace-separated `key=value` pairs: `host`, `port`,
`user`, `password`, `dbname`, `unix_socket`, and `connect_timeout` (seconds,
5 by default). Values may be quoted with single or double quotes; backslash
escapes the next character. Unknown, duplicate, malformed, or invalid numeric
options fail before connecting. The client library's default TLS policy applies.

The same repository and typed SELECT APIs work with MySQL. The SQL compiler
emits backtick-quoted identifiers and `?` parameters. Native parameterized SQL
also accepts `$1`, `$2`, including repeated and reordered placeholders. Values
are bound through the C API; they are never interpolated into SQL. Each call
accepts one statement. SQLSTATE and native error numbers are preserved in errors.

MySQL differences:

- `insert()` refreshes a generated integral primary key and server defaults.
  Mutable `update()` refreshes the row, including optimistic-lock versions.
  These operations use a write followed by a primary-key SELECT on the same
  leased connection, scoped by a transaction or a savepoint inside an existing
  transaction. Use InnoDB tables for rollback and locking guarantees.
- Generated-key refresh supports one integral generated primary key. Composite
  keys must be supplied explicitly. Bulk `insert_many()` is one multi-row
  statement and leaves generated keys unset, as on the other backends.
- `upsert()` requires explicit primary keys and uses MySQL's `ON DUPLICATE KEY
  UPDATE`. Any unique index can trigger MySQL's duplicate handling. If the result
  cannot be found by the supplied key, the operation fails and rolls back its
  savepoint/transaction. `upsert_many()` with entity refresh is unsupported and
  fails before writing; use individual upserts in an explicit transaction.
- The compiler rejects `RETURNING`, targeted `ON CONFLICT`, and `FULL JOIN`.
  PostgreSQL-specific COPY, arrays, and other vendor-specific types have no MySQL
  mapping. Scalar text/numeric values, booleans, NULL, enums and compatible
  date/time values use the shared codecs. The `bytea` codec is PostgreSQL-specific;
  native string parameters/results preserve embedded NUL bytes when needed.
- Isolation levels, read-only transactions, commit, rollback, and savepoints are
  supported. `DEFERRABLE` is rejected. `CLIENT_FOUND_ROWS` gives ordinary UPDATE
  operations matched-row semantics, including unchanged rows.
- The generic `schema_migrator` is rejected before creating its history table:
  MySQL DDL implicitly commits, so its atomic migration contract cannot be met.
  Apply MySQL schema SQL through `connection::execute()` with explicit tracking.
- Connections use `utf8mb4` and add `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES` to
  their session SQL mode. Keep those modes enabled; use single quotes and doubled
  single quotes in native SQL literals. Parameter values need no SQL escaping.

`just mysql-up` starts the opt-in Compose service on port 3307. Integration tests
use `ASTERORM_MYSQL_TEST_CONNINFO` or the local Compose defaults and fail if the
server is unavailable. When the adapter is enabled, start this service before
running the full `just test` or `just test-all` suite. CI exercises MySQL 8.4
alongside PostgreSQL and SQLite.
