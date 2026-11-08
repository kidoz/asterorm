# AsterORM

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![Meson Build](https://img.shields.io/badge/Meson-Build-green.svg)](https://mesonbuild.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

AsterORM is a C++23 data mapper with connection pooling, entity CRUD, and
composable SQL queries. Entities stay plain structs, with explicit mappings
based on pointers to members. Database operations return
`asterorm::result<T>` (`std::expected<T, db_error>`).

PostgreSQL is the primary backend. SQLite supports embedded applications and
tests without a server. MySQL is optional; ClickHouse support is experimental.

## Documentation

Start with the [documentation guide](docs/README.md), or choose a section:

| What you need | Where to go |
| --- | --- |
| A working first application | [Tutorials](docs/tutorials/README.md) |
| Steps for a specific task | [How-to guides](docs/how-to/README.md) |
| API contracts, build options, and backend capabilities | [Reference](docs/reference/README.md) |
| Architecture and design tradeoffs | [Explanation](docs/explanation/README.md) |

The [SQLite tutorial](docs/tutorials/first-application.md) covers installation,
schema creation, a full CRUD cycle, and typed queries. For existing applications,
see the [string-to-typed query migration guide](docs/typed-query-migration.md).

## Features

- [Shared pool/lease ownership and non-draining shutdown](docs/how-to/pool-ownership-and-shutdown.md),
  eager size/timeout validation and explicit closed/capacity-timeout errors.
- Plain struct entities with explicit column, generated-value, composite-key,
  version, and relationship mappings.
- CRUD, patches, bulk inserts, upserts, and optional optimistic locking.
- [Predicate UPDATE/DELETE and INSERT-from-SELECT](docs/how-to/bulk-predicate-mutations.md) with checked
  affected-row counts, bound addition/multiplication, and pre-lease validation on
  PostgreSQL, SQLite, and MySQL.
- Typed SELECT filters, ordering, pagination, grouping, aggregates, and inferred
  tuple or named DTO projections.
- Runtime SQL builders and native SQL with entity, scalar, or custom row mapping.
- [Native PostgreSQL single-row reads](docs/how-to/postgresql-single-row-reader.md)
  with owned row results, explicit completion and connection discard on abandonment.
- [Mapped entity streams](docs/how-to/stream-entities.md) on PostgreSQL with
  single-row or declared-cursor batches, owned hydration, structured late errors
  and one terminal query event.
- [Inline windows and typed helpers](docs/how-to/window-functions.md) for ranking,
  nullable navigation, checked ROWS frames and windowed aggregates.
- [Filtered aggregates and DISTINCT ON](docs/how-to/aggregate-extensions.md), with
  explicit backend support and row-selection tie policies.
- [Constrained recursive CTEs](docs/how-to/recursive-common-table-expressions.md)
  for parent/child traversal with checked anchor and recursive-step shapes.
- [Set operations](docs/how-to/set-operations.md), including ALL forms where supported,
  with explicit grouping, branch pagination and backend capability checks.
- [Ordinary common table expressions](docs/how-to/common-table-expressions.md) with
  owned query roots, shared bindings and explicitly named result mapping on PostgreSQL,
  SQLite and MySQL.
- [Scoped advisory locks](docs/how-to/advisory-locking.md) on PostgreSQL, with
  connection pinning, nested scopes, and cleanup on errors and exceptions.
- [SELECT row locking](docs/how-to/select-locking.md) on PostgreSQL and MySQL,
  through both builders, with rejection on unsupported backends.
- Exclusive connection leases, bounded prepared-statement caches, pool statistics,
  PostgreSQL statement-timeout defaults and [transaction-bound overrides](docs/how-to/statement-timeouts.md),
  and query-observer callbacks.
- Scoped transactions, savepoints, transaction retries, and a schema migration
  runner for PostgreSQL and SQLite, with automatic PostgreSQL executor
  serialization.
- Optional [transactional test scopes](docs/how-to/transactional-test-scopes.md)
  for PostgreSQL/SQLite, with explicit finish and conservative failure cleanup.
- Optional [Catch2 transactional fixtures](docs/how-to/catch2-transactional-tests.md),
  with lazy preparation, isolation for each generator/section path and runner diagnostics.
- [Worked reference fixtures](docs/how-to/worked-reference-fixtures.md) for PostgreSQL
  and SQLite, with checked TEMP baselines and no automatic replacement connection.
- PostgreSQL codecs for JSONB, arrays, UUID, bytea, date/time, exact numeric values,
  and enum labels, plus raw and structured COPY helpers.

Relationship loading is explicit. Async APIs, automatic
lazy/eager loading, an identity map, and compile-time checking of raw SQL are
not implemented. See the [API reference](docs/reference/README.md) for contracts
and limitations. The [locking and timeout review](docs/locking-timeout-acceptance.md)
records the accepted v1 scope and local checks.
The [bulk predicate DML review](docs/bulk-predicate-dml-acceptance.md) records its
accepted v1 limits and fresh three-backend verification.
The [streaming inventory and contract](docs/streaming-design.md) records the portable
reader API, both implementations and native feasibility probes.
Both PostgreSQL streaming strategies are implemented and
[locally accepted within the revised v1 contract](docs/streaming-acceptance.md).
The [transactional test harness contract](docs/transactional-test-harness-design.md)
records the generic scope, Catch2 adapter and worked fixture migration, now
[locally accepted within the revised v1 contract](docs/transactional-test-harness-acceptance.md).
The [advanced-query inventory and contract](docs/advanced-query-design.md) defines
implemented ordinary/recursive CTEs, set operations and windows with typed helpers.
PG/SQLite FILTER and PostgreSQL DISTINCT ON are implemented. The complete subset is
[locally accepted within the revised v1 contract](docs/advanced-query-acceptance.md).
The [production-pool inventory and v1 contract](docs/production-pool-design.md)
record the delivery boundaries. Shared ownership, configuration validation and
shutdown are implemented; bounded health and dirty-return handling are next,
followed by retirement, diagnostics and cache/profile policy.

## Backends

| Backend | Driver | Build option | Default |
| --- | --- | --- | --- |
| PostgreSQL | `asterorm::pg::driver` | `postgres` | Enabled |
| SQLite 3.37+ | `asterorm::sqlite::driver` | `sqlite` | Enabled |
| MySQL 8.0.19+ | `asterorm::mysql::driver` | `mysql` | Disabled |
| ClickHouse (experimental) | `asterorm::ch::driver` | `clickhouse` | Disabled |

Pool, session, and repository templates share the same API across adapters.
SQL types, transaction behavior, generated-value refresh, and upsert support
have backend-specific limits; consult the
[capability matrix and connection settings](docs/reference/backends.md).

PostgreSQL, SQLite, and MySQL bind values as parameters. Native SQL, `raw_expr()`,
and COPY statement text remain caller-owned. ClickHouse interpolates parameter
literals and must not receive untrusted input.

## Getting started

You need a C++23 compiler and standard library supporting `std::expected`,
Meson 1.1.0 or newer, Python 3, a build backend such as Ninja, pkg-config, and
development headers for each enabled adapter. Tests use Catch2, which Meson can
resolve through the supplied wrap.

For a SQLite-only build and tests without external services, run from the
repository root:

```sh
meson setup build-sqlite -Dpostgres=disabled -Dsqlite=enabled
meson compile -C build-sqlite
meson test -C build-sqlite --print-errorlogs
```

A default `meson setup buildDir` enables both PostgreSQL and SQLite and requires
both client dependencies. PostgreSQL integration tests also require a running
server; `docker compose up -d --wait postgres` starts the supplied development
service. Enable MySQL with `-Dmysql=enabled`; start its test service with
`docker compose --profile mysql up -d --wait mysql`.

See [build and install](docs/how-to/build-and-install.md) for dependency discovery,
MySQL client fallback, installation, and linking, and
[testing](docs/how-to/testing.md) for suite selection and connection settings.

## Example

This complete SQLite program creates an in-memory table, inserts a mapped entity,
and reads a typed tuple projection. Save it as `main.cpp` and link it against
AsterORM as described below.

```cpp
#include <asterorm/repository.hpp>
#include <asterorm/session.hpp>
#include <asterorm/sql/typed_query.hpp>
#include <asterorm/sqlite/driver.hpp>

#include <iostream>
#include <optional>
#include <string>
#include <tuple>

struct user {
    std::optional<int> id;
    std::string email;
};

template <> struct asterorm::entity_traits<user> {
    static constexpr const char* table = "users";
    static constexpr auto primary_key = asterorm::pk<&user::id>("id");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&user::id>("id", asterorm::generated::by_default),
        asterorm::column<&user::email>("email"));
};

int main() {
    asterorm::pool_config cfg;
    cfg.conninfo = ":memory:";
    cfg.min_size = 1;
    cfg.max_size = 1;
    asterorm::connection_pool<asterorm::sqlite::driver> pool{
        asterorm::sqlite::driver{}, cfg};
    asterorm::session db{pool};
    asterorm::repository repo{db};

    auto schema = db.with_connection([](auto& conn) {
        return conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)");
    });
    if (!schema) {
        std::cerr << schema.error().message << '\n';
        return 1;
    }

    user alice{.email = "alice@example.com"};
    auto inserted = repo.insert(alice);
    if (!inserted) {
        std::cerr << inserted.error().message << '\n';
        return 1;
    }

    namespace typed = asterorm::sql::typed;
    auto contacts = typed::query(db,
        typed::select_cols(typed::col<&user::id>, typed::col<&user::email>)
            .where(typed::col<&user::email> == typed::val("alice@example.com")));
    if (!contacts) {
        std::cerr << contacts.error().message << '\n';
        return 1;
    }
    for (const auto& [id, email] : *contacts)
        std::cout << id.value() << ": " << email << '\n';
}
```

Output: `1: alice@example.com`. Each private SQLite `:memory:` database belongs
to one connection, so this pool is limited to one connection. Entity traits
map columns; the SQL statement creates the table.

## Install and link

For a fresh SQLite-only installation build:

```sh
meson setup build-install --prefix="$PWD/build-install/install" --libdir=lib \
  -Dpostgres=disabled -Dsqlite=enabled -Dtests=false -Dexamples=false
meson compile -C build-install
meson install -C build-install
```

Installation provides headers, shared libraries, the `asterorm` pkg-config
package, and CMake targets. In your application's `CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.20)
project(my_app LANGUAGES CXX)
find_package(AsterORM CONFIG REQUIRED)
add_executable(my_app main.cpp)
target_compile_features(my_app PRIVATE cxx_std_23)
target_link_libraries(my_app PRIVATE AsterORM::asterorm)
```

Set `CMAKE_PREFIX_PATH` to the installation prefix when configuring your
application. The umbrella target includes core and installed adapters; specific
adapter targets such as `AsterORM::sqlite` are also available. See the
[build reference](docs/reference/build.md) for targets and packaging constraints.

## Development

[Just](https://just.systems/man/en/) recipes wrap the shared `buildDir` workflow:

```sh
just format-check  # Check C++ formatting
just lint          # Run clang-tidy
just test-sqlite   # Run SQLite integration tests
just test-pg       # Start the PostgreSQL service and run its integration tests
just test-mysql    # Enable MySQL, start its service, and run its tests
```

For performance work, see the [SQL DSL benchmarks](benchmarks/README.md) and
[row-decoding benchmarks](benchmarks/ROW_DECODING.md). They measure specific
client-side work and do not establish end-to-end database performance.

See the [changelog](CHANGELOG.md) for release changes.

## License

[MIT](LICENSE).
