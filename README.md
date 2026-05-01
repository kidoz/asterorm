# AsterORM

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B-336791.svg)](https://www.postgresql.org/)
[![Meson Build](https://img.shields.io/badge/Meson-Build-green.svg)](https://mesonbuild.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A PostgreSQL-first ORM for C++23 with connection pooling, CRUD, and composable SQL APIs.**

AsterORM is a modern, data-mapper style Object Relational Mapper for C++23. By heavily leveraging modern C++ features like `std::expected` and compile-time reflection via pointer-to-members, it keeps your domain entities as plain structs without any intrusive inheritance or persistence logic.

## Features

* **Data Mapper Architecture:** Entities remain plain `C++` structs. No magic base classes.
* **Compile-Time Traits:** Define mappings using simple pointer-to-member templates (`&User::id`). No macros required.
* **PostgreSQL Native:** Built directly on top of `libpq` using text parameters/results, per-connection prepared statement caching, and `RETURNING` for generated values.
* **Connection Pooling:** Built-in connection pool with timeout leasing, explicit close, basic statistics, and open-connection checks.
* **Transactions and Migrations:** Scoped `db.begin()` guards, callback-based transaction helpers, bounded transaction retries, and a small schema migration runner.
* **CRUD:** Insert, find, update, erase, patch, single/bulk insert, single/bulk upsert, single/composite primary keys, generated-column refresh, and optional optimistic locking through version-column traits.
* **SQL AST:** `select({"id"}).from("users").where(col("active") == val(true))` style SQL generation with parameterized values.
* **Native Escape Hatch:** `db.native<User>("SELECT * FROM users WHERE active = $1", true)` hydrates mapped structs from raw SQL. Scalar and row-mapper helpers are also available.
* **PostgreSQL Extras:** Supports `JSONB` text wrappers, simple arrays (`std::vector<T>`), nullable array elements, UUID/bytea/timestamp/date/time/numeric codecs, enum label traits, and raw or structured `COPY IN/OUT` helpers.

> ClickHouse adapter is experimental and disabled by default (`-Dclickhouse=disabled`). It emulates parameterized queries via string interpolation; do not use with untrusted input without additional validation.

## Current Scope

AsterORM currently focuses on a small PostgreSQL-first v0.1 surface:

* connection pooling with exclusive leases
* explicit transactions and savepoints
* entity traits for plain structs
* basic CRUD (`insert`, `find`, `update`, `erase`, `patch`, `upsert`, `insert_many`, `upsert_many`)
* single-column and tuple-based composite primary keys for repository CRUD/upsert paths
* generated-value refresh through `RETURNING` on mutable insert/upsert/update paths
* optimistic locking through an optional `version_column` entity trait
* transaction helpers and schema migrations
* PostgreSQL codecs for common scalars, optional values, arrays, nullable array elements, JSONB, UUID, bytea, timestamp, date, time, numeric, and enum labels
* structured PostgreSQL errors with SQLSTATE and server diagnostics where libpq exposes them
* raw SQL mapping through `native<T>()`, `native_scalar<T>()`, and `native_map()`
* a small SQL AST/compiler for parameterized statements

Not implemented yet: relationship mapping, lazy/eager loading, async APIs, SQL parsing, raw SQL type checking, a complete typed query DSL, and a production-ready multi-database abstraction. PostgreSQL binary protocol support is also not implemented yet.

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

## Getting Started

### Prerequisites

* C++23 compliant compiler (GCC 13+, Clang 16+, MSVC v19.38+)
* [Meson Build System](https://mesonbuild.com/) (>= 1.1.0)
* PostgreSQL development headers (`libpq`)

### Building

```bash
# Configure the build directory
meson setup buildDir

# Compile the library
meson compile -C buildDir

# Run the test suite (requires a local PostgreSQL instance)
meson test -C buildDir
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

A `docker-compose.yaml` file is provided to quickly spin up a test database:
```bash
docker compose up -d
```

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

## License

Distributed under the MIT License.
