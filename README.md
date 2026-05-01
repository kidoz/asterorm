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
* **PostgreSQL Native:** Built directly on top of `libpq`. Uses binary formats, prepared statement caching, and `RETURNING` clauses seamlessly.
* **Smart Connection Pooling:** Built-in connection pool with transparent `std::condition_variable` timeout leasing, automatic health checks, and thread safety.
* **RAII Transactions:** Simple, scoped `db.begin()` -> `transaction_guard` layer. Dropped guards automatically execute `ROLLBACK`.
* **Fluent Query DSL:** `select({"id"}).from("users").where(col("active") == val(true))` style AST generation, completely immune to SQL injection.
* **Native Escape Hatch:** `db.native<User>("SELECT * FROM users WHERE tags @> $1", "{c++}")` automatic struct hydration from raw SQL.
* **PostgreSQL Extras:** Supports `JSONB` mappings, Arrays (`std::vector<T>`), and lightning-fast `COPY IN/OUT` data streaming.

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
