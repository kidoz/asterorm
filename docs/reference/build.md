# Build options and package targets

The project version is `0.1.0`. Meson requires version 1.1.0 or newer and selects
C++23, shared libraries, warning level 3, and project `-Werror` by default.

## Project options

| Option | Type | Default | Effect |
| --- | --- | --- | --- |
| `postgres` | feature | `enabled` | Build the libpq adapter |
| `sqlite` | feature | `enabled` | Build the sqlite3 adapter |
| `mysql` | feature | `disabled` | Build the MySQL C API adapter |
| `clickhouse` | feature | `disabled` | Build the experimental clickhouse-cpp adapter |
| `tests` | boolean | `true` | Build and register Catch2 and compile-check tests |
| `examples` | boolean | `true` | Build examples; quickstart requires PostgreSQL |
| `benchmarks` | boolean | `false` | Build standalone benchmark executables |

Feature values are `enabled`, `disabled`, and `auto`. See
[meson.options](../../meson.options) and [meson.build](../../meson.build).

## Libraries and installed targets

| Meson dependency in this source tree | Library | Installed CMake target |
| --- | --- | --- |
| `asterorm_core_dep` | `asterorm_core` | `AsterORM::core` |
| `asterorm_pg_dep` | `asterorm_pg` | `AsterORM::postgres` |
| `asterorm_sqlite_dep` | `asterorm_sqlite` | `AsterORM::sqlite` |
| `asterorm_mysql_dep` | `asterorm_mysql` | `AsterORM::mysql` |
| `asterorm_ch_dep` | `asterorm_ch` | `AsterORM::clickhouse` |

`AsterORM::asterorm` is an umbrella target over core and the installed adapters.
The pkg-config package is `asterorm`; its `Requires.private` includes enabled
PostgreSQL, SQLite, and MySQL client dependencies. A bundled MySQL client is
installed with `asterorm_mysqlclient.pc`.

The CMake config currently imports shared libraries from `<prefix>/lib`; use
Meson's shared-library default and `--libdir=lib` for that consumption path.
See [build and install](../how-to/build-and-install.md) for commands.

## Header entry points

| Header | Purpose |
| --- | --- |
| `asterorm/entity_traits.hpp` | Mapping declarations |
| `asterorm/session.hpp` | Sessions, transaction options and guards |
| `asterorm/repository.hpp` | CRUD and repository queries |
| `asterorm/migration.hpp` | Migration definitions and runner |
| `asterorm/pool/connection_pool.hpp` | Pool, lease and configuration |
| `asterorm/sql/ast.hpp` | Runtime AST and string builders |
| `asterorm/sql/compiler.hpp` | SQL compilation and dialect selection |
| `asterorm/sql/typed.hpp` | Typed builders without query execution |
| `asterorm/sql/typed_query.hpp` | Typed query execution and hydration |
| `asterorm/{postgres,sqlite,mysql,clickhouse}/driver.hpp` | Backend drivers |
| `asterorm/postgres/copy.hpp` | Structured PostgreSQL COPY helpers |

The top-level entity, session, repository and migration headers forward to
their corresponding `core/` headers. Backend headers under `detail/` are
implementation details.
