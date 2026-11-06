# Changelog

All notable changes to AsterORM will be documented in this file.

## [Unreleased]

### Added

- Shared SELECT locking in runtime and typed builders: `for_update`, `for_share`,
  `nowait`, and `skip_locked`, with PostgreSQL/MySQL support and backend checks.
- C++23 entity mapping for plain structs, including composite primary keys,
  generated values, optimistic locking, and explicit relationship loading.
- Repository CRUD, bulk inserts and upserts, and raw SQL entity, scalar, and
  custom projection mapping.
- Connection pooling with exclusive leases, acquisition timeouts, statistics,
  explicit shutdown, and configurable prepared-statement caches.
- Scoped transactions, savepoints, transaction retries, and schema migration
  apply and rollback helpers for PostgreSQL and SQLite.
- PostgreSQL support through libpq, including structured SQLSTATE errors,
  scalar and array codecs, JSONB, UUID, bytea, temporal and numeric types, and
  raw or structured COPY input and output.
- SQLite support for file, URI, and in-memory databases, with native parameter
  binding, foreign keys, a busy timeout, and transaction mode selection.
- Optional MySQL support with native prepared statements, generated-key refresh,
  transactions, and system or bundled client dependencies.
- An experimental ClickHouse adapter, disabled by default.
- A parameterized SQL AST and dialect-aware compiler, plus a typed query DSL
  with checked columns, projections, filters, aggregates, grouping, and HAVING.
- Meson builds, installable libraries and headers, pkg-config metadata, CMake
  package configuration, examples, and CI build and test workflows.

### Changed

- Added a lock clause to the public SELECT AST and explicit SQLite/ClickHouse
  dialect tags. Rebuild the library and consumers together for the AST layout
  change, and handle new dialect values in exhaustive switches.
- Organized backend implementations under `src/database`.
- Added typed query parameters and shared entity hydration across backends.
- Made repository writes and schema migrations aware of backend capabilities.

### Fixed

- Routed typed query execution through session observation, retaining direct
  prepared execution for custom sessions without a compatible observer helper.
- Classified PostgreSQL query cancellation (`57014`) as a query failure while
  preserving SQLSTATE and diagnostics, rather than reporting a connection failure.
- Prevented stale transaction guards and failed transactions from committing.
- Corrected SQLite parameter binding and statement execution handling.
- Ensured pending connection acquisitions fail when the pool closes.
- Discarded PostgreSQL connections after COPY protocol failures.
- Checked affected rows when updating or deleting entities.
- Refreshed the schema migration version after rollback.
- Corrected clang-tidy source selection for relative compilation commands.

### Security

- Quoted SQL identifiers and validated entity columns used for ordering.
- Hardened ClickHouse parameter interpolation against control bytes and
  malformed values.
