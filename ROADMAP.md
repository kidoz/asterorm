# AsterORM Full Implementation Plan

This plan splits the missing work into releasable milestones. Each milestone should end with passing unit/integration tests and updated examples.

## Milestone 1: Honest v0.1 Foundation

Status: implemented in this pass; CI runtime still needs confirmation on GitHub.

Exit criteria:
- README separates implemented features from planned features.
- Installed headers, pkg-config, and CMake package config work.
- CI runs configure, build, format check, tests, and lint.
- Native SQL supports strict/lenient entity hydration, scalar queries, and custom row mappers.
- Connection pool exposes explicit close and basic stats.
- Prepared statement cache is bounded and recoverable after invalid statement errors.

## Milestone 2: Transaction and Migration Core

Status: implemented in this pass.

Exit criteria:
- `session::transact()` runs a callback inside a transaction and commits or rolls back.
- `session::retry_transaction()` retries serialization/deadlock failures with bounded attempts.
- Migration runner creates a schema history table, applies ordered migrations atomically, and rolls back to a target version.
- Integration tests cover apply, no-op reapply, and rollback paths.

## Milestone 3: Query API v1

Status: partially implemented; ordering, limit, and offset are available through repository query options.

Exit criteria:
- Repository query options support ordering, limit, and offset.
- Typed predicates remain parameterized.
- Projection APIs support scalar and tuple-like/custom mapper result shapes.
- Joins are available through the SQL AST and documented as SQL-shape queries, not relationship loading.

## Milestone 4: CRUD Completeness

Status: implemented for the v0.1 repository surface; generated-value refresh, bulk upsert, composite keys, and version-column optimistic locking are available.

Exit criteria:
- Generated non-primary-key columns can be refreshed through `RETURNING`. Done for mutable insert/upsert/update paths.
- Bulk upsert is implemented. Done for single-table primary-key conflict upserts.
- Composite primary key design is specified and implemented. Done for tuple-based repository CRUD/upsert paths.
- Optimistic locking supports a version column trait and reports stale updates distinctly. Done for mutable `update()`.

## Milestone 5: PostgreSQL Type Coverage

Status: partially implemented; date, time, numeric, enum-label, and nullable-array codecs are available.

Exit criteria:
- Date-only, time-only, decimal/numeric, enum, and nullable array codecs exist. Done for `date`, `time_of_day`, `numeric`, `enum_traits<T>`, and `std::vector<std::optional<T>>`.
- Array codec behavior is documented for escaping, NULLs, and multidimensional arrays. Done for nullable elements; multidimensional arrays remain out of scope.
- COPY helpers have safe row encoding/decoding APIs rather than raw line-only calls. Done for PostgreSQL text `COPY` rows.
- PostgreSQL error paths consistently populate SQLSTATE, detail, hint, table, column, and constraint where libpq exposes them. Done for PGresult-backed execution paths; connection-only failures return a structured fallback message.

## Milestone 6: Relationship Layer

Status: planned.

Exit criteria:
- Relationship traits model one-to-one, one-to-many, and many-to-one associations.
- Eager loading works through explicit joins or follow-up SELECTs.
- Lazy loading, if added, is opt-in and never hides connection/session ownership.
- Object graph behavior has clear transaction and lifetime rules.

## Milestone 7: Async and Multi-Database Strategy

Status: planned.

Exit criteria:
- Async API decision is made: thread-backed futures, event-loop integration, or no async in v1.
- Database adapter interface is formalized beyond PostgreSQL.
- ClickHouse either graduates with tests and safe parameter behavior or remains explicitly experimental.
- PostgreSQL binary protocol support is designed and benchmarked before implementation.

## Release Gates

- `just format-check` passes.
- `just test` passes with PostgreSQL integration enabled.
- `just lint` passes in CI.
- Install introspection includes only AsterORM artifacts and intentional dependencies.
- README and examples match the implemented surface.
