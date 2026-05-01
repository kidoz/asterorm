# AsterORM Feature Expansion Plan

This plan extends `ROADMAP.md` (M1–M7) with the next wave of work derived from a survey of mature ORMs (Hibernate/JPA, SQLAlchemy, Diesel, sqlx, sea-orm, Entity Framework Core, ActiveRecord, GORM, Doctrine, ODB, sqlpp11, Drogon ORM). Each milestone is sized to ship independently with passing unit/integration tests and updated examples or docs.

Conventions used below:

- **Status:** `planned` unless noted otherwise.
- **Depends on:** prior AsterORM milestone or feature that must land first.
- **Exit criteria:** what "done" looks like for the milestone, expressed as observable behavior or APIs.
- Milestones are ordered by strategic value, not strict dependency.

---

## Milestone 8: Compile-time-checked Query DSL

Status: planned.

Why: AsterORM's traits already encode column names and member types. Promoting the DSL to a typed form turns invalid SQL into a compile error and is the single biggest C++-native differentiator the library can ship.

Depends on: M3 (Query API v1), M4 (CRUD completeness).

Exit criteria:

- `col<&User::email>()` compiles to a typed column reference; `col("email")` remains as a stringly-typed escape hatch.
- Predicates check operand types at compile time: `col<&User::age>() > val(18)` compiles, `col<&User::age>() > val(std::string{"x"})` does not.
- `select_builder` deduces the result tuple shape from the selected columns when typed.
- Unknown columns and unknown tables produce diagnostics with the offending name in the error.
- Runtime cost is unchanged versus the string-based path (templates collapse to the same SQL).
- A migration guide shows how to convert string-based queries to typed ones incrementally.

---

## Milestone 9: Streaming Cursors and Large Result Sets

Status: planned.

Why: today every result set is fully materialized in memory. Production users hit OOM on million-row scans; raw SQL with manual paging is the current workaround.

Depends on: M3.

Exit criteria:

- `repository::stream<T>(query, chunk_size)` returns a `std::generator<T>` (C++23) that lazily fetches rows.
- Two backends are supported behind the same generator API:
  - libpq single-row mode (`PQsetSingleRowMode`) for low-memory streaming.
  - Server-side declared cursor (`DECLARE ... CURSOR; FETCH N`) for predictable batch size.
- A streamed lease pins the underlying connection for its lifetime and surfaces a typed error if the caller tries to use the session for unrelated work mid-stream.
- Cancellation: dropping the generator releases the cursor and connection without leaking server-side state.
- An integration test scans 100k rows under a bounded RSS budget.

---

## Milestone 10: Locks, Advisory Locks, and Statement Timeouts

Status: planned.

Why: small AST/runtime work with outsized practical wins, especially for queue-like workloads, the migration runner, and runaway-query containment.

Depends on: M3.

Exit criteria:

- `select_builder::for_update()`, `for_share()`, `nowait()`, `skip_locked()` compile to the corresponding PostgreSQL clauses.
- `session::with_advisory_lock(key, callback)` wraps `pg_advisory_lock`/`pg_advisory_unlock` and tolerates exceptions from the callback.
- `schema_migrator::apply` acquires a transaction-scoped advisory lock by default so concurrent migrators on the same database serialize.
- `pool_config::statement_timeout` (per-lease) and `session::with_statement_timeout(duration, callback)` (per-call) emit `SET LOCAL statement_timeout`.
- An integration test covers `SELECT ... FOR UPDATE SKIP LOCKED LIMIT n` queue-pop semantics.

---

## Milestone 11: Bulk Predicate-based DML

Status: planned.

Why: the new DML AST already supports `UPDATE ... SET ... WHERE ...` and `DELETE ... WHERE ...`. Surface that through the repository so set-based mutation does not require raw SQL.

Depends on: M3, M4.

Exit criteria:

- `repository::update_where<T>(predicate, assignments)` returns affected rows.
- `repository::delete_where<T>(predicate)` returns affected rows.
- `repository::insert_select<T>(select_query)` for `INSERT ... SELECT`.
- All three paths route through the existing observed-execute pipeline so query observers and timeouts apply.
- Tests cover the typical patterns: archive-after-cutoff, soft-flag-flip, bulk price update.

---

## Milestone 12: CTEs, Window Functions, and Set Operations

Status: planned.

Why: feature parity with mature SQL builders. CTEs and window functions are pure AST work and unblock common analytics queries.

Depends on: M3.

Exit criteria:

- `with_cte(name, select_ast).recursive()` compiles to `WITH name AS (...)` and `WITH RECURSIVE`.
- `func_expr` carries an optional `over_clause` payload; `row_number()`, `rank()`, `dense_rank()`, `lag`/`lead`, `first_value`, `last_value`, and `nth_value` are exposed as helpers.
- `union_`, `intersect_`, and `except_` combine selects, with `_all` variants.
- `DISTINCT ON` and aggregate `FILTER (WHERE ...)` are supported as PostgreSQL-flavored extensions.
- Compiler tests cover each construct, with at least one integration test exercising a recursive CTE on real data.

---

## Milestone 13: Transactional Test Harness

Status: planned.

Why: each integration test currently does manual `DROP TABLE IF EXISTS` setup. A `BEGIN`/`ROLLBACK`-per-test harness is faster and safer.

Depends on: M2.

Exit criteria:

- `asterorm::testing::transactional_session` wraps a session, opens a savepoint at scope entry, and rolls back on exit.
- Catch2 listener integration so wrapping is opt-in via tag rather than per-test boilerplate.
- A documented pattern for fixture functions that load reference data once per process.
- The existing integration suite migrates at least one file (test_crud) onto the harness as a worked example.

---

## Milestone 14: PostgreSQL Type Coverage Expansion

Status: planned.

Why: round out PostgreSQL-first positioning with the types real applications need beyond scalars and JSONB.

Depends on: M5.

Exit criteria:

- Codecs and entity-trait support for: `tstzrange`/`int4range`/`int8range`/`numrange`/`daterange` (`asterorm::range<T>`), `interval` (`asterorm::interval`), `inet`/`cidr` (`asterorm::network`), `macaddr`, and `hstore` (`std::unordered_map<std::string, std::optional<std::string>>`).
- JSONB AST builders for `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, plus `jsonb_path_query` / `jsonb_path_exists`.
- Composite types: nested struct mapping via recursive `entity_traits` lookup, documented for one and two levels of nesting.
- Multidimensional array codec or an explicit "still unsupported" decision recorded.
- Integration tests cover at least one query per new type that is GIN-index-friendly.

---

## Milestone 15: Identity Map and Dirty Tracking

Status: planned.

Why: today `repository::patch` requires manual diffing. Many consumers want "load, mutate, save what changed" without writing SQL.

Depends on: M3, M4. Should land after M8 to benefit from typed columns.

Exit criteria:

- `tracked_session<Pool>` is a session wrapper that snapshots loaded entities and tracks the entity-pointer + snapshot pair in an identity map keyed by `(type, primary_key)`.
- `flush()` emits one `UPDATE` per dirty entity, listing only changed columns; ordering respects FK declarations once relationships exist.
- Repeated `find<T>(pk)` within the session returns the same logical entity (no double-load) and stays consistent with locally mutated state.
- Cascade rules are explicit and opt-in via entity traits (no implicit cascade).
- A short doc compares stateless `repository` versus tracked sessions, including when each is appropriate.

---

## Milestone 16: Lifecycle, Soft Delete, and Audit Hooks

Status: planned.

Why: every production app reinvents these. A small, opinionated convention saves users from rolling their own.

Depends on: M4. Lifecycle hooks compose cleanly with M15.

Exit criteria:

- ADL-found free functions `before_insert(T&)`, `after_insert(T&)`, `before_update`, `after_update`, `before_erase`, `after_load` are invoked when defined; absent overloads cost nothing.
- A `timestamps_trait` mixin auto-fills `created_at`/`updated_at` columns when the trait is declared on an entity.
- Soft delete: `entity_traits<T>::soft_delete_column` makes `repository::erase` emit `UPDATE ... SET <col> = now()`. `find` and `select` filter by default; `with_deleted()` opts back in.
- Optional `audit_trait<T>` writes a JSONB diff to a shadow table on update/erase, capturing the actor through a `session::current_actor` GUC bridge.

---

## Milestone 17: Validation Framework

Status: planned.

Why: short-circuit obviously-bad data before it round-trips through the database, and produce error messages independent of `db_error::detail` parsing.

Depends on: none, but pairs naturally with M16.

Exit criteria:

- `validation::not_null`, `length(min, max)`, `match(regex)`, `range(lo, hi)`, `one_of(values...)`, and a custom-predicate hook.
- Validation rules are declared on entity traits; `validate(entity)` returns a `result<void>` with structured failure information (field, rule, message).
- Repository writes call `validate` before SQL when validation is declared; users can opt out per call.
- Tests confirm the validator does not leak strings into compiled binaries when validation is unused.

---

## Milestone 18: Production-grade Connection Pool

Status: planned.

Why: M1's pool covers the basics. Production deployments need health-aware borrow, idle reaping, statement-timeout integration, and PgBouncer compatibility.

Depends on: M1.

Exit criteria:

- Pre-ping (`SELECT 1` with timeout) on borrow, gated by `pool_config::pre_ping`.
- Idle-timeout reaper closes connections idle past `pool_config::idle_timeout`; max-lifetime reaper rotates connections past `pool_config::max_lifetime`.
- Acquire-latency histogram and queue-depth gauges are exposed via `pool_stats` (extended) or a dedicated `pool_metrics` callback.
- Lease-leak detection: warn when a lease is held longer than `pool_config::leak_threshold`.
- `pool_config::statement_caching = off | per_connection | per_session` knob; `off` mode uses `PQexecParams` exclusively for PgBouncer transaction-pooling compatibility.
- Shutdown behavior: `close()` interrupts in-flight `acquire()` waiters with a typed error rather than blocking.

---

## Milestone 19: Pipelining and Async

Status: planned. Tied to M7.

Why: even synchronous code benefits from libpq's pipeline mode for batch workloads. Async coroutine support unlocks event-loop integration.

Depends on: M7 decision on async direction.

Exit criteria:

- `repository::pipeline(callback)` enters libpq pipeline mode (`PQenterPipelineMode`) for the lease, queues issued statements, and synchronizes at scope exit; results are correlated back to issuing call sites.
- A documented pure-throughput benchmark vs. one-statement-at-a-time on a representative bulk-insert workload.
- If async is chosen for v1, a `co_await`-able session API is offered behind `-Dasync=enabled`, integrating with at least one well-known executor (e.g., `asio` or `stdexec`).

---

## Milestone 20: Read-Replica Routing and Multi-DB Strategy

Status: planned. Extends M7.

Depends on: M7, M18.

Exit criteria:

- `multi_pool` exposes `primary` and `replicas` pools with a routing policy (round-robin, least-loaded).
- Reads default to a replica unless inside a transaction, behind a `for_update` query, or explicitly targeted at primary.
- Replica-only queries can be marked stale-tolerant; a query observer event reports replication lag where surfaced.
- Document a worked Citus / partitioning scenario or explicitly mark sharding as out of scope.

---

## Milestone 21: PostgreSQL LISTEN/NOTIFY and Pub/Sub

Status: planned.

Depends on: M19 if delivered async; otherwise can ship synchronous.

Exit criteria:

- Synchronous `session::wait_notification(timeout)` returns the next notification on a dedicated listener connection.
- `session::notify(channel, payload)` is parameterized and rejects payloads larger than 8000 bytes (PostgreSQL limit) with a typed error.
- A worked example shows cache invalidation using LISTEN/NOTIFY across two processes.
- Listener connections are excluded from regular pool rotation so they survive idle-timeout reaping.

---

## Milestone 22: Migration Tooling Maturation

Status: planned. Extends M2.

Depends on: M2, M10 (advisory locks).

Exit criteria:

- `migration::up_concurrent` runs an op outside a transaction (e.g., `CREATE INDEX CONCURRENTLY`) with explicit caller acknowledgment.
- The migrator rejects ops that would take a heavy lock unless `lock_timeout` is set on the migration; this is configurable globally and per migration.
- `asterorm-migrate diff` (standalone CLI binary) compares `entity_traits<T>` and `information_schema` and emits a candidate up/down SQL pair. Phase 1 ships read-only diff reporting; phase 2 emits SQL.
- Schema dump (`asterorm-migrate dump`) writes a canonical schema file that fresh databases load without replaying every migration.

---

## Milestone 23: Observability and Tracing

Status: planned.

Depends on: existing `query_observer`.

Exit criteria:

- `query_event` carries optional correlation/span identifiers; an example header-only OpenTelemetry adapter bridges to `opentelemetry-cpp`.
- Slow-query log: when an event exceeds a threshold, an opt-in hook captures the query plan via `EXPLAIN (FORMAT JSON, ANALYZE OFF) ...`.
- `repository::explain(query)` returns the plan as parsed JSON.
- Pool metrics (acquire latency, queue depth, leak count) are exposed through the same observer hook surface so users can ship a single integration.

---

## Milestone 24: Read-Only Views and Materialized Views

Status: planned.

Depends on: M3.

Exit criteria:

- `entity_traits<T>::read_only = true` disables write methods at compile time; the trait may declare a `view_query` (`select_ast`) instead of a `table` name.
- Repository `find` and `select` work transparently against view-backed entities.
- A worked example demonstrates a materialized view entity refreshed by a migration.

---

## Milestone 25: Field-Level Encryption

Status: planned.

Depends on: M4.

Exit criteria:

- `encrypted<T>` codec wrapper: encrypt on encode, decrypt on decode, using a `key_provider` interface.
- A reference `static_key_provider` for tests; users provide production providers.
- `pgcrypto`-backed mode and app-level AES-GCM mode are both documented; the choice is per-column.
- Key rotation is supported via versioned ciphertext format.

---

## Cross-cutting non-functional commitments

These apply across all milestones above:

- Every milestone updates the README "Current Scope" list.
- New PostgreSQL-specific features state which PostgreSQL versions they require.
- Every public API change is exercised by a unit test that does not require a live database.
- Every PostgreSQL-touching feature has at least one integration test.
- Header-only constraints are preserved unless a milestone explicitly justifies a `.cpp` addition.
- `just format-check`, `just test`, and `just lint` continue to pass.

---

## Out of scope (for the foreseeable future)

These features were considered and intentionally deferred. They may revisit later but are not in this plan:

- **Inheritance mappings** (single-table, joined, table-per-class). Substantial type-system complexity for niche domains.
- **Polymorphic associations** ("belongs_to_any"). Same reason.
- **Second-level cross-session cache** (Redis/memcached-backed). Often a footgun without a strong identity map; revisit only after M15 is mature.
- **Query result cache.** Marginal value alongside PostgreSQL's plan cache and our statement cache.
- **SQL parser.** Stated as out-of-scope in M7.
- **Heavy reflection magic** (RTTI-based field discovery). Conflicts with the data-mapper, no-runtime-overhead direction.

---

## Suggested execution order

Given the dependency graph, a reasonable order to ship is:

1. M8 (typed DSL) — shapes everything downstream.
2. M10 (locks/timeouts) — small, high-value, unblocks M22.
3. M11 (bulk DML) — natural extension of existing AST.
4. M9 (streaming) — fixes the most common production failure mode.
5. M13 (test harness) — speeds the project's own integration suite.
6. M12 (CTEs/windows) — completeness.
7. M18 (production pool) — preconditions production deployments.
8. M14 (PG types) — broad coverage win.
9. M15 (identity map / dirty tracking) — bigger lift, foundation for M16/M17.
10. M16, M17 — opinionated conveniences once tracking exists.
11. M19, M20, M21 — async/replica/notify cluster, after M7 decisions.
12. M22, M23, M24, M25 — operational and specialized features.
