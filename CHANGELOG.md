# Changelog

All notable changes to AsterORM will be documented in this file.

## [Unreleased]

### Added

- Combined production-pool local acceptance review and composed health/age/profile/
  shutdown, native transaction/stream and forced proxy-backend-switch checks.
  Fresh installed consumers execute all four pool guides; existing delivery
  captures remain historical. No production code change is introduced here.

- Explicit off/per-connection statement cache policy and a PostgreSQL transaction-
  pool profile with pre-connect cache/timeout validation and pre-SQL advisory/
  migration admission. Added a digest-pinned opt-in PgBouncer transaction-mode
  fixture and scoped integration checks. Rebuild pool/config/PG consumers.

- Optional idle/lifetime retirement with a stop-aware worker, non-revoking held
  leases and checkout/return age validation. Added saturating acquisition counters,
  fixed duration buckets and bounded structured long-held-lease warning polling.
  Package exports carry thread linkage; rebuild consumers for pool/config/stats
  and internal entry layouts. Cache/profile policy remains planned.

- Optional bounded PostgreSQL pool pre-ping through nonblocking uncached SELECT 1,
  with terminal drain, mode restoration and timeout/failure discard. Unsupported
  enabled backends reject before connecting. PG/SQLite/MySQL now discard known
  dirty transactions/unread work on return. Rebuild consumers for appended pool
  configuration fields and adapter API changes.

- Shared pool/lease state, allocation-free return bookkeeping, eager size/acquire
  timeout validation and non-draining shutdown. Leases may outlive the facade;
  closed work returns `pool_closed`, capacity wait expiry returns `timeout`.
  Stats expose held/setup/retiring/waiter states. Rebuild consumers for pool/lease/
  stats layout changes; direct lease/raw return construction is removed.

- Production-pool inventory and proposed v1 contract, with configured API/native
  probes for size validation, shutdown, stale PostgreSQL sockets, raw transaction
  reuse and disabled statement caching. Implementation remains planned; the next
  slice is shared ownership, configuration validation and shutdown.

- Source-bound local advanced-query acceptance across ordinary/recursive CTEs,
  sets, windows/typed helpers and backend-specific aggregates. Fresh composition
  cases check binding, NULL/duplicate results, pagination, pre-lease rejection and
  observer/session behavior; all five public guides and the composition fixture
  run through installed CMake/shared pkg-config consumers.

- PG/SQLite aggregate FILTER, including aggregate windows, with owned predicates,
  typed nullable results and shared validation/binding. PostgreSQL DISTINCT ON
  adds checked ordering and an explicit tie policy; unsupported backends reject
  before leasing. Rebuild library/consumers: SELECT and containing query layouts
  changed. The subsequent combined review records scoped local v1 acceptance.

- Inline OVER windows, checked ROWS frames, ranking/navigation/frame-value helpers
  and typed window aggregates on PostgreSQL/SQLite/MySQL. Typed selectables now
  retain instance values while supporting existing static lowering methods.
- Window validation and primitive numeric AST hints/casts for SQLite computed
  comparisons and MySQL navigation defaults. Plain SELECT SQL remains unchanged.
  Rebuild library/consumers: function/parameter AST and compiler layouts changed.
  The extension delivery activates the shared-const FILTER predicate.

- Constrained recursive CTEs through the existing root `.recursive()` flag: one
  self-referencing definition, explicit outputs and direct anchor/step UNION ALL.
  Validation checks references, expressions and shared budgets before leasing.
  Data termination and native recursion limits remain the caller/server's concern.

- Owned UNION/INTERSECT/EXCEPT trees with ALL factories, explicit grouping and
  branch/final pagination through the existing mapped query executor. MySQL
  INTERSECT/EXCEPT requires the explicit 8.0.31+ profile; SQLite rejects their ALL
  forms. Both operands validate before leasing, and output labels come from the left.

- Ordinary CTE query roots, ordered owned definitions, shared parameter bindings
  and query-wide graph validation on PostgreSQL, SQLite and MySQL. The separate
  `sql::query_as<Row>` API maps named trait columns through observed execution.
  Compiler/result layouts changed; rebuild library and consumers together.

- CTE/window/set-operation inventory and v1 design contract, with configured AST
  probes and native PostgreSQL/SQLite/MySQL semantics. Ordinary CTEs and set
  operations, constrained recursion, windows and extensions are implemented;
  combined acceptance remains planned.

- Source-bound local transactional harness acceptance review, combining generic
  scopes, optional Catch2 integration and worked reference fixtures. Fresh native
  checks also reject corrupted reference data on an otherwise healthy connection.

- Checked PostgreSQL/SQLite reference fixtures with TEMP baselines, retained
  reference IDs, one physical connection attempt and validation before tagged
  body access. Native loss never silently reconnects or re-runs preparation.
- Optional Catch2 fixture `validate(Session&) -> result<void>` hook after scope
  pinning, with retained diagnostics and provider poisoning on failure.

- Optional `<asterorm/testing/catch2.hpp>` fixture registration, typed session
  access and custom runner. Tagged Catch2 partial invocations use independent
  rollback scopes; setup/cleanup failures poison providers and affect process
  status without assertions in listener callbacks. Normal library dependencies
  remain independent of Catch2.

- Optional `testing::transactional_session<Session>` for PostgreSQL/SQLite, with
  owned root/private savepoint, managed completion gates, observer restoration,
  generation-safe rollback and fail-closed session poisoning. Worked reference
  fixtures and CRUD migration remain planned.
- SQLite native idle-state inspection and explicit transactional testing
  capabilities on PostgreSQL/SQLite connections.

- Source-bound local streaming acceptance review with five fresh-process repository
  memory scenarios, a materializing control, larger payloads, independent server/
  transaction-lock cleanup checks and reproducible installed guide consumers.

- PostgreSQL `cursor_reader` and repository cursor mode with parameterized DECLARE,
  bounded native batches, managed-transaction admission and explicit CLOSE that
  preserves prior writes. Unfinished destruction, cleanup/read/decode failures and
  terminal observer exceptions discard; each strategy has a fresh-process RSS test.

- Portable move-only `row_stream<T>` and PostgreSQL repository `stream<T>` with
  mapped SELECT validation before leasing, owned per-row hydration, structured
  late errors and terminal query observation.
- Explicit PostgreSQL single-row capability and nonthrowing readiness admission
  for repository streams, preserving caller rollback ownership on local rejection.

- Native PostgreSQL `open_single_row` / `single_row_reader` with owned single-tuple
  results, terminal protocol checks, preserved diagnostics and connection discard
  on abandonment.

- Internal session stream reservations with independent connection pins, busy-state
  gates, generation validation and nonthrowing abandonment.

- Repository `insert_select` and dedicated INSERT-from-SELECT AST/compiler support,
  with generated/default destination inference, source-shape checks and observed execution.

- Structured assignment arithmetic through `sql::add` and `sql::multiply`, with
  bound values, nested parentheses, mapped-operand validation, and traversal limits.

- Repository `update_where` and `delete_where` with checked counts, explicit
  backend admission, mapping/predicate validation, and observed execution.

- No-server PostgreSQL adapter unit coverage and a source-bound local acceptance
  review for locking and timeout behavior.
- PostgreSQL `session::with_statement_timeout` with nested local restoration,
  transaction-completion gates, and cleanup shared with advisory scopes.
- Optional PostgreSQL pool statement-timeout baselines on connection creation and
  checkout, with range/backend validation and low-level adapter read/local setters.
- Automatic PostgreSQL migration serialization across processes, covering history
  setup, apply, rollback, and version reads while preserving per-migration commits.
- PostgreSQL `session::with_advisory_lock(key, callback)` with nested scopes,
  independent transaction/lease ownership, and connection discard on failed cleanup.
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

- PostgreSQL/SQLite integration executables use the optional runner. The main
  seven-path PostgreSQL CRUD case and four SQLite reference paths use registered
  fixtures; unrelated integration cases keep their own sessions.

- Session layout includes transactional testing ownership; rebuild library and
  consumers together. SQLite connection close/cache cleanup are now nonthrowing.

- PostgreSQL connection layout includes reader ownership; rebuild consumers.
- Pools honor an optional nonthrowing reuse check and close opted-in discarded
  connections outside the pool mutex before freeing their capacity slot.

- Session layout now includes stream ownership and active-operation tracking;
  rebuild the library and consumers together.

- `sql::value_source` includes immutable arithmetic nodes; rebuild consumers and
  update exhaustive variant visitors. SELECT/predicate expression shapes are unchanged.

- Relational result affected-row counts and storage are now 64-bit; PostgreSQL
  native results validate command counts before success. Rebuild consumers.
- SQLite now requires 3.37+ for `sqlite3_changes64` and `sqlite3_total_changes64`;
  non-mutating results no longer inherit a previous mutation count.

- Session layout now tracks timeout pins and transaction generations alongside
  advisory ownership; rebuild consumers with the library.
- Savepoint rollback is rejected while a timeout scope is active; creating and
  releasing savepoints and full transaction rollback remain supported.
- Appended `statement_timeout` to `pool_config`; rebuild the library and consumers
  together for the layout change.
- PostgreSQL migration operations reject caller-owned transactions before history
  setup, preserving the caller transaction and per-migration commit behavior.
- Added a lock clause to the public SELECT AST and explicit SQLite/ClickHouse
  dialect tags. Rebuild the library and consumers together for the AST layout
  change, and handle new dialect values in exhaustive switches.
- Organized backend implementations under `src/database`.
- Added typed query parameters and shared entity hydration across backends.
- Made repository writes and schema migrations aware of backend capabilities.

### Fixed

- SELECT function projection aliases now accept either alias location, emit equal
  aliases once and reject conflicts/nested aliases. Encoded SELECT parameter errors
  and text NULs reject at compilation. SQLite offset-only SELECT emits `LIMIT -1`.

- Transaction-guard abandonment on connections with nonthrowing close now catches
  rollback/pool-return exceptions, defers release of borrowed connections and
  preserves newer managed transaction generations.

- Prevented late connection/setup completion from underflowing pool capacity
  after the close wait deadline expires.
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
