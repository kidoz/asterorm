# Locking and timeout acceptance

Reviewed on 2026-11-07 against the current source tree, including the staged pool
and session changes. **Outcome: locally accepted within the revised v1 contract.** This review covers
the revised v1 contract; it does not claim every ORM feature or a release gate is
complete. [Provenance](evidence/locking-timeout-acceptance/metadata.json) records
source/test hashes, build settings, and current command results.

## Requirement-to-evidence matrix

| Requirement | Implementation and executable evidence | Accepted boundary |
| --- | --- | --- |
| Shared SELECT lock methods | [AST/compiler unit tests](../tests/unit/test_select_locking.cpp), [compiler](../src/sql/compiler.cpp), and [shared live fixture](../tests/support/select_locking.hpp) cover both builders, SQL/parameter parity, strength/wait combinations, pagination, repeated calls, aliases, invalid AST state, and dialect rejection | PostgreSQL/MySQL; single base table and column/entity projections. Joins, function projections, grouping, and HAVING reject locking |
| Queue-style `SKIP LOCKED` with a limit | Two live transactions claim IDs 1 and 2 using generated `ORDER BY ... LIMIT 1 FOR UPDATE SKIP LOCKED`; NOWAIT errors and lock release after commit/rollback are checked in the same fixture | Locks coordinate active transactions. The API does not persist a claimed/processed state or provide a queue abstraction |
| Scoped advisory ownership and exceptions | [Unit](../tests/unit/test_advisory_lock.cpp) and [PostgreSQL tests](../tests/integration/test_advisory_lock.cpp) check balanced same/different-key nesting, existing/new transactions, move-only results/guards, callback errors/exceptions, acquisition cancellation, failed cleanup, lost ownership, and stale guards | PostgreSQL session advisory locks; one pinned connection and synchronous callback. No implicit transaction |
| Migration serialization before history decisions | [Migrator](../include/asterorm/core/migration.hpp), [process harness](../tests/check_migration_serialization.py), [worker](../tests/support/migration_worker.cpp), and [failure cases](../tests/integration/test_migrations.cpp) cover setup, apply, rollback, current-version reads, competing processes, fresh version decisions, partial commits, errors/exceptions, and release | Stable database-wide bigint key `0x41535445524f524d`; all histories serialize. Existing per-migration commits remain. PostgreSQL rejects caller transactions; SQLite executors require external coordination |
| Optional pool timeout baseline | [Pool](../include/asterorm/pool/connection_pool.hpp), [protocol](../include/asterorm/core/statement_timeout.hpp), [unit cases](../tests/unit/test_pool_timeout.cpp), and [live cases](../tests/integration/test_statement_timeout.cpp) cover absent/zero/max/invalid values, lazy validation, warmup/new/idle setup, same-PID reapplication, active/failed transaction discard, failed/throwing setup, capacity release, waiters, and close races | PostgreSQL session setting at checkout; absent option preserves behavior. Return/destructors perform no timeout SQL; setup is outside the pool mutex. Close-expiry accounting is checked after the actual ten-second deadline |
| Transaction-bound scoped timeouts | [Session](../include/asterorm/core/session.hpp), [unit cases](../tests/unit/test_timeout_scope.cpp), and live cases cover actual-prior restoration, nesting, zero/max, callback results/exceptions, move-only values/guards, admission, completion gates, stale guards, rollback failure, and shared advisory poisoning | Explicit transaction required; no automatic transaction or savepoint. Commit/full rollback is allowed; further work waits until frames unwind. Savepoint rollback is rejected inside the scope; create/release remains supported |
| Runtime execution and diagnostics | Live timeout cases cancel native/typed prepared statements, raw DDL, COPY IN/OUT, and contended advisory acquisition. They check SQLSTATE `57014`, callback/observer exception identity, aborted snapshots (`25P02`), explicit rollback recovery, default restoration, and advisory continuation | Per-server-statement limit, not a callback/C++/pool-wait deadline. Local restoration on an aborted transaction is rejected by adapter state checks; the primary statement diagnostic is preserved |
| Observation and cancellation prerequisites | [Error mapping](../src/core/error.cpp), [unit controls](../tests/unit/test_core.cpp), [typed-query unit tests](../tests/unit/test_typed_query.cpp), and [shared live observer fixture](../tests/support/typed_query_observer.hpp) establish correct cancellation classification and typed/native/CRUD event behavior | `57014` remains `query_failed` and is not assumed to mean timeout exclusively. Control SQL emits no session events. Compatible custom facades retain documented fallback behavior |
| Unsupported backend behavior | SQLite/MySQL integration cases reject explicit pool defaults and scoped advisory/timeouts before leasing/connecting as applicable; compiler/typed tests reject unsupported SELECT locking | SQLite/ClickHouse reject SELECT locking. MySQL supports row locking but not the PostgreSQL advisory/timeout contract. ClickHouse remains disabled and is checked through headers, not a live server |
| Public adapter API unit coverage | [PostgreSQL unit cases](../tests/unit/test_postgres_connection.cpp) construct a disconnected connection and exercise timeout range/context validation, snapshot failure, physical-state checks, and advisory error propagation without a server | Positive server behavior remains covered by integration cases; the unit target needs libpq but opens no connection |
| Public examples and compatibility | Five guide examples compile together via [check_guides.py](evidence/locking-timeout-acceptance/check_guides.py); the normal build includes examples, backend tests, typed header checks, and typed compile cases | Rebuild library and consumers together after changes to public SELECT AST, `pool_config`, and session layouts |

## Recorded design revisions

These revisions resolve the original proposal and are part of the accepted scope:

1. A migration lock is **session-scoped**, spanning history setup/read decisions
   and all per-migration commits. A transaction lock would end at the first commit.
2. Pool defaults use a **session setting outside a transaction**. A blanket
   per-lease `SET LOCAL` has no effect in autocommit.
3. Scoped overrides use **transaction-local settings inside an existing managed
   transaction**. They do not silently start or commit callback work.
4. Savepoint rollback is explicitly rejected while timeout frames live, because
   it can remove active overrides or resurrect completed inner ones. Full rollback
   remains supported.
5. SELECT locking has the documented restricted query shapes and explicit backend
   capabilities; ordinary unlocked SQL/custom-facade compatibility is retained.

The [runtime audit](locking-and-timeout-audit.md) retains the original inventory,
probes, implementation sequence, and revision history. The
[timeout contract](statement-timeout-design.md) specifies error precedence,
transaction generation, pin lifetime, and cleanup behavior in detail.

## Fresh verification

The delivery-rule review found that actual PostgreSQL adapter methods had live
coverage but lacked a no-server unit target. `postgres unit tests` now covers both
timeout and advisory public APIs, and `just test-pg` includes it. The public guides
also state the PostgreSQL 16+ support baseline explicitly. Runtime code did not
need a correction.

| Command | Observed result |
| --- | --- |
| `just test-pg` | `Ok: 3`, `Fail: 0` (adapter unit, live integration, migration processes) |
| `just test-sqlite` | `Ok: 1`, `Fail: 0` |
| `just test` | `Ok: 12`, `Fail: 0`, including MySQL |
| `meson compile -C buildDir` | Exit 0; no work remaining |
| `just format-check` | Exit 0 |
| `just lint` | Exit 0 across 48 translation units; existing warnings remain |
| `python3 docs/evidence/locking-timeout-acceptance/check_guides.py` | Five guide examples plus ClickHouse header/rejection checks; compiler exit 0 |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: existing 53 errors and 12 warnings; no findings in the three edited context documents |

The README feature inventory describes the supported surface. Shared templates
remain in headers; native libpq operations extend the existing PostgreSQL `.cpp`
adapter, preserving that established boundary. Public API changes have no-server
unit coverage and PostgreSQL integration coverage; the guide checks verify the
published examples against current headers.

This review initially
found the configured MySQL service offline: the first full run printed `Ok: 9`,
`Fail: 2`, both from connection refusal at the local test endpoint. The existing
service was started with `just mysql-up` before repeating the MySQL and full runs.
No production fix was required for that environment failure.

## Limits of the conclusion

This is local acceptance of the revised feature surface. It is not a remote CI,
release-publication, or all-platform result. ClickHouse runtime behavior is not
validated. Complex SELECT locking shapes, autocommit timeout scopes, savepoint
rollback within timeout scopes, generic MySQL timeout/advisory semantics, and
concurrent SQLite migration execution are outside v1.

The existing strict-standards failures remain a separate repository governance
issue. They are not waived or relabeled a pass by feature acceptance. Historical
native probe captures remain unchanged and are not substituted for current API
tests. This review adds no-server adapter unit cases, test wiring, and documentation/
evidence. Runtime implementation source is unchanged.

## Next work

Follow the feature plan with a bulk predicate-DML inventory and contract review:
`repository::update_where`, `delete_where`, and `insert_select`. Reconcile the
existing UPDATE/DELETE AST, assignment validation, affected-row results, observer
routing, transaction/timeout behavior, and PostgreSQL/SQLite/MySQL capabilities
before implementing the first bounded API slice. These public methods are absent
from the reviewed tree; existing bulk insert/upsert does not fulfill that contract.
