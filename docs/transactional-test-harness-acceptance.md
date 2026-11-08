# Transactional test harness acceptance

Reviewed on 2026-11-08 against the Git base and file hashes in
[metadata.json](evidence/transactional-test-harness-acceptance/metadata.json).
**Outcome: locally accepted within the revised v1 contract.** This covers the
generic PostgreSQL/SQLite scope, session ownership, optional Catch2 adapter/runner,
checked process-owned reference fixtures and the worked CRUD migration.
The [design](transactional-test-harness-design.md) retains the inventory and
delivery history. Earlier captures remain historical; this review has fresh receipts.

## Requirement-to-evidence matrix

| Requirement | Implementation and executable evidence | Accepted boundary |
| --- | --- | --- |
| Rollback-only scope with explicit error handling | [Public helper](../include/asterorm/testing/transactional_session.hpp), [internal state](../include/asterorm/core/testing_scope.hpp) and [unit cases](../tests/unit/test_transactional_session.cpp): creation/finish, moves, idempotence, root/private-savepoint SQL order and body access | Own a new root transaction plus private savepoint on an idle session. Noncopyable, move-constructible, no move assignment/commit. Session/pool outlive the handle; explicit finish observes failures |
| Protect the owned transaction | [Session bridge](../include/asterorm/core/session.hpp) and unit/native cases check admission, generations, stale guards, reserved names, user savepoints and busy callback rejection | Managed commit/rollback, nested scopes, streaming/advisory/timeout composition reject. Ordinary repository/typed/native work and user savepoints remain available. Raw transaction SQL and independent connections are outside isolation |
| Conservative cleanup and retained diagnostics | Unit cases inject SQL failures at each control step, native C++ exceptions and pool-return allocation failure; [shared native cases](../tests/support/transactional_session.hpp) check failed statements, TEMP DDL and missing boundaries | ROLLBACK TO / RELEASE / root ROLLBACK, native-idle confirmation, then release. First diagnostic/C++ exception preserved; terminal failure poisons the session. One safe same-generation fallback root rollback, with conservative discard rather than retry/replay |
| Safe destruction and observation | Unit cases check handle destruction during controls/callbacks, observer copy/destruction, restoration, lease capacity and ordinary-guard abandonment | Control SQL invokes no query observer. Entering observer restored once. Destruction never throws; borrowed connection close/release waits for callback unwind. Ordinary guard hardening applies to connections with nonthrowing close; legacy fallback remains |
| PostgreSQL/SQLite readiness | [Native PG cases](../tests/integration/test_transactional_session.cpp), [SQLite cases](../tests/integration/test_sqlite_crud.cpp), adapter capabilities and SQLite close/idle implementation | Explicit opt-in with nonthrowing close/native-idle probe. SQLite automatic rollback is detected before later work escapes into autocommit. MySQL/ClickHouse/unknown and legacy protocols reject scope admission; no live ClickHouse claim |
| Opt-in Catch2 lifecycle | [Adapter](../include/asterorm/testing/catch2.hpp), [child cases](../tests/integration/test_catch2_harness.cpp) and [43-scenario parent](../tests/check_catch2_harness.py) check SECTION/GENERATE, fatal REQUIRE, SKIP, body exceptions, early abort, listing/filtering and worker access | One scope per partial invocation, after lazy checked preparation; cleanup follows body-local destruction. Named/type-unique registration and typed accessor required. Untagged cases do no fixture work. One serial runner invocation per process; unrelated worker-thread access rejects |
| Report listener failures without assertions | Adapter scenarios cover unknown/multiple tags, duplicate/late registration, factory/preparation/entry/validation/cleanup failures, ignored access and a JUnit probe | No assertion macros or escaping setup/cleanup exceptions in listeners. Stderr summary reports count/first diagnostic; harness failure promotes success to 1. Existing Catch2 nonzero statuses survive. JUnit/body totals may omit cleanup failure; process status plus harness summary are authoritative |
| Reference data survives every invocation | [Shared fixture/driver](../tests/support/reference_fixture.hpp), [PG fixture](../tests/support/pg_crud_fixture.hpp), [SQLite fixture](../tests/support/sqlite_crud_fixture.hpp) and [native process parent](../tests/check_reference_fixtures.py) | One permitted native connection attempt and one checked TEMP preparation per fixture. Retain generated reference IDs; construct fresh mutable entities in each body. No promise to reset sequences, C++ caches or external side effects |
| Validate the actual pinned baseline | Optional checked `validate(Session&)` runs after root/private-savepoint admission and before typed access; process checks remove identity, close native connections, terminate their own PG backend and corrupt reference rows | Explicit TEMP qualification and full baseline checks. Validation failure/exception poisons the provider and cleans its new scope. Loss cannot silently reconnect/reseed. New corruption scenarios verify rejection while the physical connection remains healthy |
| Worked migration and process isolation | [Main PG CRUD case](../tests/integration/test_crud.cpp) passes seven section paths; [SQLite example](../tests/integration/test_sqlite_reference_fixture.cpp) passes four generator/section paths; two simultaneous processes per backend use identical table names | Only the main PG case migrates in `test_crud.cpp`; its separate shared bulk-DML case remains untagged. Other transaction/migration/locking/streaming/multi-session cases retain their own sessions. Connection-local TEMP schema/private in-memory databases isolate processes |
| Optional packaging and compiled examples | [Generic consumer probe](evidence/transactional-test-scope/check_consumers.py) runs the manual guide on PG/SQLite through fresh CMake/shared pkg-config installs; [Catch2 consumer probe](evidence/catch2-transactional-adapter/check_consumers.py) runs four SQLite guide paths with separately installed Catch2 | Generic core/testing headers expose no vendor API or Catch2 dependency; normal package metadata adds no Catch2 requirement. Adapter users explicitly include its header, supply Catch2 and link without its main. Fresh tests-disabled install uses enabled PG/SQLite/MySQL libraries. Static AsterORM linking is not tested |
| Existing library behavior remains usable | Fresh 23-target suite and separately configured nine-target SQLite-only build include CRUD, typed queries, transactions, migrations, pooling, locking/timeouts and streaming regressions | Acceptance is local on the recorded toolchain/dependencies. Normal pool reconnection policy remains unchanged; fixture drivers opt out. Rebuild consumers for the session layout changes introduced by generic scope ownership |

## Recorded v1 revisions

1. A savepoint alone cannot enter an idle managed session or stop a body from
   committing its root. V1 owns BEGIN plus a protected private savepoint and gates
   managed completion. Borrowed roots and nested testing scopes reject.
2. Isolation follows Catch2 partial invocations rather than a whole TEST_CASE or
   nested SECTION callbacks. Explicit registered fixtures and a typed accessor
   replace the proposal's implicit wrapping of sessions created inside old bodies.
3. Listener assertions are not used. A custom runner supplies separate diagnostics
   and process status, while Catch2's ordinary per-case/XML accounting remains its
   own. Fatal signals/process termination have no in-process cleanup guarantee.
4. Any detected ownership/setup/cleanup failure poisons the provider. There is no
   silent reconnect/reseed/retry. Pinned validation and the worked fixture driver's
   single native attempt make the TEMP baseline policy explicit.
5. The worked file migration covers the main seven-path PostgreSQL CRUD case;
   the separate bulk-DML case is explicitly excluded. SQLite gets a four-path
   worked example. This is not a claim that every integration case is transactional.

The original plan's “faster” motivation remains a hypothesis. Functional reuse and
isolation are accepted here; elapsed-time improvement is not an exit claim.

## Setup observations

| Normal worked case | Partial paths | Preparations | Schema CREATE dispatches | Identity CREATE dispatches | Fixture native connection attempts |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL main CRUD | 7 | 1 | 7 | 1 | 1 |
| SQLite reference CRUD | 4 | 1 | 1 | 1 | 1 |

The current native runs check these counters and original row contents on each
path. They count top-level fixture CREATE dispatches, not implicit server objects,
all SQL, or elapsed time. Reference seeds and validation queries are additional work.
The old PG source contained seven setup DROP, seven CREATE and seven cleanup DROP
calls per successful section path: 21 per path, or 147 across seven paths. That is
a Git-bound source comparison, not a freshly executed legacy performance baseline.
No speedup follows from this count alone.

## Fresh verification

| Command/check | Observed result |
| --- | --- |
| `just test` | `Ok: 23`, `Fail: 0`, including all enabled PostgreSQL/SQLite/MySQL regressions |
| Fresh SQLite-only configure/build/test | `Ok: 9`, `Fail: 0`; PostgreSQL/MySQL/ClickHouse disabled, tests enabled and examples disabled |
| Focused `[transactional-testing]` | Generic: 19 cases / 304 assertions; PG: 3 / 79; SQLite: 2 / 71 |
| Adapter and reference parents | 43 adapter scenarios, 10 PG reference scenarios and 9 SQLite reference scenarios passed; intentional child failures checked by parents |
| Build, format and clang-tidy | Exit 0; lint covers 65 distinct translation units, with existing diagnostics retained |
| Focused AddressSanitizer | Generic scope unit TU, selected worked/main/loss/adapter TUs and their matrices pass; remaining objects/libraries are not instrumented |
| Fresh generic and Catch2 installed consumers | CMake/shared pkg-config paths pass; generic PG/SQLite guide and four SQLite Catch2 paths run, with no implicit normal-package Catch2 dependency |
| Strict standards (`--mode strict-governance --workspace --today 2026-11-08`) | Exit 1: existing 53 errors, 12 warnings, 0 infos; finding lists match the same-date quiescent baseline |

[Reproduction and provenance](evidence/transactional-test-harness-acceptance/README.md)
bind current source/test/build/docs and fresh command receipts. The new process
receipts include healthy-connection baseline corruption; earlier evidence remains
untouched. Snapshot verification detects file drift and does not rerun tests or
independently attest execution.

## Limits and next work

This is local acceptance of the revised functional v1 contract, not remote CI or
release readiness. The pre-existing standards findings are not waived. All platforms,
server/framework versions, full-project sanitizers, static AsterORM linking,
concurrent fixture sharing and timing claims remain outside the evidence.
The scope is trusted infrastructure, not an SQL sandbox, and synchronous cleanup
has no whole-scope deadline.

Next follow the roadmap with a **CTE/window/set-operation inventory and v1
contract**, reconciling the proposed AST/builder APIs, parameter ordering, backend
capabilities and recursive-query acceptance before implementation.
