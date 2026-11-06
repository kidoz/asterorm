# Locking, statement timeout, and execution audit

Reviewed on 2026-11-06 against `d79d9ff` and the working tree. The planned lock and
timeout feature is **partially implemented**. Existing transactions, native SQL,
prepared execution, and backend error diagnostics provide useful foundations.
Cancellation classification and typed-query observation are implemented in the
follow-ups below. Shared SELECT locking and scoped PostgreSQL advisory locks
are now available. PostgreSQL migration serialization is implemented; timeout
scope and pool-default semantics remain to be settled.

The initial audit added a reproducible probe and changed no production behavior.
The original findings and probe snapshot below describe that audited revision;
follow-ups record later fixes.

## Follow-up: migration serialization (2026-11-07)

PostgreSQL `apply`, `rollback_to`, and `current_version` now acquire the reserved
bigint advisory key `0x41535445524f524d` before history setup or version reads.
One pinned connection holds the lock across all per-migration transactions. A
waiting executor re-reads history after acquiring ownership. The fixed database-wide
key avoids process-specific hashes, search-path aliases, and history-creation races;
all histories within a database intentionally share the lock. Other built-in
backend behavior is retained: SQLite requires external executor coordination and
MySQL rejects atomic schema migrations before leasing.

**Design revision:** the original criterion proposed a transaction advisory lock.
That would end at the first existing per-migration commit. The implementation uses
a session advisory scope to retain those commits and partial-progress behavior.
PostgreSQL operations reject caller-owned transactions before history setup or lock
waiting; they leave those transactions active. `has_active_transaction()` exposes
a thread-safe snapshot, and the scoped callback checks it again under the session
mutex. Recursive migration operations from observers remain unsupported.

A new Meson test launches independent client processes. A gate process holds the
reserved key until both workers appear as waiting advisory-lock requests in
`pg_locks`; history must still be absent. Apply/apply, apply/rollback, and
apply/version-read pairs then verify committed history and fresh version decisions.
Server statements and subprocess waits are bounded. Separate PostgreSQL integration
cases verify later-migration failure, partial rollback, observer exceptions, caller
transaction rejection, lock release, and retries with one-connection pools.

Observed verification:

- Against the pre-change migration header,
  `meson test -C buildDir 'migration serialization' --print-errorlogs`:
  `Ok: 0`, `Fail: 1`; workers did not wait for the reserved lock. The working
  implementation was restored immediately after that baseline check.
- With serialization implemented, that command printed `Ok: 1`, `Fail: 0`.
- `meson test -C buildDir 'migration serialization' --repeat=5 --print-errorlogs`:
  `Ok: 5`, `Fail: 0`, covering all three worker pairs on each iteration.
- Updated `just test-pg`: `Ok: 2`, `Fail: 0`, including the process regression
  alongside the full PostgreSQL integration executable.
- `just test-sqlite`: `Ok: 1`, `Fail: 0`. Expanded SQLite migration coverage also
  checks version reads, reapplication, rollback, and transaction release.
- `just test` and the final `meson test -C buildDir --print-errorlogs`:
  each printed `Ok: 11`, `Fail: 0`, including enabled backends and compile checks.
- Final `meson compile -C buildDir` and `just format-check`: exit 0.
- `just lint`: exit 0 across 44 translation units, with warnings. New fixture
  warnings were corrected; final
  `clang-tidy -p buildDir tests/integration/test_migrations.cpp tests/support/migration_worker.cpp`:
  exit 0, with remaining diagnostics confined to Catch2 headers.
- After fixture cleanup, the PostgreSQL integration/process selection again
  printed `Ok: 2`, `Fail: 0`.
- Strict standards validation via
  `uv run --no-project --with pyyaml --with jsonschema python .agents/tools/validate_standards_package.py --root . --mode strict-governance --workspace --today 2026-11-07 --json-out /tmp/asterorm-migration-standards.json`:
  exit 1, retaining 53 existing errors and 12 warnings, with no findings on the
  three edited agent context files.

The next task is timeout scope and pool-default design (item 6), followed by its
implementation and regression coverage.

## Follow-up: scoped advisory locking (2026-11-07)

`session::with_advisory_lock(int64_t key, callback)` now pins one connection
through acquisition, a synchronous `result<T>` callback, and balanced release.
The generic session detects adapter capabilities; PostgreSQL owns the bound
`pg_catalog` function calls. Unsupported adapters reject before acquisition.
See the [guide](how-to/advisory-locking.md) for the public contract.

Transaction and advisory ownership are independent. Existing transactions are
reused; beginning or completing a transaction inside a callback retains the
scope's pin. Nested same-key acquisitions balance individually. Uncertain
acquisition or failed cleanup closes/discards the connection, invalidates its
transaction, and prevents reuse until enclosing scopes unwind. Callback errors
retain their original diagnostics with cleanup detail; callback exceptions
survive secondary cleanup exceptions.

Unit tests cover nested keys, value/move-only results, transaction entry and
completion, error/exception exits, uncertain acquisition, poisoned nested scopes,
rollback failure, stale guards, and unsupported adapters. Live PostgreSQL cases
use a separate peer and bounded server waits to prove exclusion and release,
connection identity, aborted-transaction cleanup, lost ownership, and acquisition
cancellation. SQLite/MySQL cases prove pre-lease rejection.

Observed verification:

- `meson test -C buildDir 'core unit tests' 'pg integration' 'sqlite integration' 'mysql integration' --test-args='[advisory]' --print-errorlogs`:
  `Ok: 4`, `Fail: 0`.
- `just test-pg` and `just test-sqlite`: each printed `Ok: 1`, `Fail: 0`.
- Final `just test`: `Ok: 10`, `Fail: 0`, including all enabled backends, guide
  checks, typed compile cases, and the added cleanup-exception regression.
- `meson compile -C buildDir`: exit 0, rebuilding the remaining quickstart target.
- `just format-check`: exit 0 after the cleanup-exception regression.
- Standalone C++23 syntax checks: exit 0 for the guide's example instantiated
  with a PostgreSQL session using its compilation-database flags, and for the
  ClickHouse connection header with an unsupported-advisory capability assertion.
- `just lint`: exit 0 across 43 translation units, with warnings. Three new
  test-fixture attribute warnings were corrected and checked separately with
  `clang-tidy -p buildDir tests/unit/test_advisory_lock.cpp`.
- Strict standards validation via
  `uv run --no-project --with pyyaml --with jsonschema python .agents/tools/validate_standards_package.py --root . --mode strict-governance --workspace --today 2026-11-07 --json-out /tmp/asterorm-advisory-standards.json`:
  exit 1, retaining the 53 existing errors and 12 warnings; no findings concern
  the three edited agent context files.

This advisory slice preceded migration serialization above. The later follow-up
records the session-lock decision that preserves per-migration commits.

## Follow-up: shared SELECT locking (2026-11-07)

Both builders now lower `for_update`, `for_share`, `nowait`, and `skip_locked`
into one `select_lock_clause` in the existing AST. The compiler emits the clause
after pagination, preserving parameters. Strength and wait policy each use the
last call. A wait modifier without strength, invalid enum values, and unsupported
shapes return `parse_failed`.

This slice supports one base table with whole entities, plain columns, aliases,
or wildcard projections. Locking with joins, function projections (including
scalar functions), grouping, or HAVING is rejected. See the
[locking guide](how-to/select-locking.md) for transaction lifetime and API limits.
The AST layout changed, so rebuild the library and its binary consumers together.

PostgreSQL and MySQL support locking. SQLite/ClickHouse now declare explicit
dialect tags and reject it. Typed locking also requires a session connection type
with an explicit supported dialect; unknown custom facades retain unlocked reads
but cannot infer locking support from the legacy PostgreSQL-compatible fallback.
These rejections occur before leasing or observer callbacks. Native SQL remains
caller-owned. Unlocked SQLite/ClickHouse lowering retains its prior common SQL.

Unit coverage exercises strengths/wait policies on both supported compiler
dialects, typed/string SQL and parameter parity, pagination order, repeated
methods, aliases/wildcards, malformed AST state, and unsupported cases. Live
PostgreSQL/MySQL tests use two sessions and private table names. They verify
distinct SKIP LOCKED claims, NOWAIT errors through observed execution, shared
readers, blocked exclusive upgrades, and release on commit/rollback. Server
timeouts bound waits; no timed sleeps coordinate the test. SQLite proves rejection
before connection creation; a ClickHouse header-only check proves its disabled
capability without enabling that experimental adapter.

Observed verification:

- Before implementation, `meson compile -C buildDir core_tests` failed on the
  new methods and dialect tags required by the test fixture.
- `meson test -C buildDir 'core unit tests' --test-args='[select-locking]' --print-errorlogs`:
  `Ok: 1`, `Fail: 0` after implementation.
- `meson test -C buildDir 'pg integration' 'mysql integration' 'sqlite integration' --test-args='[select-locking]' --print-errorlogs`:
  `Ok: 3`, `Fail: 0`.
- `c++ -std=c++23 -Iinclude -x c++ -fsyntax-only -` with the ClickHouse connection
  header and static assertions for its dialect/unsupported capability: exit 0.
- `just test-pg` and `just test-sqlite`: each printed `Ok: 1`, `Fail: 0`.
- `just test`: `Ok: 10`, `Fail: 0`, including all backends, guide checks, and
  all 18 typed compile cases.
- `meson compile -C buildDir`: exit 0, rebuilding all normal targets including
  the remaining quickstart binary after the AST layout change.
- `just format-check`: exit 0.
- `just lint`: exit 0 across 41 translation units, with warnings.
- Strict standards validation via
  `uv run --no-project --with pyyaml --with jsonschema python .agents/tools/validate_standards_package.py --root . --mode strict-governance --workspace --today 2026-11-07 --json-out /tmp/asterorm-select-locking-standards.json`:
  exit 1, retaining 53 existing errors and 12 warnings, with no findings on the
  three edited agent context files.

This SELECT-locking slice preceded the scoped advisory implementation above.
Migration serialization is addressed above; timeout helpers remain pending.

## Follow-up: typed-query observation (2026-11-07)

`typed::query` now calls a compatible `session.observed_execute` inside the
existing `with_connection` callback. Standard sessions emit one execution event
for success (including empty results) or failure, before row decoding. SQL,
parameter values/types, hydration, and transaction pinning are retained.

Custom facades remain supported without adding a session base class or requiring
`connection_type`. An accessible observer helper is used only if its result type
exactly matches `connection.execute_prepared`; facades without a compatible
helper keep direct execution. The fallback adds no observer behavior. Observer
exceptions propagate, with ephemeral leases released by the existing scope.

New unit coverage checks string-AST SQL/parameter parity, no-observer execution,
tuple and whole-entity hydration, copied error diagnostics, transaction
invalidation, observer-exception cleanup, and custom facade compatibility. A
shared integration fixture runs on PostgreSQL, SQLite, and MySQL: whole entities,
named tuple builders, nullable values, empty results, events while a lease is
held, reads in an active one-connection transaction, missing-table errors, and
observers being cleared. The isolated typed-query header compile check remains.

Verification for this follow-up:

- Before the fix,
  `meson test -C buildDir 'core unit tests' --test-args='[typed-query]' --print-errorlogs`
  failed three of five unit cases: session success/failure events and custom
  helper dispatch. The direct-fallback cases passed.
- Before the fix,
  `meson test -C buildDir 'pg integration' 'sqlite integration' 'mysql integration' --test-args='[typed-query]' --print-errorlogs`
  printed `Ok: 0`, `Fail: 3`; all three returned rows but emitted zero events.
- After the fix,
  `meson test -C buildDir 'core unit tests' 'pg integration' 'sqlite integration' 'mysql integration' --test-args='[typed-query]' --print-errorlogs`
  printed `Ok: 4`, `Fail: 0`, including the added observer-exception case.
- `just test-pg`: `Ok: 1`, `Fail: 0`.
- `just test`: `Ok: 10`, `Fail: 0`, including all backends, guide checks, and
  the 18 typed compile cases.
- `just format-check`: exit 0.
- `just lint`: exit 0 across 40 translation units, with warnings.
- Strict standards validation via
  `uv run --no-project --with pyyaml --with jsonschema python .agents/tools/validate_standards_package.py --root . --mode strict-governance --workspace --today 2026-11-07 --json-out /tmp/asterorm-typed-observer-standards.json`:
  exit 1 after build completion, with the existing 53 errors and 12 warnings;
  no findings concern the two edited feature-expansion context files.

The original audit probe and benchmark artifacts remain unchanged snapshots of
their recorded versions. No timeout or locking API is introduced by this fix.
That next task is now addressed by the later SELECT-locking follow-up above.

## Follow-up: cancellation classification (2026-11-07)

PostgreSQL query cancellation (`57014`) now maps to `query_failed` before the
class-57 fallback. SQLSTATE and server diagnostics are retained. Shutdown and
connection errors still map to `connection_failed`; MySQL's native timeout
mapping and pool acquisition behavior are unchanged.

The unit suite updates its former `57014` expectation and adds shutdown controls
(`57P01`, `57P02`, `57P03`) alongside existing connection, deadlock, serialization,
constraint, parse, and ordinary query cases. A new PostgreSQL integration test
runs with prepared caches disabled and enabled. It checks cancellation kind and
SQLSTATE, one observer event with the same diagnostic message, successful
rollback, timeout restoration, the same backend PID after returning to the pool,
and successful reuse of the cancelled statement with a zero sleep parameter.

Verification for this follow-up:

- Before the implementation change,
  `meson test -C buildDir 'core unit tests' 'pg integration' --test-args='[error]' --print-errorlogs`
  printed `Ok: 0`, `Fail: 2`. Both suites failed on the cancellation kind, and
  the integration test failed for both cache configurations.
- After the fix, that command printed `Ok: 2`, `Fail: 0`.
- `just test-pg`: `Ok: 1`, `Fail: 0` for the full PostgreSQL suite.
- `just test`: `Ok: 10`, `Fail: 0`, including PostgreSQL, SQLite, MySQL, guide
  checks, and all 18 typed compile cases.
- `just format-check`: exit 0.
- `just lint`: exit 0 across 39 translation units, with existing warnings.
- Strict standards validation via
  `uv run --no-project --with pyyaml --with jsonschema python .agents/tools/validate_standards_package.py --root . --mode strict-governance --workspace --today 2026-11-07 --json-out /tmp/asterorm-cancellation-standards.json`:
  exit 1, with 53 unrelated errors and 12 warnings and no findings on the two
  edited feature-expansion context files. The increase from 52 on November 6 is
  the existing style profile crossing its verification TTL; that file is unchanged.

The original [probe snapshot](evidence/locks-and-timeouts/README.md) remains
unchanged. It records the pre-fix behavior and source hashes, not a result for the
current implementation. Typed-query observation is addressed in the later
follow-up above.

## Initial acceptance inventory

| Planned requirement | Observed implementation | Work remaining |
|---|---|---|
| `for_update()`, `for_share()`, `nowait()`, `skip_locked()` | Absent from both string and typed builders. `select_ast` has no locking node; the SELECT compiler ends after pagination. | One shared AST representation, lowering from both builders, dialect/capability validation, compiler tests, and real competing-transaction tests. |
| `session::with_advisory_lock(key, callback)` | Absent. Raw PostgreSQL advisory SQL is available through native/connection APIs. | A scope that pins one connection through acquisition, callback, and release, with error and exception cleanup. |
| Migration apply serializes through a transaction-scoped advisory lock | Absent. `apply()` ensures the history table and reads its maximum version before entering a separate transaction for each migration. | Lock before history initialization/version decisions; recheck under the lock, coordinate rollback too, and preserve or explicitly revise per-migration commit behavior. |
| Pool default and scoped statement timeout | Neither `pool_config::statement_timeout` nor `session::with_statement_timeout` exists. `acquire_timeout` limits waiting for a pool lease. | Define transaction, nesting, restoration, unsupported-backend, and autocommit semantics before adding setters. |
| Queue consumption with `SKIP LOCKED` and a limit | No existing automated locking-specific integration case. The live raw-SQL probe returns different jobs to two transactions. | Exercise generated SQL, transaction lifetime, rollback/commit release, and incompatible modes in the regular suite. |

Relevant implementation: [AST/builders](../include/asterorm/sql/ast.hpp),
[typed builder](../include/asterorm/sql/typed.hpp),
[compiler](../src/sql/compiler.cpp), [session](../include/asterorm/core/session.hpp),
[pool](../include/asterorm/pool/connection_pool.hpp), and
[migrator](../include/asterorm/core/migration.hpp).

## Initial findings reproduced

The retained [probe](evidence/locks-and-timeouts/probe.cpp),
[stdout](evidence/locks-and-timeouts/output.txt),
[stderr](evidence/locks-and-timeouts/stderr.txt), and
[provenance](evidence/locks-and-timeouts/metadata.json) are audit evidence. The
probe uses PostgreSQL 16.14 and SQLite; it is not a newly passing feature suite.

### Query cancellation is reported as connection failure

An explicit PostgreSQL transaction sets a 50 ms statement timeout and executes
`pg_sleep(0.2)` through the production `execute_prepared` path. It returns SQLSTATE
`57014` with `kind == connection_failed`. The connection is still open; after
rollback, its previous timeout is restored and `SELECT 1` succeeds.

`classify_sqlstate` maps all class-57 codes to connection failure. The existing
[unit test](../tests/unit/test_core.cpp) explicitly expects that result for
`57014`, so a green suite currently preserves this defect. PostgreSQL identifies
`57014` as query cancellation, which can also result from explicit cancellation;
it is not proof of a broken connection or exclusively of a timeout.
[PostgreSQL error codes](https://www.postgresql.org/docs/16/errcodes-appendix.html)

Use `query_failed` for this code in the first fix, retaining `sqlstate` and server
diagnostics. The existing `timeout` enum value is already used by the
[MySQL adapter](../src/database/mysql/connection.cpp) for error 1205. Preserve
that mapping; do not label every PostgreSQL query cancellation a timeout. Pool
acquisition expiry also currently returns `connection_failed`; changing that
separate contract is outside the first fix.

### Typed reads bypass the observer helper

On both PostgreSQL and SQLite, a native scalar read and a repository read each
emit one event. An equivalent typed read emits zero. A failing native read emits
one event containing its error; the typed failure emits zero.

`typed::query` invokes `connection.execute_prepared` directly inside
`session.with_connection`. Repository reads and native helpers call
`session.observed_execute`. Transaction pinning and error recording in
`with_connection` still apply to typed queries; the observation is an observer
bypass, not evidence that typed queries escape active transactions.

Migration history reads/inserts/deletes use observed execution, while migration
DDL and setup call raw `execute`. BEGIN/COMMIT/ROLLBACK, savepoints, direct
connection callbacks, and COPY are also outside that prepared-query observer
helper. A future timeout design must account for these paths explicitly.

### Concurrent migrators can race on the same version

The probe precreates an isolated history table and synchronizes two sessions
after both read version zero. Both attempt migration one. One succeeds and one
returns `23505` on the duplicate history version. Only one migration side effect
is committed, so this probe demonstrates a serialization gap rather than lost
transaction atomicity. It does not test the separate history-table creation race.

Each worker has its own session and connection. The observer rendezvous forces
the interleaving without sleeps; statements have a five-second server timeout.
The probe removes its unique schema on exit. This rendezvous is a fixture for
the current unlocked implementation: replace it when testing a future lock,
since waiting at that point while holding the new lock would obstruct the peer.

## Scope and lifecycle constraints

The raw queue probe holds job 1 in one transaction and obtains job 2 in another
using `ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`. `NOWAIT` on job 1 returns
`55P03`. Row locks require an explicit transaction for a useful multi-statement
claim/update operation. PostgreSQL permits locking after pagination; aggregate
and grouped queries have restrictions. `SKIP LOCKED` is suitable for queues and
does not promise a complete consistent view.
[PostgreSQL SELECT](https://www.postgresql.org/docs/16/sql-select.html)

The advisory probe confirms that a session lock survives rollback and needs
matching explicit release, while a transaction lock disappears on rollback.
Consequently, session advisory cleanup must not rely on a transaction guard. A
failed unlock must never return a connection with an unaccounted lock to the
pool; an aborted transaction may prevent an unlock SQL statement from running.
Lease pinning must also cover nested callback work and any transaction started
inside an advisory scope. At the initial audit, the session pinned only through
`begin()`; the advisory follow-up above now adds independent scope ownership.
[PostgreSQL advisory lock lifetimes](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS)

`SET LOCAL` outside a transaction emits a warning and leaves the prior timeout
unchanged in the probe. Inside a transaction, the timeout takes effect and
rollback restores the baseline. The original “per-lease ... emit SET LOCAL”
proposal therefore needs reconciliation for autocommit leases. A connection
baseline can use a session-level setting with checkout/reset rules; a scoped
local override needs an explicit transaction policy. Restore prior settings on
normal and exception exits, define nested scope behavior, and handle failed
transactions without leaking settings or silently committing callback work.
[PostgreSQL SET](https://www.postgresql.org/docs/16/sql-set.html)

Statement timeout limits server statement execution, not pool acquisition or the
entire C++ callback. A callback with multiple queries does not acquire one shared
wall-clock budget merely by setting this parameter.
[PostgreSQL statement timeout](https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-STATEMENT-TIMEOUT)

At the initial audit, the compiler distinguished PostgreSQL and MySQL while
SQLite used the PostgreSQL-compatible default. That fallback was not a row-lock
capability check. The later SELECT-locking follow-up adds explicit dialect tags
and checks, including MySQL locking tests. SQLite has no `FOR UPDATE` SELECT
clause, and unsupported adapters must still fail clearly.
[SQLite SELECT grammar](https://www.sqlite.org/lang_select.html),
[MySQL locking reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)

## Ordered implementation work

1. **Correct cancellation classification (implemented).** Special-case `57014` before the
   class-57 fallback. Update the existing unit expectation and add controls for
   connection failures/shutdown, deadlock, serialization, and ordinary query
   errors. Add live prepared-execution cancellation coverage proving SQLSTATE,
   observer error delivery, rollback, and connection reuse. Avoid new enum values
   or pool-acquisition changes in this slice.
2. **Route typed queries through session observation (implemented).** Cover success and error
   events, SQL/parameter identity, no-observer behavior, and transaction pinning.
   Preserve a deliberate compatibility path for existing custom session facades
   that only provide `with_connection`; document what that fallback guarantees.
   Verify PostgreSQL, SQLite, and enabled MySQL behavior.
3. **Add shared SELECT locking (implemented with the documented shape limits).** Model strength and wait policy once in the AST;
   expose the planned methods on both builders. Specify modifier-only misuse,
   repeated/conflicting calls, unsupported backends, and aggregate/grouping
   restrictions. Choose the backend validation boundary before implementation.
   Assert SQL/parameter parity and test competing transactions with bounded waits.
4. **Add scoped advisory locking and lease ownership (implemented).** Define callback result
   and exception propagation, nested locks, transaction entry, and failed cleanup.
   Prove no lease/lock leaks on every exit path, including aborted transactions.
5. **Serialize migrations (implemented on PostgreSQL).** Use a stable cross-process lock identity, acquire
   before history setup/read decisions, and recheck versions under the lock on
   the same connection. Coordinate apply and rollback. Preserve per-migration
   commits unless a batch-atomic API change is explicitly designed. Test two
   migrators, initial history creation, later-migration failure, and lock release.
6. **Implement timeout scopes and pool defaults.** First settle the autocommit
   mismatch above. Cover zero/negative/out-of-range durations, nested restoration,
   callback errors/exceptions, query cancellation, failed cleanup, transaction
   completion inside callbacks, and connection reuse across leases. State which
   raw/DDL/COPY paths the guarantee covers and reject unsupported backends.

The immediate next task is item 6. Feature status stays planned until the original
acceptance criteria and the cross-backend behavior have executable coverage.

## Verification

- `just db-up`: database healthy.
- `just test-pg`: `Ok: 1`, `Fail: 0`.
- `just test`: `Ok: 10`, `Fail: 0`, including PostgreSQL, SQLite, MySQL, guide
  checks, and all 18 typed compile cases. These existing checks are a baseline;
  they do not establish the missing lock/timeout feature.
- Audit probe compiled with C++23, `-Wall -Wextra -Werror`, against the current
  core, PostgreSQL, and SQLite libraries; exit 0. Its run exited 0 with the
  observations above. Exact commands and hashes are retained with the evidence.
- Standalone `clang-tidy` on the probe: exit 0; no user-code warnings.
- `just format-check`: exit 0, including the standalone probe source.
- `just lint`: exit 0 across the 39 configured translation units, with existing
  warnings. The probe is outside the compilation database and was checked
  separately as described above.
- Strict standards validation via
  `uv run --no-project --with pyyaml --with jsonschema python .agents/tools/validate_standards_package.py --root . --mode strict-governance --workspace --json-out /tmp/asterorm-runtime-audit-standards.json`:
  exit 1, retaining 52 existing errors and 12 warnings. None concern the two
  edited feature-expansion context files; this is not a repository-wide pass.

Only PostgreSQL native lock/timeout semantics and PostgreSQL/SQLite observer
routing were probed in the initial audit. The later follow-ups add advisory-helper
exception safety, MySQL locking coverage, and real multi-process PostgreSQL
migration coverage. Timeout-helper behavior remains unverified.
