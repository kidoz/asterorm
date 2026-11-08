# Transactional test harness: inventory and implementation contract

Reviewed on 2026-11-08. **The generic scope, optional Catch2 adapter/runner and
worked PostgreSQL/SQLite reference fixtures are implemented and locally accepted
within the revised v1 contract.** The
[combined review](transactional-test-harness-acceptance.md) records current evidence
and release boundaries. The historical [capture](evidence/transactional-test-harness-inventory/README.md)
was compiled and run against the configured PostgreSQL, SQLite and Catch2 targets.
Its results establish the inventory baseline; the implementation follow-up below
records the delivered API; the original inventory itself was not harness acceptance.

## Inventory before implementation

| Area | Observed implementation | Consequence |
| --- | --- | --- |
| Session transactions | [Session](../include/asterorm/core/session.hpp) pins a connection for a generation-checked `transaction_guard`; nested BEGIN rejects. Savepoints require a session-managed transaction | A standalone SAVEPOINT wrapper cannot work on an idle session, even though native SQLite supports an outer savepoint |
| Savepoint cleanup | PostgreSQL and SQLite probes recover ordinary constraint errors with ROLLBACK TO. Its target remains valid until RELEASE; temporary DDL created after it disappears on rollback | Cleanup needs an explicit policy for the savepoint and its enclosing transaction; RELEASE alone does not undo writes |
| Root completion | The probe successfully calls `session::commit_transaction()` while a savepoint exists; inserted data survives. An old guard then correctly rejects rollback of a fresh generation | Savepoints alone do not protect test isolation. Add a generation-bound ownership gate rather than treating an ordinary guard as a complete test harness |
| Cleanup safety | `transaction_guard` ignores structured rollback errors in its destructor; it has no catch-all around C++ exceptions. PostgreSQL close/idle checks are nonthrowing; SQLite currently declares neither nonthrowing close nor a native idle check | The testing scope needs explicit fallible finish, nonthrowing fallback discard and an audited SQLite adapter prerequisite; do not copy existing destructor behavior blindly |
| Observer policy | The probe observes zero callbacks for BEGIN/savepoint/ROLLBACK control methods; ordinary query observation is separate | Keep harness control SQL outside user observation. Restore the entering observer before reusing a process-owned session so body-local captures cannot leak into another test |
| PostgreSQL CRUD | [test_crud.cpp](../tests/integration/test_crud.cpp) has one main CRUD case with seven SECTION paths, plus a separate shared bulk-DML case. Each main-case entry builds a pool/session and repeats unchecked DROP/CREATE/cleanup for seven tables | A per-process connection-local schema and per-partial rollback can remove repeated DDL. Setup errors must become checked failures before body access. No measured speedup is claimed |
| Existing test infrastructure | [Meson](../tests/meson.build) links `catch2-with-main`; there is no transactional helper, fixture registry, listener or custom runner | A listener cannot automatically wrap sessions/pools that unmodified test bodies construct themselves. Tagged tests need a registered fixture and an accessor; the integration executable needs a small custom main |
| Catch2 lifetime | Configured Catch2 3.14.0 emits one case-start/end pair but four partial-start/end pairs for two generator values × two sections. Partial-end also runs after the probe's fatal assertion and explicit SKIP; listing emits no case/partial callbacks | Isolate each partial invocation, not the whole TEST_CASE and not each nested SECTION callback |
| Identity allocation | After a rolled-back generated-ID insert the probe gets PostgreSQL ID 2, SQLite ID 1 | Restore data, not cross-backend sequence counters or C++ object state. Reference IDs belong to fixture preparation; do not assert gapless/per-test-reset identities |

The capture uses PostgreSQL 16.14 and the configured SQLite 3.54.0 library. It
touches connection-local temporary tables only. Catch2's intentional failed
assertion exits 42 and its all-skipped probe exits 4; both are expected observations,
while the capture itself exits 0.

[PostgreSQL ROLLBACK TO](https://www.postgresql.org/docs/16/sql-rollback-to.html)
and [SQLite savepoints](https://www.sqlite.org/lang_savepoint.html) retain the
rollback target and distinguish rollback from release. Native SQLite's ability
to start a transaction with SAVEPOINT does not override the current session's
managed-transaction contract. PostgreSQL
[sequence allocation](https://www.postgresql.org/docs/16/functions-sequence.html)
is not undone by transaction rollback; the probe confirms that distinction here.

## Required v1 scope

The initial helper supports PostgreSQL and SQLite through explicit adapter
capabilities and session hooks. MySQL, ClickHouse and unknown/legacy capabilities
reject before leasing; MySQL's transactional-DML/DDL policy needs a later extension.
The helper is optional testing infrastructure. Normal library operations and
untagged tests retain their current behavior.

V1 owns a new root transaction for each scope. Entry requires an idle managed
session and a healthy, natively idle connection; borrowed transactions and nested
testing scopes reject. This resolves the proposal's missing transaction prerequisite
without committing caller work. A private savepoint is opened after BEGIN. Fixture
schema/reference preparation happens once, outside these per-invocation transactions.

Delivered core declarations in
[`transactional_session.hpp`](../include/asterorm/testing/transactional_session.hpp):

```cpp
// <asterorm/testing/transactional_session.hpp>, no Catch2 dependency
template <typename Session> class transactional_session {
public:
    static result<transactional_session> create(Session& session);
    transactional_session(transactional_session&&) noexcept;
    transactional_session& operator=(transactional_session&&) = delete;
    Session& session();               // borrowed, valid only while the scope is active
    result<void> finish();            // undo and release, never commit
    ~transactional_session() noexcept;
    // noncopyable; transfer ownership once; no public commit operation
};
```

The session/pool must outlive the scope and borrowed repositories. Calls are
single-consumer and non-reentrant; no mutex stays locked across test-body work.
Move construction transfers ownership; move assignment is disabled so fallible
cleanup of an old scope stays explicit. A moved-from finish owns nothing.
`finish()` is idempotent, retaining a terminal cleanup error
without retrying. A temporary busy/reentrant rejection must leave the live scope
available for cleanup after the active callback unwinds.

### Ownership and admission

The internal session bridge uses a unique testing-scope generation
bound to its root transaction generation. It alone may finish or discard that
root while the scope is active. Public managed commit/rollback, including guard
completion, reject before SQL; nested BEGIN already rejects. Protect the private
savepoint from public rollback/release/redefinition. Ordinary native/repository/
typed reads and writes remain usable, including user savepoints outside the
reserved private-name space.

Entry during a connection callback, observer callback, stream, advisory scope or
timeout scope rejects. V1 also rejects admission of those owned stream/advisory/
timeout scopes inside the testing boundary; their integration cases stay untagged.
Preserve existing stream ownership gates and ordinary session behavior when no
testing boundary is active.

The private identifier is generated, bounded and quoted through existing identifier
rules; it is never a user SQL fragment. Capability checks precede acquisition.
Allocate/snapshot diagnostics and the entering observer before mutating transaction
state. Entry failure after dispatch must roll back the root or discard if uncertain,
preserving the primary structured diagnostic or C++ exception identity.

Managed completion gates are not an SQL sandbox: raw COMMIT/ROLLBACK, manual pool
shutdown, session-level locks/settings and use of independent connections are
outside the supported tagged-test contract. Do not add a SQL parser or promise
to undo externally committed work. The harness must fail closed on detected
generation/connection loss and never clean up a newer managed generation.

### Finish and failure handling

Normal finish validates ownership, restores the entering observer without invoking
it, performs ROLLBACK TO the private savepoint, RELEASE, then root ROLLBACK. It
confirms native idle state before releasing ownership. Rows/reference schema from
preparation remain; this invocation's writes and transactional DDL disappear. The
pool may reuse the same healthy physical connection.

| Condition | Required outcome |
| --- | --- |
| Body succeeds, throws, asserts fatally or skips | Finish still executes; no commit, retry or user observer callback from cleanup |
| Ordinary constraint error leaves PostgreSQL transaction failed | Recover at the private savepoint, release it and roll back the root; preserve the body's diagnostic |
| Savepoint/rollback/release/native-idle confirmation fails | Attempt safe root rollback only for the same generation, discard conservatively if state is uncertain, return the first cleanup diagnostic and poison the fixture |
| Connection/generation is lost or transaction is invalidated | Do not issue cleanup against a newer generation; mark the scope failed and prevent fixture reuse |
| C++ exception during finish/destructor cleanup | Preserve the original exception on explicit calls; nonthrowing discard fallback, no destructor exception or observer callback |
| Destruction inside an active connection callback | Never free a leased connection object while its callback still uses it. Defer same-generation discard/release until safe unwind, poison the fixture and issue no nested rollback SQL |
| Observer replaced by a body-local closure | Restore the captured entry observer before future fixture use, including failure paths; do not call a closure after body locals have unwound |
| Destructor fallback only | Best-effort rollback/discard, never throw. Manual callers must use explicit finish to observe cleanup errors; the Catch2 adapter always does so |

For initial v1, any cleanup/ownership failure poisons the process-owned fixture,
even if fallback root rollback succeeds. No silent reconnect/reseed/retry is
allowed. Subsequent tagged access reports the stored failure rather than operating
on missing temporary tables or an uncertain baseline.

SQLite prerequisites are an audited `close() noexcept` contract and a nonthrowing
native idle check using
[`sqlite3_get_autocommit`](https://www.sqlite.org/c3ref/get_autocommit.html).
The check must handle automatic native transaction rollback without trusting only
the session's bookkeeping. PostgreSQL already has the required close/idle methods.
These prerequisites are delivered in the implementation follow-up below; the
inventory records their earlier absence.

## Optional Catch2 integration

Keep Catch2 out of the core testing header and normal library dependencies. A
separately included optional adapter registers one listener and supplies a small
runner around `Catch::Session`. Integration targets link `catch2` rather than
`catch2-with-main` when they adopt that runner. Installed users of the optional
adapter supply their own Catch2 dependency; the library umbrella gains none.

Register fixture providers explicitly before running Catch2, keyed by names such
as `pg-crud`. A tag such as `[transactional:pg-crud]` selects one provider; zero
tags leaves the test untouched, multiple/unknown registrations fail closed. Body
code obtains the ordinary session through a fixture-typed accessor, for example
`testing::catch2::session<pg_crud_fixture>()`. It cannot accidentally return a
different provider or a session outside an active matching invocation.

Use `testCasePartialStarting` to lazily prepare the provider and enter a fresh
scope. Use `testCasePartialEnded` to call explicit finish and clear the active
context after body locals unwind. Do not isolate using `testCaseStarting` or
`sectionStarting`: the former spans sibling paths, the latter nests boundaries
inside one body. Listing/filtering must not connect to a database.

Catch2's [listener guidance](https://catch2-temp.readthedocs.io/en/latest/event-listeners.html)
forbids assertion macros in listeners. Listener callbacks therefore catch failures
and record structured diagnostics; they neither call REQUIRE/CHECK/FAIL nor throw
through Catch2's callback machinery. A body accessor raises a normal test-body
exception when setup failed. The runner also emits a separate harness-failure
summary and returns nonzero if setup/cleanup failed, even when the body never
requested a session. Existing Catch2 nonzero statuses are preserved; otherwise
harness failure promotes success to a failing exit status. Catch2's own per-case
totals/XML need not include listener cleanup failures in v1; process status and
the explicit harness summary are authoritative for that additional failure.

Fatal signals/process termination are not ordinary C++ unwinding. No in-process
cleanup or SQL rollback deadline is promised for those cases. Concurrent fixture
use and access from unrelated worker threads are outside v1.

## Reference data and worked migration

A registered provider owns one pool/session for its process, with a single physical
connection (`min_size=0`, `max_size=1`) and no automatic reset after failure. Its
checked `prepare(Session&) -> result<void>` runs once before the first scope, in
autocommit, creating connection-local temporary tables and immutable reference
rows. Record reference IDs there and construct fresh mutable C++ entities in each
body. Do not reset identity counters, caches or external side effects per test.

Connection-local PostgreSQL TEMP tables and SQLite's private `:memory:` connection
avoid collisions between Meson processes. Schema lifetime is the provider's
physical connection lifetime; unexpected loss poisons it. Preparation failure is
stored once rather than silently retried by static initialization/call-once logic.

Migrate the main seven-section CRUD case in `test_crud.cpp` to one `pg-crud`
provider/tag/accessor, replacing its repeated unchecked DROP/CREATE/cleanup with
checked preparation. The separate shared bulk-DML case constructs its own session
and remains explicitly untagged until adapted independently. This is a worked
file-level migration, not a claim that every case in the file is transactional.
Add a SQLite tagged worked case for parity. Transaction-commit, migration,
locking/timeout, streaming, connection-loss and multi-session tests stay untagged.

## Delivery and executable acceptance

| Delivery | Required acceptance |
| --- | --- |
| Generic scope and session ownership — **implemented** | [No-server cases](../tests/unit/test_transactional_session.cpp), [shared native fixture](../tests/support/transactional_session.hpp) and [PG cases](../tests/integration/test_transactional_session.cpp): SQL/order, moves, stale handles, pre-lease rejection, completion gates, observer restoration and injected SQL/C++/pool-return allocation failures. Native PG/SQLite cases verify row/schema baselines survive, invocation writes disappear, failed statements recover and dirty connections never return to reuse |
| Catch2 adapter and runner — **implemented** | [43 subprocess scenarios](../tests/check_catch2_harness.py) prove one prepare and one scope per partial invocation across SECTION/GENERATE, fatal REQUIRE, SKIP, body exceptions, filters/listing and early abort; unknown/multiple tags and setup/validation/cleanup errors produce nonzero process status without assertion macros in listeners. Untagged cases do no fixture work |
| Worked fixtures and migration — **implemented** | [Main PostgreSQL CRUD case](../tests/integration/test_crud.cpp) uses its provider across seven paths; the [SQLite example](../tests/integration/test_sqlite_reference_fixture.cpp) uses four generator/section paths. [Process checks](../tests/check_reference_fixtures.py) verify baselines, concurrent processes, lazy startup and native/identity loss without replacement connections or reseeding |
| Public contract and combined review — **locally accepted** | [Combined review](transactional-test-harness-acceptance.md) maps each revised v1 requirement to current code, native/no-server/subprocess checks, installed consumers and compatibility limits. Current setup dispatch counts are recorded; old DDL is a source comparison and no timing/speedup claim is made. Existing standards failures are not waived |

## Validation of this inventory

| Command/check | Observed result |
| --- | --- |
| `python3 docs/evidence/transactional-test-harness-inventory/capture.py --build-root buildDir` | Both probes compile/run; capture exits 0. Catch2 child statuses 0/42/4/0 are expected for flow/fatal/skip/list |
| `just test` | `Ok: 19`, `Fail: 0`, including PostgreSQL/SQLite/MySQL regressions and streaming memory scenarios |
| `meson compile -C buildDir`, `just format-check` | Exit 0, including formatting of both inventory C++ files |
| `just lint` and separate configured-flag lint of both probes | Exit 0 across the 56 existing translation units; new probe diagnostics corrected and final probe lint is clean |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: 53 errors, 12 warnings, 0 infos, with identical finding lists to the streaming baseline. Validated document count differs; no new findings on the edited canonical contexts |

The [verification record](evidence/transactional-test-harness-inventory/verification.json)
binds current document/context hashes and fresh local command receipts. Existing
implementation/acceptance snapshots remain historical; this work does not rewrite
them. No helper API, fixture migration, performance improvement, remote CI or
release readiness was established by this inventory. The core implementation below
is a subsequent delivery; earlier inventory hashes/results remain unchanged.

## Implementation follow-up: generic transactional scope (2026-11-07)

The optional public header now implements fallible creation/finish, move-only
ownership with deleted move assignment, borrowed active-session access and
nonthrowing destruction. Generic state in
[`core/testing_scope.hpp`](../include/asterorm/core/testing_scope.hpp) and the
session bridge keep transaction generations, prepared control SQL, observer
snapshots and preallocated fallback diagnostics. Core exposes no vendor or Catch2
types. PostgreSQL/SQLite explicitly opt in; unsupported connections and old session
facades reject before executing or leasing through the scope.

Creation checks managed admission before lease acquisition and snapshots the
observer before native mutation. A non-idle checked-out connection is discarded
and poisons the session rather than returning an uncertain transaction to reuse.
Normal finish restores the observer, performs ROLLBACK TO / RELEASE / ROLLBACK,
confirms native idle and explicitly releases the lease so a pool-return allocation
failure can be caught before lease destruction. SQL failures retain their first
diagnostic; C++ exceptions preserve identity and discard. Failed sessions retain
their diagnostic and cannot silently reconnect. Destruction during a callback
defers physical close/release until the connection object is no longer borrowed.

SQLite close and cached-statement cleanup are audited nonthrowing operations, and
its native idle probe reads `sqlite3_get_autocommit`. Automatic native rollback is
detected before another statement can run in autocommit. Managed completion,
reserved savepoint names and stream/advisory/timeout composition are gated; ordinary
repository, typed and native queries and user savepoints remain available.

Fault injection also exposed the ordinary transaction guard's exceptional
destruction path. For connections with nonthrowing close, abandonment now catches
native rollback/pool-return exceptions, defers discard while a callback borrows
the connection, and rechecks the managed generation before touching ownership.
Legacy connection cleanup is unchanged. Regression cases cover these failures,
stale guards and generation replacement during exceptional rollback.

The [guide](how-to/transactional-test-scopes.md) is compiled in the suite using
configured SQLite flags. The [installed consumer probe](evidence/transactional-test-scope/check_consumers.py)
runs its example on both PostgreSQL and SQLite through fresh CMake/shared pkg-config
consumption, with no Catch2 dependency. Session layout changed; rebuild library
and consumers together. The optional adapter, process-owned reference providers,
CRUD migration and combined harness acceptance remain open.

The [generic scope verification](evidence/transactional-test-scope/README.md)
records the local 20-target suite, separate seven-target SQLite-only build,
fault-injection/native cases, focused sanitizer check and installed consumers.
Its source and receipt hashes are separate from the historical inventory.

## Implementation follow-up: optional Catch2 adapter (2026-11-07)

[`testing/catch2.hpp`](../include/asterorm/testing/catch2.hpp) now provides explicit
`register_fixture<Fixture>(name, factory)`, `session<Fixture>()` and `run(argc, argv)`.
Each fixture returns its borrowed session and implements checked
`prepare(Session&) -> result<void>`. A copyable factory returning
`unique_ptr<Fixture>` can supply configuration; construction/preparation are lazy,
run once, and are never retried after provider failure. Names/types are unique,
registration freezes before execution, and tag matching follows Catch2's ASCII
case-insensitive behavior. Typed access rejects missing/mismatched contexts and
unrelated worker threads.

The runner registers one listener at runtime, allowing the header in multiple
translation units. Partial-start/end callbacks catch all exceptions, retain the
first diagnostic without allocating in failure bookkeeping, poison only the
affected provider and perform explicit finish after body locals unwind. No
assertion macros run in listeners. A separate stderr summary reports the count
and first error/exception; existing Catch2 nonzero statuses survive and harness
failure promotes success to 1. JUnit can report a passing body alongside failed
listener cleanup; process status and harness summary remain authoritative.

The subprocess executable links Catch2 without its main and verifies native
SQLite baselines, four SECTION/GENERATE paths, fatal REQUIRE, SKIP, body exceptions,
early abort, listing/filter laziness, bad tags/access/registration, sticky poison,
independent providers and injected SQL/standard/non-standard/allocation exceptions.
The current workspace supplies Catch2 3.16.1; the earlier 3.14.0 inventory remains
historical. The [complete guide](how-to/catch2-transactional-tests.md) runs through
fresh installed CMake/shared pkg-config clients with separately installed Catch2.
The [adapter verification](evidence/catch2-transactional-adapter/README.md) records
the local checks and limitations separately from the frozen scope evidence.

Normal AsterORM packaging and generic scope headers remain independent of Catch2.
This step does not change session/connection layout or migrate the integration
executables to the runner. Next deliver the checked PostgreSQL CRUD and SQLite
reference providers, including native connection-loss detection before body access,
then perform combined harness acceptance. No setup reduction or speedup is claimed.

## Implementation follow-up: worked reference fixtures (2026-11-08)

The PostgreSQL and SQLite integration executables now register lazy fixtures and
link Catch2 without its main. Only the main seven-section PostgreSQL CRUD case and
the new four-path SQLite reference case use transactional tags; the separate shared
bulk-DML case and transaction/migration/locking/streaming/multi-session cases remain
untagged with their own sessions.

Each fixture owns a `min_size=0,max_size=1` pool/session and a test driver that
permits only its first native connection attempt. Checked preparation creates TEMP
identity/schema and commits reference rows outside per-invocation transactions,
recording returned IDs. PostgreSQL prepares seven mapped tables once; SQLite uses
one reference table. Bodies construct fresh mutable objects and prove original
reference data returns after preceding path mutations, without ID rewind promises.

The adapter supports an optional checked `validate(Session&) -> result<void>` hook.
It runs after the root/private savepoint pins the connection, before publishing
typed access, including the first invocation. Explicit `pg_temp`/`temp` identity
and full baseline checks prevent accidental use of permanent names or lost data.
Validation error/exception poisons the provider and cleans the opened scope,
retaining the primary diagnostic. Four additional adapter subprocess scenarios
cover SQL/standard/allocation validation failures and failed body access (43 total).

Nine PostgreSQL and eight SQLite reference-process scenarios verify one prepare
and native connection, seven/four validations and body paths, two simultaneous
processes, lazy listing/untagged startup and deliberate between-invocation loss.
Closed connections cannot be replaced; missing TEMP identity fails validation;
terminated PostgreSQL backends fail entry/validation. Access is blocked, preparation
is never repeated, and expected child status 1 is checked by the parent. Probe
executables are separate so selecting normal transactional tags runs worked cases.

The [worked fixture guide](how-to/worked-reference-fixtures.md) and
[verification record](evidence/worked-transactional-fixtures/README.md) describe
current sources and fresh local checks. Normal PostgreSQL work dispatches seven
schema CREATE statements plus one identity CREATE for seven paths, compared with
the prior source's repeated DROP/CREATE/cleanup calls. That source comparison is
not a fresh old-run timing measurement; no speedup is claimed. Normal library
reconnection policy, dependencies and session/connection layout are unchanged.
Combined acceptance remained open at worked-fixture delivery.

## Combined acceptance (2026-11-08)

The [combined review](transactional-test-harness-acceptance.md) locally accepts the
revised functional v1 contract. Fresh checks cover generic scope ownership and
failure handling, optional Catch2 lifecycle/reporting, native fixture reuse and
installed generic/adapter examples. One additional reference-process scenario per
backend corrupts a baseline row while keeping the connection healthy; pinned
validation blocks access without reconnecting or repeating preparation. The current
matrix has 10 PG and 9 SQLite reference scenarios plus 43 adapter scenarios.

The 23-target configured suite and a fresh nine-target SQLite-only build pass.
Build/format/lint, focused generic/native sanitizers and CMake/shared pkg-config
consumers pass. Standards remains at its same-date baseline of 53 errors and
12 warnings. Source hashes and command receipts are recorded separately from all
historical captures. No timing/speedup, remote CI or release readiness is claimed.
Next follow the roadmap with a CTE/window/set-operation inventory and v1 contract.
