# Prepare reference data once for transactional tests

The PostgreSQL integration runner registers `pg-crud`; the SQLite runner registers
`sqlite-crud`. Both use the optional [Catch2 adapter](catch2-transactional-tests.md).
Their fixtures own a lazy single-connection pool/session, prepare checked TEMP
schema and a generated reference ID once, then validate each pinned invocation's
baseline before allowing typed session access.

The repository examples are:

- [Shared fixture/driver](../../tests/support/reference_fixture.hpp): no native
  replacement connection after the first attempt; explicitly qualified TEMP
  identity checks and preparation/validation counters.
- [PostgreSQL fixture](../../tests/support/pg_crud_fixture.hpp): seven TEMP tables,
  a committed reference user and checks of all seven table baselines.
- [PostgreSQL CRUD case](../../tests/integration/test_crud.cpp): seven section paths
  use the registered session and fresh entities. Each path checks and mutates the
  reference user, whose original value must return for the next path. The separate
  shared bulk-DML case stays untagged and constructs its own session.
- [SQLite fixture](../../tests/support/sqlite_crud_fixture.hpp) and
  [worked CRUD case](../../tests/integration/test_sqlite_reference_fixture.cpp):
  a private `:memory:` connection and TEMP reference table, with two generator
  values across update/insert and delete/replace sections. All four paths see the
  original generated reference ID and data.

`prepare(Session&)` returns `result<void>` and runs outside per-invocation
transactions. Every DDL/seed result is checked. No fixture uses DROP/CREATE as a
retry strategy. Preparation stores reference IDs; bodies construct fresh mutable
C++ values. Rollback restores data and transactional DDL, without promising to
rewind PostgreSQL sequences or arbitrary C++ state.

`validate(Session&)` runs after BEGIN/private SAVEPOINT, on the same connection
the body will borrow. It checks the explicitly qualified `pg_temp`/`temp` identity
table and expected row contents/counts. Missing identity, changed baselines,
native errors or exceptions fail setup and poison the provider before access.
Checking on this pinned connection avoids validating one lease and executing the
body on another. Failed validation also rolls back its newly opened scope.

The shared test driver permits only the first native connection attempt. A closed
idle connection therefore produces a retained failure when the pool requests a
replacement; no replacement native connection or reseeding occurs. A killed
PostgreSQL backend can instead fail the next BEGIN, and missing TEMP identity
fails validation. The adapter remembers all these failures and rejects subsequent
access. Fixtures are serial; this driver is a test-support pattern, not a change
to normal pool reconnection behavior.

Run the worked cases or their process checks from the repository root:

```sh
buildDir/tests/pg_integration '[transactional:pg-crud]'
buildDir/tests/sqlite_integration '[transactional:sqlite-crud]'
meson test -C buildDir 'PostgreSQL reference fixtures' 'SQLite reference fixtures' --print-errorlogs
```

PostgreSQL needs the normal disposable test service or `ASTERORM_TEST_CONNINFO`.
SQLite needs no server. The parent checks run identical fixture names in two
simultaneous processes, verify preparation/connection/partial counts, and prove
that listing and untagged cases stay lazy even with invalid PostgreSQL settings.
Separate probe executables deliberately close connections, remove TEMP identity
and terminate only their own PostgreSQL backend between invocations. Their expected
nonzero statuses are checked by the parent; running those loss cases directly is
not a passing test suite.

The worked PostgreSQL run dispatches seven schema CREATE statements once for seven
paths, plus one identity-table CREATE. The SQLite run dispatches one schema CREATE
plus its identity-table CREATE for four paths. Counters are checked against native
successful runs. The old PostgreSQL source had seven DROP, seven CREATE and seven
cleanup DROP calls per successful section path; that is a source-level comparison,
not a freshly measured timing baseline. No speedup is claimed.

Commit/transaction, migration, locking/timeout, streaming, native loss and
multi-session integration cases retain their own sessions and stay untagged.
The [delivery contract](../transactional-test-harness-design.md) and
[worked fixture verification](../evidence/worked-transactional-fixtures/README.md)
record the current scope. Combined acceptance and timing evidence remain separate.
