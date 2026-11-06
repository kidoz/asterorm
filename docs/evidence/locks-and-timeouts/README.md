# Locking and timeout audit evidence

This is a snapshot of the pre-feature implementation, reviewed on 2026-11-06.
Read the [audit](../../locking-and-timeout-audit.md) for findings and scope.

- `probe.cpp`: API-presence checks, PostgreSQL/SQLite observer routing, native
  PostgreSQL lock/timeout behavior, and a controlled migration race.
- `output.txt`: final probe stdout.
- `stderr.txt`: server notices/warnings, including the ineffective autocommit
  `SET LOCAL` and unique-schema cleanup.
- `metadata.json`: compiler/build settings, source and artifact hashes, exact
  compile/run arguments, exit statuses, and check results. Connection credentials
  are not recorded.

Run from the repository root with a built core and both PostgreSQL/SQLite
adapters, using `ASTERORM_TEST_CONNINFO` or the compose test database defaults.
The recorded compile command uses this machine's macOS/Homebrew paths and shared
libraries; adjust include/library/runtime search paths for other toolchains.
`just db-up` starts the test PostgreSQL service.

The probe creates a temporary table for each routing check and a uniquely named
PostgreSQL schema for the lock/migration checks, then drops it. It uses two
connections for competing locks and two sessions for migrations, with five-second
server statement timeouts. Normal execution finishes in less than a second on
the capture machine; that observation is not a benchmark or a test deadline.

This fixture reports missing APIs and existing defects; exit 0 means the audit
ran successfully, not that the planned feature passed acceptance. It is not
registered as a CI regression test. The migration rendezvous forces both workers
past the current unlocked version read. Do not reuse that rendezvous unchanged
after adding migration locks: it could wait for a peer blocked on the new lock.
Create new regression coverage around observable serialization instead.

Keep this snapshot and use a separate result directory for future comparisons.
