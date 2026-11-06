# Statement timeout design evidence

Captured on 2026-11-07 for the [timeout contract](../../statement-timeout-design.md).
This is native PostgreSQL semantic evidence, not a test of an implemented timeout
API. The earlier [locking audit snapshot](../locks-and-timeouts/README.md) is unchanged.

- `probe.sql`: assertions and expected-error checks on one private connection.
- `capture.py`: standard-library runner, with a 20-second subprocess limit.
- `output.txt` / `stderr.txt`: observed output, including intentional server errors.
- `metadata.json`: source/output hashes, revision, redacted command, and exit code.

Run from the repository root:

```sh
just db-up
python3 docs/evidence/statement-timeout-design/capture.py
```

Use `ASTERORM_TEST_CONNINFO` or the Compose PostgreSQL defaults. Set `ASTERORM_PSQL`
if `psql` is not on PATH or at a supported Homebrew libpq location. Connection
configuration is not recorded. The runner inherits connection environment options
and adds a three-second lock timeout; its wall-clock limit also bounds setup.

Exit 0 requires all SQL assertions and expected SQLSTATE checks to pass. Nine
`PASS` lines were recorded on PostgreSQL 16.14. The cancellation uses `pg_sleep`
as the statement being cancelled, not as synchronization. Expected stderr includes
the autocommit `SET LOCAL` warning, cancellation (`57014`), failed restoration in
an aborted transaction (`25P02`), and invalid values (`22023`).

The probe changes only settings on its own connection, uses commit/rollback and
savepoints, creates no persistent objects, and restores the original timeout.
Its server setting allows 0 through 2,147,483,647 milliseconds. In particular,
rollback can undo an override or resurrect an already-finished inner override;
the v1 contract therefore rejects managed savepoint rollback inside timeout scopes.
