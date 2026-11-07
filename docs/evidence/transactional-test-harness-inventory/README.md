# Transactional test harness inventory evidence

This capture supports the [inventory and contract](../../transactional-test-harness-design.md).
It implements no harness API, listener adapter, fixture migration or speedup claim.

- `session_probe.cpp` uses the actual session/pool and PostgreSQL/SQLite adapters,
  touching only connection-local TEMP tables. It checks idle savepoint rejection,
  rollback/release/error/DDL behavior, managed commit escaping an ordinary
  savepoint, stale guards, control observation, generated IDs and cleanup/readiness
  declarations.
- `catch2_probe.cpp` registers an observational listener against the configured
  Catch2. It checks two generator values × two SECTION paths, fatal assertion,
  explicit SKIP and listing. There are no assertion macros inside the listener.
- `capture.py` reuses configured C++23 compile flags, links the current Meson
  libraries, runs both probes and records results and source/probe hashes.
- `results.json` records backend versions and checked facts, plus callback order,
  counts and expected child exit statuses. On configured Catch2 3.14.0 the fatal
  assertion exits 42 and the all-skipped case exits 4; these intentional probe
  statuses do not mean the capture failed. The capture itself must exit 0.
- `metadata.json` binds the Git base, compiler/platform, selected inspected source
  hashes, compile flags and probe/result hashes. This is unsigned local inventory,
  not independent provenance or proof of execution coverage for every hashed file.
- `verification.json` separately binds the reviewed design/context hashes and
  fresh local build/test/format/lint/standards receipts. Logs stay local; receipt
  hashes do not independently attest execution. The capture rewrites its own
  results/metadata, so verification must be reviewed again after a recapture.

From the repository root with the configured PostgreSQL/SQLite/Catch2 build and
the usual integration test service available:

```sh
just db-up
meson compile -C buildDir
python3 docs/evidence/transactional-test-harness-inventory/capture.py --build-root buildDir
```

The capture expects configured `asterorm_core`, `asterorm_pg`, `asterorm_sqlite`,
`Catch2` and `Catch2Main` targets, as provided by this checkout's fallback build.
An external Catch2 package without those Meson targets needs a link-discovery
adaptation before reusing this script. Existing Homebrew libpq/SQLite pkg-config
paths are added when present; ordinary discovery settings are otherwise honored.
PostgreSQL uses `ASTERORM_TEST_CONNINFO` or the normal local service defaults;
connection settings and credentials are not included in result/metadata output.

Temporary executables are removed after capture. No system installation or shared
application-table mutation occurs. SQLite is `:memory:`; PostgreSQL tables are
temporary and disappear with the probe connection. The capture observes native
behavior and current declarations; it does not alter session or adapter code.

Run the normal suite, format and lint checks separately. This new capture does
not overwrite the earlier streaming inventory or acceptance evidence. Re-running
it deliberately replaces only this directory's own inventory results/metadata;
compare source hashes before treating an older snapshot as current.
