# Streaming acceptance evidence

This directory supports the [local acceptance review](../../streaming-acceptance.md).
It is separate from the frozen pre-implementation native inventory.

- `metadata.json` records the Git base, SHA-256 source/test/build and document
  hashes, compiler/platform/build settings, selected dependency/server versions and
  fresh command results. Receipt hashes bind local logs under `/tmp`; full logs,
  connection settings and credentials are not published. This is unsigned local
  evidence, not release provenance or execution coverage of every hashed file.
- `memory.json` records the five fresh-process repository measurements from the
  captured full test run. The 1 KiB streaming cases enforce 32 MiB peak RSS growth;
  buffered `find_all` must exceed it. Large cases scan 4,096 rows of 64 KiB each
  (256 MiB total payload), with growth below half the total payload as a separate
  anti-materialization sanity check. Neither is a universal byte budget.
- `toolchain.json` records fresh configured coroutine and optional generator
  compile probes. The probe runner succeeds when coroutines compile; missing
  `<generator>` is a recorded capability result, not a failed runtime gate.
- `check_consumers.py` creates a fresh Meson build/install under a temporary
  prefix, then compiles/runs both public guide snippets through CMake and shared
  pkg-config. PostgreSQL/SQLite/MySQL are enabled, ClickHouse disabled. Both paths
  check row counts, one pinned terminal SELECT event, lease release and no private
  portal after EOF. No system installation occurs.

From the repository root, with normal test services/configuration and client
development dependencies available:

```sh
just db-up
# Enable/start the optional MySQL path to reproduce the captured three-backend run:
just test-mysql
just test
meson compile -C buildDir
just format-check
just lint
python3 tests/check_repository_stream_memory.py buildDir/tests/pg_integration --matrix --json-out /tmp/streaming-memory.json
python3 docs/evidence/streaming-acceptance/check_consumers.py
python3 docs/evidence/streaming-acceptance/check_toolchain.py --build-root buildDir --json-out /tmp/streaming-toolchain.json
python3 docs/evidence/streaming-acceptance/verify_snapshot.py --include-canonical-contexts
```

The full suite already runs each memory scenario in a separate process; the
explicit matrix command is useful for an isolated rerun. The runner skips with
exit 77 outside Linux/macOS. The cleanup test's five-second polling deadline is
an observation limit for this fixture, not an API guarantee. It checks both
`pg_stat_activity` disappearance and transaction-lock availability from an
independent connection, after first verifying the lock is held.

The consumer script honors `ASTERORM_TEST_CONNINFO`, `CXX` and `PKG_CONFIG_PATH`;
otherwise it uses the integration suite's local test-service defaults. Existing
Homebrew dependency paths are added when present. Other layouts may need the
normal discovery environment. Temporary consumer files remain at the printed path.
Static linking is not tested; the existing CMake SQLite imported-target warning
is retained.

Metadata excludes the unrelated `.gitignore` edit and all build outputs. Ignored
canonical context hashes are separate; `.agents/` is maintained locally and never
force-added. Compare hashes before reusing the review, and repeat relevant checks
after material source/contract changes. Prior acceptance/inventory captures remain
historical and are not rewritten by these probes.

`verify_snapshot.py` checks the recorded reviewed file hashes; it does not rerun
tests or validate the unavailable full logs. Omit `--include-canonical-contexts`
when using a checkout without the ignored local `.agents/` contexts. The metadata
is unsigned and can be rewritten, so this is a drift check rather than an
independent attestation of execution.
