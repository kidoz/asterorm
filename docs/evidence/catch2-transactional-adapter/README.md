# Optional Catch2 adapter verification

This directory records local checks of the [adapter/runner](../../how-to/catch2-transactional-tests.md)
delivered after the generic [transactional scope](../transactional-test-scope/README.md).
It does not refresh either historical scope evidence or the Catch2 3.14.0 inventory.
The configured workspace now supplies Catch2 3.16.1 through a separate dependency
edit; this change preserves that edit and records its hash without staging it.

| Check | Local result |
| --- | --- |
| Adapter subprocess matrix | 39 scenarios passed |
| Full PostgreSQL/SQLite/MySQL suite | 21 targets passed |
| Separate SQLite-only suite | 8 targets passed |
| Build and format | Passed |
| clang-tidy | 60 translation units plus final affected-file checks; exit 0, existing diagnostics retained |
| Focused AddressSanitizer | Both adapter translation units; all 39 subprocess scenarios passed |
| Fresh installed consumers | CMake/shared pkg-config guide each passed four isolated paths |
| Strict workspace standards | Existing 53 errors, 12 warnings; no new findings relative to this step's initial report |

The matrix uses a real SQLite in-memory database and an instrumented connection
wrapper. Counters prove one factory/preparation/physical connection and four
BEGIN/private-savepoint/ROLLBACK TO/RELEASE/ROLLBACK sequences for two generator
values and two section paths. Each path checks the original row baseline before
writing; trace ordering places body destruction before cleanup. The executable
includes the adapter from two translation units and links Catch2 without its main.

Intentional child failures are checked by the parent: fatal assertion/body exception
exit 42, SKIP exit 4, and harness-only failures exit 1. Scenarios include early
abort, lazy listing/filtering, malformed/unknown/multiple tags, frozen/duplicate
registration, wrong/outside/thread access, factory/preparation/entry failures,
unsupported capabilities rejected before fixture construction/preparation,
cleanup SQL/C++/allocation failures, sticky poison without retry, an independent
healthy provider, and a body that never requests a session. A JUnit probe confirms
that cleanup failure can coexist with passing body totals; the stderr harness
summary and nonzero process status are the additional authoritative result.

From the repository root, with normal development dependencies/test services:

```sh
just test
meson compile -C buildDir
just format-check
just lint
python3 tests/check_catch2_harness.py buildDir/tests/catch2_harness
python3 docs/evidence/catch2-transactional-adapter/check_sanitizer.py --build-root buildDir
python3 docs/evidence/catch2-transactional-adapter/check_consumers.py --build-root buildDir
```

`check_sanitizer.py` uses the configured Ninja compile/link commands to instrument
both adapter test translation units with AddressSanitizer, links existing
uninstrumented AsterORM/Catch2/SQLite binaries, and runs the matrix. This is not a
full-project sanitizer build or a leak/thread-sanitizer claim.

`check_consumers.py` freshly installs AsterORM with PostgreSQL/SQLite/MySQL enabled,
tests/examples disabled and no Catch2 discovery. It separately builds/installs
the configured Catch2 source, then compiles/runs the literal complete guide with
CMake and shared pkg-config, linking Catch2 without its main. Temporary prefixes
remain at the printed path; no system installation occurs. It honors `CXX` and
`PKG_CONFIG_PATH` and adds existing Homebrew dependency paths. Use
`--catch2-source` if the configured source is not a repository wrap. Static
linking of AsterORM is not tested. The existing CMake SQLite imported-target
warning remains.

`verification.json` binds selected sources/tests/build/docs, ignored canonical
contexts, the external dependency edit and local command/log receipts with
SHA-256. Logs under `/tmp` can expire. This unsigned record does not independently
attest execution or establish remote CI/release readiness. The `.gitignore` edit
and build outputs are excluded. Strict standards findings remain open.

The adapter owns registered fixture instances and poisons them after reported
setup/ownership/cleanup failure. Worked PostgreSQL/SQLite reference providers,
including native connection-loss detection before body access, and the seven-path
CRUD migration remain next. Combined harness acceptance, preparation-count
reduction in that suite and timing/speedup evidence are not claimed here.
