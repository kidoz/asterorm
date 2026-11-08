# Production-pool inventory evidence

Reviewed locally on 2026-11-08. The [inventory and proposed v1 contract](../../production-pool-design.md)
describe the current pool and define the next implementation slices. **This is
inventory evidence, not acceptance of a production-pool implementation.**

`inventory.cpp` compiles against configured public headers and native libraries.
It captures API feature presence, invalid capacity handling, a gated connect that
outlives the capacity deadline, and acquisition/close interleaving. The close
probe holds a lease until the pending acquisition has had a chance to reject;
then it releases the lease and joins every job. Short waits are functional probe
controls, not latency/performance measurements.

The native probes use independent PostgreSQL/MySQL connections and private SQLite
memory. Size-zero caches retain no statements; size two retains one after the
same bound query executes twice. A raw BEGIN survives return/reborrow with no
timeout baseline on all three; each probe explicitly rolls back. A separate PG
case terminates **its own disposable backend** through another test connection,
checks stale local readiness, reborrows and observes the failed first query. It
requires permission to terminate that same-role test backend and touches no user
tables. No PgBouncer service is configured or tested.

Observed current results:

| Probe | Result |
| --- | --- |
| Proposed health/aging/cache-mode/leak/waiter fields | Absent |
| `min_size=3,max_size=1` | Warmup accepts three connections |
| Lazy zero maximum | No retained configuration error; acquisition returns `connection_failed` |
| Blocked connect past capacity deadline | Acquisition still succeeds when connect is allowed to finish |
| Close with a held lease and pending acquisition | Pending acquisition rejects before lease return; close itself waits for that return; closed error is `connection_failed` |
| PG/SQLite/MySQL cache sizes 0 / 2 | Zero / one retained entries after repeat bound SELECT |
| Raw transaction on returned connection | Still active on all three, with baseline absent |
| Terminated PG backend | Local `is_open` remains true; checkout succeeds without health SQL; first query fails |
| Configured C++23 `std::jthread` | Compile, link and run succeed |
| Existing `[pool],[pool-timeout]` | 17 cases / 219 assertions pass |
| Full configured regression | 28 targets pass, zero failures |
| Inventory source lint and formatting | Strict lint and formatting pass; no emitted user diagnostics from the two probes |
| Strict standards comparison | Identical existing 53 errors / 12 warnings / 0 infos |

From the repository root with the normal three-adapter development build and
PostgreSQL/MySQL test services:

```sh
meson test -C buildDir --print-errorlogs
buildDir/tests/core_tests '[pool],[pool-timeout]'
python3 docs/evidence/production-pool-inventory/run.py --build-root buildDir
python3 tools/run_clang_format.py --source-root . --check
```

`run.py` obtains compiler/include flags and library paths from that Meson build.
It compiles/links/runs the inventory, then separately probes `std::jthread`.
The printed temporary workspace contains compile database, command arguments and
thread compiler log; executables/objects are kept outside the repository. A
nonzero thread compile result is an intentional feasibility observation, not a
successful thread-feature claim. The recorded run has exit zero for both probes.

Use the printed workspace with `clang-tidy -p=<workspace>` for the two `.cpp`
files and `--warnings-as-errors='*'`; on macOS also pass `--extra-arg=-isysroot`
and `--extra-arg="$(xcrun --show-sdk-path)"`. The receipt records the final successful
strict run. The initial lint attempt incorrectly included linker arguments in
its compile database; `run.py` now records separate compile and link commands.

These probes intentionally record missing APIs and existing gaps. They are not
registered as production regression tests and their old gap observations must not
be treated as future feature acceptance. Existing tests remain unchanged.

`verification.json` binds source/test/build/document files, ignored canonical
contexts, final observations, command/log hashes and the external documentation
used for the proxy/protocol boundary. Logs may expire. This unsigned local capture
does not establish proxy compatibility, new timeout guarantees, benchmarks,
older-server/ClickHouse behavior, remote CI or release provenance. Existing
standards findings are not waived. No production API/layout changes in this step;
the unrelated `.gitignore` edit and ignored contexts are not staged. Next implement
the ownership/configuration/shutdown foundation.

Check snapshot bytes with
`python3 docs/evidence/production-pool-inventory/verify_snapshot.py`; add `--logs`
while temporary command logs exist. This verifies hashes, not command execution.
