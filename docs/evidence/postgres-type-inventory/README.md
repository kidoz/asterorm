# PostgreSQL type inventory evidence

Reviewed locally on 2026-11-09. The [inventory and v1 contract](../../postgresql-type-design.md)
record the current implementation and planned expansion. **No production type,
codec or JSONB AST helper is implemented by this inventory.** These gap probes are
historical capture tools, not future feature acceptance or registered failing tests.

The standalone [inventory](inventory.cpp) compiles against the configured public
headers and PG/core libraries. It captures current codec/AST behavior and native
PostgreSQL 16 semantics. [Results](results.json) contain **185 observations** and
**11 compile controls**. The positive existing-value/custom-codec control compiles;
ten intended missing/unsupported API cases fail with checked diagnostics. Their
nonzero compiler exits are expected observations, not failed verification runs.

| Area | Captured behavior |
| --- | --- |
| Planned APIs | Range, interval, network, MAC and hstore names are absent. The exact hstore map and automatic nested entity encoding are unsupported. Checked runtime/typed JSON access and JSON path builder names are absent. |
| Extension point | Existing numeric/jsonb/vector values and a codec<T> specialization instantiate successfully. |
| Arrays | Nested vectors encode native multidimensional text but cannot decode it. Dimension-prefix input rejects. A malformed flat decode leaves partial output [1]. vector<jsonb> emits array text rejected by the server. |
| Numeric/time | Numeric NaN decoding rejects; invalid numeric text has no local encode error. Malformed timestamp text normalizes to 2026-03-03 02:39 UTC; offset seconds are ignored, so equivalent instants compare unequal. These are prerequisites to fix, not new supported behavior. |
| Ranges | int4/date canonicalization, empty/unbounded/SQL NULL, explicit timestamp infinity, native numeric NaN, timezone output, reversed/overflow error states are captured. |
| Interval | All four IntervalStyle formats preserve mixed-sign month/day/time data. Calendar addition distinguishes one month from thirty days despite native comparison equality. |
| Network/MAC | inet retains host bits, cidr rejects them with 22P02, IPv6/prefix and six-byte MAC canonical output are captured. |
| Composite/hstore | SQL NULL differs from a non-NULL all-null record; empty text/NULL and nested escaping are distinct. hstore keeps NULL, literal NULL and empty text separate. |
| JSONB | JSON null versus SQL NULL/missing, bound path variables, invalid-path SQLSTATE and two-row set-returning path output are captured. Generic named functions can compile for SQLite without constituting a checked PG feature. |
| Indexes | Native catalog classes and six eligible plans distinguish range GiST, network GiST, scalar B-tree and JSONB/hstore GIN. Sequential scan is disabled locally only for eligibility inspection; this is not a speed/production-plan claim. |
| Cleanup | An owned schema/types, TEMP tables/indexes and hstore setup when absent are confined to a transaction. Explicit rollback is checked against schema absence and the original extension count. Existing extension/schema objects are never dropped/recreated. |

The [runner](run.py) records compile/link commands, a two-source compile database,
native stdout/stderr and per-case compiler logs in a temporary workspace. Native
stdout uses hex payloads; the runner decodes them into JSON without losing escapes
or newlines. It uses
savepoints around expected native failures and restores local settings through
rollback. hstore availability/permission is a fixture prerequisite; installation
is rollback-scoped only when absent. The frozen fixture targets PG 16 and rejects
an unexpected server major instead of silently reusing its conclusions.

Fresh verification:

- Normal probe/compile-control capture exits **0**. The server reports PostgreSQL
  **16.14**. All 185 codec/native observations match a separate probe run with
  address/undefined instrumentation.
- Sanitizer coverage applies to the probe and header-inline codecs; the existing
  linked core/PG libraries and vendor SDK are **not** rebuilt/instrumented here.
  No sanitizer error is reported. This is not full adapter sanitizer acceptance.
- All **35 configured regression targets** pass with no production change.
- Formatting and full project lint pass across **83 registered translation units**.
  A separate strict lint of **two standalone inventory sources** passes, treating
  all emitted warnings as errors. Negative compile modes are not lint targets.
- Strict standards findings match the same-date pre-change baseline byte for byte:
  **53 errors, 12 warnings, 0 infos**. No waiver is introduced.

Run from the repository root with the configured PG development build/server:

```sh
meson compile -C buildDir -j4
python3 docs/evidence/postgres-type-inventory/run.py --build-root buildDir
python3 docs/evidence/postgres-type-inventory/run.py --build-root buildDir --sanitize
meson test -C buildDir --no-rebuild --print-errorlogs
python3 tools/run_clang_format.py --source-root . --check
meson compile -C buildDir clang-tidy
python3 docs/evidence/postgres-type-inventory/verify_snapshot.py --logs
```

The default result goes into the printed temporary workspace. Use --output only
to deliberately regenerate a capture. With that workspace, run clang-tidy on the
two sources with -p pointing to its compile database and --warnings-as-errors='*';
on macOS also pass --extra-arg=-isysroot and the xcrun SDK path. Its compile database
contains compile arguments, not linker arguments.

[Verification metadata](verification.json) hashes final sources/build/tests/docs,
results and temporary logs, plus ignored canonical contexts and temporary command/
compiler-control captures. Hashes verify bytes rather than independently attest
execution; local logs may expire. Previous pool acceptance/delivery captures stay
frozen. No new public API/ABI, type implementation, installed new-type consumer,
other-host/server, binary transport, performance, remote CI or release conclusion
follows. The unrelated .gitignore edit stays unstaged; ignored contexts are not
force-added. Next implement the value/text-codec foundations in the linked contract.
