# Advanced query inventory evidence

This supports the [CTE/window/set-operation contract](../../advanced-query-design.md).
The snapshot records current implementation gaps and native feasibility; it does
not implement or accept the proposed structured API.

- `ast_probe.cpp` compiles the current public AST and records parameter traversal,
  ignored function-item aliases, SQLite OFFSET rendering and deferred encoded errors.
  Source inspection covers recursion/validation limitations without executing
  dangerous null/cyclic graphs.
- `native_probe.cpp` is built separately with configured PG/SQLite/MySQL dependencies.
  It checks binding, CTE reuse, a recursive graph, ranking/navigation/frames,
  duplicate/NULL set semantics, grouping/pagination and native rejections.
- `results.json` records observed rows and error codes. Native versions are
  PostgreSQL 16.14, SQLite 3.54.0 and MySQL 8.4.13. Every probe exits 0, including
  explicit checks of expected statement failures.
- `capture.json` records the Git base, platform, configured flags and probe/result
  hashes. `verification.json` additionally binds reviewed sources/docs/ignored
  canonical contexts and fresh command/log receipts.

From the repository root, with normal PG/MySQL test services and development
dependencies available in an all-three-backend build:

```sh
meson compile -C buildDir
python3 docs/evidence/advanced-query-inventory/capture.py --build-root buildDir --lint
just test
just format-check
```

The capture uses configured Ninja compile/link commands, excludes Catch2 libraries
and writes temporary executables outside the source tree. `--lint` applies
configured-flag clang-tidy with warnings promoted to errors to each of the four
configurations. Both new C++ sources also pass project formatting. The current
23-target regression suite and normal build pass. Existing framework diagnostics
and strict standards failures remain separate; no standards waiver is introduced.

MySQL's TEMP-table reopen restriction is deliberately observed, not treated as an
ORM failure: simple and DISTINCT CTE bodies over the reused TEMP source both yield
1137 in this fixture. A literal-row CTE reuse succeeds. Separate root/edge and
left/right TEMP tables let the recursive/set probes avoid unrelated reopen failures.
DISTINCT is never inserted as an ORM workaround, and no materialization guarantee
is inferred from successful CTE syntax.

The mixed UNION/INTERSECT query yields different row counts on SQLite versus
PG/MySQL; derived wrappers with local tails work on all three. Default frames and
NULL ordering are recorded explicitly. Window DISTINCT rejects on all configured
engines. SQLite rejects INTERSECT/EXCEPT ALL; MySQL rejects FILTER and DISTINCT ON.
These are observed native capabilities, distinct from the contract's version-gated
planned ORM policy.

Only connection-local TEMP data is created. The scripts honor
`ASTERORM_TEST_CONNINFO` / `ASTERORM_MYSQL_TEST_CONNINFO` or the repository's local
service defaults, and omit supplied settings/credentials from captured output.
No application tables, installed library or public API changes. Native success is
not evidence of structured AST support, execution-layer acceptance, timing or
performance. ClickHouse and older-server configurations are not executed.

Earlier acceptance/delivery captures remain historical and untouched. This is
unsigned local evidence, not independent execution attestation, remote CI or
release provenance. Raw logs under `/tmp` can expire; compare hashes before reuse.
After implementation changes the assumptions these probes intentionally assert,
retain this snapshot as history rather than relabeling it as acceptance.
