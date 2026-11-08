# Aggregate extension delivery evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/aggregate-extensions.md)
and [design follow-up](../../advanced-query-design.md#aggregate-extension-implementation-follow-up-2026-11-08)
cover PG/SQLite aggregate FILTER, including windows, and PostgreSQL DISTINCT ON.
Combined advanced-query acceptance remains next.

FILTER owns a shared-const predicate and copies the decorated function. Its
predicate shares query-wide graph/depth/occurrence validation; arguments bind
before FILTER and OVER. Projection/HAVING contexts are admitted and aggregate/
window/nested FILTER predicates reject. Typed helpers retain entity checks and
nullable result types. SQLite filtered statements use the established numeric
parameter-cast policy. DISTINCT ON requires unique keys matching the exact leading
ORDER BY names and an explicit tie policy. An extra sort key is a syntactic
requirement; callers must ensure uniqueness for deterministic selection.
Unsupported backends reject before leasing, with no emulation or hidden queries.
SELECT and containing query layouts change; rebuild library and consumers together.

Local verification covers:

- 155 focused unit assertions in eight cases: golden SQL/bind order, owned
  decoration, typed lowering/nullability, invalid contexts/policies/keys, graph
  cycles/depth/exact occurrence limits, narrow API guards and fake-session
  zero-lease rejection/one observed execution.
- 34 positive/negative typed compile controls, including mixed entities,
  unsupported FILTER inputs, incompatible result nullability and boolean SUM.
- 97 PostgreSQL and 82 SQLite native assertions: runtime/typed SQL, binds and
  results agree; NULL/empty/all-excluded aggregate inputs, full-partition windows,
  scalar DISTINCT FILTER, HAVING, caller rollback, testing scopes and native errors
  retain existing behavior. PostgreSQL verifies nullable-key DISTINCT ON winners,
  explicit peer acceptance and DISTINCT ON combined with filtered windows.
  Six MySQL assertions verify rejection before connection acquisition/observation.
- All 28 configured regression targets pass. A fresh SQLite-only address/undefined
  sanitizer build passes all 14 targets. Formatting and full project lint pass;
  existing project diagnostics remain. Strict query/window source lint emits no
  user diagnostics. A focused build/run and unit lint supplement cover the final
  test-expression/initializer cleanup after the full lint run.
- Fresh installed CMake and shared pkg-config consumers extract the literal public
  guide, verify PG/SQLite totals and PostgreSQL winners, and reject unsupported
  combinations. Tests/examples are disabled and Catch2 is not required. Existing
  CMake SQLite target deprecation notices remain framework diagnostics.
- Standards validation retains the identical existing 53 errors/12 warnings;
  no waiver or unrelated standards repair is introduced.

From the repository root with normal PostgreSQL/MySQL test services:

```sh
meson compile -C buildDir
meson test -C buildDir --print-errorlogs
meson compile -C buildDir clang-format-check
meson compile -C buildDir clang-tidy
python3 docs/evidence/aggregate-extensions/check_consumers.py
```

`verification.json` binds final sources/docs, ignored canonical contexts and local
command/log receipts. Temporary logs may expire. This unsigned local evidence
claims no remote CI, release provenance, older-server/ClickHouse execution,
performance result or combined advanced-query acceptance. Earlier captures remain
historical. Next review composition across CTEs, sets, constrained recursion,
windows and these extensions with fresh source-bound acceptance evidence.
