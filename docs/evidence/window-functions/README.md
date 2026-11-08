# Window-function delivery evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/window-functions.md) and
[design follow-up](../../advanced-query-design.md#window-implementation-follow-up-2026-11-08)
cover inline OVER/ROWS, ranking/navigation/frame-value helpers and typed window
aggregates. FILTER ownership is reserved and non-null data rejects; FILTER,
DISTINCT ON and combined advanced-query review remain next.

Runtime and typed helpers lower into one function AST/compiler. Typed selectable
values now retain specs/aliases, with existing static lowering still supported.
Results conservatively encode navigation/frame/aggregate nullability; COUNT and
ranking use int64. Frame constants/ordering, function arity/context, entity/type
compatibility and graph budgets validate before dispatch. Windows in predicates,
nested/unknown/DISTINCT windows and recursive steps reject. A presence scan scopes
bounded legacy preflight to structured window SELECTs, preserving the existing
plain SELECT/stream-boundary behavior.

Native testing exposed two text-bind affinity issues: SQLite computed CTE output
comparisons did not acquire a base column's numeric affinity, and MySQL widened an
integer LEAD default to decimal text. Primitive AST kind hints now emit numeric
casts on SQLite/MySQL throughout window statements. PostgreSQL and non-window SQL
remain unchanged. Payloads/adapters stay text based; unsigned SQLite window values
outside int64 range reject. New function/parameter AST and compiler layouts require
rebuilding the library and consumers together.

Local verification covers:

- Golden PG/SQLite/MySQL SQL and lexical binds across projection arguments,
  defaults, JOIN/WHERE/HAVING, with checked literal positions/frames. Tests cover
  every helper/aggregate, malformed frames/arity/keys/context, reserved FILTER,
  exact occurrence budgets, numeric casts/NULL/range and compiler reuse.
- Positive and negative typed compile controls total 29 cases, including mixed
  entities, missing required order/frame, bad offset/NTH/frame types, widening
  defaults, unsupported OVER/SUM arguments and incompatible DTO nullability.
- Fake-session tests reject invalid specs before leasing and execute valid runtime
  and typed windows through one observer call; unsupported typed dialects reject.
- Native PG/SQLite/MySQL runtime/typed SQL, parameters and result DTOs agree.
  Ranking ties, NULL navigation/defaults, running/full/empty ROWS frames, all
  aggregate helpers and default peer RANGE versus ROWS behavior are checked.
  Native NULL ordering differences remain explicit. A window CTE is filtered with
  a bound numeric row-number threshold, and empty results retain named metadata.
  Caller rollback and supported PG/SQLite test scopes preserve rows.
- All 27 configured regression targets pass. A fresh SQLite-only address/undefined
  sanitizer build passes all 13 targets. Formatting and project lint pass;
  existing project diagnostics remain. Strict query/window compiler-source lint
  passes with no emitted user diagnostics.
- Installed CMake and shared pkg-config consumers extract the literal typed guide
  and verify partitioned ranking, NULL navigation and running sums on all three
  enabled adapters, with tests/examples disabled and no Catch2 dependency. Existing
  CMake SQLite target deprecation notices remain framework diagnostics.
- Standards validation retains the identical existing 53 errors/12 warnings;
  no waiver or unrelated standards repair is introduced.

From the repository root with normal PG/MySQL test services:

```sh
meson compile -C buildDir
meson test -C buildDir --print-errorlogs
meson compile -C buildDir clang-format-check
meson compile -C buildDir clang-tidy
python3 docs/evidence/window-functions/check_consumers.py
```

`verification.json` binds final sources/docs, ignored canonical contexts and local
command/log receipts. Temporary logs may expire. This unsigned local evidence
claims no remote CI, release provenance, older-server/ClickHouse execution,
performance result or full advanced-query acceptance. Earlier captures remain
historical. Next implement PG DISTINCT ON and PG/SQLite FILTER, including window
aggregates, then combined review.
