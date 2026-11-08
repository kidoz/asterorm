# Recursive CTE delivery evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/recursive-common-table-expressions.md)
and [design follow-up](../../advanced-query-design.md#recursive-cte-implementation-follow-up-2026-11-08)
cover the delivered constrained form: one statement-root recursive flag, exactly
one self-referencing CTE, explicit output columns and direct anchor/step UNION ALL.
Windows, typed helpers and extensions remain planned.

The validator admits self visibility only inside a checked, plain SELECT step.
The anchor has no self relation; the step has exactly one in FROM/INNER JOIN.
Functions, grouping/HAVING and outer/self-CROSS joins reject in the step. Both
arms and their connector reject local tails, including the anchor: this explicit
portability refinement lets SQLite use direct SELECTs. Consumer tails remain
available. All arms share existing graph budgets, bindings and CTE visibility;
forward/mutual references, nested flags/WITH and multiple self definitions reject
before dispatch. Ordinary definitions may precede/follow the recursive definition.

Recursive arms bypass ordinary set wrappers, keeping the self-reference visible
to all three engines. Other compounds still use their wrappers, including a main
consumer. No new public data fields, native adapter path, version query, hidden
transaction, iteration cap or materialization promise is added. The CTE foundation's
rebuild notes remain applicable.

Local verification covers:

- Exact PG/SQLite/MySQL recursive SQL, shared anchor/step/consumer binds and left
  labels. Tests cover malformed connectors/arms/columns, self counts/placement,
  functions/grouping/tails, nested flags/WITH, forward/mutual and multiple self
  definitions, encoded-error identity, structured arm paths and compiler reuse.
  Inclusive depth/node boundaries include both direct arms.
- Existing fake-session fixtures verify malformed recursion rejects before leasing
  or observation, while valid recursive roots use one normal named materializing
  dispatch. Previous CTE/set capability, hydration, exception and scope coverage
  remains in the full regression suite.
- Native PG/SQLite/MySQL tests traverse real nullable parent/child rows with bound
  seed/anchor/step/consumer predicates. Both self FROM and INNER JOIN forms pass.
  UNION ALL preserves duplicate traversal occurrences, and empty results retain
  named metadata. Caller rollback and supported PG timeout/PG-SQLite test scopes
  preserve the connection-local fixtures. Native missing-table errors emit once.
- A valid echo step that could fail to terminate is admitted in a unit test without
  executing it. Only PG executes a data-cycle case, under an explicit server-side
  timeout; SQLSTATE 57014 is retained, with caller rollback inside the timeout
  callback before cleanup. The same TEMP fixture is readable after recovery.
  No unbounded cycle runs on SQLite/MySQL, and no termination/visited-set/native
  resource guarantee is inferred from syntax budgets.
- All 26 configured regression targets pass. A fresh SQLite-only address/undefined
  sanitizer build passes all 12 targets. Formatting and the project lint command
  pass; existing project diagnostics remain. Strict production query-source lint
  passes without emitted user diagnostics.
- `check_consumers.py` installs all three adapters with tests/examples disabled,
  extracts the literal guide and runs it through installed CMake and shared
  pkg-config consumers. Both verify four traversal rows and nullable parents on
  PG/SQLite/MySQL, without a Catch2 dependency. Existing CMake SQLite target
  deprecation notices remain framework diagnostics.
- Strict standards validation retains the identical existing 53 errors/12 warnings
  baseline. No findings are waived or unrelated standards repaired.

From the repository root with normal PG/MySQL test services:

```sh
meson compile -C buildDir
meson test -C buildDir --print-errorlogs
meson compile -C buildDir clang-format-check
meson compile -C buildDir clang-tidy
python3 docs/evidence/recursive-cte/check_consumers.py
```

`verification.json` binds final sources/docs, ignored canonical contexts and local
command/log receipts. Temporary logs may expire. This is unsigned local evidence,
without remote CI, release provenance, older-server, ClickHouse, performance or
full advanced-query acceptance claims. Older inventory/CTE/set captures remain
historical. Next implement inline windows and typed helpers, then FILTER/DISTINCT
ON and combined advanced-query review.
