# Ordinary CTE foundation evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/common-table-expressions.md)
and [design follow-up](../../advanced-query-design.md#ordinary-cte-implementation-follow-up-2026-11-08)
describe the delivered scope. This implements ordinary CTEs and the shared
root/compiler/mapped-execution foundation. Sets, recursive CTEs, windows and
extensions remain planned; reserved compound/recursive roots reject locally.

The core implementation lives in `sql/query_ast.hpp`, `src/sql/query.cpp` and
`sql/query.hpp`. Existing SELECT rendering now takes the statement's shared
parameter vector. No independently compiled SQL is concatenated/renumbered.
The validator checks labels, definition visibility, encoded errors, graph cycles,
null/invalid nodes and inclusive depth/node/definition budgets before dispatch.
Explicit mapped rows use existing codecs and one observed execution, without
starting or completing a transaction. Public compiler/result layout changes
require rebuilding the library and consumers together.

Verification covers:

- Unit golden SQL for PG/SQLite/MySQL and binding order across dependent CTEs,
  function/projection/JOIN/WHERE/HAVING occurrences and shared function DAGs.
  Move ownership, alias normalization, SQLite offset-only SELECT, structured
  errors/NULL, malformed names/references/shapes/operators and exact budget
  boundaries are exercised. Compiler reuse after failure has fresh state.
- Session tests reject invalid compilation/mapping, unknown/ClickHouse dialects
  and missing/incompatible observer helpers before connection acquisition or SQL.
  Named decoding is independent of trait order and requires no table metadata.
  Empty/missing/duplicate native metadata, incompatible NULL/text, preserved
  native diagnostics and one throwing observer callback are covered.
- Shared native tests on all three configured adapters check dependent CTEs,
  renamed outputs, moved typed leaves, nullable rows, one SELECT event and caller
  rollback. PostgreSQL timeout scopes and PostgreSQL/SQLite transactional test
  scopes retain their existing behavior. Native offset normalization, COUNT(*)
  aliases and missing-table diagnostics are checked. TEMP sources are referenced
  once per definition to avoid MySQL's unrelated reopen restriction.
- The configured 24-target regression suite passes. A fresh SQLite-only build
  with address/undefined-behavior sanitizers passes all 10 targets. Build and
  project formatting pass. The project lint command covers 67 distinct TUs;
  it exits 0 with project diagnostics, including existing findings and intentional
  malformed-enum fixtures. Strict lint of the new query compiler source passes
  with no emitted user-code diagnostics.
- `check_consumers.py` installs all three enabled adapters to a temporary prefix
  with tests/examples disabled, extracts the literal public guide, and builds/runs
  it through both CMake's `AsterORM::asterorm` and shared pkg-config. Both consumers
  verify rows 2/3 and NULL on PG/SQLite/MySQL without requiring Catch2. Existing
  CMake SQLite target deprecation notices remain framework diagnostics.
- Standards validation remains the identical existing baseline: 53 errors,
  12 warnings, no infos. No findings are waived or unrelated standards repaired.

Run normal PG/MySQL test services and, from the repository root:

```sh
meson compile -C buildDir
meson test -C buildDir --print-errorlogs
meson compile -C buildDir clang-format-check
meson compile -C buildDir clang-tidy
python3 docs/evidence/ordinary-cte/check_consumers.py
```

`verification.json` binds final reviewed sources/docs and ignored canonical
contexts to local command/log receipts. Logs remain temporary and may expire;
this is unsigned local evidence, without remote CI, older-server, ClickHouse,
performance, release-provenance or full advanced-query acceptance claims.
The [original inventory capture](../advanced-query-inventory/README.md) is frozen:
its pre-implementation alias/parameter/pagination gap assertions intentionally
represent the earlier implementation and must not be refreshed as feature tests.
The next delivery is set-expression trees, derived wrappers and arity/profile
checks; constrained recursion follows sets, then windows and extensions.
