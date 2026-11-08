# Set-operation delivery evidence

Reviewed locally on 2026-11-08. The [guide](../../how-to/set-operations.md) and
[design follow-up](../../advanced-query-design.md#set-expression-implementation-follow-up-2026-11-08)
cover this delivery: six owned set factories, explicit binary grouping, branch and
compound tails, left output labels, shared bindings and backend/profile checks.
Recursive CTEs, windows and extensions remain planned; recursive flags still reject.

Validation traverses every operand using the same query-wide depth/node budget
and CTE visibility rules. It rejects null children, invalid operators/quantifiers,
unequal/empty arity, invalid final ordering and unsupported capabilities before
dispatch. Derived SELECT wrappers preserve operand grouping/local tails on all
three adapters. Internal aliases remain scoped outside children and do not change
output labels. Types, coercions, NULL equality and collations remain native.
There is no multiset emulation, materialization guarantee or version-discovery SQL.

Local verification covers:

- Golden PG/SQLite/MySQL SQL for every admitted operator/quantifier and exact
  shared bindings. Default distinct syntax omits DISTINCT for SQLite compatibility.
  Public factories, local/final tails, offset normalization, CTE compounds, nested
  operand paths, inclusive depth/node limits and compiler reuse are covered.
- Session-level malformed/nested-WITH/arity/profile failures reject before
  acquisition/events. Valid CTE/compound/main bindings use one observed dispatch
  and named result decoding. The existing CTE execution/error/observer tests remain
  in the same regression suite.
- Native PG/SQLite/MySQL cases check duplicate/NULL set and bag results, differently
  named right projections, opposed groupings, branch/final pagination, empty-result
  metadata, compound CTE definitions and bound execution. Caller rollback, supported
  PostgreSQL timeout scopes and PostgreSQL/SQLite test scopes retain behavior.
  Each missing-table failure produces one native error observation. Fixtures use
  separate TEMP sources to avoid MySQL's unrelated reopen limitation.
- MySQL's baseline rejects INTERSECT/EXCEPT throughout nested trees; the explicit
  8.0.31+ profile admits them. Native modern-profile ALL semantics pass on the
  configured MySQL engine. SQLite rejects INTERSECT ALL and EXCEPT ALL locally;
  UNION ALL remains admitted. Older-server execution is not established.
- All 25 configured regression targets pass. A fresh SQLite-only address/undefined
  sanitizer build passes 11 targets. Formatting and the project lint command pass;
  lint retains project diagnostics, including intentional malformed enum fixtures.
  Strict lint of the production query compiler passes without emitted user diagnostics.
- `check_consumers.py` installs all enabled adapters with tests/examples disabled,
  extracts the literal guide and builds/runs both installed CMake and shared
  pkg-config consumers. Both verify bound UNION ALL duplicates and pagination on
  PG/SQLite/MySQL, without a Catch2 dependency. Existing CMake SQLite target
  deprecation notices remain framework diagnostics.
- Strict standards validation retains the identical existing 53 errors/12 warnings
  baseline; no waiver or unrelated standards repair is introduced.

From the repository root with normal PG/MySQL test services available:

```sh
meson compile -C buildDir
meson test -C buildDir --print-errorlogs
meson compile -C buildDir clang-format-check
meson compile -C buildDir clang-tidy
python3 docs/evidence/set-operations/check_consumers.py
```

`verification.json` binds final sources/docs, ignored canonical contexts and local
command/log receipts. Temporary logs may expire. This unsigned local evidence
makes no remote-CI, release-provenance, performance, ClickHouse or full advanced-query
acceptance claim. Previous inventory/CTE captures stay historical.

The reserved compound storage is now active; this delivery adds no new data fields.
The CTE foundation's layout/rebuild notes remain applicable. The next delivery is
constrained recursive CTEs: checked anchor/step UNION ALL and real parent/child
traversal, followed by windows/typed helpers and extensions/combined review.
