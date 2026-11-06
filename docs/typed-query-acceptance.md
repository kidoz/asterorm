# Typed query DSL acceptance review

Reviewed on 2026-11-06. The typed SELECT implementation and incremental migration
guide are complete. Local acceptance checks pass for the mapped C++ API and the
client-side workloads described below. This is a feature review, not a release
or a claim of universal runtime equivalence.

## Acceptance contract and evidence

The original feature proposal used callable `col<&User::email>()` spelling and
the broad terms “unknown columns/tables” and “unchanged runtime cost.” This review
keeps those distinctions explicit rather than implying a SQL schema checker or
an unbounded performance guarantee.

| Original requirement | Implemented behavior and evidence |
|---|---|
| Member-based column references, retaining string columns | Public spelling is `sql::typed::col<&User::email>` (a variable template). Existing `sql::col("email")` remains. Unit tests check trait-name resolution and lowering; both APIs use the existing AST/compiler. No callable alias was added. |
| Compile-time operand checks | The compile suite accepts a positive control and rejects 17 invalid cases, including numeric/text mismatches, boolean/string and boolean/integer comparisons, cross-entity predicates, incompatible DTOs, and NULL equality. Unit tests verify that text literals remain bound text. |
| Deduced result tuple | `typed::select_cols(items...)` infers `std::tuple<Items::projected_type...>` in selection order. Explicit `select_cols<DTO>` remains supported. Unit and backend tests cover optional values, aggregates, single columns, and explicit/inferred equivalence. |
| Named diagnostics for unknown columns/tables | Unmapped C++ members and entities without `entity_traits` are rejected. The configured compiler names `unmapped` and `unknown_entity` in its diagnostic context. This checks C++ mappings; an incorrect SQL name inside otherwise valid traits is a runtime database error. There is no SQL parser or live-schema validation. |
| Unchanged runtime cost and same SQL | SQL and parameter parity checks cover 192 input/dialect/workload pairs. After the measured storage fixes, construction/compiler cases have no extra typed allocations and no repeatable slowdown above the predeclared 10% investigation screen. Production decoding helpers have matching allocation counts/bytes in all 54 cases. These are bounded observations on one toolchain/machine; exact timing equality and arbitrary end-to-end equivalence are not established. |
| Incremental migration guide | The [guide](typed-query-migration.md) covers mapping, errors, whole entities, inferred tuples, DTOs, NULL predicates, aggregates, and SQL fallbacks. Nine snippets match the [executable source](../examples/typed_query_migration.cpp); that source verifies known results on PostgreSQL, SQLite, and MySQL. |

The first and fourth rows reconcile proposed terminology with the existing
public API. The fifth states the measurement boundary of acceptance; it does not
replace the original performance goal with a universal 10% allowance.

## Retained performance evidence

The [initial baseline](../benchmarks/results/typed-vs-string-dsl-2026-11-06/ANALYSIS.md)
exposed extra typed allocations and a simple-query construction slowdown. The
[vector reservation](../benchmarks/results/typed-dsl-vector-reserve-2026-11-06/ANALYSIS.md)
and [plain-column storage results](../benchmarks/results/typed-dsl-column-storage-2026-11-06/ANALYSIS.md)
retain the separate measurements after those fixes. The latest construction
ratios are 0.829–0.871 typed/string; construction plus compilation is 0.910–0.968,
and prebuilt compilation is 1.000–1.038. The latter is within the investigation
screen, not proof of identical cost.

The [row-decoding results](../benchmarks/results/row-decoding-2026-11-06/ANALYSIS.md)
compare the production typed and repository tuple-decoding helpers on native
PostgreSQL, SQLite, and MySQL result objects with synthetic text fixtures. Across
three shapes, three row counts, and two phases, all 54 comparisons have matching
C++ allocation calls/bytes; paired median timing ratios span 0.835–1.007. This
includes vector collection and destruction, but excludes SQL execution, network,
pooling, arbitrary DTO constructors, and error-path timing. Raw samples, build
settings, machine details, and source hashes remain with each result set.

No production source changed for the original guide/review. At that review,
all 18 source hashes in the row-decoding metadata matched. The earlier
column-storage production hashes also matched; the allocation probe and benchmark
build definitions had changed to add decoding measurements. Later fixes,
including typed observation routing, are recorded in the linked runtime audit.
The benchmark artifacts remain historical measurements of their recorded source
versions; no new timings are claimed here.

## Remaining API limits

The guide documents current limits: one mapped entity per typed query, no typed
aggregate-result predicate operators, nonoptional aggregate projections even for
nullable SQL outputs, no SQL grouping-legality checks, and single-use builders.
Native/string queries remain the migration fallback for unsupported constructs.

As of the 2026-11-07 runtime follow-up, `typed::query` uses the session
observed-execute helper inside `with_connection`. Success/error events and
transaction pinning have unit and PostgreSQL/SQLite/MySQL coverage. Custom
facades without a helper returning the native prepared result type retain direct
execution. Timeout scopes remain planned; see the runtime audit for those limits.

## Checks run for this review

- `meson test -C buildDir 'typed query guide*' --print-errorlogs`: `Ok: 4`,
  `Fail: 0` (nine-snippet consistency check and three backend runs).
- `just test`: `Ok: 10`, `Fail: 0`, including all backend suites and all 18 compile
  cases. The compile runner printed `0 failed compile checks`.
- The configured compile commands for cases 9 and 10 were also rerun explicitly:
  both rejected the invalid code and included the offending member/entity name.
- `meson test -C buildRowDecode --print-errorlogs`: `Ok: 4`, `Fail: 0` for the
  SQL/parameter parity, allocation-probe, decoding, and decoding-allocation checks.
- `just format-check`: exit 0.
- `just lint`: exit 0 across 39 translation units, with existing warnings. The
  new migration example has no reported warnings after its style corrections.
- SQLite-only example configuration: `meson setup /tmp/asterorm-typed-guide-sqlite
  -Dpostgres=disabled -Dmysql=disabled -Dsqlite=enabled -Dtests=false
  -Dexamples=true -Ddefault_library=static` and
  `meson compile -C /tmp/asterorm-typed-guide-sqlite typed_query_migration` both
  exited 0. Running the resulting executable with `sqlite` printed
  `typed query migration: sqlite passed` (exit 0).
- Strict standards validation via
  `uv run --no-project --with pyyaml --with jsonschema python .agents/tools/validate_standards_package.py --root . --mode strict-governance --workspace --json-out /tmp/asterorm-typed-guide-standards.json`:
  exit 1, retaining 52 existing errors and 12 warnings (vendor frontmatter,
  framework freshness/configuration, and a local build symlink). None concern
  the three edited feature-expansion context files. This is not a passing
  repository-wide standards check.

The next planned feature is locks and timeouts. The
[implementation audit](locking-and-timeout-audit.md) now records the existing
paths, live probes, and ordered work. Cancellation classification and typed-query
observer routing are corrected with regression coverage. Shared SELECT locking
now has a restricted supported surface and backend checks. Scoped PostgreSQL
advisory locking and safe lease ownership are implemented; migration serialization
is next.
