# Bulk predicate DML acceptance

Reviewed on 2026-11-07 against the source tree at the Git base recorded in
[metadata.json](evidence/bulk-predicate-dml-acceptance/metadata.json).
**Outcome: locally accepted within the revised v1 contract.** The review covers
predicate UPDATE/DELETE, bound assignment arithmetic, INSERT-from-SELECT, and
checked relational affected-row transport. The
[contract](bulk-predicate-dml-design.md) retains the implementation history; the
[guide](how-to/bulk-predicate-mutations.md) describes the supported public API.

## Requirement-to-evidence matrix

| Requirement | Implementation and executable evidence | Accepted boundary |
| --- | --- | --- |
| `update_where<T>` returns affected rows | [Repository](../include/asterorm/core/repository.hpp), [no-server API tests](../tests/unit/test_bulk_predicate.cpp), and [shared live fixture](../tests/support/bulk_predicate.hpp) check exact SQL/parameter order, soft-flag changes, zero matches, matched no-change updates, NULLs and hostile text | Explicit PostgreSQL/SQLite/MySQL capability; required mapped predicate and nonempty assignments. Primary keys and always-generated destinations reject; non-key by-default fields remain writable |
| `delete_where<T>` returns affected rows | The same unit/live fixtures check cutoff deletion, NULL predicates, zero matches, one execution/event, commit and caller rollback | No omitted predicate, joins, LIMIT, RETURNING, entity refresh or implicit transaction. Versioned UPDATE/DELETE reject before leasing |
| Structured bulk price changes | [AST/compiler cases](../tests/unit/test_ast.cpp), API cases and shared native fixture cover nested addition/multiplication, bound leaves, quoting, parameter order, ownership, mapping checks, NULL/type errors and rollback | Assignment values only; immutable factory nodes, depth 64 and 1024 visits per expression. SQL numeric types and cross-target SET evaluation remain native |
| `insert_select<T>` copies rows and returns a count | [Compiler](../src/sql/compiler.cpp), unit cases and [three-backend copy fixture](../tests/support/insert_select.hpp) check inferred destination order, aliases/pagination, typed-source lowering, nullable/text copy, generated identity/default/computed values, empty sources, constraint failure, rollback and commit | Dedicated AST; one-table explicit column projection of matching arity. Both generated categories are omitted. Source schema and target type compatibility are checked by the server |
| All three APIs use existing observation and timeouts | Shared repository execution helper compiles before leasing and calls `observed_execute` once. Spy sessions and live fixtures check success/error events, preserved diagnostics, no event for local rejection, and observer exceptions after autocommit. [PG timeout cases](../tests/integration/test_statement_timeout.cpp) exercise all three methods with both pool and scoped settings | Six native cancellation cases check SQLSTATE `57014`, explicit rollback, unchanged rows, empty archive and reuse of the same physical connection. The timeout is per server statement; no automatic rollback or retry |
| Typical set-based workflows | Shared live fixtures cover cutoff deletion, soft-flag flip, multi-row price adjustment and archive copying with rows left untouched outside the predicate | These are individual statements. An atomic copy-and-delete workflow requires a caller-owned transaction |
| Wide affected-row counts | [Count helpers](../include/asterorm/core/row_count.hpp), core tests and native adapter unit targets cover 0, INT_MAX, INT_MAX+1, UINT64_MAX, malformed/negative/overflow input, result storage/moves and checked narrowing. [SQLite unit cases](../tests/unit/test_sqlite_result.cpp) check stale SELECT/DDL counts and direct counts with triggers/RETURNING | Relational results return `uint64_t`; repository methods return checked `size_t`. Native trigger/cascade side effects are excluded. Billion-row mutations were not executed |
| Unsupported inputs/backends fail before acquisition | API/compiler cases cover unknown/duplicate/malformed columns, composite keys, version policy, invalid/null AST nodes, codec errors, arithmetic budgets, source widths/shapes, generated-only targets, legacy/custom capabilities and ClickHouse dialect admission | Local validation is structural/mapping validation. INSERT sources do not require mapped source traits; ClickHouse runtime remains disabled |
| Public API delivery and compatibility | All three public guide snippets compile in the full suite. [Installed consumers](evidence/bulk-predicate-dml-acceptance/check_consumers.py) build and run literal/arithmetic UPDATE, INSERT SELECT and DELETE using CMake and shared pkg-config consumption | PostgreSQL 16+, SQLite 3.37+, MySQL 8.0.19+ baseline; local MySQL service is 8.4. Rebuild library and consumers after result getter/constructor/layout and `value_source` variant changes; update exhaustive visitors |

## Recorded v1 decisions

The original feature proposal names three methods without defining their full
shape or backend policies. The accepted contract makes those choices explicit:

1. Public methods consume runtime ASTs. A typed predicate can lower through
   `inner`, and a typed SELECT can lower through `build()`. These bridges do not
   prove assignment types or destination compatibility at compile time.
2. UPDATE/DELETE reject entities with a version column. INSERT may project a
   non-generated version field as an ordinary value. Counts do not implement
   entity optimistic locking or report a zero match as `stale_write`.
3. Arithmetic extends `value_source` through separate immutable nodes, preserving
   `expr_ast`. Raw assignment fragments retain the existing explicit escape hatch;
   raw/function arithmetic operands are unavailable. MySQL can read an earlier
   assignment's new value; PostgreSQL/SQLite read the old row. The fixture verifies
   that difference, and the guide explains portable single-target adjustments.
4. INSERT-from-SELECT infers all non-generated destination columns in trait order.
   It accepts column-only projections, aliases, ordering, LIMIT/OFFSET (OFFSET
   requires LIMIT), and bounded column/parameter predicates. Joins, functions,
   grouping/HAVING, locking, conflicts, RETURNING, generated-key overrides and
   custom target subsets are outside v1. The source predicate budget is depth 64
   and 1024 nodes; arithmetic has its separate budget.
5. MySQL's same-source guard compares final table-name components with ASCII
   case folding, even across schemas. This narrows the earlier broad self-source
   rejection proposal to a conservative syntactic rule. It does not resolve views,
   aliases of server objects, Unicode normalization or catalog identity. It is an
   API restriction, with no promise that every rejected native query would fail.
6. Affected counts preserve the direct backend statement count. SQLite uses
   `sqlite3_changes64`/`sqlite3_total_changes64` and therefore requires 3.37+;
   build/package checks enforce that baseline. Row/column hydration indices remain
   unchanged. A count conversion error or observer exception can occur after an
   autocommit write; callers requiring rollback must own a transaction.

## Fresh verification

The combined review found no runtime correction necessary. It adds this acceptance
record, a reproducible installed-consumer probe, source/log provenance and updated
documentation/context status. Existing implementation tests supply the behavior
evidence; historical inventory captures remain unchanged.

| Command | Observed result |
| --- | --- |
| `just test-pg` | `Ok: 3`, `Fail: 0`: no-server adapter, live integration and migration serialization |
| `just test-sqlite` | `Ok: 2`, `Fail: 0`: native result unit and live integration |
| `just test` | `Ok: 14`, `Fail: 0`, including MySQL and all three bulk guide examples |
| `meson compile -C buildDir` | Exit 0; normal library/example targets built |
| `just format-check` | Exit 0 |
| `just lint` | Exit 0 across 50 translation units; existing warnings remain |
| `python3 docs/evidence/bulk-predicate-dml-acceptance/check_consumers.py` | Exit 0: fresh temporary-prefix build/install; CMake and shared pkg-config consumers compile/run; SQLite private dependency requires 3.37+ |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: 53 errors and 12 warnings, identical to the pre-review baseline; no findings on the edited canonical contexts |

[Provenance](evidence/bulk-predicate-dml-acceptance/README.md) binds the reviewed
source/tests, selected build settings, compiler/platform, documents and command
logs. Connection strings and credentials are omitted.

## Limits and next work

This is scoped local acceptance of the revised v1 feature. Remote CI, releases,
all platforms/server versions, sanitizer runs, static linking and live ClickHouse
behavior are not established by these results. Synthetic wide-count boundaries
do not establish performance at that scale. The existing standards failures remain
open and are not waived by feature acceptance.

Next follow the plan with a **streaming cursors and large result sets inventory
and contract review**. Check current materialization, libpq single-row and declared
cursor support, compiler/toolchain availability of `std::generator`, lease/session
ownership, cancellation/error delivery and the bounded-memory test before choosing
an implementation slice. Streaming is still planned.
