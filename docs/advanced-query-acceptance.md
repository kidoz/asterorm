# Advanced-query acceptance

Reviewed locally on 2026-11-08 against the Git base, final file hashes and command
receipts in [verification.json](evidence/advanced-query-acceptance/verification.json).
**Outcome: locally accepted within the revised v1 contract.** This covers ordinary
and constrained recursive CTEs, owned set expressions, inline windows/ROWS, typed
helpers, PG/SQLite aggregate FILTER and PostgreSQL DISTINCT ON. The
[design](advanced-query-design.md) retains the original inventory and delivery
history. Earlier evidence remains historical; this review runs fresh checks.

## Requirement-to-evidence matrix

| Requirement | Implementation and executable evidence | Accepted boundary |
| --- | --- | --- |
| Owned ordinary CTEs and explicit mapped execution | `sql/query_ast.hpp`, `sql/query.hpp`, `src/sql/query.cpp`; core `[cte]` and shared native CTE cases | Move-only owned roots, ordered definitions, explicit output labels, `query_as<Row>` named mapping through existing codecs/observation; no arbitrary typed CTE-schema inference |
| Constrained recursion on real data | Core `[recursive-query]`, shared recursive native cases and the new [composition fixture](evidence/advanced-query-acceptance/composition.hpp) | Exactly one self-referencing CTE with explicit columns and direct two-leaf anchor/step UNION ALL; one direct self relation, no step functions/grouping/outer joins. Syntax budgets do not prove data termination or bound server work |
| Set grouping, multiplicity and backend admission | Core `[set-query]`, native set cases and composition tests | Owned binary grouping, branch/final pagination, equal arity and left output labels. SQLite rejects INTERSECT/EXCEPT ALL. MySQL INTERSECT/EXCEPT requires the explicit 8.0.31+ profile; no discovery query or emulation |
| Inline windows and typed helpers | `sql/window.hpp`, `sql/typed_window.hpp`, core/native `[window]`, typed compile controls | Ranking/navigation/frame-value/known aggregates; checked inline PARTITION/ORDER/ROWS. Conservative nullable navigation/value/aggregate results, int64 COUNT/ranking. No named windows, RANGE/GROUPS/EXCLUDE or DISTINCT window aggregates |
| FILTER and DISTINCT ON | `sql/typed_extensions.hpp`, core/native `[aggregate-extension]`, composition variants | PG/SQLite FILTER in projections/HAVING and aggregate windows; structured predicates reject aggregates/windows/nested FILTER. DISTINCT ON is PG-only, with exact leading order keys and an explicit tie policy. Callers ensure ordering uniqueness; no hidden latest-row rule |
| Cross-feature binding and result semantics | New core `[advanced-query]` and [shared native tests](../tests/support/advanced_query.hpp) | Set seeds feed a recursive walk, then a window CTE and paginated UNION ALL. Runtime/typed lowering has identical SQL, labels and lexical binds; known result vectors check duplicates, NULLs, running totals, navigation defaults and both pagination levels |
| Whole-statement validation before dispatch | New core cases mutate later CTEs and compound branches; native SQLite/MySQL rejection before setup | Forward references, malformed nested FILTER/windows, recursive-step violations, operand arity and query-wide budgets reject before any lease/event. Existing graph/cycle/depth/profile and narrow DML/stream guards remain covered by the regression suite |
| Execution, ownership and diagnostics | Native composition cases on PG/SQLite/MySQL | Each successful composed query emits exactly one matching event while one lease is held. Caller rollback and PG/SQLite testing scopes restore source values. Empty results retain named metadata; native failures observe once. A throwing observer propagates once, releases the lease and leaves subsequent ordinary work usable |
| Fresh installed public consumers | [Consumer check](evidence/advanced-query-acceptance/check_consumers.py) | One tests/examples-disabled install supplies CMake and shared pkg-config builds of all five literal public guides plus the composition fixture, each run on PG/SQLite/MySQL with supported/rejected extensions. No Catch2 dependency in consumers; static linking is not tested |

## Composition checked by this review

The fixture has three root CTE definitions. `seeds` combines two physical TEMP
sources using UNION ALL; `walk` recursively joins a separate edge source; `ranked`
adds row numbers, a running SUM and nullable LAG with a bound default. The main
compound filters the computed row number, paginates its left branch, adds two
identical nullable tail rows, then applies final ordering/pagination. The paged
result has IDs **4, 99, 99**, a running total of **70** for ID 4, and SQL NULL in the
tail totals/navigation. Separate physical sources avoid MySQL TEMP-table reopening.

PG/SQLite variants apply FILTER before OVER: the first total is NULL because no
input satisfies `value > 10`; subsequent totals are 20, 20, 60 and 150. PostgreSQL
also applies DISTINCT ON to the windowed recursive output. Explicit parent/ID
ordering selects IDs 3, 4 and 9 before the duplicate tails; window values still
reflect the input before DISTINCT ON. Typed and runtime versions agree.

All values bind in emitted statement order, including early CTE predicates, the
recursive anchor/step, FILTER, navigation default and outer branches. The SQLite/
MySQL statement-wide numeric-cast policy preserves computed-column affinity and
integer navigation results. Compiler reuse after failure returns to ordinary
uncast SELECT SQL with fresh parameter numbering.

## Verification and compatibility

The [reproduction record](evidence/advanced-query-acceptance/README.md) and JSON
receipt bind current production sources, test/build files, public guides,
acceptance fixtures and ignored canonical contexts. Checks include the complete
28-target configured suite, focused native/core cases, 34 typed compile controls,
a fresh 14-target SQLite-only address/undefined sanitizer build, formatting,
project lint and both installed package consumers. Exact counts and local log
hashes are recorded in the receipt. Source hashes detect drift; they do not
independently attest execution.

The pre-existing standards findings remain **53 errors and 12 warnings**, identical
to the same-date baseline. Local feature acceptance does not waive them or imply
remote CI/release readiness. Lint retains project diagnostics, including the
analyzer's initialized-constexpr-trait report described in the reproduction record.

No production API or layout changes in this acceptance step. Earlier deliveries
changed `compiler`, `compiled_query`, function/parameter/SELECT AST and containing
query layouts: rebuild the library and consumers together. Two-element structured
bindings of `compiled_query` must account for `result_columns`. Streaming and
INSERT SELECT retain their narrow leaf interfaces; no implicit advanced-root
conversion is added.

## Limits and next work

Acceptance is local to the recorded toolchain and configured PG/SQLite/MySQL
versions: Clang 21.0.0, PostgreSQL 16.14, SQLite 3.54.0 and MySQL 8.4.13.
Older servers, ClickHouse, other platforms, static package linking,
remote CI, release provenance and performance are outside this evidence. Native
types/coercions/collations/NULL/peer ordering stay native. Callers own deterministic
ordering, recursion termination/resource limits and explicit transactions. The
accepted v1 subset does not imply every possible cross-product of query features.

Next follow the shared feature order with a **production connection-pool inventory
and v1 contract**: reconcile current borrow/return/shutdown behavior with proposed
health checks, idle/lifetime handling, metrics, leak detection, statement caching
and PgBouncer requirements before implementation.
