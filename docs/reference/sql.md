# SQL builders and typed queries

## Runtime AST

`<asterorm/sql/ast.hpp>` supplies SELECT, INSERT, UPDATE, and DELETE AST nodes,
predicates, joins, expressions, and fluent builders. `<asterorm/sql/compiler.hpp>`
compiles an AST into `result<compiled_query>` containing SQL and parameters.

```cpp
namespace sql = asterorm::sql;
auto selection = sql::select({"id", "email"})
    .from("users")
    .where(sql::col("active") == sql::val(true))
    .build();
sql::compiler statement_compiler;
auto compiled = statement_compiler.compile(selection);
```

The default dialect is PostgreSQL-compatible. Select `sql::dialect::mysql`
for MySQL quoting and placeholders. Repository and typed execution choose the
session's dialect automatically. Identifiers are quoted; values are kept in
parameters. Empty or NUL-containing identifiers are rejected. `func()` names
must be bare identifiers. `raw_expr()` inserts caller-owned SQL text.

## Ordinary CTE query roots

`<asterorm/sql/query_ast.hpp>` adds move-only `query_ast`, `cte_ast`, and the
single-use `query(select_ast).with_cte(name, query_ast, columns={}).build()` builder.
`compiler::compile(query_ast)` validates the whole graph and emits ordered
nonrecursive WITH definitions and the main SELECT with shared bindings.
`compiled_query::result_columns` contains validated output labels for these roots;
it is empty for legacy SELECT/DML. Compiler and result layout changes require a
library/consumer rebuild; two-element structured bindings of `compiled_query`
must be updated.

`<asterorm/sql/query.hpp>` adds `query_as<Row>(session, ast, options={})`, a named,
materializing execution path using `entity_traits<Row>::columns` and the existing
codecs/observer. It requires explicit connection dialect metadata and a compatible
`observed_execute` helper. It validates mapping before leasing and native metadata
even when no rows are returned. See the [CTE guide](../how-to/common-table-expressions.md)
for names, budgets, mapping/alias rules and migration notes.

PostgreSQL, SQLite and MySQL are admitted; ClickHouse/unknown dialects reject.
MySQL profile options offer baseline 8.0.19 and modern 8.0.31, with no hidden
version query. `compound_ast` and the constrained recursive form are implemented.
Windows, typed helpers and aggregate extensions are implemented. There is no implicit
conversion to narrow INSERT SELECT/stream leaves.

## Set expressions

`union_`, `union_all`, `intersect_`, `intersect_all`, `except_` and `except_all`
take two owned `query_ast` values and return `query_builder`. Binary construction
preserves grouping. Operand-local tails are wrapped; builder `order_by`, `limit`
and `offset` affect the current body. Compound ORDER BY uses exact unqualified
left output labels. Operands require equal nonzero arity; types/coercions remain
native. Output labels come from the left and named execution remains `query_as`.

MySQL's default profile admits both UNION forms. INTERSECT/EXCEPT, including ALL,
require `mysql_query_profile::set_operations_8_0_31`. SQLite rejects their ALL
forms without emulation. All new graph nodes/order keys share the query-wide
budgets; nested WITH, unsupported profiles and malformed operands reject before
leasing. Root WITH definitions may contain compounds. See the
[set-operation guide](../how-to/set-operations.md) for grouping, NULL/duplicate
semantics, paging and a complete installed-consumer example.

## Constrained recursive CTEs

Set `query_builder::recursive()` on the statement root and provide exactly one
self-referencing definition with explicit output columns. Its body is a two-leaf
UNION ALL: the anchor has no self-reference, and the step has exactly one self
relation in FROM/INNER JOIN. Functions/grouping/HAVING and outer/self-CROSS joins
reject in the step. Both arms and their connector reject local tails. Direct arms
keep the self-reference visible to PG/SQLite/MySQL; other sets retain wrappers.

All existing validation, binding, named execution and session scopes apply. Earlier
ordinary definitions may be used, later ordinary definitions may consume the result,
and forward/mutual references remain rejected. The library does not detect data
cycles, prove termination or impose native iteration/time/result limits. See the
[recursive guide](../how-to/recursive-common-table-expressions.md) for the precise
subset, nullable parent/child example and timeout recovery notes.

## Inline windows

`sql/window.hpp` provides runtime ranking/navigation/frame-value helpers and
`over(function, over_clause)`. Clauses own partition columns, ordered keys and
optional checked ROWS bounds. `sql/typed_window.hpp` adds entity-checked window
specs and stateful selectables; existing static custom selectables remain valid.
Typed navigation/frame-value/SUM/AVG/MIN/MAX results are optional; COUNT/ranking
results are `int64_t`. Missing required order/frame and bad argument/entity/DTO
types have compile-time diagnostics; runtime values validate before leasing.

Only top-level projections admit windows. Native types/order remain native; use a
CTE to filter a computed result. Window-bearing SELECTs use bounded graph preflight
and numeric placeholder casts on SQLite/MySQL to preserve parameter affinity.
Plain legacy SELECT behavior/SQL is retained. `param_expr` carries primitive kind
hints and `func_expr` owns OVER/position and shared-const FILTER data.
These AST/compiler layout changes require rebuilding consumers. See the [window guide](../how-to/window-functions.md)
for exact types, frames, binding, backend policy and a complete installed example.

## FILTER and DISTINCT ON

Runtime `sql::filter(function, predicate)` copies the function and owns its predicate.
PG/SQLite admit COUNT/SUM/AVG/MIN/MAX in projections and HAVING, including supported
aggregate windows. FILTER predicates reject aggregates/windows/nested FILTER.
Arguments bind before FILTER, then OVER; empty COUNT is zero and the other results
are NULL. `<asterorm/sql/typed_extensions.hpp>` supplies entity-checked
`typed::filter(aggregate, predicate)` and `typed::over(filtered, spec)`.
COUNT is `int64_t`; other filtered aggregate result types are optional.

`select_builder::distinct_on(columns, policy)` and typed
`.distinct_on(policy, typed_columns...)` are PostgreSQL-only. Unique nonempty keys
must exactly match the leftmost ORDER BY names and order. `distinct_on_ties::require_tiebreaker`
requires an extra ordering column; the caller must ensure its uniqueness for
deterministic selection. `allow_peers` explicitly permits arbitrary peer selection.
No default rule, emulation or hidden schema query is added. Row locking rejects.
Both extensions validate before leasing and retain narrow stream/INSERT SELECT/
recursive-step boundaries. `select_ast` and containing query layouts changed;
rebuild library and consumers. See the [extension guide](../how-to/aggregate-extensions.md)
for the complete example and [evidence](../evidence/aggregate-extensions/README.md).

## INSERT-from-SELECT

`insert_select_ast` contains `table`, explicit destination `columns`, and a
`select_ast source`. `compiler::compile` validates the restricted relational
source and emits one statement using the existing SELECT compiler and parameter
vector. `insert_ast` remains VALUES-only. Repository `insert_select<T>` infers
non-generated destinations; see the [mutation guide](../how-to/bulk-predicate-mutations.md)
for projection/default rules, MySQL name guards and supported source shapes.

## Assignment arithmetic

`sql::add(left, right)` and `sql::multiply(left, right)` return immutable
`arithmetic_ptr` nodes accepted by `sql::value_source`. Operands are runtime
columns, encoded parameters or nested arithmetic. The compiler quotes columns,
binds values and parenthesizes every operation; SELECT/predicate `expr_ast`
remains unchanged. Literal/raw assignment construction remains supported.
See the [mutation guide](../how-to/bulk-predicate-mutations.md) for mapping checks,
64-level/1024-visit limits, backend evaluation/type rules and ABI notes.

## Typed SELECT

Namespace: `asterorm::sql::typed`. Builder header: `sql/typed.hpp`.
Execution header: `sql/typed_query.hpp`.

| API | Purpose |
| --- | --- |
| `col<&Entity::field>` | Mapped member expression |
| `val(value)` | Bound value expression |
| `is_null(column)`, `is_not_null(column)` | SQL NULL predicates |
| `select<Entity>()` | Whole mapped entity selection |
| `select_cols(items...)` | Tuple projection inferred in selection order |
| `select_cols<DTO>(items...)` | Aggregate-initialized named result |
| `.where(predicate)` | Row filter |
| `.order_by(column, asc/desc)` | Sort expression |
| `.limit(count)`, `.offset(count)` | Pagination |
| `.group_by(columns...)`, `.having(predicate)` | Grouping and group filter |
| `.for_update()`, `.for_share()` | Exclusive/shared SELECT locking on supported backends |
| `.nowait()`, `.skip_locked()` | Fail or skip on conflicting row locks |
| `.build()` | Lower to a runtime SELECT AST |
| `query(db, builder)` | Execute and return `result<vector<Result>>` |

Comparisons, `&&`, `||`, and `!` compose supported typed predicates. Compile-time
checks cover member mappings, operand compatibility, entity consistency, and
projection construction. SQL validity beyond those checks remains a database
responsibility, including GROUP BY and HAVING semantics.

Typed execution uses `session::observed_execute()` within `with_connection()`,
emitting success/error events before hydration and retaining transaction pinning.
For custom facades, the helper must return exactly the connection's prepared
execution result type; otherwise the existing direct execution fallback applies.

A projection must contain at least one entity-bound item and all such items
must identify the same entity. `count_all()` is not entity-bound, so pair it
with a column or bound aggregate. Plain column projection types preserve
`std::optional`.

## Aggregate projected types

| Expression | Projected C++ type |
| --- | --- |
| `count_all()`, `count(column)` | `std::int64_t` |
| `sum(column)` for integral input | `std::int64_t` |
| `sum(column)` for floating input | `double` |
| `avg(column)` for arithmetic input | `double` |
| `min(column)`, `max(column)` | Column type with optional removed |
| Non-arithmetic `sum` or `avg` | Input type with optional removed |

These aggregates do not add optionality for SQL NULL results. Empty-set or
all-NULL `sum`, `avg`, `min`, and `max` can therefore fail decoding. For such
queries use native SQL with a nullable result type or an explicit SQL default.

Typed SELECT currently has no typed joins or mutation builders, and results
are materialized in memory. Native SQL text is not compile-time checked.

Locking is shared by runtime and typed builders. It supports single-table,
column-only SELECTs on PostgreSQL and MySQL; SQLite and ClickHouse reject it.
An explicit supported connection dialect is required for typed locking queries.
See [locking selected rows](../how-to/select-locking.md) for transaction lifetime,
method precedence, restrictions, and the custom-adapter contract.
