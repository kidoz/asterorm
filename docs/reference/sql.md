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
