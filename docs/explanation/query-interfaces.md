# Choosing a query interface

AsterORM offers several interfaces because entity persistence, typed selections,
dynamic query construction, and vendor SQL need different kinds of control.

| Interface | Useful when | What remains your responsibility |
| --- | --- | --- |
| Repository | Saving and finding mapped entities | Mapping, schema, transaction boundaries |
| Typed SELECT | Filtering mapped fields and selecting typed results | Database semantics beyond supported compile-time checks |
| Runtime AST | Building statements from runtime structure | Column names and result mapping |
| Native SQL | Using vendor features or SQL outside the builders | Statement text, parameter use, and matching result types |

Typed builders lower to the same runtime AST and SQL compiler used by string
builders. Compile-time checking adds knowledge of mapped members and projected
C++ types. It does not parse arbitrary SQL or establish that every grouping,
aggregate, or database operation is valid.

Whole-entity queries hydrate according to mapping metadata. Tuple and DTO
projections hydrate positionally in selection order. Native entity queries
match result column names, so aliases matter; strict hydration rejects missing
mapped columns. These contracts help choose a result type that represents the
actual query rather than a partially populated domain entity.

## Parameters and SQL text

On PostgreSQL, SQLite, and MySQL, values are bound separately from SQL text.
The compiler quotes identifiers for the selected dialect. Parameters cannot
substitute table names, operators, or an arbitrary statement fragment; dynamic
structure must be represented by a builder or selected from trusted SQL text.
`native()`, `raw_expr()`, and COPY statement text remain caller-owned.

ClickHouse has a different experimental contract: it interpolates parameter
literals. The adapter is disabled by default and should not receive untrusted
input.

## Portability and cost

Shared templates reduce application changes between backends, while schema
types, isolation behavior, upsert semantics, and generated-key refresh can
differ. Consult the [backend matrix](../reference/backends.md) when changing
databases.

Results are materialized, so fetching an entire large table consumes memory
for all rows. Use projections and pagination to bound work. The library does
not currently provide streaming cursors or async query APIs. Benchmark claims
also have a scope: the [recorded benchmarks](../../benchmarks/README.md) describe
measured cases, not every application's database and network costs.

See [query how-to](../how-to/query-data.md) for examples and
[SQL reference](../reference/sql.md) for supported operations.
