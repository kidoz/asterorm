# Read mapped rows through ordinary CTEs

Include `<asterorm/sql/query.hpp>` for the owned query root, compiler overload and
`sql::query_as<Row>`. PostgreSQL, SQLite and MySQL 8.0.19+ support this path.
`Row` needs default construction and `entity_traits<Row>::columns`; table and
primary-key metadata are unnecessary. Mapping uses names, including on empty
results. Extra selected columns are permitted; missing/duplicate mapped names
reject before leasing. Native metadata must match the validated projection.

This complete example creates connection-local data and returns rows 2 and 3.
Pass the usual driver and connection settings, such as `sqlite::driver{}, ":memory:"`.

```cpp
#include <asterorm/session.hpp>
#include <asterorm/sql/query.hpp>

struct eligible_event {
    int id{};
    std::optional<std::string> note;
};

template <> struct asterorm::entity_traits<eligible_event> {
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&eligible_event::note>("note"),
        asterorm::column<&eligible_event::id>("id"));
};

template <typename Driver>
asterorm::result<std::vector<eligible_event>> cte_example(Driver driver, std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.max_size = 1; // Keep the TEMP table on the same connection.
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    auto setup = db.with_connection([](auto& connection) -> asterorm::result<void> {
        auto created = connection.execute(
            "CREATE TEMPORARY TABLE cte_guide_events(id INTEGER PRIMARY KEY, note TEXT)");
        if (!created)
            return std::unexpected(created.error());
        auto inserted = connection.execute(
            "INSERT INTO cte_guide_events VALUES(1,'first'),(2,NULL),(3,'third'),(4,'fourth')");
        if (!inserted)
            return std::unexpected(inserted.error());
        return {};
    });
    if (!setup)
        return std::unexpected(setup.error());

    namespace sql = asterorm::sql;
    auto eligible = sql::select({"id", "note"}).from("cte_guide_events")
                        .where(sql::col("id") > sql::val(1)).build();
    auto consumer = sql::select({"id", "note"}).from("eligible")
                        .where(sql::col("id") < sql::val(4)).order_by("id").build();
    auto report = sql::query(std::move(consumer))
                      .with_cte("eligible", sql::query(std::move(eligible)).build(), {"id", "note"})
                      .build();
    return sql::query_as<eligible_event>(db, report);
}
```

Bindings are `1, 4`: CTEs in declaration order, followed by the final SELECT.
PostgreSQL/SQLite use `$1`, `$2`; MySQL uses `?` in the same occurrence order.
All leaves share one parameter vector, including function arguments, JOIN ON,
WHERE and HAVING. Reused function nodes bind again at each occurrence.
`compiler{dialect}.compile(report)` exposes SQL, parameters and validated
`result_columns` without opening a connection.

The builder moves each leaf/body and is single-use. Existing runtime and typed
builders can supply `.build()` leaves. Definitions can reference earlier CTEs;
optional output-column lists rename columns and must match the body's width.
WITH is restricted to the statement root. Names and output labels must be unique
case-insensitively, single ASCII identifiers up to 63 bytes; CTE references must
match declaration case exactly. Plain columns use their unqualified basename;
function projections need an alias. Explicit projections are required: no wildcard.

Compilation validates before `with_connection`, including encoded parameter
errors, embedded text NULs, null/cyclic function nodes, predicates and joins.
Budgets are depth 64 (root zero), 4096 visited occurrences across the whole
statement, and 64 definitions. They bound syntax, not server resources or runtime.
`COUNT(*)` is the only raw function argument admitted on this path. Function
resolution, base schemas, SQL types/collations and execution errors remain native;
a SELECT AST is not a purity guarantee for server functions.

Execution uses one `observed_execute` call and the session's existing transaction,
stream, timeout and test-scope gates. It starts no transaction. Optional fields
accept SQL NULL; incompatible NULL or text decoding returns an error. Native
errors retain diagnostics, and observer exceptions propagate once. Custom session
facades must declare an explicit `connection_type::sql_dialect` and a compatible
`observed_execute` helper. Unknown dialects and ClickHouse reject before leasing.

Ordinary CTEs, [set operations](set-operations.md) and
[constrained recursive CTEs](recursive-common-table-expressions.md) are implemented.
Other recursive shapes reject. [Windows and typed helpers](window-functions.md)
and [FILTER/DISTINCT ON](aggregate-extensions.md) are implemented. The
[design](../advanced-query-design.md) records the remaining combined review.
MySQL defaults to the baseline 8.0.19 profile;
INTERSECT/EXCEPT requires an explicit set-operation 8.0.31 profile. There is no
server-version discovery query. INSERT SELECT and
streams continue to accept their existing restricted `select_ast` shapes.

Rebuild the library and consumers together: `compiler` and `compiled_query` layouts
changed. `compiled_query` adds `result_columns`; two-element structured bindings
must be updated. Legacy SELECT function aliases now accept either `select_item.alias` or
`func_expr.alias`, emit equal aliases once, and reject conflicts/nested aliases.
Encoded SELECT parameter errors now reject during compilation; SQLite offset-only
SELECT now emits `LIMIT -1 OFFSET ...`. MySQL retains its sentinel LIMIT and
PostgreSQL retains native OFFSET. These corrections also apply to legacy SELECTs.
The original [inventory capture](../evidence/advanced-query-inventory/README.md)
is historical and intentionally retains its earlier observations.
