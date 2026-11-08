# Combine mapped query results with set operations

Include `<asterorm/sql/query.hpp>`. The six factories take two owned `query_ast`
values and return a single-use `query_builder`:

| Factory | Duplicate semantics | Backends |
| --- | --- | --- |
| `union_`, `union_all` | Distinct union / concatenate occurrences | PostgreSQL, SQLite, MySQL |
| `intersect_`, `except_` | Distinct intersection / left difference | PostgreSQL, SQLite, modern-profile MySQL |
| `intersect_all`, `except_all` | Minimum / subtract occurrence counts | PostgreSQL, modern-profile MySQL |

Both operands need explicit, nonempty, equally wide projections. Each leaf must
satisfy the [query-root validation rules](common-table-expressions.md). Result
labels come from the left operand; right labels may differ. Column types,
coercions, collation and NULL duplicate equivalence are evaluated by the database.
`query_as<Row>` maps those left labels using `entity_traits<Row>::columns`.
There is no inferred compound C++ result type or automatic cast.

This complete example returns IDs `2, 2, 3` on each supported backend. Pass a driver
and connection settings, for example `sqlite::driver{}, ":memory:"`.

```cpp
#include <asterorm/session.hpp>
#include <asterorm/sql/query.hpp>

struct combined_id {
    int id{};
};
template <> struct asterorm::entity_traits<combined_id> {
    static constexpr auto columns = std::make_tuple(asterorm::column<&combined_id::id>("id"));
};

template <typename Driver>
asterorm::result<std::vector<combined_id>> set_example(Driver driver, std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.max_size = 1; // Keep the connection-local fixtures available.
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    auto setup = db.with_connection([](auto& connection) -> asterorm::result<void> {
        for (const auto* statement : {
                 "CREATE TEMPORARY TABLE set_guide_left(id INTEGER NOT NULL)",
                 "CREATE TEMPORARY TABLE set_guide_right(id INTEGER NOT NULL)",
                 "INSERT INTO set_guide_left VALUES(1),(2)",
                 "INSERT INTO set_guide_right VALUES(2),(3)"}) {
            auto executed = connection.execute(statement);
            if (!executed)
                return std::unexpected(executed.error());
        }
        return {};
    });
    if (!setup)
        return std::unexpected(setup.error());
    namespace sql = asterorm::sql;
    auto left = sql::query(sql::select({"id"}).from("set_guide_left")
                               .where(sql::col("id") >= sql::val(1)).build()).build();
    auto right = sql::query(sql::select({"id"}).from("set_guide_right")
                                .where(sql::col("id") <= sql::val(3)).build()).build();
    auto report = sql::union_all(std::move(left), std::move(right))
                      .order_by("id").limit(3).offset(1).build();
    return sql::query_as<combined_id>(db, report);
}
```

Construction determines grouping. `union_(a, intersect_(b, c).build())` keeps the
intersection on the right; `intersect_(union_(a, b).build(), c)` intersects the
whole union. SQL precedence cannot regroup the tree. The compiler emits each
operand as `SELECT * FROM (<operand>) AS <generated-alias>`, including nested
compounds. This internal wildcard expands a validated explicit projection;
generated aliases do not become output labels. The wrapper's alias lives outside
the child's scope. Users still need an outer ORDER BY for deterministic ordering.

`.order_by(label, ascending=true)`, `.limit(count)` and `.offset(count)` affect the
builder's current body. A tail on an operand stays inside its wrapper; a tail on
the outer compound affects its complete result. Compound ordering accepts exact,
unqualified left output labels only: no positions, expressions or right-only
labels. Offset without limit uses PostgreSQL OFFSET, SQLite `LIMIT -1` and MySQL's
sentinel LIMIT. Default distinct operators omit the optional DISTINCT keyword
for SQLite compatibility. NULL order remains native.

WITH definitions belong to the statement root. Attach them to the final builder
with `.with_cte(...)`; definitions can have compound bodies. Operand-local WITH
is rejected rather than hoisted. Definition visibility and case rules apply in
every arm. All definition and main-query arms share one parameter vector, visited
left before right in emitted SQL order; there is no SQL-string renumbering.
The depth-64, 4096-occurrence and 64-definition budgets cover the entire tree,
including operand envelopes and compound ordering keys. Null operands, bad enums,
arity/label errors and unsupported capabilities reject before leasing or events.

MySQL defaults to `mysql_query_profile::baseline_8_0_19`, admitting both UNION
forms. INTERSECT/EXCEPT require explicit
`query_compile_options{mysql_query_profile::set_operations_8_0_31}` passed to the
compiler or `query_as`; this asserts server support, without a version query.
SQLite rejects INTERSECT ALL and EXCEPT ALL with no emulation. ClickHouse and
unknown dialects reject. Local integration covers the configured modern engines;
execution on older MySQL servers is not claimed by that evidence.

Execution remains one observed SELECT through the existing session, retaining
caller transaction, timeout, stream and test-scope gates. Native diagnostics and
codecs retain their normal behavior. MySQL's TEMP-table reopen restriction still
applies: wrapping a query does not promise CTE materialization or permit repeated
reads of one TEMP source in a statement.

The [design](../advanced-query-design.md) records the remaining work. Recursive
CTEs still reject; constrained recursion is next, followed by windows and
extensions. Existing INSERT SELECT/stream leaf restrictions remain in place.
