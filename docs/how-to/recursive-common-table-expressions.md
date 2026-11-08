# Traverse parent/child rows with a recursive CTE

Use the existing query root and `union_all` builder. Set `.recursive()` on the
statement root and give the self-referencing CTE an explicit column list. Exactly
one definition may recurse. Earlier ordinary CTEs remain visible and later
ordinary definitions may consume the recursive result; forward/mutual recursion
and nested WITH are rejected. PostgreSQL, SQLite and baseline-profile MySQL admit
this constrained form. ClickHouse and unknown dialects still reject before leasing.

This complete example traverses four acyclic rows, returning IDs 1, 2, 3, 4 and
nullable parent IDs. Pass a driver and connection settings, for example
`sqlite::driver{}, ":memory:"`.

```cpp
#include <asterorm/session.hpp>
#include <asterorm/sql/query.hpp>

struct descendant_row {
    int id{};
    std::optional<int> parent;
};
template <> struct asterorm::entity_traits<descendant_row> {
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&descendant_row::parent>("parent"),
        asterorm::column<&descendant_row::id>("id"));
};

template <typename Driver>
asterorm::result<std::vector<descendant_row>> recursive_example(Driver driver,
                                                              std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    auto setup = db.with_connection([](auto& connection) -> asterorm::result<void> {
        for (const auto* statement : {
                 "CREATE TEMPORARY TABLE recursive_guide_roots(id INTEGER, parent INTEGER)",
                 "CREATE TEMPORARY TABLE recursive_guide_edges(id INTEGER, parent INTEGER)",
                 "INSERT INTO recursive_guide_roots VALUES(1,NULL)",
                 "INSERT INTO recursive_guide_edges VALUES(2,1),(3,2),(4,1)"}) {
            auto executed = connection.execute(statement);
            if (!executed)
                return std::unexpected(executed.error());
        }
        return {};
    });
    if (!setup)
        return std::unexpected(setup.error());
    namespace sql = asterorm::sql;
    auto anchor = sql::query(sql::select({"id", "parent"}).from("recursive_guide_roots")
                                 .where(sql::col("id") == sql::val(1)).build()).build();
    auto step = sql::query(sql::select({"recursive_guide_edges.id", "recursive_guide_edges.parent"})
                               .from("recursive_guide_edges")
                               .inner_join("walk", sql::col("recursive_guide_edges.parent") == sql::col("walk.id"))
                               .where(sql::col("recursive_guide_edges.id") <= sql::val(10))
                               .build()).build();
    auto report = sql::query(sql::select({"id", "parent"}).from("walk")
                                 .where(sql::col("id") >= sql::val(1)).order_by("id").build())
                      .with_cte("walk", sql::union_all(std::move(anchor), std::move(step)).build(),
                                {"id", "parent"})
                      .recursive().build();
    return sql::query_as<descendant_row>(db, report);
}
```

Bindings are `1, 10, 1`: anchor, recursive step, then consumer. Earlier WITH
bodies bind before them. The compiler shares the statement's parameter vector and
emits `WITH RECURSIVE ... AS (SELECT ... UNION ALL SELECT ...)` with direct arms.
Ordinary [set-operation wrappers](set-operations.md) still preserve other compound
trees, but cannot wrap the recursive arms: the self table must remain visible in
the direct step. A compound consumer remains supported.

Both arms must be plain SELECT leaves with equal nonzero arity. The anchor cannot
read the self relation. The step must read it exactly once, at top-level FROM or
an INNER JOIN; aliases are allowed. Outer joins and self CROSS JOIN reject.
Additional non-self INNER/CROSS joins retain the normal validation rules. The
step projects columns and uses only column/parameter expressions in comparisons,
logical and NULL predicates. Functions, aggregates, grouping and HAVING reject
there. The existing AST has no direct parameter-only projection item.

The connector and both arms reject local ORDER BY/LIMIT/OFFSET, including anchor
tails, so this portable subset can emit direct SELECTs on SQLite too. Put result
ordering/pagination on the consumer. The explicit CTE columns must match the arm
width; names, labels, case, graph and budget rules from the
[ordinary CTE guide](common-table-expressions.md) still apply. `.recursive()` on
an operand/definition or a root without an actual self definition rejects; it is
not an ignored SQL modifier. Names in schema-qualified tables denote base tables,
not CTE self-references.

SQL self-reference is a relation name, distinct from a cyclic C++ function graph.
Syntax validation does not prove data termination, detect data cycles, insert a
visited set or impose an iteration/time/result limit. UNION ALL keeps duplicate
traversal occurrences. Use terminating data/predicates and appropriate native
server limits; a consumer WHERE/LIMIT is not a general recursion guard. The
example's data is acyclic; its ID predicate alone would not stop every data cycle.
Result order requires the consumer ORDER BY. Native type inference, coercions and
errors remain native, including MySQL's anchor-derived type/width behavior.

This is one materializing observed SELECT, using the caller's transaction, timeout,
stream and test-scope gates. PostgreSQL cancellation retains its SQLSTATE; explicit
rollback inside a statement-timeout callback is needed to recover an aborted
transaction before timeout cleanup, as described in the
[timeout guide](statement-timeouts.md). No transaction is started or completed by
`query_as`. The separate TEMP sources avoid MySQL's reopen restriction; recursion
adds no materialization promise. Library syntax budgets do not bound server work.

The [design](../advanced-query-design.md) and
[delivery evidence](../evidence/recursive-cte/README.md) record the implemented
subset and local checks. Windows and typed helpers are next, followed by FILTER,
DISTINCT ON and combined review. INSERT SELECT and streams retain their narrow
leaf-only interfaces. This delivery activates the existing recursive flag and
adds no public data fields; the CTE foundation's rebuild notes remain applicable.
