# Rank and aggregate rows with inline windows

Use `<asterorm/sql/window.hpp>` for runtime helpers and
`<asterorm/sql/typed_window.hpp>` for typed helpers. They lower to the same function
AST, compiler and observed execution path on PostgreSQL, SQLite and MySQL 8.0.19+.
ClickHouse/unknown dialects reject. Typed window execution requires explicit
connection dialect metadata and a compatible `observed_execute` helper.

| Function | Typed result | Required window keys |
| --- | --- | --- |
| `row_number`, `rank`, `dense_rank` | `int64_t` | ORDER BY |
| `lag`, `lead` | `optional<column value type>` | ORDER BY |
| `first_value`, `last_value`, `nth_value` | `optional<column value type>` | ORDER BY and explicit ROWS frame |
| Windowed `count_all`, `count` | `int64_t` | Optional |
| Windowed `sum`, `avg`, `min`, `max` | Optional existing aggregate result type | Optional |

SUM/AVG typed inputs must be numeric and non-boolean. Navigation defaults must
match the column's unwrapped type; string-like defaults are accepted for strings.
Results stay conservatively optional even with a non-NULL default. Signed integer
offsets start at zero; NTH positions start at one. Values are checked before leasing.
Frame bounds use signed integer constants too. Functions require OVER; unknown
functions with OVER, DISTINCT windows, nested windows and windows in WHERE/JOIN/
HAVING reject locally. Filter window results through a CTE instead.

This complete example returns four rows with partitioned row numbers, previous
values and a running sum. Pass a driver and settings, such as `sqlite::driver{},
":memory:"`.

```cpp
#include <asterorm/session.hpp>
#include <asterorm/sql/query.hpp>
#include <asterorm/sql/typed_window.hpp>

struct window_event {
    int id{};
    int group{};
    std::optional<int> value;
};
struct window_report {
    int id{};
    std::int64_t number{};
    std::optional<int> previous;
    std::optional<std::int64_t> running;
};
template <> struct asterorm::entity_traits<window_event> {
    static constexpr const char* table = "window_guide_events";
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&window_event::id>("id"),
        asterorm::column<&window_event::group>("grp"),
        asterorm::column<&window_event::value>("value"));
};
template <> struct asterorm::entity_traits<window_report> {
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&window_report::id>("id"),
        asterorm::column<&window_report::number>("number"),
        asterorm::column<&window_report::previous>("previous"),
        asterorm::column<&window_report::running>("running"));
};

template <typename Driver>
asterorm::result<std::vector<window_report>> window_example(Driver driver, std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    auto setup = db.with_connection([](auto& connection) -> asterorm::result<void> {
        auto created = connection.execute(
            "CREATE TEMPORARY TABLE window_guide_events(id INTEGER, grp INTEGER, value INTEGER)");
        if (!created)
            return std::unexpected(created.error());
        auto inserted = connection.execute(
            "INSERT INTO window_guide_events VALUES(1,1,10),(2,1,NULL),(3,1,30),(4,2,40)");
        if (!inserted)
            return std::unexpected(inserted.error());
        return {};
    });
    if (!setup)
        return std::unexpected(setup.error());
    namespace sql = asterorm::sql;
    namespace typed = sql::typed;
    auto ordered = typed::window().partition_by(typed::col<&window_event::group>)
                       .order_by(typed::col<&window_event::id>);
    auto running = ordered.rows_between(sql::unbounded_preceding(), sql::current_row());
    auto selection = typed::select_cols<window_report>(typed::col<&window_event::id>,
        typed::over(typed::row_number(), ordered).as("number"),
        typed::over(typed::lag(typed::col<&window_event::value>), ordered).as("previous"),
        typed::over(typed::sum(typed::col<&window_event::value>), running).as("running"));
    selection.order_by(typed::col<&window_event::id>);
    auto report = sql::query(selection.build()).build();
    return sql::query_as<window_report>(db, report);
}
```

The typed selectable values retain their window specs and aliases. Existing static
custom selectables remain supported. `typed::query(db, selection)` also executes
window projections into positional DTOs/inferred tuples; named query roots require
explicit function aliases as above. Mixed entities, missing required order/frame,
bad offset/position types and incompatible defaults/DTO nullability fail compilation.
Runtime window values and frame ordering are checked during compilation of the SQL.

Runtime `sql::over(function, over_clause)` accepts owned partition-column/order-key
lists and an optional `rows_frame`. `rows_between(unbounded_preceding(),
unbounded_following())` selects a full partition; `following(1)` to `following(1)`
can yield an empty frame at a partition edge. Bounds must be ordered, nonnegative,
and use valid endpoints. No named windows, RANGE/GROUPS/EXCLUDE or NULL-treatment
modifiers are exposed. Default native frames still apply when ROWS is omitted:
peer groups can change a running aggregate's result. Explicit frame-value helpers
avoid default-frame surprises. Ranking/navigation retain native frame semantics.
Window ordering does not set final output order; add a consumer ORDER BY and
sufficient tie keys. NULL/peer ordering, collation and server type/coercion rules
remain native; no tie-breaker is inserted.

Window decoration copies the function node and owns its inline OVER data; existing
shared arguments keep their ownership. All window keys/frame nodes share the
query-wide depth/occurrence budgets, active-path cycle checks and CTE traversal.
Window-bearing legacy SELECTs receive bounded preflight too; plain legacy SELECTs
keep their prior behavior. Navigation values/defaults bind in SQL order; offsets,
NTH positions and frame bounds are checked literals, not binds. CTE and main
queries still share one parameter vector.

For window statements, primitive numeric AST parameters emit explicit numeric
casts on SQLite/MySQL. This preserves numeric comparison of computed CTE outputs
on SQLite and avoids MySQL widening an integer navigation default to decimal text.
Bind numeric thresholds as numeric values, not numeric-looking strings. PostgreSQL
keeps normal inferred placeholders. SQLite window unsigned values outside signed
64-bit range reject. Plain legacy/generated SELECT SQL is unchanged by this policy.
Parameter payloads remain encoded text; adapters are unchanged.

This remains one materializing observed SELECT through existing session scopes,
with codec/native errors and observer exceptions preserved. Recursive steps still
reject functions, including windows. Rebuild the library and consumers together:
`func_expr`, `param_expr` and `compiler` layouts changed. FILTER's owned predicate
field is reserved and any non-null value rejects; FILTER and DISTINCT ON are next,
followed by combined advanced-query review. See the [design](../advanced-query-design.md)
and [delivery evidence](../evidence/window-functions/README.md).
