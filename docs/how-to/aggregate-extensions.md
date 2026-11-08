# Filter aggregates and choose DISTINCT ON rows

FILTER is available on PostgreSQL and SQLite for COUNT/SUM/AVG/MIN/MAX, including
aggregate windows. DISTINCT ON is PostgreSQL-only. Unsupported backends reject
before acquiring a connection; neither construct is emulated. Include
`<asterorm/sql/typed_extensions.hpp>` for typed FILTER/OVER composition, or use
runtime `sql::filter(function, predicate)` from `<asterorm/sql/window.hpp>`.

This complete example includes two entry points. `filtered_example` returns one
row with `matched == 3` and `total == 18` on PG/SQLite. `distinct_example` returns
IDs 3 and 5 on PG. Other backends return structured local errors before setup.

```cpp
#include <asterorm/session.hpp>
#include <asterorm/sql/query.hpp>
#include <asterorm/sql/typed_extensions.hpp>

struct extension_guide_event {
    int id{};
    int group{};
    int stamp{};
    std::optional<int> value;
    bool active{};
};
struct extension_guide_total {
    std::int64_t matched{};
    std::optional<std::int64_t> total;
};
template <> struct asterorm::entity_traits<extension_guide_event> {
    static constexpr const char* table = "extension_guide_events";
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&extension_guide_event::id>("id"),
        asterorm::column<&extension_guide_event::group>("grp"),
        asterorm::column<&extension_guide_event::stamp>("stamp"),
        asterorm::column<&extension_guide_event::value>("value"),
        asterorm::column<&extension_guide_event::active>("active"));
};
template <> struct asterorm::entity_traits<extension_guide_total> {
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&extension_guide_total::matched>("matched"),
        asterorm::column<&extension_guide_total::total>("total"));
};
template <typename Session> asterorm::result<void> prepare_extension_rows(Session& db) {
    return db.with_connection([](auto& connection) -> asterorm::result<void> {
        auto created = connection.execute(
            "CREATE TEMPORARY TABLE extension_guide_events("
            "id INTEGER PRIMARY KEY, grp INTEGER, stamp INTEGER, value INTEGER, active BOOLEAN)");
        if (!created)
            return std::unexpected(created.error());
        auto inserted = connection.execute(
            "INSERT INTO extension_guide_events VALUES"
            "(1,1,10,5,TRUE),(2,1,20,NULL,TRUE),(3,1,20,7,FALSE),"
            "(4,2,10,11,FALSE),(5,2,30,13,TRUE)");
        if (!inserted)
            return std::unexpected(inserted.error());
        return {};
    });
}
template <typename Driver>
asterorm::result<std::vector<extension_guide_total>> filtered_example(Driver driver,
                                                                    std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    namespace typed = asterorm::sql::typed;
    auto selection = typed::select_cols<extension_guide_total>(
        typed::filter(typed::count_all(),
                      typed::col<&extension_guide_event::active> == typed::val(true)).as("matched"),
        typed::filter(typed::sum(typed::col<&extension_guide_event::value>),
                      typed::col<&extension_guide_event::active> == typed::val(true)).as("total"));
    auto report = asterorm::sql::query(selection.build()).build();
    if constexpr (asterorm::sql::dialect_for_session<decltype(db)>() != asterorm::sql::dialect::postgres &&
                  asterorm::sql::dialect_for_session<decltype(db)>() != asterorm::sql::dialect::sqlite)
        return asterorm::sql::query_as<extension_guide_total>(db, report);
    auto setup = prepare_extension_rows(db);
    if (!setup)
        return std::unexpected(setup.error());
    return asterorm::sql::query_as<extension_guide_total>(db, report);
}
template <typename Driver>
asterorm::result<std::vector<extension_guide_event>> distinct_example(Driver driver,
                                                                    std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    namespace sql = asterorm::sql;
    namespace typed = sql::typed;
    auto selection = typed::select<extension_guide_event>();
    selection.distinct_on(sql::distinct_on_ties::require_tiebreaker,
                          typed::col<&extension_guide_event::group>)
             .order_by(typed::col<&extension_guide_event::group>)
             .order_by(typed::col<&extension_guide_event::stamp>, typed::desc)
             .order_by(typed::col<&extension_guide_event::id>, typed::desc);
    auto report = sql::query(selection.build()).build();
    if constexpr (sql::dialect_for_session<decltype(db)>() != sql::dialect::postgres)
        return sql::query_as<extension_guide_event>(db, report);
    auto setup = prepare_extension_rows(db);
    if (!setup)
        return std::unexpected(setup.error());
    return sql::query_as<extension_guide_event>(db, report);
}
```

FILTER owns a shared-const predicate and decorating copies the function node.
Function arguments bind first, then FILTER, then OVER keys (no value binds),
JOIN/WHERE/HAVING in emitted SQL order. It changes which rows enter the aggregate;
it does not remove result rows. COUNT returns zero for no matches; SUM/AVG/MIN/MAX
return SQL NULL. Typed results reflect that: COUNT is `int64_t`, the other filtered
aggregates use optional existing aggregate result types. Types/coercions remain
native. MySQL FILTER rejects even when its modern set-operation profile is selected.

Runtime filtered aggregates are allowed in projections and HAVING. FILTER's
predicate admits normal scalar expressions/logical/NULL predicates but rejects
aggregates, windows and nested FILTER. Projection windows stay subject to the
[window rules](window-functions.md). Typed composition applies FILTER before OVER:
`typed::over(typed::filter(typed::sum(column), predicate), spec)`.
Mixed entities, unsupported aggregates/ranking FILTER and incompatible DTO
nullability fail compilation. DISTINCT aggregate arguments are allowed with scalar
FILTER; DISTINCT window aggregates remain rejected. There is no aggregate-in-WHERE
shortcut: use a derived CTE when that behavior is needed.

Runtime DISTINCT ON uses `.distinct_on({"grp"}, policy)` on a SELECT builder;
typed SELECTs use `.distinct_on(policy, typed_columns...)`. Keys must be nonempty,
unique column names matching the exact leftmost ORDER BY names and order. Qualified
and unqualified spellings are not normalized. A tie policy is mandatory:

- `require_tiebreaker` requires an additional ORDER BY column beyond the DISTINCT
  keys. The caller must make the ordering unique when deterministic selection is
  required; schema/data uniqueness is not inferred or verified.
- `allow_peers` explicitly accepts arbitrary selection among remaining peers.

The example chooses descending stamp, then unique descending ID; the library
inserts neither rule. Ordering direction and NULL/collation semantics stay native.
PostgreSQL treats NULL keys as one DISTINCT group. Row locking rejects with
DISTINCT ON. DISTINCT ON stays inside its SELECT leaf through CTE/set wrapping;
it is not a new compound modifier. Recursive steps, INSERT SELECT and streams
retain their existing narrow restrictions.

Both features share graph/cycle/depth/occurrence validation and named observation/
codec/session execution. FILTER predicates and DISTINCT keys count against the
statement budget. Window/FILTER statements retain the numeric-cast affinity policy
on SQLite/MySQL; ordinary SQL remains unchanged. Typed extension execution requires
explicit dialect metadata and compatible observation. FILTER adds no transaction,
retry, CASE emulation or hidden schema query. DISTINCT ON adds a field to
`select_ast` and changes containing query layouts: rebuild the library and consumers
together. The [design](../advanced-query-design.md) and
[evidence](../evidence/aggregate-extensions/README.md) record the implemented subset.
The next step is the combined advanced-query acceptance review.
