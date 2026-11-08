#pragma once
#include "asterorm/session.hpp"
#include "asterorm/sql/query.hpp"
#include "asterorm/sql/typed_extensions.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/testing/transactional_session.hpp"
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

struct aggregate_event {
    int id{};
    std::optional<int> group;
    int stamp{};
    std::optional<int> value;
    bool active{};
};
struct aggregate_stats {
    std::int64_t matched{};
    std::optional<std::int64_t> total;
    std::int64_t nonnull{};
    std::optional<double> mean;
    std::optional<int> minimum, maximum;
    bool operator==(const aggregate_stats&) const = default;
};
struct latest_event {
    int id{};
    std::optional<int> group;
    int stamp{};
    std::optional<int> value;
    bool operator==(const latest_event&) const = default;
};
struct filtered_window_row {
    int id{};
    std::optional<int> group;
    std::optional<std::int64_t> total;
    std::int64_t matched{};
};
template <> struct asterorm::entity_traits<aggregate_event> {
    static constexpr const char* table = "extension_events";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&aggregate_event::id>("id"),
                        asterorm::column<&aggregate_event::group>("grp"),
                        asterorm::column<&aggregate_event::stamp>("stamp"),
                        asterorm::column<&aggregate_event::value>("value"),
                        asterorm::column<&aggregate_event::active>("active"));
};
template <> struct asterorm::entity_traits<aggregate_stats> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&aggregate_stats::matched>("matched"),
                        asterorm::column<&aggregate_stats::total>("total"),
                        asterorm::column<&aggregate_stats::nonnull>("nonnull"),
                        asterorm::column<&aggregate_stats::mean>("mean"),
                        asterorm::column<&aggregate_stats::minimum>("minimum"),
                        asterorm::column<&aggregate_stats::maximum>("maximum"));
};
template <> struct asterorm::entity_traits<latest_event> {
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&latest_event::id>("id"), asterorm::column<&latest_event::group>("grp"),
        asterorm::column<&latest_event::stamp>("stamp"),
        asterorm::column<&latest_event::value>("value"));
};
template <> struct asterorm::entity_traits<filtered_window_row> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&filtered_window_row::id>("id"),
                        asterorm::column<&filtered_window_row::group>("grp"),
                        asterorm::column<&filtered_window_row::total>("total"),
                        asterorm::column<&filtered_window_row::matched>("matched"));
};

namespace asterorm::test {
template <typename Driver> void check_aggregate_extensions(Driver driver, std::string settings) {
    namespace typed = sql::typed;
    pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    constexpr auto target = sql::dialect_for_session<decltype(db)>();
    auto active = [] { return sql::col("active") == sql::val(true); };
    auto scalar = [&] {
        sql::select_ast leaf;
        leaf.table = "extension_events";
        auto add = [&](auto function, const char* alias) {
            leaf.items.push_back(
                {.column_name = {}, .fn = sql::filter(function, active()), .alias = alias});
        };
        add(sql::count_all(), "matched");
        add(sql::sum(sql::col("value")), "total");
        add(sql::count(sql::col("value")), "nonnull");
        add(sql::avg(sql::col("value")), "mean");
        add(sql::min_(sql::col("value")), "minimum");
        add(sql::max_(sql::col("value")), "maximum");
        leaf.where_clause = sql::col("id") >= sql::val(1);
        return sql::query(std::move(leaf)).build();
    };
    auto latest = [&] {
        return sql::query(sql::select({"id", "grp", "stamp", "value"})
                              .from("extension_events")
                              .distinct_on({"grp"}, sql::distinct_on_ties::require_tiebreaker)
                              .where(sql::col("id") > sql::val(0))
                              .order_by("grp")
                              .order_by("stamp", false)
                              .order_by("id", false)
                              .build())
            .build();
    };
    int events{};
    db.set_observer([&](const query_event&) { ++events; });
    if constexpr (target != sql::dialect::postgres) {
        REQUIRE_FALSE(sql::query_as<latest_event>(db, latest()));
        REQUIRE(pool.stats().total == 0);
        REQUIRE(events == 0);
    }
    if constexpr (target == sql::dialect::mysql) {
        REQUIRE_FALSE(sql::query_as<aggregate_stats>(db, scalar()));
        REQUIRE(pool.stats().total == 0);
        REQUIRE(events == 0);
        return;
    }
    auto execute = [&](const char* statement) {
        auto executed =
            db.with_connection([&](auto& connection) { return connection.execute(statement); });
        INFO((executed ? "success" : executed.error().message));
        REQUIRE(executed);
    };
    execute("CREATE TEMPORARY TABLE extension_events(id INTEGER PRIMARY KEY, grp INTEGER, stamp "
            "INTEGER, value INTEGER, active BOOLEAN)");
    execute("INSERT INTO extension_events "
            "VALUES(1,1,10,5,TRUE),(2,1,20,NULL,TRUE),(3,1,20,7,FALSE),(4,2,10,11,FALSE),(5,2,30,"
            "13,TRUE),(6,NULL,10,5,TRUE),(7,NULL,20,NULL,FALSE)");
    auto predicate = [] { return typed::col<&aggregate_event::active> == typed::val(true); };
    auto typed_scalar = [&] {
        auto selection = typed::select_cols<aggregate_stats>(
            typed::filter(typed::count_all(), predicate()).as("matched"),
            typed::filter(typed::sum(typed::col<&aggregate_event::value>), predicate()).as("total"),
            typed::filter(typed::count(typed::col<&aggregate_event::value>), predicate())
                .as("nonnull"),
            typed::filter(typed::avg(typed::col<&aggregate_event::value>), predicate()).as("mean"),
            typed::filter(typed::min(typed::col<&aggregate_event::value>), predicate())
                .as("minimum"),
            typed::filter(typed::max(typed::col<&aggregate_event::value>), predicate())
                .as("maximum"));
        selection.where(typed::col<&aggregate_event::id> >= typed::val(1));
        return selection;
    };
    auto query = scalar();
    auto compiled = sql::compiler{target}.compile(query);
    auto typed_compiled = sql::compiler{target}.compile(typed_scalar().build());
    REQUIRE(compiled);
    REQUIRE(typed_compiled);
    REQUIRE(compiled->sql == typed_compiled->sql);
    REQUIRE(compiled->params == typed_compiled->params);
    db.set_observer([&](const query_event& event) {
        ++events;
        REQUIRE(event.param_count == 7);
        REQUIRE_FALSE(event.error);
        REQUIRE(pool.stats().in_use == 1);
    });
    auto rows = sql::query_as<aggregate_stats>(db, query);
    INFO((rows ? "success" : rows.error().message));
    REQUIRE(rows);
    REQUIRE(rows->size() == 1);
    REQUIRE(rows->front().matched == 4);
    REQUIRE(rows->front().total == 23);
    REQUIRE(rows->front().nonnull == 3);
    REQUIRE(rows->front().mean);
    REQUIRE(*rows->front().mean == Catch::Approx(23.0 / 3));
    REQUIRE(rows->front().minimum == 5);
    REQUIRE(rows->front().maximum == 13);
    auto typed_rows = typed::query(db, typed_scalar());
    REQUIRE(typed_rows);
    REQUIRE(*typed_rows == *rows);
    REQUIRE(events == 2);
    auto transaction = db.begin();
    REQUIRE(transaction);
    execute("INSERT INTO extension_events VALUES(8,3,1,100,TRUE)");
    auto changed = sql::query_as<aggregate_stats>(db, query);
    REQUIRE(changed);
    REQUIRE(changed->front().total == 123);
    REQUIRE(db.has_active_transaction());
    REQUIRE(transaction->rollback());
    REQUIRE(sql::query_as<aggregate_stats>(db, query)->front().total == 23);
    REQUIRE(events == 4);
    using scope_type = testing::transactional_session<decltype(db)>;
    auto scope = scope_type::create(db);
    REQUIRE(scope);
    execute("INSERT INTO extension_events VALUES(8,3,1,100,TRUE)");
    REQUIRE(sql::query_as<aggregate_stats>(db, query)->front().matched == 5);
    REQUIRE(scope->finish());
    REQUIRE(sql::query_as<aggregate_stats>(db, query)->front().matched == 4);
    REQUIRE(events == 6);
    std::get<sql::select_ast>(query.body).where_clause = sql::col("id") > sql::val(99);
    auto empty = sql::query_as<aggregate_stats>(db, query);
    REQUIRE(empty);
    REQUIRE(empty->front().matched == 0);
    REQUIRE_FALSE(empty->front().total);
    REQUIRE_FALSE(empty->front().mean);
    db.set_observer({});
    sql::over_clause full{
        .partition_bys = {"grp"},
        .order_bys = {{.column = "id"}},
        .rows = sql::rows_between(sql::unbounded_preceding(), sql::unbounded_following())};
    auto windowed = [&] {
        sql::select_ast leaf;
        leaf.table = "extension_events";
        leaf.items = {{.column_name = "id", .fn = {}, .alias = {}},
                      {.column_name = "grp", .fn = {}, .alias = {}},
                      {.column_name = {},
                       .fn = sql::over(sql::filter(sql::sum(sql::col("value")), active()), full),
                       .alias = "total"},
                      {.column_name = {},
                       .fn = sql::over(sql::filter(sql::count_all(), active()), full),
                       .alias = "matched"}};
        leaf.order_bys = {{.column = "id"}};
        return leaf;
    };
    auto window_rows = sql::query_as<filtered_window_row>(db, sql::query(windowed()).build());
    REQUIRE(window_rows);
    REQUIRE(window_rows->size() == 7);
    REQUIRE(window_rows->at(2).total == 5);
    REQUIRE(window_rows->at(2).matched == 2);
    REQUIRE(window_rows->at(3).total == 13);
    REQUIRE(window_rows->at(6).total == 5);
    auto excluded = windowed();
    for (std::size_t index = 2; index < excluded.items.size(); ++index) {
        excluded.items[index].fn =
            sql::filter(excluded.items[index].fn, sql::col("id") < sql::val(0));
    }
    auto excluded_rows =
        sql::query_as<filtered_window_row>(db, sql::query(std::move(excluded)).build());
    REQUIRE(excluded_rows);
    REQUIRE(excluded_rows->size() == 7);
    REQUIRE_FALSE(excluded_rows->front().total);
    REQUIRE(excluded_rows->front().matched == 0);
    auto spec = typed::window()
                    .partition_by(typed::col<&aggregate_event::group>)
                    .order_by(typed::col<&aggregate_event::id>)
                    .rows_between(sql::unbounded_preceding(), sql::unbounded_following());
    auto typed_window = typed::select_cols<filtered_window_row>(
        typed::col<&aggregate_event::id>, typed::col<&aggregate_event::group>,
        typed::over(typed::filter(typed::sum(typed::col<&aggregate_event::value>), predicate()),
                    spec)
            .as("total"),
        typed::over(typed::filter(typed::count_all(), predicate()), spec).as("matched"));
    typed_window.order_by(typed::col<&aggregate_event::id>);
    auto window_result = typed::query(db, typed_window);
    REQUIRE(window_result);
    REQUIRE(window_result->at(2).total == window_rows->at(2).total);
    REQUIRE(window_result->at(2).matched == 2);
    auto distinct_count = sql::count(sql::col("value"));
    distinct_count->distinct = true;
    auto count_query =
        sql::select({})
            .from("extension_events")
            .items({{.column_name = {},
                     .fn = sql::filter(distinct_count, active()),
                     .alias = "matched"},
                    {.column_name = {},
                     .fn = sql::filter(sql::sum(sql::col("value")), active()),
                     .alias = "total"},
                    {.column_name = {}, .fn = sql::count_all(), .alias = "nonnull"},
                    {.column_name = {}, .fn = sql::avg(sql::col("value")), .alias = "mean"},
                    {.column_name = {}, .fn = sql::min_(sql::col("value")), .alias = "minimum"},
                    {.column_name = {}, .fn = sql::max_(sql::col("value")), .alias = "maximum"}})
            .build();
    REQUIRE(sql::query_as<aggregate_stats>(db, sql::query(std::move(count_query)).build())
                ->front()
                .matched == 2);
    auto having = windowed();
    having.items.resize(2);
    having.items.erase(having.items.begin()); // grp only, with filtered aggregate outputs.
    having.items.push_back({.column_name = {},
                            .fn = sql::filter(sql::sum(sql::col("value")), active()),
                            .alias = "total"});
    having.items.push_back(
        {.column_name = {}, .fn = sql::filter(sql::count_all(), active()), .alias = "matched"});
    having.items.insert(having.items.begin(),
                        {.column_name = {}, .fn = sql::min_(sql::col("id")), .alias = "id"});
    having.group_bys = {"grp"};
    having.order_bys.clear();
    having.having_clause = sql::filter(sql::count_all(), active()) > sql::val(1);
    auto groups = sql::query_as<filtered_window_row>(db, sql::query(std::move(having)).build());
    REQUIRE(groups);
    REQUIRE(groups->size() == 1);
    REQUIRE(groups->front().group == 1);
    REQUIRE(groups->front().matched == 2);
    if constexpr (target == sql::dialect::postgres) {
        auto winners = sql::query_as<latest_event>(db, latest());
        REQUIRE(winners);
        REQUIRE(winners->size() == 3);
        REQUIRE(winners->at(0).id == 3);
        REQUIRE(winners->at(1).id == 5);
        REQUIRE(winners->at(2).id == 7);
        REQUIRE_FALSE(winners->at(2).group);
        REQUIRE_FALSE(winners->at(2).value);
        auto typed_latest = typed::select_cols<latest_event>(
            typed::col<&aggregate_event::id>, typed::col<&aggregate_event::group>,
            typed::col<&aggregate_event::stamp>, typed::col<&aggregate_event::value>);
        typed_latest
            .distinct_on(sql::distinct_on_ties::require_tiebreaker,
                         typed::col<&aggregate_event::group>)
            .where(typed::col<&aggregate_event::id> > typed::val(0))
            .order_by(typed::col<&aggregate_event::group>)
            .order_by(typed::col<&aggregate_event::stamp>, typed::desc)
            .order_by(typed::col<&aggregate_event::id>, typed::desc);
        auto typed_winners = typed::query(db, typed_latest);
        REQUIRE(typed_winners);
        REQUIRE(*typed_winners == *winners);
        auto combined = windowed();
        combined.distinct_on =
            sql::distinct_on_clause{{"grp"}, sql::distinct_on_ties::require_tiebreaker};
        combined.order_bys = {{.column = "grp"},
                              {.column = "stamp", .ascending = false},
                              {.column = "id", .ascending = false}};
        auto combined_rows =
            sql::query_as<filtered_window_row>(db, sql::query(std::move(combined)).build());
        REQUIRE(combined_rows);
        REQUIRE(combined_rows->size() == 3);
        REQUIRE(combined_rows->at(0).id == 3);
        REQUIRE(combined_rows->at(0).total == 5);
        REQUIRE(combined_rows->at(2).id == 7);
        REQUIRE(combined_rows->at(2).total == 5);
        auto peers = latest();
        auto& leaf = std::get<sql::select_ast>(peers.body);
        leaf.distinct_on->ties = sql::distinct_on_ties::allow_peers;
        leaf.order_bys.resize(1);
        REQUIRE(sql::query_as<latest_event>(db, peers)->size() == 3);
        leaf.where_clause = sql::col("id") > sql::val(99);
        auto none = sql::query_as<latest_event>(db, peers);
        REQUIRE(none);
        REQUIRE(none->empty());
    }
    execute("DROP TABLE extension_events");
    int failures{};
    db.set_observer([&](const query_event& event) {
        ++failures;
        REQUIRE(event.error);
    });
    auto failed = sql::query_as<aggregate_stats>(db, scalar());
    REQUIRE_FALSE(failed);
    REQUIRE(failures == 1);
}
} // namespace asterorm::test
