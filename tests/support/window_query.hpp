#pragma once
#include "asterorm/session.hpp"
#include "asterorm/sql/query.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/sql/typed_window.hpp"
#include "asterorm/testing/transactional_session.hpp"
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

struct window_native_event {
    int id{};
    int group{};
    int key{};
    std::optional<int> value;
};
struct window_native_row {
    int id{};
    std::int64_t number{}, rank{}, dense{};
    std::optional<int> previous, next, first, last, nth;
    std::optional<std::int64_t> running;
    std::int64_t count{};
    std::optional<std::int64_t> next_sum;
    std::int64_t next_count{};
    std::optional<double> mean;
    std::optional<int> minimum, maximum;
    std::optional<std::int64_t> peers;
    bool operator==(const window_native_row&) const = default;
};
struct window_rank_row {
    int id{};
    std::int64_t number{};
};
template <> struct asterorm::entity_traits<window_native_event> {
    static constexpr const char* table = "window_events";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&window_native_event::id>("id"),
                        asterorm::column<&window_native_event::group>("grp"),
                        asterorm::column<&window_native_event::key>("sort_key"),
                        asterorm::column<&window_native_event::value>("value"));
};
template <> struct asterorm::entity_traits<window_rank_row> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&window_rank_row::id>("id"),
                        asterorm::column<&window_rank_row::number>("number"));
};
template <> struct asterorm::entity_traits<window_native_row> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&window_native_row::id>("id"),
                        asterorm::column<&window_native_row::number>("number"),
                        asterorm::column<&window_native_row::rank>("ranked"),
                        asterorm::column<&window_native_row::dense>("dense"),
                        asterorm::column<&window_native_row::previous>("previous"),
                        asterorm::column<&window_native_row::next>("next"),
                        asterorm::column<&window_native_row::first>("first"),
                        asterorm::column<&window_native_row::last>("last"),
                        asterorm::column<&window_native_row::nth>("nth"),
                        asterorm::column<&window_native_row::running>("running"),
                        asterorm::column<&window_native_row::count>("counted"),
                        asterorm::column<&window_native_row::next_sum>("next_sum"),
                        asterorm::column<&window_native_row::next_count>("next_count"),
                        asterorm::column<&window_native_row::mean>("mean"),
                        asterorm::column<&window_native_row::minimum>("minimum"),
                        asterorm::column<&window_native_row::maximum>("maximum"),
                        asterorm::column<&window_native_row::peers>("peers"));
};

namespace asterorm::test {
template <typename Driver> void check_window_query(Driver driver, std::string settings) {
    namespace typed = sql::typed;
    pool_config config;
    config.conninfo = std::move(settings);
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    auto execute = [&](const char* statement) {
        auto executed =
            db.with_connection([&](auto& connection) { return connection.execute(statement); });
        INFO((executed ? "success" : executed.error().message));
        REQUIRE(executed);
    };
    execute("CREATE TEMPORARY TABLE window_events(id INTEGER, grp INTEGER, sort_key INTEGER, value "
            "INTEGER)");
    execute("INSERT INTO window_events "
            "VALUES(1,1,1,10),(2,1,1,20),(3,1,2,NULL),(4,1,3,40),(5,2,1,50),(6,2,2,60)");
    sql::over_clause ordered{.partition_bys = {"grp"}, .order_bys = {{.column = "id"}}, .rows = {}};
    auto peer = ordered;
    peer.order_bys = {{.column = "sort_key"}};
    auto full = ordered;
    full.rows = sql::rows_between(sql::unbounded_preceding(), sql::unbounded_following());
    auto running = ordered;
    running.rows = sql::rows_between(sql::unbounded_preceding(), sql::current_row());
    auto future = ordered;
    future.rows = sql::rows_between(sql::following(1), sql::following(1));
    auto runtime = [&] {
        sql::select_ast selected;
        selected.table = "window_events";
        selected.items = {{.column_name = "id", .fn = {}, .alias = {}}};
        auto add = [&](std::shared_ptr<sql::func_expr> function, const sql::over_clause& clause,
                       const char* alias) {
            selected.items.push_back({.column_name = {},
                                      .fn = sql::as(sql::over(function, clause), alias),
                                      .alias = {}});
        };
        add(sql::row_number(), ordered, "number");
        add(sql::rank(), peer, "ranked");
        add(sql::dense_rank(), peer, "dense");
        add(sql::lag(sql::col("value")), ordered, "previous");
        add(sql::lead(sql::col("value"), 1, sql::expr_ast{sql::val(-1)}), ordered, "next");
        add(sql::first_value(sql::col("value")), full, "first");
        add(sql::last_value(sql::col("value")), full, "last");
        add(sql::nth_value(sql::col("value"), 2), full, "nth");
        add(sql::sum(sql::col("value")), running, "running");
        add(sql::count(sql::col("value")), running, "counted");
        add(sql::sum(sql::col("value")), future, "next_sum");
        add(sql::count_all(), future, "next_count");
        add(sql::avg(sql::col("value")), full, "mean");
        add(sql::min_(sql::col("value")), full, "minimum");
        add(sql::max_(sql::col("value")), full, "maximum");
        add(sql::sum(sql::col("value")), peer, "peers");
        selected.where_clause = sql::col("grp") >= sql::val(1);
        selected.order_bys = {{.column = "id"}};
        return sql::query(std::move(selected)).build();
    };
    auto typed_order = typed::window()
                           .partition_by(typed::col<&window_native_event::group>)
                           .order_by(typed::col<&window_native_event::id>);
    auto typed_peer = typed::window()
                          .partition_by(typed::col<&window_native_event::group>)
                          .order_by(typed::col<&window_native_event::key>);
    auto typed_full =
        typed_order.rows_between(sql::unbounded_preceding(), sql::unbounded_following());
    auto typed_running = typed_order.rows_between(sql::unbounded_preceding(), sql::current_row());
    auto typed_future = typed_order.rows_between(sql::following(1), sql::following(1));
    auto typed_selection = [&] {
        auto selected = typed::select_cols<window_native_row>(
            typed::col<&window_native_event::id>,
            typed::over(typed::row_number(), typed_order).as("number"),
            typed::over(typed::rank(), typed_peer).as("ranked"),
            typed::over(typed::dense_rank(), typed_peer).as("dense"),
            typed::over(typed::lag(typed::col<&window_native_event::value>), typed_order)
                .as("previous"),
            typed::over(typed::lead(typed::col<&window_native_event::value>, 1, typed::val(-1)),
                        typed_order)
                .as("next"),
            typed::over(typed::first_value(typed::col<&window_native_event::value>), typed_full)
                .as("first"),
            typed::over(typed::last_value(typed::col<&window_native_event::value>), typed_full)
                .as("last"),
            typed::over(typed::nth_value(typed::col<&window_native_event::value>, 2), typed_full)
                .as("nth"),
            typed::over(typed::sum(typed::col<&window_native_event::value>), typed_running)
                .as("running"),
            typed::over(typed::count(typed::col<&window_native_event::value>), typed_running)
                .as("counted"),
            typed::over(typed::sum(typed::col<&window_native_event::value>), typed_future)
                .as("next_sum"),
            typed::over(typed::count_all(), typed_future).as("next_count"),
            typed::over(typed::avg(typed::col<&window_native_event::value>), typed_full).as("mean"),
            typed::over(typed::min(typed::col<&window_native_event::value>), typed_full)
                .as("minimum"),
            typed::over(typed::max(typed::col<&window_native_event::value>), typed_full)
                .as("maximum"),
            typed::over(typed::sum(typed::col<&window_native_event::value>), typed_peer)
                .as("peers"));
        selected.where(typed::col<&window_native_event::group> >= typed::val(1))
            .order_by(typed::col<&window_native_event::id>);
        return selected;
    };
    auto query = runtime();
    constexpr auto target = sql::dialect_for_session<decltype(db)>();
    auto compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    auto typed_ast = typed_selection().build();
    auto typed_compiled = sql::compiler{target}.compile(typed_ast);
    REQUIRE(typed_compiled);
    REQUIRE(typed_compiled->sql == compiled->sql);
    REQUIRE(typed_compiled->params == compiled->params);
    REQUIRE(compiled->params == query_parameters{"-1", "1"});
    int events{};
    std::size_t expected_parameters = 2;
    db.set_observer([&](const query_event& event) {
        ++events;
        REQUIRE(event.param_count == expected_parameters);
        REQUIRE_FALSE(event.error);
        REQUIRE(pool.stats().in_use == 1);
    });
    auto rows = sql::query_as<window_native_row>(db, query);
    INFO((rows ? "success" : rows.error().message));
    REQUIRE(rows);
    REQUIRE(rows->size() == 6);
    REQUIRE(rows->at(0).number == 1);
    REQUIRE(rows->at(1).number == 2);
    REQUIRE(rows->at(4).number == 1);
    REQUIRE(rows->at(0).rank == 1);
    REQUIRE(rows->at(1).rank == 1);
    REQUIRE(rows->at(2).rank == 3);
    REQUIRE(rows->at(2).dense == 2);
    REQUIRE_FALSE(rows->at(0).previous);
    REQUIRE(rows->at(1).previous == 10);
    REQUIRE(rows->at(2).previous == 20);
    REQUIRE_FALSE(rows->at(3).previous);
    REQUIRE(rows->at(0).next == 20);
    REQUIRE_FALSE(rows->at(1).next);
    REQUIRE(rows->at(3).next == -1);
    REQUIRE(rows->at(0).first == 10);
    REQUIRE(rows->at(0).last == 40);
    REQUIRE(rows->at(0).nth == 20);
    REQUIRE(rows->at(0).running == 10);
    REQUIRE(rows->at(1).running == 30);
    REQUIRE(rows->at(2).running == 30);
    REQUIRE(rows->at(3).running == 70);
    REQUIRE(rows->at(2).count == 2);
    REQUIRE(rows->at(3).count == 3);
    REQUIRE(rows->at(0).next_sum == 20);
    REQUIRE_FALSE(rows->at(1).next_sum);
    REQUIRE_FALSE(rows->at(3).next_sum);
    REQUIRE(rows->at(3).next_count == 0);
    REQUIRE(rows->at(0).mean);
    REQUIRE(*rows->at(0).mean == Catch::Approx(70.0 / 3).epsilon(0.00001));
    REQUIRE(rows->at(0).minimum == 10);
    REQUIRE(rows->at(0).maximum == 40);
    REQUIRE(rows->at(0).peers == 30);
    REQUIRE(rows->at(1).peers == 30); // Default peer RANGE differs from ROWS running sum.
    auto typed_rows = typed::query(db, typed_selection());
    REQUIRE(typed_rows);
    REQUIRE(*typed_rows == *rows);
    REQUIRE(events == 2);
    auto transaction = db.begin();
    REQUIRE(transaction);
    execute("INSERT INTO window_events VALUES(7,1,4,70)");
    auto changed = sql::query_as<window_native_row>(db, query);
    REQUIRE(changed);
    REQUIRE(changed->size() == 7);
    REQUIRE(db.has_active_transaction());
    REQUIRE(transaction->rollback());
    REQUIRE(sql::query_as<window_native_row>(db, query)->size() == 6);
    REQUIRE(events == 4);
    if constexpr (target != sql::dialect::mysql) {
        using scope_type = testing::transactional_session<decltype(db)>;
        auto scope = scope_type::create(db);
        REQUIRE(scope);
        execute("INSERT INTO window_events VALUES(7,1,4,70)");
        REQUIRE(sql::query_as<window_native_row>(db, query)->size() == 7);
        REQUIRE(scope->finish());
        REQUIRE(sql::query_as<window_native_row>(db, query)->size() == 6);
    }
    db.set_observer({});
    sql::select_ast consumer;
    consumer.table = "ranked";
    std::apply(
        [&](const auto&... mappings) { (consumer.columns.emplace_back(mappings.name), ...); },
        entity_traits<window_native_row>::columns);
    consumer.where_clause = sql::col("number") <= sql::val(2);
    consumer.order_bys = {{.column = "id"}};
    auto report = sql::query(std::move(consumer)).with_cte("ranked", runtime()).build();
    auto top = sql::query_as<window_native_row>(db, report);
    REQUIRE(top);
    REQUIRE(top->size() == 4);
    REQUIRE(top->at(2).id == 5);
    std::get<sql::select_ast>(query.body).where_clause = sql::col("grp") > sql::val(99);
    auto empty = sql::query_as<window_native_row>(db, query);
    REQUIRE(empty);
    REQUIRE(empty->empty());
    sql::select_ast nullable;
    nullable.table = "window_events";
    auto null_order = ordered;
    null_order.order_bys = {{.column = "value"}, {.column = "id"}};
    nullable.items = {
        {.column_name = "id", .fn = {}, .alias = {}},
        {.column_name = {}, .fn = sql::over(sql::row_number(), null_order), .alias = "number"}};
    nullable.where_clause = sql::col("grp") == sql::val(1);
    nullable.order_bys = {{.column = "number"}};
    auto nullable_rows =
        sql::query_as<window_rank_row>(db, sql::query(std::move(nullable)).build());
    REQUIRE(nullable_rows);
    std::vector<int> ids;
    for (const auto& row : *nullable_rows) {
        ids.push_back(row.id);
    }
    REQUIRE(ids == (target == sql::dialect::postgres ? std::vector<int>{1, 2, 4, 3}
                                                     : std::vector<int>{3, 1, 2, 4}));
}
} // namespace asterorm::test
