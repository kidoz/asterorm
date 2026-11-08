#pragma once
#include <catch2/catch_test_macros.hpp>

#include "asterorm/session.hpp"
#include "asterorm/sql/query.hpp"
#include "asterorm/sql/typed.hpp"
#include "asterorm/testing/transactional_session.hpp"

struct cte_native_row {
    int id{};
    std::optional<std::string> note;
};
template <> struct asterorm::entity_traits<cte_native_row> {
    static constexpr const char* table = "ordinary_cte_events";
    static constexpr auto columns = std::make_tuple(asterorm::column<&cte_native_row::note>("note"),
                                                    asterorm::column<&cte_native_row::id>("id"));
};
struct cte_count_row {
    std::int64_t total{};
};
template <> struct asterorm::entity_traits<cte_count_row> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&cte_count_row::total>("total"));
};

namespace asterorm::test {
template <typename Driver> void check_cte_query(Driver driver, std::string conninfo) {
    pool_config config;
    config.conninfo = std::move(conninfo);
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    auto execute = [&](const char* statement) {
        auto executed =
            db.with_connection([&](auto& connection) { return connection.execute(statement); });
        INFO((executed ? "success" : executed.error().message));
        REQUIRE(executed);
    };
    execute("CREATE TEMPORARY TABLE ordinary_cte_events(id INTEGER PRIMARY KEY, note TEXT)");
    execute("INSERT INTO ordinary_cte_events VALUES (1,'first'),(2,NULL),(3,'third'),(4,'fourth')");
    auto selection = sql::typed::select_cols(sql::typed::col<&cte_native_row::id>,
                                             sql::typed::col<&cte_native_row::note>);
    selection.where(sql::typed::col<&cte_native_row::id> > sql::typed::val(1));
    auto renamed = sql::select({"event_id", "note"})
                       .from("eligible")
                       .where(sql::col("event_id") > sql::val(1))
                       .build();
    sql::select_ast consumer;
    consumer.table = "renamed";
    consumer.items = {{"event_id", nullptr, "id"}, {"note", nullptr, std::nullopt}};
    consumer.where_clause = sql::col("event_id") < sql::val(6);
    consumer.order_bys = {{"event_id", true}};
    auto query =
        sql::query(std::move(consumer))
            .with_cte("eligible", sql::query(selection.build()).build(), {"event_id", "note"})
            .with_cte("renamed", sql::query(std::move(renamed)).build())
            .build();
    auto expected = sql::compiler{sql::dialect_for_session<decltype(db)>()}.compile(query);
    REQUIRE(expected);
    int events{};
    db.set_observer([&](const query_event& event) {
        ++events;
        REQUIRE(event.sql == expected->sql);
        REQUIRE(event.param_count == 3);
        REQUIRE_FALSE(event.error);
        REQUIRE(pool.stats().in_use == 1);
    });
    auto rows = sql::query_as<cte_native_row>(db, query);
    INFO((rows ? "success" : rows.error().message));
    REQUIRE(rows);
    REQUIRE(rows->size() == 3);
    REQUIRE(rows->at(0).id == 2);
    REQUIRE_FALSE(rows->at(0).note);
    REQUIRE(rows->at(1).id == 3);
    REQUIRE(rows->at(1).note == "third");
    REQUIRE(events == 1);
    auto transaction = db.begin();
    REQUIRE(transaction);
    execute("INSERT INTO ordinary_cte_events VALUES(5,'rollback')");
    if constexpr (sql::dialect_for_session<decltype(db)>() == sql::dialect::postgres) {
        rows = db.with_statement_timeout(std::chrono::milliseconds{1000}, [&](auto& scoped) {
            return sql::query_as<cte_native_row>(scoped, query);
        });
    } else {
        rows = sql::query_as<cte_native_row>(db, query);
    }
    REQUIRE(rows);
    REQUIRE(rows->size() == 4);
    REQUIRE(events == 2);
    REQUIRE(db.has_active_transaction());
    REQUIRE(transaction->rollback());
    rows = sql::query_as<cte_native_row>(db, query);
    REQUIRE(rows);
    REQUIRE(rows->size() == 3);
    REQUIRE(events == 3);
    if constexpr (sql::dialect_for_session<decltype(db)>() != sql::dialect::mysql) {
        using scope_type = testing::transactional_session<decltype(db)>;
        auto scope = scope_type::create(db);
        REQUIRE(scope);
        execute("INSERT INTO ordinary_cte_events VALUES(5,'test scope')");
        rows = sql::query_as<cte_native_row>(scope->session(), query);
        REQUIRE(rows);
        REQUIRE(rows->size() == 4);
        REQUIRE(events == 4);
        REQUIRE(scope->finish());
        rows = sql::query_as<cte_native_row>(db, query);
        REQUIRE(rows);
        REQUIRE(rows->size() == 3);
        REQUIRE(events == 5);
    }
    REQUIRE(pool.stats().in_use == 0);
    db.set_observer({});
    // Native pagination and normalized function-item alias on the shared path.
    std::get<sql::select_ast>(query.body).offset_val = 1;
    rows = sql::query_as<cte_native_row>(db, query);
    REQUIRE(rows);
    REQUIRE(rows->size() == 2);
    REQUIRE(rows->front().id == 3);
    sql::select_ast counted;
    counted.table = "ordinary_cte_events";
    counted.items = {{std::nullopt, sql::count_all(), "total"}};
    auto counts = sql::query_as<cte_count_row>(db, sql::query(std::move(counted)).build());
    REQUIRE(counts);
    REQUIRE(counts->front().total == 4);
    execute("DROP TABLE ordinary_cte_events");
    int failures{};
    db.set_observer([&](const query_event& event) {
        ++failures;
        REQUIRE(event.error);
        REQUIRE(!event.error->message.empty());
    });
    rows = sql::query_as<cte_native_row>(db, query);
    REQUIRE_FALSE(rows);
    REQUIRE(!rows.error().message.empty());
    REQUIRE(failures == 1);
}
} // namespace asterorm::test
