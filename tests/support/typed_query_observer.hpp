#pragma once
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "asterorm/session.hpp"
#include "asterorm/sql/typed_query.hpp"

struct typed_observer_row {
    int id{};
    bool active{};
    std::optional<std::string> note;
};

template <> struct asterorm::entity_traits<typed_observer_row> {
    static constexpr const char* table = "typed_observer_rows";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&typed_observer_row::id>("id"),
                        asterorm::column<&typed_observer_row::active>("active"),
                        asterorm::column<&typed_observer_row::note>("note"));
};

namespace asterorm::test {
template <typename Driver> void check_typed_query_observer(Driver driver, std::string conninfo) {
    namespace typed = asterorm::sql::typed;
    asterorm::pool_config config;
    config.conninfo = std::move(conninfo);
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds{100};
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    auto created = db.with_connection([](auto& connection) {
        return connection.execute("CREATE TEMPORARY TABLE typed_observer_rows ("
                                  "id INTEGER PRIMARY KEY, active BOOLEAN NOT NULL, note TEXT)");
    });
    REQUIRE(created.has_value());
    auto inserted = db.with_connection([](auto& connection) {
        return connection.execute("INSERT INTO typed_observer_rows VALUES "
                                  "(1, TRUE, NULL), (2, FALSE, 'archived')");
    });
    REQUIRE(inserted.has_value());

    struct observed_event {
        std::string sql;
        std::size_t param_count{};
        std::optional<asterorm::db_error> error;
        std::size_t in_use{};
    };
    std::vector<observed_event> events;
    auto observer = [&](const asterorm::query_event& event) {
        events.push_back({
            .sql = event.sql,
            .param_count = event.param_count,
            .error = event.error ? std::optional{*event.error} : std::nullopt,
            .in_use = pool.stats().in_use,
        });
    };
    db.set_observer(observer);
    auto transaction = db.begin();
    REQUIRE(transaction.has_value());

    auto ast = asterorm::sql::select({"id", "active", "note"})
                   .from("typed_observer_rows")
                   .where(asterorm::sql::col("active") == asterorm::sql::val(true))
                   .build();
    asterorm::sql::compiler compiler{asterorm::sql::dialect_for_session<decltype(db)>()};
    auto expected = compiler.compile(ast);
    REQUIRE(expected.has_value());
    auto users = typed::query(db, typed::select<typed_observer_row>().where(
                                      typed::col<&typed_observer_row::active> == typed::val(true)));
    REQUIRE(users.has_value());
    REQUIRE(users->size() == 1);
    REQUIRE(users->front().id == 1);
    REQUIRE(users->front().active);
    REQUIRE_FALSE(users->front().note);
    REQUIRE(events.size() == 1);
    REQUIRE(events.back().sql == expected->sql);
    REQUIRE(events.back().param_count == expected->params.size());
    REQUIRE_FALSE(events.back().error);
    REQUIRE(events.back().in_use == 1);

    auto selection = typed::select_cols(typed::col<&typed_observer_row::id>,
                                        typed::col<&typed_observer_row::note>);
    selection.where(typed::col<&typed_observer_row::active> == typed::val(false));
    auto projected = typed::query(db, selection);
    REQUIRE(projected.has_value());
    REQUIRE(*projected ==
            std::vector<std::tuple<int, std::optional<std::string>>>{{2, "archived"}});
    REQUIRE(events.size() == 2);
    REQUIRE(events.back().param_count == 1);
    REQUIRE_FALSE(events.back().error);
    REQUIRE(events.back().in_use == 1);

    auto empty = typed::query(db, typed::select<typed_observer_row>().where(
                                      typed::col<&typed_observer_row::id> == typed::val(99)));
    REQUIRE(empty.has_value());
    REQUIRE(empty->empty());
    REQUIRE(events.size() == 3);
    REQUIRE_FALSE(events.back().error);
    REQUIRE(transaction->rollback().has_value());
    REQUIRE(pool.stats().in_use == 0);

    db.set_observer({});
    REQUIRE(typed::query(db, typed::select<typed_observer_row>()).has_value());
    REQUIRE(events.size() == 3);
    auto dropped = db.with_connection(
        [](auto& connection) { return connection.execute("DROP TABLE typed_observer_rows"); });
    REQUIRE(dropped.has_value());
    db.set_observer(observer);
    auto failed = typed::query(db, typed::select<typed_observer_row>());
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE(events.size() == 4);
    REQUIRE(events.back().error.has_value());
    REQUIRE(events.back().error->kind == failed.error().kind);
    REQUIRE(events.back().error->sqlstate == failed.error().sqlstate);
    REQUIRE(events.back().error->message == failed.error().message);
    REQUIRE(events.back().in_use == 1);
    db.set_observer({});
    REQUIRE_FALSE(typed::query(db, typed::select<typed_observer_row>()).has_value());
    REQUIRE(events.size() == 4);
}
} // namespace asterorm::test
