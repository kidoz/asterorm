#pragma once
#include "../../docs/evidence/advanced-query-acceptance/composition.hpp"
#include "asterorm/testing/transactional_session.hpp"
#include <catch2/catch_test_macros.hpp>
#include <stdexcept>

namespace asterorm::test {
template <typename Driver> void check_composed_query(Driver driver, std::string settings) {
    using namespace acceptance;
    pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    constexpr auto target = sql::dialect_for_session<decltype(db)>();
    int events{};
    db.set_observer([&](const query_event&) { ++events; });
    auto unsupported = composed_query(true, target != sql::dialect::mysql);
    if constexpr (target != sql::dialect::postgres) {
        auto rejected = sql::query_as<composed_row>(db, unsupported);
        REQUIRE_FALSE(rejected);
        REQUIRE(rejected.error().kind == db_error_kind::parse_failed);
        REQUIRE(pool.stats().total == 0);
        REQUIRE(events == 0);
    }
    REQUIRE(prepare_composed_rows(db));
    auto check = [&](bool filtered, bool distinct, bool typed, bool paged) {
        auto query = composed_query(filtered, distinct, typed, paged);
        auto compiled = sql::compiler{target}.compile(query);
        REQUIRE(compiled);
        query_parameters expected{"1", "9", "0", "0"};
        if (filtered) {
            expected.emplace_back("10");
        }
        expected.insert(expected.end(), {"-1", "5", "98"});
        REQUIRE(compiled->params == expected);
        const auto previous_events = events;
        db.set_observer([&](const query_event& event) {
            ++events;
            REQUIRE(event.sql == compiled->sql);
            REQUIRE(event.param_count == expected.size());
            REQUIRE_FALSE(event.error);
            REQUIRE(pool.stats().in_use == 1);
        });
        auto rows = sql::query_as<composed_row>(db, query);
        INFO((rows ? "success" : rows.error().message));
        REQUIRE(rows);
        // Do not retain callbacks capturing the local compiler/parameter vector.
        db.set_observer({});
        REQUIRE(events == previous_events + 1);
        REQUIRE(pool.stats().in_use == (db.has_active_transaction() ? 1 : 0));
        return *rows;
    };
    auto paged = check(false, false, false, true);
    REQUIRE(paged == std::vector<composed_row>{{4, 1, 4, 70, std::nullopt},
                                               {99, std::nullopt, 9, std::nullopt, std::nullopt},
                                               {99, std::nullopt, 9, std::nullopt, std::nullopt}});
    REQUIRE(check(false, false, true, true) == paged);
    auto all = check(false, false, false, false);
    REQUIRE(all.size() == 7);
    REQUIRE(all[0] == composed_row{1, std::nullopt, 1, 10, -1});
    REQUIRE(all[1] == composed_row{2, 1, 2, 30, 10});
    REQUIRE(all[2] == composed_row{3, 2, 3, 30, 20});
    REQUIRE(all[3] == composed_row{4, 1, 4, 70, std::nullopt});
    REQUIRE(all[4] == composed_row{9, std::nullopt, 5, 160, 40});
    if constexpr (target != sql::dialect::mysql) {
        auto filtered = check(true, false, false, false);
        REQUIRE(filtered.size() == 7);
        REQUIRE_FALSE(filtered[0].total);
        REQUIRE(filtered[1].total == 20);
        REQUIRE(filtered[2].total == 20);
        REQUIRE(filtered[3].total == 60);
        REQUIRE(filtered[4].total == 150);
        REQUIRE(check(true, false, true, false) == filtered);
    }
    if constexpr (target == sql::dialect::postgres) {
        auto distinct = check(true, true, false, false);
        REQUIRE(distinct.size() == 5);
        REQUIRE(distinct[0] == composed_row{3, 2, 3, 20, 20});
        REQUIRE(distinct[1] == composed_row{4, 1, 4, 60, std::nullopt});
        REQUIRE(distinct[2] == composed_row{9, std::nullopt, 5, 150, 40});
        REQUIRE(check(true, true, true, false) == distinct);
    }
    auto update = [&] {
        return db.with_connection([](auto& connection) {
            return connection.execute("UPDATE acceptance_edges SET value=45 WHERE id=4");
        });
    };
    {
        auto transaction = db.begin();
        REQUIRE(transaction);
        REQUIRE(update());
        REQUIRE(check(false, false, false, false)[3].total == 75);
        REQUIRE(db.has_active_transaction());
        REQUIRE(transaction->rollback());
    }
    REQUIRE(check(false, false, false, false) == all);
    if constexpr (target != sql::dialect::mysql) {
        auto scope = testing::transactional_session<decltype(db)>::create(db);
        REQUIRE(scope);
        REQUIRE(update());
        REQUIRE(check(false, false, false, false)[3].total == 75);
        REQUIRE(scope->finish());
        REQUIRE(check(false, false, false, false) == all);
    }
    auto empty = composed_query();
    auto& compound = std::get<sql::compound_ast>(empty.body);
    std::get<sql::select_ast>(compound.left->body).where_clause = sql::col("number") < sql::val(0);
    std::get<sql::select_ast>(compound.right->body).where_clause = sql::col("id") < sql::val(0);
    int terminals{};
    db.set_observer([&](const query_event& event) {
        ++terminals;
        REQUIRE_FALSE(event.error);
    });
    auto empty_rows = sql::query_as<composed_row>(db, empty);
    REQUIRE(empty_rows);
    REQUIRE(empty_rows->empty());
    REQUIRE(terminals == 1);
    db.set_observer([&](const query_event&) {
        ++terminals;
        throw std::runtime_error("composed observer");
    });
    auto portable = composed_query();
    REQUIRE_THROWS_AS(sql::query_as<composed_row>(db, portable), std::runtime_error);
    REQUIRE(terminals == 2);
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE(check(false, false, false, false) == all);
    REQUIRE(db.with_connection(
        [](auto& connection) { return connection.execute("DROP TABLE acceptance_edges"); }));
    db.set_observer([&](const query_event& event) {
        ++terminals;
        REQUIRE(event.error);
    });
    auto failed = sql::query_as<composed_row>(db, portable);
    REQUIRE_FALSE(failed);
    REQUIRE_FALSE(failed.error().message.empty());
    REQUIRE(terminals == 3);
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE(events > 3);
}
} // namespace asterorm::test
