#pragma once
#include <algorithm>
#include <catch2/catch_test_macros.hpp>

#include "asterorm/session.hpp"
#include "asterorm/sql/query.hpp"
#include "asterorm/testing/transactional_session.hpp"

struct set_native_row {
    std::optional<int> id;
};
template <> struct asterorm::entity_traits<set_native_row> {
    static constexpr auto columns = std::make_tuple(asterorm::column<&set_native_row::id>("id"));
};

namespace asterorm::test {
template <typename Driver> void check_set_query(Driver driver, std::string settings) {
    pool_config config;
    config.conninfo = std::move(settings);
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    constexpr auto target = sql::dialect_for_session<decltype(db)>();
    const sql::query_compile_options modern{sql::mysql_query_profile::set_operations_8_0_31};
    auto execute = [&](const char* statement) {
        auto executed =
            db.with_connection([&](auto& connection) { return connection.execute(statement); });
        INFO((executed ? "success" : executed.error().message));
        REQUIRE(executed);
    };
    execute("CREATE TEMPORARY TABLE set_left_rows(id INTEGER)");
    execute("CREATE TEMPORARY TABLE set_right_rows(other INTEGER)");
    execute("CREATE TEMPORARY TABLE set_third_rows(third INTEGER)");
    execute("INSERT INTO set_left_rows VALUES(NULL),(NULL),(1),(1),(1),(2)");
    execute("INSERT INTO set_right_rows VALUES(NULL),(1),(1),(3)");
    execute("INSERT INTO set_third_rows VALUES(1),(3)");
    auto left = [] {
        return sql::query(sql::select({"id"}).from("set_left_rows").build()).build();
    };
    auto right = [] {
        return sql::query(sql::select({"other"}).from("set_right_rows").build()).build();
    };
    auto third = [] {
        return sql::query(sql::select({"third"}).from("set_third_rows").build()).build();
    };
    auto check = [&](const sql::query_ast& query, std::vector<std::optional<int>> expected) {
        auto rows = sql::query_as<set_native_row>(db, query, modern);
        INFO((rows ? "success" : rows.error().message));
        REQUIRE(rows);
        std::vector<std::optional<int>> actual;
        for (const auto& row : *rows) {
            actual.push_back(row.id);
        }
        // NULL ordering is native; compare bags independently of that order.
        std::ranges::sort(actual);
        std::ranges::sort(expected);
        REQUIRE(actual == expected);
    };
    check(sql::union_(left(), right()).build(), {std::nullopt, 1, 2, 3});
    check(sql::union_all(left(), right()).build(),
          {std::nullopt, std::nullopt, std::nullopt, 1, 1, 1, 1, 1, 2, 3});
    check(sql::intersect_(left(), right()).build(), {std::nullopt, 1});
    check(sql::except_(left(), right()).build(), {2});
    if constexpr (target != sql::dialect::sqlite) {
        check(sql::intersect_all(left(), right()).build(), {std::nullopt, 1, 1});
        check(sql::except_all(left(), right()).build(), {std::nullopt, 1, 2});
    } else {
        REQUIRE_FALSE(
            sql::query_as<set_native_row>(db, sql::intersect_all(left(), right()).build()));
        REQUIRE_FALSE(sql::query_as<set_native_row>(db, sql::except_all(left(), right()).build()));
    }
    check(sql::intersect_(sql::union_(left(), right()).build(), third()).build(), {1, 3});
    check(sql::union_(left(), sql::intersect_(right(), third()).build()).build(),
          {std::nullopt, 1, 2, 3});
    // Right grouping keeps 1, which left grouping would remove.
    check(sql::except_(left(), sql::except_(right(), third()).build()).build(), {1, 2});
    auto branch_left =
        sql::query(
            sql::select({"id"}).from("set_left_rows").where(sql::col("id") > sql::val(1)).build())
            .order_by("id")
            .limit(1)
            .build();
    auto branch_right = sql::query(sql::select({"other"})
                                       .from("set_right_rows")
                                       .where(sql::col("other") > sql::val(0))
                                       .build())
                            .order_by("other", false)
                            .limit(1)
                            .build();
    auto tailed = sql::union_all(std::move(branch_left), std::move(branch_right))
                      .order_by("id", false)
                      .offset(1)
                      .build();
    check(tailed, {2});
    std::get<sql::compound_ast>(tailed.body).limit_val = 0;
    check(tailed, {}); // Metadata is available even when no rows survive.
    // WITH remains statement-level and a definition body can itself be a set.
    auto cte_query =
        sql::union_(sql::query(sql::select({"id"}).from("combined").build()).build(), third())
            .with_cte("combined", sql::union_all(left(), right()).build(), {"id"})
            .build();
    check(cte_query, {std::nullopt, 1, 2, 3});

    auto bound_left =
        sql::query(
            sql::select({"id"}).from("set_left_rows").where(sql::col("id") > sql::val(0)).build())
            .build();
    auto bound_right = sql::query(sql::select({"other"})
                                      .from("set_right_rows")
                                      .where(sql::col("other") > sql::val(0))
                                      .build())
                           .build();
    auto bound = sql::union_all(std::move(bound_left), std::move(bound_right)).build();
    auto compiled = sql::compiler{target}.compile(bound);
    REQUIRE(compiled);
    int events{};
    db.set_observer([&](const query_event& event) {
        ++events;
        REQUIRE(event.sql == compiled->sql);
        REQUIRE(event.param_count == 2);
        REQUIRE_FALSE(event.error);
        REQUIRE(pool.stats().in_use == 1);
    });
    auto transaction = db.begin();
    REQUIRE(transaction);
    execute("INSERT INTO set_left_rows VALUES(4)");
    if constexpr (target == sql::dialect::postgres) {
        auto rows = db.with_statement_timeout(std::chrono::milliseconds{1000}, [&](auto& scoped) {
            return sql::query_as<set_native_row>(scoped, bound);
        });
        REQUIRE(rows);
        REQUIRE(rows->size() == 8);
    } else {
        check(bound, {1, 1, 1, 1, 1, 2, 3, 4});
    }
    REQUIRE(events == 1);
    REQUIRE(db.has_active_transaction());
    REQUIRE(transaction->rollback());
    check(bound, {1, 1, 1, 1, 1, 2, 3});
    REQUIRE(events == 2);
    if constexpr (target != sql::dialect::mysql) {
        using scope_type = testing::transactional_session<decltype(db)>;
        auto scope = scope_type::create(db);
        REQUIRE(scope);
        execute("INSERT INTO set_left_rows VALUES(5)");
        check(bound, {1, 1, 1, 1, 1, 2, 3, 5});
        REQUIRE(scope->finish());
        check(bound, {1, 1, 1, 1, 1, 2, 3});
        REQUIRE(events == 4);
    }
    REQUIRE(pool.stats().in_use == 0);
    db.set_observer({});
    execute("DROP TABLE set_right_rows");
    int failures{};
    db.set_observer([&](const query_event& event) {
        ++failures;
        REQUIRE(event.error);
    });
    auto failed = sql::query_as<set_native_row>(db, bound);
    REQUIRE_FALSE(failed);
    REQUIRE(!failed.error().message.empty());
    REQUIRE(failures == 1);
}
} // namespace asterorm::test
