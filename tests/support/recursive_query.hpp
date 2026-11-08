#pragma once
#include <catch2/catch_test_macros.hpp>

#include "asterorm/session.hpp"
#include "asterorm/sql/query.hpp"
#include "asterorm/testing/transactional_session.hpp"

struct recursive_native_row {
    int id{};
    std::optional<int> parent;
};
template <> struct asterorm::entity_traits<recursive_native_row> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&recursive_native_row::parent>("parent"),
                        asterorm::column<&recursive_native_row::id>("id"));
};

namespace asterorm::test {
template <typename Driver> void check_recursive_query(Driver driver, std::string settings) {
    pool_config config;
    config.conninfo = std::move(settings);
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    constexpr auto target = sql::dialect_for_session<decltype(db)>();
    auto execute = [&](const char* statement) {
        auto result =
            db.with_connection([&](auto& connection) { return connection.execute(statement); });
        INFO((result ? "success" : result.error().message));
        REQUIRE(result);
    };
    // Separate TEMP roots/edges avoid MySQL's TEMP-table reopen restriction.
    execute("CREATE TEMPORARY TABLE recursive_roots(id INTEGER, parent INTEGER)");
    execute("CREATE TEMPORARY TABLE recursive_edges(id INTEGER, parent INTEGER)");
    execute("INSERT INTO recursive_roots VALUES(1,NULL)");
    execute("INSERT INTO recursive_edges VALUES(2,1),(3,2),(4,1)");
    auto seeds = sql::select({"id", "parent"})
                     .from("recursive_roots")
                     .where(sql::col("id") >= sql::val(0))
                     .build();
    auto anchor =
        sql::select({"id", "parent"}).from("seed").where(sql::col("id") == sql::val(1)).build();
    sql::select_ast step;
    step.table = "recursive_edges";
    step.table_alias = "child";
    step.columns = {"child.id", "child.parent"};
    step.joins.push_back({.kind = sql::join_kind::inner,
                          .table = "walk",
                          .alias = "path",
                          .on_clause = sql::col("child.parent") == sql::col("path.id")});
    step.where_clause = sql::col("child.id") <= sql::val(10);
    auto query = sql::query(sql::select({"id", "parent"})
                                .from("walk")
                                .where(sql::col("id") >= sql::val(1))
                                .order_by("id")
                                .build())
                     .with_cte("seed", sql::query(std::move(seeds)).build())
                     .with_cte("walk",
                               sql::union_all(sql::query(std::move(anchor)).build(),
                                              sql::query(std::move(step)).build())
                                   .build(),
                               {"id", "parent"})
                     .recursive()
                     .build();
    auto compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    REQUIRE(compiled->params == query_parameters{"0", "1", "10", "1"});
    int events{};
    db.set_observer([&](const query_event& event) {
        ++events;
        REQUIRE(event.sql.starts_with("WITH RECURSIVE "));
        REQUIRE(event.param_count == 4);
        REQUIRE_FALSE(event.error);
        REQUIRE(pool.stats().in_use == 1);
    });
    auto check = [&](const std::vector<int>& expected) {
        auto rows = sql::query_as<recursive_native_row>(db, query);
        INFO((rows ? "success" : rows.error().message));
        REQUIRE(rows);
        std::vector<int> actual;
        for (const auto& row : *rows) {
            actual.push_back(row.id);
            if (row.id == 1) {
                REQUIRE_FALSE(row.parent);
            }
        }
        REQUIRE(actual == expected);
        return rows;
    };
    check({1, 2, 3, 4});
    REQUIRE(events == 1);
    // The single self-reference may also be the top-level FROM relation.
    auto& recursive_step = std::get<sql::select_ast>(
        std::get<sql::compound_ast>(query.ctes[1].body->body).right->body);
    recursive_step.table = "walk";
    recursive_step.table_alias = "path";
    recursive_step.joins[0].table = "recursive_edges";
    recursive_step.joins[0].alias = "child";
    check({1, 2, 3, 4});
    REQUIRE(events == 2);
    {
        auto transaction = db.begin();
        REQUIRE(transaction);
        execute("INSERT INTO recursive_edges VALUES(2,1)");
        check({1, 2, 2, 3, 3, 4}); // UNION ALL preserves repeated traversal occurrences.
        REQUIRE(transaction->rollback());
    }
    check({1, 2, 3, 4});
    REQUIRE(events == 4);
    auto transaction = db.begin();
    REQUIRE(transaction);
    execute("INSERT INTO recursive_edges VALUES(5,4)");
    if constexpr (target == sql::dialect::postgres) {
        auto rows = db.with_statement_timeout(std::chrono::milliseconds{1000},
                                              [&](auto&) { return check({1, 2, 3, 4, 5}); });
        REQUIRE(rows);
    } else {
        check({1, 2, 3, 4, 5});
    }
    REQUIRE(events == 5);
    REQUIRE(db.has_active_transaction());
    REQUIRE(transaction->rollback());
    check({1, 2, 3, 4});
    REQUIRE(events == 6);
    if constexpr (target != sql::dialect::mysql) {
        using scope_type = testing::transactional_session<decltype(db)>;
        auto scope = scope_type::create(db);
        REQUIRE(scope);
        execute("INSERT INTO recursive_edges VALUES(5,4)");
        check({1, 2, 3, 4, 5});
        REQUIRE(scope->finish());
        check({1, 2, 3, 4});
        REQUIRE(events == 8);
    }
    std::get<sql::select_ast>(query.body).where_clause = sql::col("id") >= sql::val(99);
    check({}); // Empty materialization still validates named metadata.
    std::get<sql::select_ast>(query.body).where_clause = sql::col("id") >= sql::val(1);
    db.set_observer({});
    REQUIRE(pool.stats().in_use == 0);
    if constexpr (target == sql::dialect::postgres) {
        // Exercise a data cycle only under an explicit server-side timeout.
        auto cycle_transaction = db.begin();
        REQUIRE(cycle_transaction);
        execute("INSERT INTO recursive_edges VALUES(1,1)");
        int timeouts{};
        db.set_observer([&](const query_event& event) {
            ++timeouts;
            REQUIRE(event.error);
            REQUIRE(event.error->sqlstate == "57014");
        });
        auto timed = db.with_statement_timeout(std::chrono::milliseconds{20}, [&](auto& scoped) {
            auto failed = sql::query_as<recursive_native_row>(scoped, query);
            REQUIRE_FALSE(failed);
            // Recover the aborted PG transaction before timeout cleanup.
            REQUIRE(cycle_transaction->rollback());
            return failed;
        });
        REQUIRE_FALSE(timed);
        REQUIRE(timed.error().sqlstate == "57014");
        REQUIRE(timeouts == 1);
        REQUIRE_FALSE(db.has_active_transaction());
        db.set_observer({});
        check({1, 2, 3, 4});
    }
    execute("DROP TABLE recursive_edges");
    int failures{};
    db.set_observer([&](const query_event& event) {
        ++failures;
        REQUIRE(event.error);
    });
    auto failed = sql::query_as<recursive_native_row>(db, query);
    REQUIRE_FALSE(failed);
    REQUIRE(!failed.error().message.empty());
    REQUIRE(failures == 1);
}
} // namespace asterorm::test
