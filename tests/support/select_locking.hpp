#pragma once
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdio>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include "asterorm/session.hpp"
#include "asterorm/sql/typed_query.hpp"

struct select_lock_job {
    int id{};
};

template <> struct asterorm::entity_traits<select_lock_job> {
    // Each fixture chooses a private table name before starting either session.
    static inline std::string table;
    static constexpr auto columns = std::make_tuple(asterorm::column<&select_lock_job::id>("id"));
};

namespace asterorm::test {
template <typename Session> struct locking_table_guard {
    Session& session;
    std::string quoted_table;
    bool active{true};

    asterorm::result<void> drop() {
        auto dropped = session.with_connection(
            [&](auto& connection) { return connection.execute("DROP TABLE " + quoted_table); });
        if (!dropped) {
            return std::unexpected(dropped.error());
        }
        active = false;
        return {};
    }
    ~locking_table_guard() {
        if (active) {
            try {
                if (!drop()) {
                    std::fputs("locking fixture table cleanup failed\n", stderr);
                }
            } catch (...) {
                std::fputs("locking fixture table cleanup threw\n", stderr);
            }
        }
    }
};

template <typename Driver> void check_select_locking(Driver driver, std::string conninfo) {
    namespace typed = asterorm::sql::typed;
    namespace sql = asterorm::sql;
    asterorm::pool_config config;
    config.conninfo = std::move(conninfo);
    config.min_size = config.max_size = 2;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session owner{pool};
    asterorm::session worker{pool};
    constexpr auto target = sql::dialect_for_session<decltype(owner)>();
    STATIC_REQUIRE(sql::supports_select_locking(target));

    // Configure both physical connections, not two ephemeral uses of one lease.
    auto first_lease = pool.acquire();
    auto second_lease = pool.acquire();
    REQUIRE(first_lease.has_value());
    REQUIRE(second_lease.has_value());
    for (auto* lease : {&*first_lease, &*second_lease}) {
        if constexpr (target == sql::dialect::postgres) {
            REQUIRE((*lease)->execute("SET statement_timeout = '3s'").has_value());
            REQUIRE((*lease)->execute("SET lock_timeout = '1s'").has_value());
        } else {
            REQUIRE((*lease)->execute("SET SESSION innodb_lock_wait_timeout = 1").has_value());
        }
    }
    first_lease->release_to_pool();
    second_lease->release_to_pool();

    asterorm::entity_traits<select_lock_job>::table =
        "asterorm_select_lock_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    sql::compiler compiler{target};
    auto quoted = compiler.quote_ident(asterorm::entity_traits<select_lock_job>::table);
    REQUIRE(quoted.has_value());
    std::string create = "CREATE TABLE " + *quoted + " (id INTEGER PRIMARY KEY)";
    if constexpr (target == sql::dialect::mysql) {
        create += " ENGINE=InnoDB";
    }
    auto created =
        owner.with_connection([&](auto& connection) { return connection.execute(create); });
    REQUIRE(created.has_value());
    locking_table_guard guard{.session = owner, .quoted_table = *quoted};
    auto inserted = owner.with_connection([&](auto& connection) {
        return connection.execute("INSERT INTO " + *quoted + " VALUES (1), (2), (3)");
    });
    REQUIRE(inserted.has_value());

    std::string observed_sql;
    std::optional<asterorm::db_error> observed_error;
    worker.set_observer([&](const asterorm::query_event& event) {
        observed_sql = event.sql;
        observed_error = event.error ? std::optional{*event.error} : std::nullopt;
    });
    auto expect_id = [](auto result, int expected_id) {
        INFO((result ? "success" : result.error().message));
        REQUIRE(result.has_value());
        REQUIRE(result->size() == 1);
        REQUIRE(result->front().id == expected_id);
    };
    auto check_nowait_error = [&](const auto& result) {
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().kind == asterorm::db_error_kind::query_failed);
        REQUIRE(observed_error.has_value());
        REQUIRE(observed_error->sqlstate == result.error().sqlstate);
        REQUIRE(observed_sql.ends_with(" FOR UPDATE NOWAIT"));
        if constexpr (target == sql::dialect::postgres) {
            REQUIRE(result.error().sqlstate == "55P03");
        } else {
            REQUIRE(result.error().sqlstate == "HY000");
            REQUIRE(result.error().detail.has_value());
            REQUIRE(result.error().detail->contains("3572"));
        }
    };
    auto owner_transaction = owner.begin();
    auto worker_transaction = worker.begin();
    REQUIRE(owner_transaction.has_value());
    REQUIRE(worker_transaction.has_value());
    expect_id(typed::query(owner, typed::select<select_lock_job>()
                                      .where(typed::col<&select_lock_job::id> >= typed::val(1))
                                      .order_by(typed::col<&select_lock_job::id>)
                                      .limit(1)
                                      .for_update()
                                      .skip_locked()),
              1);
    expect_id(typed::query(worker, typed::select<select_lock_job>()
                                       .where(typed::col<&select_lock_job::id> >= typed::val(1))
                                       .order_by(typed::col<&select_lock_job::id>)
                                       .limit(1)
                                       .for_update()
                                       .skip_locked()),
              2);
    REQUIRE(observed_sql.ends_with(" LIMIT 1 FOR UPDATE SKIP LOCKED"));
    REQUIRE(worker_transaction->rollback().has_value());

    auto conflict_transaction = worker.begin();
    REQUIRE(conflict_transaction.has_value());
    auto string_ast = sql::select({"id"})
                          .from(asterorm::entity_traits<select_lock_job>::table)
                          .where(sql::col("id") == sql::val(1))
                          .limit(1)
                          .for_update()
                          .nowait()
                          .build();
    auto compiled = compiler.compile(string_ast);
    REQUIRE(compiled.has_value());
    auto conflict = worker.with_connection([&](auto& connection) {
        return worker.observed_execute(connection, compiled->sql, compiled->params);
    });
    check_nowait_error(conflict);
    REQUIRE(conflict_transaction->rollback().has_value());
    REQUIRE(owner_transaction->commit().has_value());

    auto after_commit = worker.begin();
    REQUIRE(after_commit.has_value());
    expect_id(typed::query(worker, typed::select<select_lock_job>()
                                       .where(typed::col<&select_lock_job::id> == typed::val(1))
                                       .for_update()
                                       .nowait()),
              1);
    REQUIRE(after_commit->rollback().has_value());

    auto owner_share = owner.begin();
    auto worker_share = worker.begin();
    REQUIRE(owner_share.has_value());
    REQUIRE(worker_share.has_value());
    expect_id(typed::query(owner, typed::select<select_lock_job>()
                                      .where(typed::col<&select_lock_job::id> == typed::val(1))
                                      .for_share()
                                      .nowait()),
              1);
    expect_id(typed::query(worker, typed::select<select_lock_job>()
                                       .where(typed::col<&select_lock_job::id> == typed::val(1))
                                       .for_share()
                                       .nowait()),
              1);
    auto upgrade =
        typed::query(worker, typed::select<select_lock_job>()
                                 .where(typed::col<&select_lock_job::id> == typed::val(1))
                                 .for_update()
                                 .nowait());
    check_nowait_error(upgrade);
    REQUIRE(worker_share->rollback().has_value());
    REQUIRE(owner_share->rollback().has_value());
    auto after_rollback = worker.begin();
    REQUIRE(after_rollback.has_value());
    expect_id(typed::query(worker, typed::select<select_lock_job>()
                                       .where(typed::col<&select_lock_job::id> == typed::val(1))
                                       .for_update()
                                       .nowait()),
              1);
    REQUIRE(after_rollback->rollback().has_value());
    worker.set_observer({});
    REQUIRE(guard.drop().has_value());
}
} // namespace asterorm::test
