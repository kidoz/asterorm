#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <cstddef>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"

TEST_CASE("PG Integration: query cancellation preserves connection reuse",
          "[pg][transactions][error][cancellation]") {
    const auto cache_size = GENERATE(std::size_t{0}, std::size_t{128});
    CAPTURE(cache_size);
    const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    asterorm::pool_config config;
    config.conninfo = configured ? configured
                                 : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                   "password=orm_test sslmode=disable";
    config.min_size = 1;
    config.max_size = 1;
    config.prepared_statement_cache_size = cache_size;
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};

    // Use a known baseline to verify restoration after rollback.
    auto baseline = db.with_connection(
        [](auto& connection) { return connection.execute("SET statement_timeout = '5s'"); });
    REQUIRE(baseline.has_value());
    auto initial_pid = db.native_scalar<int>("SELECT pg_backend_pid()");
    REQUIRE(initial_pid.has_value());
    REQUIRE(initial_pid->has_value());

    auto transaction = db.begin();
    REQUIRE(transaction.has_value());
    auto local_timeout = db.with_connection([](auto& connection) {
        return connection.execute("SET LOCAL statement_timeout = '50ms'");
    });
    REQUIRE(local_timeout.has_value());

    int observed_events = 0;
    std::string observed_sql;
    std::size_t observed_parameter_count = 0;
    std::optional<asterorm::db_error> observed_error;
    db.set_observer([&](const asterorm::query_event& event) {
        ++observed_events;
        observed_sql = event.sql;
        observed_parameter_count = event.param_count;
        if (event.error) {
            // query_event::error is only valid during the callback.
            observed_error = *event.error;
        }
    });
    constexpr std::string_view slow_query = "SELECT 1 FROM pg_sleep($1::double precision)";
    auto cancelled = db.native_scalar<int>(slow_query, 0.2);
    db.set_observer({});

    REQUIRE_FALSE(cancelled.has_value());
    REQUIRE(cancelled.error().sqlstate == "57014");
    REQUIRE(cancelled.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(cancelled.error().message.empty());
    REQUIRE(observed_events == 1);
    REQUIRE(observed_sql == slow_query);
    REQUIRE(observed_parameter_count == 1);
    REQUIRE(observed_error.has_value());
    REQUIRE(observed_error->sqlstate == cancelled.error().sqlstate);
    REQUIRE(observed_error->kind == cancelled.error().kind);
    REQUIRE(observed_error->message == cancelled.error().message);

    auto connection_open = db.with_connection(
        [](auto& connection) -> asterorm::result<bool> { return connection.is_open(); });
    REQUIRE(connection_open.has_value());
    REQUIRE(*connection_open);
    REQUIRE(transaction->rollback().has_value());

    auto restored = db.native_scalar<std::string>("SHOW statement_timeout");
    REQUIRE(restored.has_value());
    REQUIRE(*restored == std::optional<std::string>{"5s"});
    auto reused_pid = db.native_scalar<int>("SELECT pg_backend_pid()");
    REQUIRE(reused_pid.has_value());
    REQUIRE(*reused_pid == *initial_pid);
    auto recovered = db.native_scalar<int>(slow_query, 0.0);
    REQUIRE(recovered.has_value());
    REQUIRE(*recovered == std::optional<int>{1});
}

TEST_CASE("PG Integration: Transactions", "[pg][transactions]") {
    const char* env_conninfo = std::getenv("ASTERORM_TEST_CONNINFO");
    std::string conninfo = env_conninfo ? env_conninfo
                                        : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                          "password=orm_test sslmode=disable";

    asterorm::pool_config cfg;
    cfg.conninfo = conninfo;
    cfg.min_size = 1;
    cfg.max_size = 2;

    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};

    auto test_lease = pool.acquire();
    INFO("PostgreSQL integration tests require ASTERORM_TEST_CONNINFO or the default local test "
         "database");
    REQUIRE(test_lease.has_value());

    // Set up a clean dummy table for testing transactions
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS test_tx;");
    (void)(*test_lease)->execute("CREATE TABLE test_tx (id INT PRIMARY KEY, val TEXT);");
    test_lease.value().release_to_pool();

    asterorm::session<decltype(pool)> db{pool};

    SECTION("Commit persists data") {
        auto tx_res = db.begin();
        REQUIRE(tx_res.has_value());
        auto tx = std::move(tx_res.value());

        auto insert_res = db.with_connection([](auto& conn) {
            return conn.execute("INSERT INTO test_tx (id, val) VALUES (1, 'commit_test')");
        });
        REQUIRE(insert_res.has_value());

        auto commit_res = tx.commit();
        REQUIRE(commit_res.has_value());

        // Verify the insert is visible outside the transaction
        auto select_res = db.with_connection(
            [](auto& conn) { return conn.execute("SELECT id FROM test_tx WHERE id = 1"); });
        REQUIRE(select_res.has_value());
        REQUIRE(select_res.value().rows() == 1);
    }

    SECTION("Rollback discards data") {
        auto tx_res = db.begin();
        REQUIRE(tx_res.has_value());
        auto tx = std::move(tx_res.value());

        auto insert_res = db.with_connection([](auto& conn) {
            return conn.execute("INSERT INTO test_tx (id, val) VALUES (2, 'rollback_test')");
        });
        REQUIRE(insert_res.has_value());

        auto rollback_res = tx.rollback();
        REQUIRE(rollback_res.has_value());

        // Verify the insert is NOT visible
        auto select_res = db.with_connection(
            [](auto& conn) { return conn.execute("SELECT id FROM test_tx WHERE id = 2"); });
        REQUIRE(select_res.has_value());
        REQUIRE(select_res.value().rows() == 0);
    }

    SECTION("Destruction automatically rolls back") {
        {
            auto tx_res = db.begin();
            REQUIRE(tx_res.has_value());
            auto tx = std::move(tx_res.value());

            auto insert_res = db.with_connection([](auto& conn) {
                return conn.execute("INSERT INTO test_tx (id, val) VALUES (3, 'dtor_test')");
            });
            REQUIRE(insert_res.has_value());
            // tx goes out of scope here and should invoke rollback automatically
        }

        // Verify the insert is NOT visible
        auto select_res = db.with_connection(
            [](auto& conn) { return conn.execute("SELECT id FROM test_tx WHERE id = 3"); });
        REQUIRE(select_res.has_value());
        REQUIRE(select_res.value().rows() == 0);
    }

    SECTION("Commit fails after PostgreSQL has aborted the transaction") {
        auto transaction = db.begin();
        REQUIRE(transaction.has_value());
        auto inserted = db.with_connection([](auto& conn) {
            return conn.execute("INSERT INTO test_tx (id, val) VALUES (4, 'aborted')");
        });
        REQUIRE(inserted.has_value());
        auto failed_query =
            db.with_connection([](auto& conn) { return conn.execute("SELECT 1/0"); });
        REQUIRE_FALSE(failed_query.has_value());

        REQUIRE_FALSE(transaction->commit().has_value());
        auto selected = db.with_connection(
            [](auto& conn) { return conn.execute("SELECT id FROM test_tx WHERE id = 4"); });
        REQUIRE(selected.has_value());
        REQUIRE(selected->rows() == 0);
    }

    // Teardown test table cleanly
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS test_tx;");
}
