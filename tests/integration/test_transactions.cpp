#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"

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

    // Teardown test table cleanly
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS test_tx;");
}
