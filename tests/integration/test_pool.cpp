#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "asterorm/pool/connection_pool.hpp"
#include "asterorm/postgres/driver.hpp"

TEST_CASE("PG Integration: Connection Pool", "[pg][pool]") {
    const char* env_conninfo = std::getenv("ASTERORM_TEST_CONNINFO");
    std::string conninfo =
        env_conninfo ? env_conninfo
                     : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test sslmode=disable";

    asterorm::pool_config cfg;
    cfg.conninfo = conninfo;
    cfg.min_size = 1;
    cfg.max_size = 2;

    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};

    auto lease1 = pool.acquire();
    if (!lease1.has_value()) {
        WARN("Could not connect to PostgreSQL. Is it running? Skipping test.");
        return;  // Skip gracefully if DB is down locally
    }

    REQUIRE(lease1.has_value());
    REQUIRE(lease1.value()->is_open());

    auto lease2 = pool.acquire();
    REQUIRE(lease2.has_value());

    // Test that they are functional
    auto res = lease1.value()->execute("SELECT 1");
    REQUIRE(res.has_value());
    REQUIRE(res.value().rows() == 1);
}