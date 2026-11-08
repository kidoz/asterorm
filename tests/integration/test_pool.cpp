#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "asterorm/pool/connection_pool.hpp"
#include "asterorm/postgres/driver.hpp"
#include "support/pool_foundation.hpp"
#include "support/pool_health.hpp"

TEST_CASE("PG Integration: Connection Pool", "[pg][pool]") {
    const char* env_conninfo = std::getenv("ASTERORM_TEST_CONNINFO");
    std::string conninfo = env_conninfo ? env_conninfo
                                        : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                          "password=orm_test sslmode=disable";

    asterorm::pool_config cfg;
    cfg.conninfo = conninfo;
    cfg.min_size = 1;
    cfg.max_size = 2;

    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};

    auto lease1 = pool.acquire();
    INFO("PostgreSQL integration tests require ASTERORM_TEST_CONNINFO or the default local test "
         "database");
    REQUIRE(lease1.has_value());
    REQUIRE(lease1.value()->is_open());

    auto lease2 = pool.acquire();
    REQUIRE(lease2.has_value());

    // Test that they are functional
    auto res = lease1.value()->execute("SELECT 1");
    REQUIRE(res.has_value());
    REQUIRE(res.value().rows() == 1);
}

TEST_CASE("PG Integration: pool ownership foundation", "[pg][pool-foundation]") {
    const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    asterorm::test::check_pool_foundation(
        asterorm::pg::driver{}, configured ? configured
                                           : "host=127.0.0.1 port=5432 dbname=orm_test "
                                             "user=orm_test password=orm_test sslmode=disable");
}

TEST_CASE("PG Integration: pool health and dirty returns", "[pg][pool-health]") {
    const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    const std::string settings = configured ? configured
                                            : "host=127.0.0.1 port=5432 dbname=orm_test "
                                              "user=orm_test password=orm_test sslmode=disable";
    asterorm::test::check_pool_health(asterorm::pg::driver{}, settings);
    asterorm::pool_config config;
    config.conninfo = settings;
    config.pre_ping = true;
    config.max_size = 1;
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    auto held = pool.acquire();
    REQUIRE(held);
    auto identity = (*held)->execute("SELECT pg_backend_pid()");
    REQUIRE(identity);
    auto pid = identity->get_string(0, 0);
    REQUIRE(pid);
    held->release_to_pool();
    auto control = asterorm::pg::driver{}.connect(settings);
    REQUIRE(control);
    auto terminated =
        control->execute_params("SELECT pg_terminate_backend($1::integer,1000)", {*pid});
    REQUIRE(terminated);
    REQUIRE(terminated->get_string(0, 0) == "t");
    auto stale = pool.acquire();
    REQUIRE_FALSE(stale);
    REQUIRE(pool.stats().total == 0);
    auto replacement = pool.acquire();
    REQUIRE(replacement);
    auto current = (*replacement)->execute("SELECT pg_backend_pid()");
    REQUIRE(current);
    REQUIRE(current->get_string(0, 0) != pid);
}
