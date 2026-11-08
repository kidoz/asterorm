#pragma once
#include "asterorm/session.hpp"
#include <catch2/catch_test_macros.hpp>
#include <cstdio>

namespace asterorm::test {
template <typename Driver> void check_pool_health(Driver driver, const std::string& settings) {
    using connection_type = typename connection_pool<Driver>::connection_type;
    STATIC_REQUIRE(detail::has_pool_reuse_check<connection_type>);
    pool_config config;
    config.conninfo = settings;
    config.min_size = 0;
    config.max_size = 1;
    config.pre_ping = true;
    if constexpr (!detail::has_pool_ping<connection_type>) {
        connection_pool<Driver> unsupported{driver, config};
        REQUIRE(unsupported.warmup_error());
        REQUIRE(unsupported.warmup_error()->message.contains("pre-ping"));
        REQUIRE_FALSE(unsupported.acquire());
        REQUIRE(unsupported.stats().total == 0);
        config.pre_ping = false;
    }
    auto control = driver.connect(settings);
    REQUIRE(control);
    const auto table = "pool_health_" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    REQUIRE(control->execute("CREATE TABLE " + table + "(id INTEGER PRIMARY KEY)"));
    struct cleanup {
        connection_type& connection;
        std::string sql;
        ~cleanup() {
            try {
                if (!connection.execute(sql)) {
                    std::fputs("pool health table cleanup failed\n", stderr);
                }
            } catch (...) {
                std::fputs("pool health table cleanup threw\n", stderr);
            }
        }
    } drop{*control, "DROP TABLE " + table};
    connection_pool<Driver> pool{driver, config};
    for (bool failed : {false, true}) {
        auto lease = pool.acquire();
        REQUIRE(lease);
        REQUIRE((*lease)->is_pool_reusable());
        REQUIRE((*lease)->execute("BEGIN"));
        REQUIRE((*lease)->execute("INSERT INTO " + table + " VALUES(1)"));
        if (failed) {
            REQUIRE_FALSE((*lease)->execute("SELECT * FROM " + table + "_absent"));
        }
        REQUIRE_FALSE((*lease)->is_pool_reusable());
        lease->release_to_pool();
        REQUIRE(pool.stats().total == 0);
        auto count = control->execute("SELECT COUNT(*) FROM " + table);
        REQUIRE(count);
        REQUIRE(count->get_string(0, 0) == "0");
        auto fresh = pool.acquire();
        REQUIRE(fresh);
        REQUIRE((*fresh)->is_pool_reusable());
        REQUIRE((*fresh)->execute("SELECT 1"));
    }
    session db{pool};
    int events{};
    db.set_observer([&](const query_event& event) {
        ++events;
        REQUIRE_FALSE(event.error);
    });
    auto first = db.template native_scalar<int>("SELECT 41");
    auto second = db.template native_scalar<int>("SELECT 42");
    REQUIRE(first);
    REQUIRE(second);
    REQUIRE(*first == 41);
    REQUIRE(*second == 42);
    REQUIRE(events == 2);
    REQUIRE(pool.stats().total == 1);
}
} // namespace asterorm::test
