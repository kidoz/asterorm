#pragma once
#include "asterorm/pool/connection_pool.hpp"
#include <catch2/catch_test_macros.hpp>

namespace asterorm::test {
template <typename Driver> void check_pool_aging(Driver driver, const std::string& settings) {
    using namespace std::chrono_literals;
    pool_config config;
    config.conninfo = settings;
    config.min_size = 0;
    config.max_size = 1;
    config.max_lifetime = config.leak_threshold = 100ms;
    config.idle_timeout = 5ms;
    config.maintenance_interval = 1ms;
    connection_pool<Driver> pool{driver, config};
    REQUIRE_FALSE(pool.warmup_error());
    auto held = pool.acquire();
    REQUIRE(held);
    REQUIRE((*held)->execute("BEGIN"));
    std::vector<pool_diagnostic> warnings;
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    do {
        warnings = pool.take_diagnostics();
        if (!warnings.empty())
            break;
        std::this_thread::sleep_for(1ms);
    } while (std::chrono::steady_clock::now() < deadline);
    REQUIRE(warnings.size() == 1);
    REQUIRE(warnings[0].held_for >= 100ms);
    REQUIRE(pool.stats().overdue_leases == 1);
    REQUIRE(pool.stats().leased == 1);
    REQUIRE((*held)->is_open());
    REQUIRE((*held)->execute("SELECT 41"));
    REQUIRE((*held)->execute("COMMIT"));
    held->release_to_pool();
    REQUIRE(pool.stats().total == 0);
    REQUIRE(pool.stats().retirements == 1);
    REQUIRE(pool.stats().overdue_leases == 0);
    auto fresh = pool.acquire();
    REQUIRE(fresh);
    REQUIRE((*fresh)->execute("SELECT 42"));
    fresh->release_to_pool();
    const auto idle_deadline = std::chrono::steady_clock::now() + 3s;
    while (pool.stats().total != 0 && std::chrono::steady_clock::now() < idle_deadline) {
        std::this_thread::sleep_for(1ms);
    }
    REQUIRE(pool.stats().total == 0);
    REQUIRE(pool.stats().retirements == 2);
    REQUIRE(pool.stats().acquires_succeeded == 2);
    REQUIRE(pool.take_diagnostics().empty());
}
} // namespace asterorm::test
