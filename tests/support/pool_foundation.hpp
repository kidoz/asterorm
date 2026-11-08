#pragma once
#include "asterorm/session.hpp"
#include <catch2/catch_test_macros.hpp>

namespace asterorm::test {
template <typename Driver> void check_pool_foundation(Driver driver, std::string settings) {
    pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 2;
    using pool_type = connection_pool<Driver>;
    pool_type pool{driver, config};
    session db{pool};
    int callbacks{};
    db.set_observer([&](const auto&) { ++callbacks; });
    auto first = pool.acquire();
    auto second = pool.acquire();
    REQUIRE(first);
    REQUIRE(second);
    REQUIRE(pool.stats().leased == 2);
    pool.close();
    REQUIRE(pool.stats().closed);
    REQUIRE(pool.stats().total == 2);
    REQUIRE(pool.stats().leaked_at_close == 2);
    auto selected = (*first)->execute_prepared("SELECT $1 AS value", {"41"});
    REQUIRE(selected);
    REQUIRE(selected->get_string(0, 0) == "41");
    first->release_to_pool();
    REQUIRE(pool.stats().total == 1);
    second->release_to_pool();
    REQUIRE(pool.stats().total == 0);
    auto closed = pool.acquire();
    REQUIRE_FALSE(closed);
    REQUIRE(closed.error().kind == db_error_kind::pool_closed);
    auto retried = db.retry_transaction([&](auto&) -> result<int> {
        ++callbacks;
        return 1;
    });
    REQUIRE_FALSE(retried);
    REQUIRE(retried.error().kind == db_error_kind::pool_closed);
    REQUIRE(callbacks == 0);

    auto facade = std::make_unique<pool_type>(driver, config);
    auto retained = facade->acquire();
    REQUIRE(retained);
    facade.reset();
    auto after_destruction = (*retained)->execute_prepared("SELECT $1 AS value", {"73"});
    REQUIRE(after_destruction);
    REQUIRE(after_destruction->get_string(0, 0) == "73");
    retained->release_to_pool();

    pool_type transactional{std::move(driver), config};
    session owned{transactional};
    auto transaction = owned.begin();
    REQUIRE(transaction);
    transactional.close();
    auto inside =
        owned.with_connection([](auto& connection) { return connection.execute("SELECT 1"); });
    REQUIRE(inside);
    REQUIRE(owned.has_active_transaction());
    REQUIRE(transaction->rollback());
    REQUIRE(transactional.stats().total == 0);
    REQUIRE_FALSE(owned.has_active_transaction());
}
} // namespace asterorm::test
