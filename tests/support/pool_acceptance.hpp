#pragma once
#include "asterorm/session.hpp"
#include <catch2/catch_test_macros.hpp>
#include <numeric>

namespace asterorm::test {
template <typename Predicate> bool await_pool(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    do {
        if (predicate())
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    } while (std::chrono::steady_clock::now() < deadline);
    return predicate();
}
inline void check_pool_accounting(const pool_stats& snapshot) {
    REQUIRE(snapshot.total == snapshot.idle + snapshot.leased + snapshot.connecting +
                                  snapshot.checking + snapshot.retiring);
    REQUIRE(snapshot.in_use == snapshot.total - snapshot.idle);
    REQUIRE(std::accumulate(snapshot.acquisition_duration.begin(),
                            snapshot.acquisition_duration.end(), std::uint64_t{}) ==
            snapshot.acquires_succeeded + snapshot.acquires_failed);
}
template <typename Driver> pool_config acceptance_config(std::string settings) {
    using connection_type = typename connection_pool<Driver>::connection_type;
    pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds{0};
    config.statement_caching = statement_cache_policy::off;
    config.pre_ping = detail::has_pool_ping<connection_type>;
    config.pre_ping_timeout = std::chrono::milliseconds{500};
    config.max_lifetime = config.leak_threshold = std::chrono::seconds{1};
    config.idle_timeout = std::chrono::milliseconds{30};
    config.maintenance_interval = std::chrono::milliseconds{1};
    if constexpr (detail::has_statement_timeout<connection_type>) {
        config.statement_timeout = std::chrono::milliseconds{1000};
    }
    return config;
}
template <typename Driver> void check_pool_acceptance(Driver driver, const std::string& settings) {
    auto config = acceptance_config<Driver>(settings);
    connection_pool<Driver> pool{driver, config};
    REQUIRE_FALSE(pool.warmup_error());
    session db{pool};
    int events{};
    db.set_observer([&](const auto& event) {
        REQUIRE(event.error == nullptr);
        ++events;
    });
    auto transaction = db.begin();
    REQUIRE(transaction);
    auto first = db.template native_scalar<int>("SELECT $1", 41);
    REQUIRE(first);
    REQUIRE(*first == 41);
    std::vector<pool_diagnostic> records;
    REQUIRE(await_pool([&] {
        records = pool.take_diagnostics();
        return !records.empty();
    }));
    REQUIRE(records.size() == 1);
    REQUIRE(records[0].held_for >= config.max_lifetime);
    REQUIRE(pool.stats().overdue_leases == 1);
    REQUIRE(pool.stats().retirements == 0);
    REQUIRE(pool.stats().leased == 1);
    auto busy = pool.acquire();
    REQUIRE_FALSE(busy);
    REQUIRE(busy.error().kind == db_error_kind::timeout);
    REQUIRE(db.with_connection([](auto& connection) -> result<void> {
        REQUIRE(connection.is_open());
        REQUIRE(connection.prepared_statement_count() == 0);
        return {};
    }));
    pool.close();
    auto second = db.template native_scalar<int>("SELECT $1", 42);
    REQUIRE(second);
    REQUIRE(*second == 42);
    REQUIRE(transaction->commit());
    REQUIRE(events == 2);
    REQUIRE(pool.take_diagnostics().empty());
    auto closed = pool.acquire();
    REQUIRE_FALSE(closed);
    REQUIRE(closed.error().kind == db_error_kind::pool_closed);
    const auto snapshot = pool.stats();
    REQUIRE(snapshot.total == 0);
    REQUIRE(snapshot.overdue_leases == 0);
    REQUIRE(snapshot.retirements == 1);
    REQUIRE(snapshot.leaked_at_close == 1);
    REQUIRE(snapshot.acquires_succeeded == 1);
    REQUIRE(snapshot.acquires_failed == 2);
    REQUIRE(snapshot.acquire_timeouts == 1);
    REQUIRE(snapshot.health_failures == 0);
    REQUIRE(snapshot.connection_failures == 0);
    check_pool_accounting(snapshot);

    config.max_lifetime = config.leak_threshold = std::chrono::minutes{1};
    connection_pool<Driver> reusable{driver, config};
    auto dirty = reusable.acquire();
    REQUIRE(dirty);
    REQUIRE((*dirty)->execute("BEGIN"));
    dirty->release_to_pool();
    REQUIRE(reusable.stats().total == 0);
    session fresh{reusable};
    int fresh_events{};
    fresh.set_observer([&](const auto&) { ++fresh_events; });
    auto value = fresh.template native_scalar<int>("SELECT $1", 43);
    REQUIRE(value);
    REQUIRE(*value == 43);
    REQUIRE(await_pool([&] { return reusable.stats().total == 0; }));
    REQUIRE(fresh_events == 1);
    REQUIRE(reusable.take_diagnostics().empty());
    REQUIRE(reusable.stats().acquires_succeeded == 2);
    REQUIRE(reusable.stats().retirements == 2);
    REQUIRE(reusable.stats().health_failures == 0);
    reusable.close();
    check_pool_accounting(reusable.stats());
}
} // namespace asterorm::test
