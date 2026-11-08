#include "asterorm/pool/connection_pool.hpp"
#include <algorithm>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <functional>
#include <future>
#include <numeric>

namespace asterorm::detail {
struct pool_test_access {
    inline static std::atomic<std::int64_t> milliseconds;
    static pool_clock::time_point read_clock() noexcept {
        return pool_clock::time_point{std::chrono::milliseconds{milliseconds.load()}};
    }
    template <typename Driver> static auto make(Driver driver, pool_config config) {
        milliseconds = 0;
        return std::unique_ptr<connection_pool<Driver>>{
            new connection_pool<Driver>{std::move(driver), std::move(config), read_clock, false},
        };
    }
    template <typename Driver> static void tick(connection_pool<Driver>& pool) {
        pool.state_->maintain();
    }
    template <typename Driver> static bool worker_running(connection_pool<Driver>& pool) {
        return pool.worker_.joinable();
    }
};
} // namespace asterorm::detail
namespace {
using namespace std::chrono_literals;
using access = asterorm::detail::pool_test_access;
struct aging_state {
    std::atomic<int> connects, closes;
    std::function<void()> on_connect, on_setup, on_ping, on_close;
    int failure{};
};
struct aging_connection {
    std::shared_ptr<aging_state> state;
    bool open{true};
    explicit aging_connection(std::shared_ptr<aging_state> observed) : state(std::move(observed)) {}
    aging_connection(aging_connection&& other) noexcept
        : state(std::move(other.state)), open(std::exchange(other.open, false)) {}
    ~aging_connection() {
        close();
    }
    [[nodiscard]] bool is_open() const noexcept {
        return open;
    }
    [[nodiscard]] bool is_pool_reusable() const noexcept {
        return open;
    }
    void close() noexcept {
        if (std::exchange(open, false)) {
            ++state->closes;
            if (state->on_close) {
                state->on_close();
            }
        }
    }
    void set_max_prepared_statements([[maybe_unused]] std::size_t count) const {
        if (state->on_setup) {
            state->on_setup();
        }
    }
    [[nodiscard]] asterorm::result<void>
    pool_ping([[maybe_unused]] std::chrono::steady_clock::time_point deadline) const {
        if (state->on_ping) {
            state->on_ping();
        }
        if (state->failure == 3) {
            throw std::runtime_error("health exception");
        }
        if (state->failure == 2) {
            return std::unexpected(asterorm::db_error{});
        }
        return {};
    }
};
struct aging_driver {
    std::shared_ptr<aging_state> state;
    [[nodiscard]] asterorm::result<aging_connection>
    connect([[maybe_unused]] const std::string& settings) const {
        ++state->connects;
        if (state->on_connect) {
            state->on_connect();
        }
        if (state->failure == 1) {
            throw std::runtime_error("connect exception");
        }
        if (state->failure == 4) {
            return std::unexpected(asterorm::db_error{});
        }
        return aging_connection{state};
    }
};
asterorm::pool_config aging_config() {
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    config.acquire_timeout = 0ms;
    return config;
}
template <typename Predicate> bool eventually(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}
} // namespace

TEST_CASE("Pool aging: invalid durations reject before dialing", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.min_size = GENERATE(0, 1);
    const int option = GENERATE(0, 1, 2, 3, 4);
    switch (option) {
    case 0:
        config.idle_timeout = std::chrono::milliseconds::min();
        break;
    case 1:
        config.max_lifetime = -1ms;
        break;
    case 2:
        config.leak_threshold = -1ms;
        break;
    case 3:
        config.maintenance_interval = -1ms;
        break;
    default:
        config.maintenance_interval = 0ms;
        break;
    }
    asterorm::connection_pool<aging_driver> pool{aging_driver{state}, config};
    REQUIRE(pool.warmup_error());
    REQUIRE_FALSE(pool.acquire());
    REQUIRE(state->connects == 0);
    REQUIRE_FALSE(access::worker_running(pool));
}

TEST_CASE("Pool aging: idle expiry boundary and latest return time", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.idle_timeout = 10ms;
    config.min_size = GENERATE(0, 1);
    auto pool = access::make(aging_driver{state}, config);
    auto first = pool->acquire();
    REQUIRE(first);
    access::milliseconds = 8;
    first->release_to_pool();
    access::milliseconds = 17;
    access::tick(*pool);
    REQUIRE(pool->stats().idle == 1);
    access::milliseconds = 18;
    if (GENERATE(false, true)) {
        access::tick(*pool);
        REQUIRE(pool->stats().total == 0);
        REQUIRE(pool->stats().retirements == 1);
    }
    auto fresh = pool->acquire();
    REQUIRE(fresh);
    REQUIRE(state->connects == 2);
    REQUIRE(state->closes == 1);
}

TEST_CASE("Pool aging: lifetime never revokes a held lease", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.max_lifetime = 10ms;
    auto pool = access::make(aging_driver{state}, config);
    auto held = pool->acquire();
    REQUIRE(held);
    access::milliseconds = 10;
    if (GENERATE(false, true)) {
        access::tick(*pool);
    }
    REQUIRE((*held)->is_open());
    REQUIRE(state->closes == 0);
    REQUIRE(pool->stats().leased == 1);
    held->release_to_pool();
    REQUIRE(pool->stats().total == 0);
    REQUIRE(state->closes == 1);
    REQUIRE(pool->acquire());
    REQUIRE(state->connects == 2);
}

TEST_CASE("Pool aging: checkout expiry after setup fails once without retry", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.max_lifetime = 10ms;
    config.pre_ping = GENERATE(false, true);
    auto pool = access::make(aging_driver{state}, config);
    if (config.pre_ping) {
        state->on_ping = [] { access::milliseconds = 10; };
    } else {
        state->on_setup = [] { access::milliseconds = 10; };
    }
    auto expired = pool->acquire();
    REQUIRE_FALSE(expired);
    REQUIRE(expired.error().message.contains("expired during checkout"));
    REQUIRE(state->connects == 1);
    REQUIRE(state->closes == 1);
    REQUIRE(pool->stats().total == 0);
    REQUIRE(pool->stats().connection_failures == 1);
    state->on_setup = state->on_ping = {};
    REQUIRE(pool->acquire());
    REQUIRE(state->connects == 2);
}

TEST_CASE("Pool aging: disabled and huge durations do not overflow or retire", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    const auto limit = GENERATE(0ms, std::chrono::milliseconds::max());
    config.idle_timeout = config.max_lifetime = config.leak_threshold = limit;
    auto pool = access::make(aging_driver{state}, config);
    auto held = pool->acquire();
    REQUIRE(held);
    access::milliseconds = 100000;
    access::tick(*pool);
    REQUIRE(pool->stats().overdue_leases == 0);
    held->release_to_pool();
    access::tick(*pool);
    REQUIRE(pool->stats().idle == 1);
    REQUIRE(pool->take_diagnostics().empty());
    REQUIRE(state->closes == 0);
    REQUIRE_FALSE(access::worker_running(*pool));
}

TEST_CASE("Pool diagnostics: warnings are once per checkout generation", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.leak_threshold = 10ms;
    auto pool = access::make(aging_driver{state}, config);
    auto held = pool->acquire();
    REQUIRE(held);
    access::milliseconds = 9;
    access::tick(*pool);
    REQUIRE(pool->take_diagnostics().empty());
    access::milliseconds = 10;
    REQUIRE(pool->stats().overdue_leases == 1);
    access::tick(*pool);
    auto first = pool->take_diagnostics();
    REQUIRE(first.size() == 1);
    REQUIRE(first[0].slot != 0);
    REQUIRE(first[0].generation != 0);
    REQUIRE(first[0].held_for == 10ms);
    REQUIRE(first[0].severity == asterorm::pool_diagnostic_severity::warning);
    access::milliseconds = 30;
    access::tick(*pool);
    REQUIRE(pool->take_diagnostics().empty());
    auto moved = std::move(*held);
    moved.release_to_pool();
    REQUIRE(pool->stats().overdue_leases == 0);
    auto reused = pool->acquire();
    REQUIRE(reused);
    access::milliseconds = 40;
    // Return also records an overdue checkout even when the worker has not run.
    reused->release_to_pool();
    auto second = pool->take_diagnostics();
    REQUIRE(second.size() == 1);
    REQUIRE(second[0].slot == first[0].slot);
    REQUIRE(second[0].generation > first[0].generation);
    REQUIRE(pool->stats().diagnostics_dropped == 0);
}

TEST_CASE("Pool diagnostics: undrained warnings stay bounded and drops do not repeat",
          "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.leak_threshold = 1ms;
    auto pool = access::make(aging_driver{state}, config);
    for (int index = 0; index < 3; ++index) {
        auto held = pool->acquire();
        REQUIRE(held);
        ++access::milliseconds;
        access::tick(*pool);
        access::tick(*pool);
    }
    REQUIRE(pool->stats().diagnostics_dropped == 2);
    REQUIRE(pool->take_diagnostics().size() == 1);
    REQUIRE(pool->take_diagnostics().empty());
    auto held = pool->acquire();
    REQUIRE(held);
    ++access::milliseconds;
    access::tick(*pool);
    REQUIRE(pool->take_diagnostics().size() == 1);
    pool->close();
    REQUIRE((*held)->is_open());
    held->release_to_pool();
    REQUIRE(pool->stats().total == 0);
}

TEST_CASE("Pool metrics: fixed buckets measure all acquisition calls", "[pool-aging]") {
    const auto delay = GENERATE(0, 1, 2, 5, 6, 10, 11, 50, 51, 100, 101, 500, 501, 1000, 1001);
    auto state = std::make_shared<aging_state>();
    auto pool = access::make(aging_driver{state}, aging_config());
    state->on_connect = [delay] { access::milliseconds = delay; };
    auto held = pool->acquire();
    REQUIRE(held);
    constexpr std::array bounds{1, 5, 10, 50, 100, 500, 1000};
    const auto bucket =
        std::ranges::find_if(bounds, [delay](int bound) { return delay <= bound; }) -
        bounds.begin();
    auto snapshot = pool->stats();
    REQUIRE(snapshot.acquires_succeeded == 1);
    REQUIRE(snapshot.acquisition_duration[bucket] == 1);
    REQUIRE_FALSE(pool->acquire());
    pool->close();
    REQUIRE_FALSE(pool->acquire());
    snapshot = pool->stats();
    REQUIRE(snapshot.acquires_failed == 2);
    REQUIRE(snapshot.acquire_timeouts == 1);
    REQUIRE(snapshot.connection_failures == 0);
    REQUIRE(snapshot.health_failures == 0);
    REQUIRE(std::accumulate(snapshot.acquisition_duration.begin(),
                            snapshot.acquisition_duration.end(), std::uint64_t{}) == 3);
}

TEST_CASE("Pool metrics: native errors and exceptions retain separate outcomes", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    state->failure = GENERATE(1, 2, 3, 4);
    auto config = aging_config();
    config.pre_ping = state->failure == 2 || state->failure == 3;
    auto pool = access::make(aging_driver{state}, config);
    if (state->failure == 1 || state->failure == 3) {
        REQUIRE_THROWS(pool->acquire());
    } else {
        REQUIRE_FALSE(pool->acquire());
    }
    auto snapshot = pool->stats();
    REQUIRE(snapshot.acquires_failed == 1);
    REQUIRE(snapshot.health_failures == (config.pre_ping ? 1 : 0));
    REQUIRE(snapshot.connection_failures == (config.pre_ping ? 0 : 1));
    REQUIRE(snapshot.total == 0);
    state->failure = 0;
    REQUIRE(pool->acquire());
    REQUIRE(pool->stats().acquires_succeeded == 1);
}

TEST_CASE("Pool maintenance: default has no worker and worker retires only idle", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    asterorm::connection_pool<aging_driver> default_pool{aging_driver{state}, config};
    REQUIRE_FALSE(access::worker_running(default_pool));
    config.max_lifetime = config.leak_threshold = 5ms;
    config.maintenance_interval = 1ms;
    auto pool =
        std::make_unique<asterorm::connection_pool<aging_driver>>(aging_driver{state}, config);
    REQUIRE(access::worker_running(*pool));
    auto held = pool->acquire();
    REQUIRE(held);
    REQUIRE(eventually([&] { return !pool->take_diagnostics().empty(); }));
    REQUIRE((*held)->is_open());
    REQUIRE(state->closes == 0);
    pool.reset(); // Stops/joins the worker; the lease still owns shared state.
    REQUIRE((*held)->is_open());
    held->release_to_pool();
    REQUIRE(state->closes == 1);
    config.max_lifetime = config.leak_threshold = 0ms;
    config.idle_timeout = 5ms;
    asterorm::connection_pool<aging_driver> idle_pool{aging_driver{state}, config};
    REQUIRE(idle_pool.acquire());
    REQUIRE(eventually([&] { return idle_pool.stats().total == 0; }));
    REQUIRE(state->closes == 2);
}

TEST_CASE("Pool maintenance: native idle cleanup keeps slot and close does not join",
          "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    std::promise<void> entered;
    std::promise<void> proceed;
    auto started = entered.get_future();
    auto released = proceed.get_future().share();
    state->on_close = [&] {
        entered.set_value();
        released.wait();
    };
    auto config = aging_config();
    config.idle_timeout = 5ms;
    config.maintenance_interval = 1ms;
    auto pool =
        std::make_unique<asterorm::connection_pool<aging_driver>>(aging_driver{state}, config);
    REQUIRE(pool->acquire());
    const bool cleaning = started.wait_for(2s) == std::future_status::ready;
    auto snapshot = pool->stats();
    auto closing = std::async(std::launch::async, [&] { pool->close(); });
    const bool closed = closing.wait_for(1s) == std::future_status::ready;
    auto destroying = std::async(std::launch::async, [&] {
        closing.get();
        pool.reset();
    });
    const bool waiting_for_cleanup = destroying.wait_for(20ms) == std::future_status::timeout;
    proceed.set_value();
    destroying.get();
    REQUIRE(cleaning);
    REQUIRE(snapshot.total == 1);
    REQUIRE(snapshot.retiring == 1);
    REQUIRE(snapshot.idle == 0);
    REQUIRE(closed);
    REQUIRE(waiting_for_cleanup);
    REQUIRE(state->closes == 1);
}

TEST_CASE("Pool metrics: counters saturate", "[pool-aging]") {
    auto counter = std::numeric_limits<std::uint64_t>::max() - 1;
    asterorm::detail::pool_increment(counter);
    REQUIRE(counter == std::numeric_limits<std::uint64_t>::max());
    asterorm::detail::pool_increment(counter);
    REQUIRE(counter == std::numeric_limits<std::uint64_t>::max());
}

TEST_CASE("Pool aging: reaper removes expired entries behind a fresh idle head", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.max_size = 2;
    config.idle_timeout = 10ms;
    auto pool = access::make(aging_driver{state}, config);
    auto older = pool->acquire();
    auto newer = pool->acquire();
    REQUIRE(older);
    REQUIRE(newer);
    older->release_to_pool();
    access::milliseconds = 5;
    newer->release_to_pool();
    access::milliseconds = 10;
    access::tick(*pool);
    REQUIRE(pool->stats().idle == 1);
    REQUIRE(pool->stats().total == 1);
    REQUIRE(state->closes == 1);
    REQUIRE(pool->acquire());
    REQUIRE(state->connects == 2);
}

TEST_CASE("Pool diagnostics: lease move assignment updates both tracking links", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.max_size = 3;
    config.leak_threshold = 10ms;
    auto pool = access::make(aging_driver{state}, config);
    auto first = pool->acquire();
    auto second = pool->acquire();
    auto third = pool->acquire();
    REQUIRE(first);
    REQUIRE(second);
    REQUIRE(third);
    access::milliseconds = 10;
    *second = std::move(*first); // Returns the middle node and retains the first generation.
    REQUIRE(pool->stats().leased == 2);
    REQUIRE(pool->stats().overdue_leases == 2);
    access::tick(*pool);
    auto warnings = pool->take_diagnostics();
    REQUIRE(warnings.size() == 3);
    REQUIRE(warnings[0].slot != warnings[1].slot);
    REQUIRE(warnings[1].slot != warnings[2].slot);
    REQUIRE(warnings[0].generation != warnings[2].generation);
    third->release_to_pool();
    second->release_to_pool();
    REQUIRE(pool->stats().overdue_leases == 0);
    REQUIRE(pool->stats().idle == 3);
    access::tick(*pool);
    REQUIRE(pool->take_diagnostics().empty());
}

TEST_CASE("Pool maintenance: saturated wake deadline stops promptly", "[pool-aging]") {
    auto state = std::make_shared<aging_state>();
    auto config = aging_config();
    config.idle_timeout = std::chrono::milliseconds::max();
    config.maintenance_interval = std::chrono::milliseconds::max();
    auto completion = std::async(std::launch::async, [&] {
        asterorm::connection_pool<aging_driver> pool{aging_driver{state}, config};
        return pool.acquire().has_value();
    });
    REQUIRE(completion.wait_for(2s) == std::future_status::ready);
    REQUIRE(completion.get());
}
