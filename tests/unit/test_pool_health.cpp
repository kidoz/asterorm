#include "asterorm/pool/connection_pool.hpp"
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <future>

namespace {
using namespace std::chrono_literals;
struct health_state {
    std::atomic<int> connects, closes, pings, order, ping_order, timeout_order;
    int outcome{};
    bool block{};
    std::promise<void> entered, proceed;
};
struct health_connection {
    std::shared_ptr<health_state> state;
    bool open{true};
    explicit health_connection(std::shared_ptr<health_state> observed)
        : state(std::move(observed)) {}
    health_connection(health_connection&& other) noexcept
        : state(std::move(other.state)), open(std::exchange(other.open, false)) {}
    ~health_connection() {
        close();
    }
    [[nodiscard]] bool is_open() const noexcept {
        return open;
    }
    [[nodiscard]] bool is_pool_reusable() const noexcept {
        return open;
    }
    void close() noexcept {
        if (open) {
            open = false;
            ++state->closes;
        }
    }
    [[nodiscard]] asterorm::result<void>
    pool_ping(std::chrono::steady_clock::time_point deadline) const {
        ++state->pings;
        state->ping_order = ++state->order;
        if (state->block) {
            state->entered.set_value();
            state->proceed.get_future().wait();
        }
        if (state->outcome == 2) {
            throw std::runtime_error("ping threw");
        }
        if (state->outcome == 1 || deadline <= std::chrono::steady_clock::now()) {
            asterorm::db_error error;
            error.kind = asterorm::db_error_kind::timeout;
            error.message = "ping failed";
            return std::unexpected(std::move(error));
        }
        return {};
    }
    [[nodiscard]] bool is_transaction_idle() const noexcept {
        return true;
    }
    [[nodiscard]] asterorm::result<std::chrono::milliseconds> statement_timeout() const {
        return 0ms;
    }
    [[nodiscard]] asterorm::result<void>
    set_statement_timeout([[maybe_unused]] std::chrono::milliseconds value) const {
        state->timeout_order = ++state->order;
        return {};
    }
    [[nodiscard]] asterorm::result<void>
    set_local_statement_timeout([[maybe_unused]] std::chrono::milliseconds value) const {
        return {};
    }
};
struct health_driver {
    std::shared_ptr<health_state> state;
    [[nodiscard]] asterorm::result<health_connection>
    connect([[maybe_unused]] const std::string& settings) const {
        ++state->connects;
        return health_connection{state};
    }
};
asterorm::pool_config health_config() {
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    config.pre_ping = true;
    return config;
}
} // namespace

TEST_CASE("Pool health: explicit capability and timeout validation precedes dialing",
          "[pool-health]") {
    auto state = std::make_shared<health_state>();
    auto config = health_config();
    SECTION("negative") {
        config.pre_ping_timeout = -1ms;
    }
    SECTION("disabled still rejects negative") {
        config.pre_ping = false;
        config.pre_ping_timeout = -1ms;
    }
    SECTION("enabled zero") {
        config.pre_ping_timeout = 0ms;
    }
    asterorm::connection_pool<health_driver> pool{health_driver{state}, config};
    REQUIRE(pool.warmup_error());
    REQUIRE(pool.warmup_error()->message.contains("pre_ping_timeout"));
    REQUIRE_FALSE(pool.acquire());
    REQUIRE(state->connects == 0);
}

TEST_CASE("Pool health: partial ping protocol rejects without connecting", "[pool-health]") {
    struct partial {
        [[nodiscard]] bool is_open() const {
            return true;
        }
        void pool_ping([[maybe_unused]] std::chrono::steady_clock::time_point deadline) const {}
    };
    struct driver {
        int* calls;
        [[nodiscard]] asterorm::result<partial>
        connect([[maybe_unused]] const std::string& settings) const {
            ++*calls;
            return partial{};
        }
    };
    STATIC_REQUIRE_FALSE(asterorm::detail::has_pool_ping<partial>);
    int calls{};
    auto config = health_config();
    config.min_size = GENERATE(0, 1);
    asterorm::connection_pool<driver> pool{driver{&calls}, config};
    REQUIRE(pool.warmup_error());
    REQUIRE(pool.warmup_error()->kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(pool.acquire());
    REQUIRE(calls == 0);
}

TEST_CASE("Pool health: default is silent and enabled handoffs ping before baseline",
          "[pool-health]") {
    const bool enabled = GENERATE(false, true);
    const auto minimum = GENERATE(0, 1);
    auto state = std::make_shared<health_state>();
    auto config = health_config();
    config.pre_ping = enabled;
    config.pre_ping_timeout = enabled ? 1000ms : 0ms;
    config.min_size = minimum;
    config.statement_timeout = 123ms;
    asterorm::connection_pool<health_driver> pool{health_driver{state}, config};
    REQUIRE(state->pings == 0);
    auto first = pool.acquire();
    REQUIRE(first);
    REQUIRE(state->pings == (enabled ? 1 : 0));
    REQUIRE(state->ping_order < state->timeout_order);
    first->release_to_pool();
    auto second = pool.acquire();
    REQUIRE(second);
    REQUIRE(state->pings == (enabled ? 2 : 0));
    REQUIRE(state->connects == 1);
}

TEST_CASE("Pool health: failure or exception discards once without reconnect loop",
          "[pool-health]") {
    const int outcome = GENERATE(1, 2);
    auto state = std::make_shared<health_state>();
    state->outcome = outcome;
    asterorm::connection_pool<health_driver> pool{health_driver{state}, health_config()};
    if (outcome == 2) {
        REQUIRE_THROWS_WITH(pool.acquire(), "ping threw");
    } else {
        REQUIRE(pool.acquire().error().kind == asterorm::db_error_kind::timeout);
    }
    REQUIRE(state->pings == 1);
    REQUIRE(state->connects == 1);
    REQUIRE(state->closes == 1);
    REQUIRE(pool.stats().total == 0);
    state->outcome = 0;
    REQUIRE(pool.acquire());
    REQUIRE(state->connects == 2);
}

TEST_CASE("Pool health: checking is outside mutex and close preserves work outcome",
          "[pool-health]") {
    const int outcome = GENERATE(0, 1, 2);
    auto state = std::make_shared<health_state>();
    auto config = health_config();
    config.min_size = 1;
    config.pre_ping_timeout = 10s;
    asterorm::connection_pool<health_driver> pool{health_driver{state}, config};
    state->block = true;
    state->outcome = outcome;
    auto entered = state->entered.get_future();
    auto checking = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool started = entered.wait_for(2s) == std::future_status::ready;
    auto closing = std::async(std::launch::async, [&] {
        pool.close();
        return pool.stats();
    });
    const bool closed = closing.wait_for(1s) == std::future_status::ready;
    state->proceed.set_value();
    const auto stats = closing.get();
    if (outcome == 2) {
        REQUIRE_THROWS_WITH(checking.get(), "ping threw");
    } else {
        auto acquired = checking.get();
        REQUIRE_FALSE(acquired);
        REQUIRE(acquired.error().kind == (outcome ? asterorm::db_error_kind::timeout
                                                  : asterorm::db_error_kind::pool_closed));
    }
    REQUIRE(started);
    REQUIRE(closed);
    REQUIRE(stats.checking == 1);
    REQUIRE(pool.stats().total == 0);
    REQUIRE(state->closes == 1);
}
