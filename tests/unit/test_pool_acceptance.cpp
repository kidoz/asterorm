#include "asterorm/pool/connection_pool.hpp"
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <future>
#include <numeric>

namespace {
struct combined_state {
    std::atomic<int> closes, pings, capacity, profile;
    std::promise<void> entered, proceed;
    int outcome{};
};
struct combined_connection {
    static constexpr bool supports_transaction_pool_profile = true;
    std::shared_ptr<combined_state> state;
    bool open{true};
    explicit combined_connection(std::shared_ptr<combined_state> observed)
        : state(std::move(observed)) {}
    combined_connection(combined_connection&& other) noexcept
        : state(std::move(other.state)), open(std::exchange(other.open, false)) {}
    ~combined_connection() {
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
        }
    }
    void set_max_prepared_statements(std::size_t maximum) const {
        state->capacity = static_cast<int>(maximum);
    }
    void set_transaction_pool_profile(bool enabled) const noexcept {
        state->profile = enabled;
    }
    [[nodiscard]] asterorm::result<void>
    pool_ping([[maybe_unused]] std::chrono::steady_clock::time_point deadline) const {
        ++state->pings;
        state->entered.set_value();
        state->proceed.get_future().wait();
        if (state->outcome == 3) {
            throw std::runtime_error("combined health exception");
        }
        if (state->outcome == 2) {
            asterorm::db_error error;
            error.kind = asterorm::db_error_kind::timeout;
            return std::unexpected(std::move(error));
        }
        return {};
    }
};
struct combined_driver {
    std::shared_ptr<combined_state> state;
    [[nodiscard]] asterorm::result<combined_connection>
    connect([[maybe_unused]] const std::string& settings) const {
        return combined_connection{state};
    }
};
} // namespace
TEST_CASE("Combined pool: checking retains capacity while aging, profile and close interact",
          "[pool-acceptance]") {
    using namespace std::chrono_literals;
    auto state = std::make_shared<combined_state>();
    state->outcome = GENERATE(0, 1, 2, 3);
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    config.pre_ping = true;
    config.pre_ping_timeout = 5s;
    config.statement_caching = asterorm::statement_cache_policy::off;
    config.profile = asterorm::pool_profile::transaction_pool;
    config.max_lifetime = config.idle_timeout = config.leak_threshold = 10ms;
    config.maintenance_interval = 1ms;
    asterorm::connection_pool<combined_driver> pool{combined_driver{state}, config};
    auto entered = state->entered.get_future();
    auto pending = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool started = entered.wait_for(2s) == std::future_status::ready;
    std::this_thread::sleep_for(25ms);
    const auto during = pool.stats();
    const auto records = pool.take_diagnostics();
    const auto closes_during = state->closes.load();
    if (state->outcome != 0) {
        pool.close();
    }
    state->proceed.set_value();
    if (state->outcome == 3) {
        {
            REQUIRE_THROWS_WITH(pending.get(), "combined health exception");
        }
    } else {
        auto outcome = pending.get();
        REQUIRE_FALSE(outcome);
        REQUIRE(outcome.error().kind ==
                (state->outcome == 0   ? asterorm::db_error_kind::connection_failed
                 : state->outcome == 1 ? asterorm::db_error_kind::pool_closed
                                       : asterorm::db_error_kind::timeout));
        if (state->outcome == 0) {
            REQUIRE(outcome.error().message.contains("expired during checkout"));
        }
    }
    REQUIRE(started);
    REQUIRE(during.total == 1);
    REQUIRE(during.connecting == 1);
    REQUIRE(during.leased == 0);
    REQUIRE(during.overdue_leases == 0);
    REQUIRE(records.empty());
    REQUIRE(closes_during == 0);
    REQUIRE(state->capacity == 0);
    REQUIRE(state->profile == 1);
    REQUIRE(state->pings == 1);
    REQUIRE(state->closes == 1);
    const auto after = pool.stats();
    REQUIRE(after.total == 0);
    REQUIRE(after.retirements == 1);
    REQUIRE(after.acquires_failed == 1);
    REQUIRE(after.connection_failures == (state->outcome == 0 ? 1 : 0));
    REQUIRE(after.health_failures == (state->outcome >= 2 ? 1 : 0));
    REQUIRE(std::accumulate(after.acquisition_duration.begin(), after.acquisition_duration.end(),
                            std::uint64_t{}) == 1);
}
