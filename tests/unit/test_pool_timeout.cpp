#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "asterorm/pool/connection_pool.hpp"

namespace {
using std::chrono::milliseconds;
struct timeout_state {
    std::atomic<int> connects{0};
    std::atomic<int> closes{0};
    std::atomic<int> sets{0};
    std::atomic<int> idle_checks{0};
    int fail_call{};
    int throw_call{};
    int block_call{};
    std::mutex mutex;
    std::condition_variable condition;
    bool entered{};
    bool proceed{};
};
struct timeout_connection {
    std::shared_ptr<timeout_state> state;
    int number{};
    bool idle{true};
    milliseconds value{17};
    timeout_connection(std::shared_ptr<timeout_state> shared, int connection_number)
        : state(std::move(shared)), number(connection_number) {}
    timeout_connection(const timeout_connection&) = delete;
    timeout_connection& operator=(const timeout_connection&) = delete;
    timeout_connection(timeout_connection&&) noexcept = default;
    timeout_connection& operator=(timeout_connection&&) = delete;
    ~timeout_connection() {
        close();
    }
    [[nodiscard]] bool is_open() const {
        return state != nullptr;
    }
    void close() noexcept {
        if (state) {
            ++state->closes;
            state.reset();
        }
    }
    [[nodiscard]] bool is_transaction_idle() const noexcept {
        ++state->idle_checks;
        return idle;
    }
    asterorm::result<milliseconds> statement_timeout() {
        return value;
    }
    asterorm::result<void> set_local_statement_timeout(milliseconds duration) {
        value = duration;
        return {};
    }
    asterorm::result<void> set_statement_timeout(milliseconds duration) {
        value = duration; // A failing response can follow a successful server mutation.
        const int call = ++state->sets;
        if (call == state->block_call) {
            std::unique_lock lock(state->mutex);
            state->entered = true;
            state->condition.notify_all();
            state->condition.wait(lock, [&] { return state->proceed; });
        }
        if (call == state->throw_call) {
            throw std::runtime_error("setup threw");
        }
        if (call == state->fail_call) {
            asterorm::db_error error;
            error.kind = asterorm::db_error_kind::query_failed;
            error.sqlstate = "XX000";
            error.message = "setup failed";
            return std::unexpected(std::move(error));
        }
        return {};
    }
};
struct timeout_driver {
    std::shared_ptr<timeout_state> state;
    [[nodiscard]] asterorm::result<timeout_connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        return timeout_connection{state, ++state->connects};
    }
};
asterorm::pool_config timeout_config(std::size_t minimum = 0) {
    asterorm::pool_config config;
    config.min_size = minimum;
    config.max_size = 2;
    config.acquire_timeout = milliseconds{5000};
    return config;
}
bool await_setup(timeout_state& state) {
    std::unique_lock lock(state.mutex);
    return state.condition.wait_for(lock, std::chrono::seconds{2}, [&] { return state.entered; });
}
void release_setup(timeout_state& state) {
    {
        std::scoped_lock lock(state.mutex);
        state.proceed = true;
    }
    state.condition.notify_all();
}
} // namespace

TEST_CASE("Pool timeout: absent configuration never probes or sets timeout", "[pool-timeout]") {
    auto state = std::make_shared<timeout_state>();
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, timeout_config(1)};
    auto first = pool.acquire();
    auto second = pool.acquire();
    REQUIRE(first.has_value());
    REQUIRE(second.has_value());
    (*first)->idle = false; // Absent timeout configuration preserves existing pool behavior.
    first->release_to_pool();
    auto reused = pool.acquire();
    REQUIRE(reused.has_value());
    REQUIRE_FALSE((*reused)->idle);
    REQUIRE(state->sets == 0);
    REQUIRE(state->idle_checks == 0);
}

TEST_CASE("Pool timeout: warmup, new connections and reuse apply the baseline", "[pool-timeout]") {
    const auto minimum = GENERATE(0, 1);
    const auto count = GENERATE(0LL, 42LL, 2147483647LL);
    auto config = timeout_config(minimum);
    config.statement_timeout = milliseconds{count};
    auto state = std::make_shared<timeout_state>();
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    REQUIRE_FALSE(pool.warmup_error().has_value());
    REQUIRE(state->sets == minimum);
    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    REQUIRE((*lease)->value == milliseconds{count});
    const int number = (*lease)->number;
    (*lease)->value = milliseconds{99};
    const int calls = state->sets;
    lease->release_to_pool();
    REQUIRE(state->sets == calls); // Return is free of configuration SQL.
    auto reused = pool.acquire();
    REQUIRE(reused.has_value());
    REQUIRE((*reused)->number == number);
    REQUIRE((*reused)->value == milliseconds{count});
    REQUIRE(state->sets == calls + 1);
}

TEST_CASE("Pool timeout: invalid and unsupported settings fail before connecting",
          "[pool-timeout]") {
    const auto minimum = GENERATE(0, 1);
    const auto count = GENERATE(-1LL, 2147483648LL);
    auto config = timeout_config(minimum);
    config.statement_timeout = milliseconds{count};
    auto state = std::make_shared<timeout_state>();
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    REQUIRE(pool.warmup_error().has_value());
    REQUIRE(pool.warmup_error()->kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(pool.acquire().has_value());
    REQUIRE(state->connects == 0);
    REQUIRE(pool.stats().total == 0);
}

TEST_CASE("Pool timeout: warmup failure closes every connection and persists", "[pool-timeout]") {
    const bool throws = GENERATE(false, true);
    auto config = timeout_config(2);
    config.statement_timeout = milliseconds{100};
    auto state = std::make_shared<timeout_state>();
    state->fail_call = throws ? 0 : 2;
    state->throw_call = throws ? 2 : 0;
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    REQUIRE(pool.warmup_error().has_value());
    REQUIRE(pool.warmup_error()->message == (throws ? "setup threw" : "setup failed"));
    REQUIRE(state->closes == 2);
    REQUIRE(pool.stats().total == 0);
    REQUIRE_FALSE(pool.acquire().has_value());
    REQUIRE(state->connects == 2);
}

TEST_CASE("Pool timeout: failed new or reused setup discards and permits retry", "[pool-timeout]") {
    const auto minimum = GENERATE(0, 1);
    const bool throws = GENERATE(false, true);
    auto config = timeout_config(minimum);
    config.statement_timeout = milliseconds{100};
    auto state = std::make_shared<timeout_state>();
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    state->fail_call = throws ? 0 : minimum + 1;
    state->throw_call = throws ? minimum + 1 : 0;
    if (throws) {
        REQUIRE_THROWS_AS(pool.acquire(), std::runtime_error);
    } else {
        auto failed = pool.acquire();
        REQUIRE_FALSE(failed.has_value());
        REQUIRE(failed.error().sqlstate == "XX000");
        REQUIRE(failed.error().message == "setup failed");
    }
    REQUIRE(state->closes == 1);
    REQUIRE(pool.stats().total == 0);
    REQUIRE_FALSE(pool.warmup_error().has_value());
    state->fail_call = state->throw_call = 0;
    REQUIRE(pool.acquire().has_value());
    REQUIRE(state->connects == 2);
}

TEST_CASE("Pool timeout: returned active transaction is discarded", "[pool-timeout]") {
    auto config = timeout_config();
    config.statement_timeout = milliseconds{100};
    auto state = std::make_shared<timeout_state>();
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    (*lease)->idle = false;
    lease->release_to_pool();
    REQUIRE_FALSE(pool.acquire().has_value());
    REQUIRE(state->sets == 1);
    REQUIRE(state->closes == 1);
    REQUIRE(pool.stats().total == 0);
    REQUIRE(pool.acquire().has_value());
}

TEST_CASE("Pool timeout: setup runs outside the pool mutex", "[pool-timeout]") {
    auto config = timeout_config(2);
    config.statement_timeout = milliseconds{100};
    auto state = std::make_shared<timeout_state>();
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    auto held = pool.acquire();
    REQUIRE(held.has_value());
    state->block_call = state->sets + 1;
    auto pending = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool entered = await_setup(*state);
    auto returned = std::async(std::launch::async, [&] { held->release_to_pool(); });
    const bool returned_while_blocked =
        returned.wait_for(std::chrono::seconds{2}) == std::future_status::ready;
    release_setup(*state);
    returned.get();
    auto acquired = pending.get();
    REQUIRE(entered);
    REQUIRE(returned_while_blocked);
    REQUIRE(acquired.has_value());
}

TEST_CASE("Pool timeout: close during new or reused setup returns no lease", "[pool-timeout]") {
    const auto minimum = GENERATE(0, 1);
    auto config = timeout_config(minimum);
    config.statement_timeout = milliseconds{100};
    auto state = std::make_shared<timeout_state>();
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    state->block_call = state->sets + 1;
    auto pending = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool entered = await_setup(*state);
    auto closed = std::async(std::launch::async, [&] { pool.close(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (!pool.stats().closed && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    release_setup(*state);
    auto acquired = pending.get();
    closed.get();
    REQUIRE(entered);
    REQUIRE_FALSE(acquired.has_value());
    REQUIRE(acquired.error().message == "Connection pool is closed");
    REQUIRE(pool.stats().total == 0);
    REQUIRE(state->closes == 1);
}

TEST_CASE("Pool timeout: unsupported adapters reject even explicit zero", "[pool-timeout]") {
    struct unsupported_connection {
        [[nodiscard]] bool is_open() const {
            return true;
        }
        // A partial protocol must not opt into pool configuration.
        [[nodiscard]] asterorm::result<void>
        set_statement_timeout([[maybe_unused]] milliseconds duration) const {
            throw std::logic_error("partial timeout protocol must not be called");
        }
    };
    static_assert(!asterorm::detail::has_statement_timeout<unsupported_connection>);
    struct unsupported_driver {
        int* connects;
        [[nodiscard]] asterorm::result<unsupported_connection>
        connect([[maybe_unused]] const std::string& conninfo) const {
            ++*connects;
            return unsupported_connection{};
        }
    };
    const auto minimum = GENERATE(0, 1);
    const auto count = GENERATE(-1, 0, 100);
    auto config = timeout_config(minimum);
    config.statement_timeout = milliseconds{count};
    int connects = 0;
    asterorm::connection_pool<unsupported_driver> pool{unsupported_driver{&connects}, config};
    auto error = pool.warmup_error();
    REQUIRE(error.has_value());
    REQUIRE(error->kind == asterorm::db_error_kind::query_failed);
    REQUIRE(error->message.contains(count < 0 ? "between" : "does not support"));
    REQUIRE_FALSE(pool.acquire().has_value());
    REQUIRE(connects == 0);
    REQUIRE(pool.stats().total == 0);
}

TEST_CASE("Pool timeout: failed setup frees capacity for a waiting borrower", "[pool-timeout]") {
    auto config = timeout_config();
    config.max_size = 1;
    config.statement_timeout = milliseconds{100};
    auto state = std::make_shared<timeout_state>();
    state->block_call = state->fail_call = 1;
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    auto first = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool entered = await_setup(*state);
    auto waiter = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool waited = waiter.wait_for(milliseconds{50}) == std::future_status::timeout;
    release_setup(*state);
    auto failed = first.get();
    const bool notified = waiter.wait_for(std::chrono::seconds{2}) == std::future_status::ready;
    auto acquired = waiter.get();
    REQUIRE(entered);
    REQUIRE(waited);
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(notified);
    REQUIRE(acquired.has_value());
    REQUIRE(state->connects == 2);
    REQUIRE(state->closes == 1);
}

TEST_CASE("Pool timeout: late setup retains accounting after non-draining close",
          "[pool-timeout]") {
    auto config = timeout_config();
    config.max_size = 1;
    config.statement_timeout = milliseconds{100};
    auto state = std::make_shared<timeout_state>();
    state->block_call = 1;
    asterorm::connection_pool<timeout_driver> pool{timeout_driver{state}, config};
    auto pending = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool entered = await_setup(*state);
    pool.close();
    const auto closed = pool.stats();
    release_setup(*state);
    auto acquired = pending.get();
    REQUIRE(entered);
    REQUIRE(closed.total == 1);
    REQUIRE(closed.checking + closed.connecting == 1);
    REQUIRE_FALSE(acquired.has_value());
    REQUIRE(pool.stats().total == 0);
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE(pool.stats().leaked_at_close == 1);
    REQUIRE(state->closes == 1);
}
