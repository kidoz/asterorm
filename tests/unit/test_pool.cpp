#include <catch2/catch_test_macros.hpp>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#include "asterorm/pool/connection_pool.hpp"

struct mock_connection {
    int id{0};
    bool open{true};
    mock_connection(int i) : id(i) {}
    mock_connection(const mock_connection&) = delete;
    mock_connection& operator=(const mock_connection&) = delete;
    mock_connection(mock_connection&& other) noexcept : id(other.id), open(other.open) {
        other.open = false;
    }
    mock_connection& operator=(mock_connection&& other) noexcept {
        id = other.id;
        open = other.open;
        other.open = false;
        return *this;
    }
    [[nodiscard]] bool is_open() const {
        return open;
    }
    void close() {
        open = false;
    }
};

struct mock_driver {
    mutable int next_id{1};
    [[nodiscard]]
    asterorm::result<mock_connection> connect(const std::string& /*conninfo*/) const {
        return mock_connection{next_id++};
    }
};

struct blocking_driver_state {
    std::mutex mutex;
    std::condition_variable condition;
    bool connect_started{false};
    bool allow_connect{false};
};

struct blocking_driver {
    std::shared_ptr<blocking_driver_state> state;

    [[nodiscard]]
    asterorm::result<mock_connection> connect(const std::string& /*conninfo*/) const {
        std::unique_lock lock(state->mutex);
        state->connect_started = true;
        state->condition.notify_all();
        state->condition.wait(lock, [&] { return state->allow_connect; });
        return mock_connection{1};
    }
};

TEST_CASE("Core: Connection Pool Max Size and Timeout", "[pool]") { // NOLINT
    asterorm::pool_config cfg;
    cfg.min_size = 1;
    cfg.max_size = 2;
    cfg.acquire_timeout = std::chrono::milliseconds(50);

    mock_driver drv;
    asterorm::connection_pool<mock_driver> pool{drv, cfg};

    auto l1 = pool.acquire();
    REQUIRE(l1.has_value());

    auto l2 = pool.acquire();
    REQUIRE(l2.has_value());

    // Pool is maxed out at 2. This should timeout.
    auto l3 = pool.acquire();
    REQUIRE(!l3.has_value());
    REQUIRE(l3.error().message == "Connection pool acquire timeout");

    // Release l1 by dropping it
    {
        auto drop_it = std::move(l1.value());
    } // returns to pool

    // Now l3 should succeed
    l3 = pool.acquire();
    REQUIRE(l3.has_value());
}

TEST_CASE("Core: Connection Pool Stats and Close", "[pool]") {
    asterorm::pool_config cfg;
    cfg.min_size = 1;
    cfg.max_size = 2;

    mock_driver drv;
    asterorm::connection_pool<mock_driver> pool{drv, cfg};

    auto initial = pool.stats();
    REQUIRE(initial.total == 1);
    REQUIRE(initial.idle == 1);
    REQUIRE(initial.in_use == 0);
    REQUIRE_FALSE(initial.closed);

    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    auto leased = pool.stats();
    REQUIRE(leased.total == 1);
    REQUIRE(leased.idle == 0);
    REQUIRE(leased.in_use == 1);

    lease.value().release_to_pool();
    pool.close();
    auto closed = pool.stats();
    REQUIRE(closed.total == 0);
    REQUIRE(closed.idle == 0);
    REQUIRE(closed.in_use == 0);
    REQUIRE(closed.closed);

    auto after_close = pool.acquire();
    REQUIRE_FALSE(after_close.has_value());
}

TEST_CASE("Core: Connection Pool acquire fails if close wins connect race", "[pool]") {
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds(500);

    auto driver_state = std::make_shared<blocking_driver_state>();
    asterorm::connection_pool<blocking_driver> pool{blocking_driver{driver_state}, config};

    std::optional<std::string> acquire_error_message;
    std::thread acquire_thread([&] {
        auto acquire_result = pool.acquire();
        if (!acquire_result) {
            acquire_error_message = acquire_result.error().message;
        }
    });

    {
        std::unique_lock lock(driver_state->mutex);
        REQUIRE(driver_state->condition.wait_for(lock, std::chrono::seconds(1),
                                                 [&] { return driver_state->connect_started; }));
    }

    std::thread close_thread([&] { pool.close(); });
    while (!pool.stats().closed) {
        std::this_thread::yield();
    }

    {
        std::scoped_lock lock(driver_state->mutex);
        driver_state->allow_connect = true;
    }
    driver_state->condition.notify_all();

    acquire_thread.join();
    close_thread.join();

    REQUIRE(acquire_error_message.has_value());
    REQUIRE(*acquire_error_message == "Connection pool is closed");
}
