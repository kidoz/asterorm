#include <catch2/catch_test_macros.hpp>
#include <thread>

#include "asterorm/pool/connection_pool.hpp"

struct mock_connection {
    int id{0};
    bool open{true};
    mock_connection(int i) : id(i) {}
    mock_connection(const mock_connection&) = delete;
    mock_connection& operator=(const mock_connection&) = delete;
    mock_connection(mock_connection&& other) noexcept : id(other.id), open(other.open) { other.open = false; }
    mock_connection& operator=(mock_connection&& other) noexcept {
        id = other.id;
        open = other.open;
        other.open = false;
        return *this;
    }
    [[nodiscard]] bool is_open() const { return open; }
    void close() { open = false; }
};

struct mock_driver {
    mutable int next_id{1};
    asterorm::result<mock_connection> connect(const std::string& /*conninfo*/) const { return mock_connection{next_id++}; }
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
    }  // returns to pool

    // Now l3 should succeed
    l3 = pool.acquire();
    REQUIRE(l3.has_value());
}