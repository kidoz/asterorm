#include <asterorm/mysql/driver.hpp>
#include <asterorm/pool/connection_pool.hpp>
#include <asterorm/postgres/driver.hpp>
#include <asterorm/sqlite/driver.hpp>

#include <cstdlib>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <utility>

namespace {
using namespace std::chrono_literals;
template <typename Value> Value checked(asterorm::result<Value> result) {
    if (!result) {
        throw std::runtime_error(result.error().message);
    }
    return std::move(*result);
}
void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}
void observation(const char* name, bool value) {
    std::cout << name << '=' << std::boolalpha << value << '\n';
}
template <typename Config>
constexpr bool has_health = requires(Config config) {
    config.pre_ping;
    config.pre_ping_timeout;
};
template <typename Config>
constexpr bool has_aging = requires(Config config) {
    config.idle_timeout;
    config.max_lifetime;
};
template <typename Config>
constexpr bool has_cache_mode = requires(Config config) { config.statement_caching; };
template <typename Config>
constexpr bool has_leak_detection = requires(Config config) { config.leak_threshold; };
template <typename Stats> constexpr bool has_waiters = requires(Stats stats) { stats.waiters; };

struct fake_connection {
    bool open{true};
    fake_connection() = default;
    fake_connection(const fake_connection&) = delete;
    fake_connection& operator=(const fake_connection&) = delete;
    fake_connection(fake_connection&& other) noexcept : open(std::exchange(other.open, false)) {}
    fake_connection& operator=(fake_connection&& other) noexcept {
        open = std::exchange(other.open, false);
        return *this;
    }
    [[nodiscard]] bool is_open() const noexcept {
        return open;
    }
    void close() noexcept {
        open = false;
    }
};
struct fake_driver {
    [[nodiscard]] asterorm::result<fake_connection>
    connect([[maybe_unused]] const std::string& settings) const {
        return fake_connection{};
    }
};
struct connect_gate {
    std::promise<void> entered;
    std::promise<void> proceed;
};
struct gated_driver {
    std::shared_ptr<connect_gate> gate;
    [[nodiscard]] asterorm::result<fake_connection>
    connect([[maybe_unused]] const std::string& settings) const {
        gate->entered.set_value();
        gate->proceed.get_future().wait();
        return fake_connection{};
    }
};
void generic_inventory() {
    observation("api.pre_ping", has_health<asterorm::pool_config>);
    observation("api.aging", has_aging<asterorm::pool_config>);
    observation("api.cache_mode", has_cache_mode<asterorm::pool_config>);
    observation("api.leak_detection", has_leak_detection<asterorm::pool_config>);
    observation("api.waiter_gauge", has_waiters<asterorm::pool_stats>);
    {
        asterorm::pool_config config;
        config.min_size = 3;
        config.max_size = 1;
        asterorm::connection_pool<fake_driver> pool{fake_driver{}, config};
        observation("bounds.minimum_exceeds_maximum_is_accepted",
                    !pool.warmup_error() && pool.stats().total == 3);
    }
    {
        asterorm::pool_config config;
        config.min_size = 0;
        config.max_size = 0;
        config.acquire_timeout = 0ms;
        asterorm::connection_pool<fake_driver> pool{fake_driver{}, config};
        auto acquired = pool.acquire();
        observation("bounds.zero_maximum_is_not_configuration_error", !pool.warmup_error());
        observation("bounds.zero_maximum_reports_connection_failed",
                    !acquired &&
                        acquired.error().kind == asterorm::db_error_kind::connection_failed);
    }
    {
        auto gate = std::make_shared<connect_gate>();
        auto entered = gate->entered.get_future();
        asterorm::pool_config config;
        config.min_size = 0;
        config.max_size = 1;
        config.acquire_timeout = 10ms;
        asterorm::connection_pool<gated_driver> pool{gated_driver{gate}, config};
        auto acquired = std::async(std::launch::async, [&] { return pool.acquire(); });
        const bool started = entered.wait_for(2s) == std::future_status::ready;
        const bool pending = acquired.wait_for(30ms) == std::future_status::timeout;
        gate->proceed.set_value();
        auto lease = acquired.get();
        require(started, "connect gate did not start");
        observation("timeout.connect_can_outlive_capacity_deadline", pending && lease.has_value());
    }
    {
        asterorm::pool_config config;
        config.min_size = 1;
        config.max_size = 1;
        config.acquire_timeout = 5s;
        asterorm::connection_pool<fake_driver> pool{fake_driver{}, config};
        auto held = checked(pool.acquire());
        std::promise<void> entered;
        auto started = entered.get_future();
        auto waiter = std::async(std::launch::async, [&] {
            entered.set_value();
            return pool.acquire();
        });
        started.wait();
        const bool queued = waiter.wait_for(30ms) == std::future_status::timeout;
        auto closer = std::async(std::launch::async, [&] { pool.close(); });
        const bool awakened = waiter.wait_for(1s) == std::future_status::ready;
        const bool close_pending = closer.wait_for(0ms) == std::future_status::timeout;
        held.release_to_pool();
        auto acquired = waiter.get();
        closer.get();
        observation("close.pending_acquisition_rejected_before_held_lease_returns",
                    queued && awakened && !acquired);
        observation("close.waits_for_held_lease", close_pending);
        observation("close.error_kind_is_connection_failed",
                    !acquired &&
                        acquired.error().kind == asterorm::db_error_kind::connection_failed);
    }
}
template <typename Driver>
void native_inventory(const char* name, Driver driver, const std::string& settings) {
    for (std::size_t cache : {0, 2}) {
        asterorm::pool_config config;
        config.conninfo = settings;
        config.min_size = 0;
        config.max_size = 1;
        config.prepared_statement_cache_size = cache;
        asterorm::connection_pool<Driver> pool{driver, config};
        auto lease = checked(pool.acquire());
        for (int iteration = 0; iteration < 2; ++iteration) {
            auto result = checked(lease->execute_prepared("SELECT $1 AS value", {"41"}));
            require(result.rows() == 1 && result.get_string(0, 0) == "41", "native bound result");
        }
        std::cout << name << ".cache_" << cache << "_entries=" << lease->prepared_statement_count()
                  << '\n';
        require(lease->prepared_statement_count() == (cache ? 1 : 0), "cache policy mismatch");
        checked(lease->execute("BEGIN"));
        lease.release_to_pool();
        auto reused = checked(pool.acquire());
        bool active{};
        if constexpr (requires { reused->is_transaction_idle(); }) {
            active = !reused->is_transaction_idle();
        } else {
            active = reused->in_transaction();
        }
        std::cout << name << ".cache_" << cache
                  << "_raw_transaction_survives_return=" << std::boolalpha << active << '\n';
        checked(reused->execute("ROLLBACK"));
    }
}
void postgres_stale_socket(const std::string& settings) {
    asterorm::pool_config config;
    config.conninfo = settings;
    config.min_size = 0;
    config.max_size = 1;
    asterorm::pg::driver driver;
    asterorm::connection_pool<asterorm::pg::driver> pool{driver, config};
    auto lease = checked(pool.acquire());
    auto identity = checked(lease->execute("SELECT pg_backend_pid()"));
    const auto pid = *identity.get_string(0, 0);
    auto control = checked(driver.connect(settings));
    auto terminated =
        checked(control.execute_params("SELECT pg_terminate_backend($1::integer,1000)", {pid}));
    require(terminated.get_string(0, 0) == "t", "disposable backend was not terminated");
    const bool cached_open = lease->is_open();
    lease.release_to_pool();
    auto reused = pool.acquire();
    const bool acquired = reused.has_value();
    bool query_failed{};
    if (reused) {
        query_failed = !(*reused)->execute("SELECT 1");
    }
    observation("postgres.terminated_socket_still_reports_open", cached_open);
    observation("postgres.terminated_socket_is_leased_without_probe", acquired);
    observation("postgres.first_query_after_termination_fails", query_failed);
}
} // namespace
int main() {
    try {
        generic_inventory();
        const char* postgres = std::getenv("ASTERORM_TEST_CONNINFO");
        const std::string pg_settings = postgres
                                            ? postgres
                                            : "host=127.0.0.1 port=5432 dbname=orm_test "
                                              "user=orm_test password=orm_test sslmode=disable";
        const char* mysql = std::getenv("ASTERORM_MYSQL_TEST_CONNINFO");
        native_inventory("postgres", asterorm::pg::driver{}, pg_settings);
        native_inventory("sqlite", asterorm::sqlite::driver{}, ":memory:");
        native_inventory(
            "mysql", asterorm::mysql::driver{},
            mysql ? mysql
                  : "host=127.0.0.1 port=3307 dbname=orm_test user=orm_test password=orm_test");
        postgres_stale_socket(pg_settings);
    } catch (const std::exception& exception) {
        std::cerr << exception.what() << '\n';
        return 1;
    }
}
