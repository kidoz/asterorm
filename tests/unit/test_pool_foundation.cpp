#include "asterorm/pool/connection_pool.hpp"
#include <atomic>
#include <barrier>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <future>
#include <thread>

namespace {
using namespace std::chrono_literals;
struct gate {
    std::mutex mutex;
    std::condition_variable condition;
    bool entered{}, proceed{};
    void block() {
        std::unique_lock lock(mutex);
        entered = true;
        condition.notify_all();
        condition.wait(lock, [&] { return proceed; });
    }
    bool await() {
        std::unique_lock lock(mutex);
        return condition.wait_for(lock, 2s, [&] { return entered; });
    }
    void unblock() {
        std::scoped_lock lock(mutex);
        proceed = true;
        condition.notify_all();
    }
};
struct native_state {
    std::atomic<int> connects, closes;
    std::atomic<bool> block_connect, block_close, fail_connect, throw_connect, throw_cache,
        throw_readiness, reusable{true};
    gate connecting, closing;
};
struct native_connection {
    std::shared_ptr<native_state> state;
    bool open{true};
    explicit native_connection(std::shared_ptr<native_state> observed)
        : state(std::move(observed)) {}
    native_connection(const native_connection&) = delete;
    native_connection& operator=(const native_connection&) = delete;
    native_connection(native_connection&& other) noexcept
        : state(std::move(other.state)), open(std::exchange(other.open, false)) {}
    native_connection& operator=(native_connection&&) = delete;
    ~native_connection() noexcept {
        close();
    }
    [[nodiscard]] bool is_open() const {
        if (state->throw_readiness) {
            throw std::runtime_error("readiness failed");
        }
        return open;
    }
    [[nodiscard]] bool is_pool_reusable() const noexcept {
        return state->reusable;
    }
    void close() noexcept {
        if (open) {
            open = false;
            if (state->block_close) {
                state->closing.block();
            }
            ++state->closes;
        }
    }
    void set_max_prepared_statements([[maybe_unused]] std::size_t maximum) const {
        if (state->throw_cache) {
            throw std::runtime_error("cache setup failed");
        }
    }
};
struct native_driver {
    std::shared_ptr<native_state> state;
    [[nodiscard]] asterorm::result<native_connection>
    connect([[maybe_unused]] const std::string& settings) const {
        ++state->connects;
        if (state->block_connect) {
            state->connecting.block();
        }
        if (state->throw_connect) {
            throw std::runtime_error("connect exception");
        }
        if (state->fail_connect) {
            asterorm::db_error error;
            error.kind = asterorm::db_error_kind::connection_failed;
            error.sqlstate = "08006";
            error.message = "native connect failed";
            return std::unexpected(std::move(error));
        }
        return native_connection{state};
    }
};
using pool_type = asterorm::connection_pool<native_driver>;
asterorm::pool_config config() {
    asterorm::pool_config settings;
    settings.min_size = 0;
    settings.max_size = 1;
    settings.acquire_timeout = 5s;
    return settings;
}
template <typename Predicate> bool eventually(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}
void balanced(const asterorm::pool_stats& stats) {
    REQUIRE(stats.total == stats.idle + stats.in_use);
    REQUIRE(stats.in_use == stats.leased + stats.connecting + stats.checking + stats.retiring);
}
} // namespace

TEST_CASE("Pool foundation: validate existing options before dialing", "[pool-foundation]") {
    auto observed = std::make_shared<native_state>();
    auto settings = config();
    const char* option{};
    SECTION("zero maximum") {
        settings.max_size = 0;
        option = "max_size";
    }
    SECTION("warmup exceeds capacity") {
        settings.min_size = 2;
        option = "min_size";
    }
    SECTION("negative capacity timeout") {
        settings.acquire_timeout = GENERATE(-1ms, std::chrono::milliseconds::min());
        option = "acquire_timeout";
    }
    pool_type pool{native_driver{observed}, settings};
    REQUIRE(pool.warmup_error());
    REQUIRE(pool.warmup_error()->kind == asterorm::db_error_kind::connection_failed);
    REQUIRE(pool.warmup_error()->message.contains(option));
    REQUIRE_FALSE(pool.acquire());
    REQUIRE(observed->connects == 0);
    balanced(pool.stats());
    pool.close();
    REQUIRE(pool.acquire().error().kind == asterorm::db_error_kind::pool_closed);
    REQUIRE(pool.warmup_error());
}

TEST_CASE("Pool foundation: zero and huge timeouts preserve immediate capacity",
          "[pool-foundation]") {
    auto observed = std::make_shared<native_state>();
    auto settings = config();
    settings.acquire_timeout = GENERATE(0ms, std::chrono::milliseconds::max());
    pool_type pool{native_driver{observed}, settings};
    auto lease = pool.acquire();
    REQUIRE(lease);
    balanced(pool.stats());
    if (settings.acquire_timeout == 0ms) {
        REQUIRE(pool.acquire().error().kind == asterorm::db_error_kind::timeout);
        REQUIRE(pool.stats().waiters == 0);
    } else {
        auto waiting = std::async(std::launch::async, [&] { return pool.acquire(); });
        const bool queued = eventually([&] { return pool.stats().waiters == 1; });
        pool.close();
        auto rejected = waiting.get();
        REQUIRE(queued);
        REQUIRE_FALSE(rejected);
        REQUIRE(rejected.error().kind == asterorm::db_error_kind::pool_closed);
    }
    lease->release_to_pool();
    balanced(pool.stats());
}

TEST_CASE("Pool foundation: close rejects waiters without draining a held lease",
          "[pool-foundation]") {
    auto observed = std::make_shared<native_state>();
    pool_type pool{native_driver{observed}, config()};
    auto held = pool.acquire();
    REQUIRE(held);
    std::vector<std::future<asterorm::result<pool_type::lease_type>>> waiters;
    waiters.reserve(4);
    for (int index = 0; index < 4; ++index) {
        waiters.push_back(std::async(std::launch::async, [&] { return pool.acquire(); }));
    }
    const bool queued = eventually([&] { return pool.stats().waiters == 4; });
    auto closing = std::async(std::launch::async, [&] { pool.close(); });
    const bool returned = closing.wait_for(1s) == std::future_status::ready;
    const auto closed = pool.stats();
    const auto closes_before_return = observed->closes.load();
    held->release_to_pool();
    closing.get();
    REQUIRE(queued);
    REQUIRE(returned);
    REQUIRE(closed.closed);
    REQUIRE(closed.leased == 1);
    REQUIRE(closed.leaked_at_close == 1);
    REQUIRE(closes_before_return == 0);
    for (auto& waiter : waiters) {
        auto rejected = waiter.get();
        REQUIRE_FALSE(rejected);
        REQUIRE(rejected.error().kind == asterorm::db_error_kind::pool_closed);
    }
    REQUIRE(pool.stats().waiters == 0);
    REQUIRE(pool.stats().total == 0);
    REQUIRE(observed->closes == 1);
    pool.close();
    REQUIRE(pool.stats().leaked_at_close == 1);
    balanced(pool.stats());
}

TEST_CASE("Pool foundation: native cleanup never holds the pool mutex", "[pool-foundation]") {
    auto observed = std::make_shared<native_state>();
    auto settings = config();
    settings.min_size = 1;
    pool_type pool{native_driver{observed}, settings};
    observed->block_close = true;
    auto closing = std::async(std::launch::async, [&] { pool.close(); });
    const bool entered = observed->closing.await();
    auto snapshot = std::async(std::launch::async, [&] { return pool.stats(); });
    const bool unlocked = snapshot.wait_for(1s) == std::future_status::ready;
    observed->closing.unblock();
    const auto stats = snapshot.get();
    closing.get();
    REQUIRE(entered);
    REQUIRE(unlocked);
    REQUIRE(stats.closed);
    REQUIRE(stats.retiring == 1);
    balanced(stats);
    REQUIRE(pool.stats().total == 0);
}

TEST_CASE("Pool foundation: retiring native work keeps its capacity slot", "[pool-foundation]") {
    auto observed = std::make_shared<native_state>();
    pool_type pool{native_driver{observed}, config()};
    auto held = pool.acquire();
    REQUIRE(held);
    observed->reusable = false;
    observed->block_close = true;
    auto returning = std::async(std::launch::async, [&] { held->release_to_pool(); });
    const bool entered = observed->closing.await();
    const auto retiring = pool.stats();
    auto waiting = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool queued = eventually([&] { return pool.stats().waiters == 1; });
    const auto connects_while_closing = observed->connects.load();
    observed->reusable = true;
    observed->closing.unblock();
    returning.get();
    auto replacement = waiting.get();
    REQUIRE(entered);
    REQUIRE(queued);
    REQUIRE(retiring.retiring == 1);
    balanced(retiring);
    REQUIRE(connects_while_closing == 1);
    REQUIRE(replacement);
    REQUIRE(observed->connects == 2);
    balanced(pool.stats());
}

TEST_CASE("Pool foundation: late connections preserve failure precedence", "[pool-foundation]") {
    const int outcome = GENERATE(0, 1, 2);
    auto observed = std::make_shared<native_state>();
    observed->block_connect = true;
    observed->fail_connect = outcome == 1;
    observed->throw_connect = outcome == 2;
    pool_type pool{native_driver{observed}, config()};
    auto connecting = std::async(std::launch::async, [&] { return pool.acquire(); });
    const bool entered = observed->connecting.await();
    pool.close();
    const auto reserved = pool.stats();
    observed->connecting.unblock();
    if (outcome == 2) {
        REQUIRE_THROWS_WITH(connecting.get(), "connect exception");
    } else {
        auto acquired = connecting.get();
        REQUIRE_FALSE(acquired);
        REQUIRE(acquired.error().kind == (outcome == 1 ? asterorm::db_error_kind::connection_failed
                                                       : asterorm::db_error_kind::pool_closed));
        REQUIRE(acquired.error().sqlstate == (outcome == 1 ? "08006" : ""));
    }
    REQUIRE(entered);
    REQUIRE(reserved.connecting == 1);
    REQUIRE(reserved.leaked_at_close == 1);
    REQUIRE(pool.stats().total == 0);
    balanced(pool.stats());
}

TEST_CASE("Pool foundation: exceptions release ownership without a throwing return",
          "[pool-foundation]") {
    auto observed = std::make_shared<native_state>();
    pool_type pool{native_driver{observed}, config()};
    observed->throw_cache = true;
    REQUIRE_THROWS_WITH(pool.acquire(), "cache setup failed");
    REQUIRE(pool.stats().total == 0);
    REQUIRE(observed->closes == 1);
    observed->throw_cache = false;
    auto lease = pool.acquire();
    REQUIRE(lease);
    observed->throw_readiness = true;
    lease->release_to_pool();
    lease->release_to_pool();
    REQUIRE(pool.stats().total == 0);
    REQUIRE(observed->closes == 2);
    balanced(pool.stats());
}

TEST_CASE("Pool foundation: move assignment does not require assignable connections",
          "[pool-foundation]") {
    STATIC_REQUIRE(std::is_nothrow_move_assignable_v<pool_type::lease_type>);
    STATIC_REQUIRE(noexcept(std::declval<pool_type::lease_type&>().release_to_pool()));
    STATIC_REQUIRE(noexcept(std::declval<pool_type&>().close()));
    STATIC_REQUIRE(static_cast<int>(asterorm::db_error_kind::unknown) == 8);
    STATIC_REQUIRE(static_cast<int>(asterorm::db_error_kind::pool_closed) == 9);
    auto observed = std::make_shared<native_state>();
    auto settings = config();
    settings.max_size = 2;
    pool_type pool{native_driver{observed}, settings};
    auto first = pool.acquire();
    auto second = pool.acquire();
    REQUIRE(first);
    REQUIRE(second);
    *first = std::move(*second);
    second->release_to_pool();
    REQUIRE(pool.stats().leased == 1);
    REQUIRE(pool.stats().idle == 1);
    first->release_to_pool();
    REQUIRE(pool.stats().idle == 2);
    balanced(pool.stats());
}

TEST_CASE("Pool foundation: an idle readiness exception retires its reserved entry",
          "[pool-foundation]") {
    auto observed = std::make_shared<native_state>();
    auto settings = config();
    settings.min_size = 1;
    pool_type pool{native_driver{observed}, settings};
    observed->throw_readiness = true;
    REQUIRE_THROWS_WITH(pool.acquire(), "readiness failed");
    REQUIRE(pool.stats().total == 0);
    REQUIRE(observed->closes == 1);
    balanced(pool.stats());
    observed->throw_readiness = false;
    REQUIRE(pool.acquire());
    balanced(pool.stats());
}

TEST_CASE("Pool foundation: a lease can race return against facade destruction",
          "[pool-foundation]") {
    for (int iteration = 0; iteration < 64; ++iteration) {
        auto observed = std::make_shared<native_state>();
        auto pool = std::make_unique<pool_type>(native_driver{observed}, config());
        auto acquired = pool->acquire();
        REQUIRE(acquired);
        std::barrier start{2};
        std::thread returning([&, lease = std::move(*acquired)] mutable {
            start.arrive_and_wait();
            lease.release_to_pool();
        });
        start.arrive_and_wait();
        pool.reset();
        returning.join();
        REQUIRE(observed->closes == 1);
    }
    auto observed = std::make_shared<native_state>();
    auto pool = std::make_unique<pool_type>(native_driver{observed}, config());
    auto retained = pool->acquire();
    REQUIRE(retained);
    pool.reset();
    REQUIRE((*retained)->is_open());
    retained->release_to_pool();
    REQUIRE(observed->closes == 1);
}
