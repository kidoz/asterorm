#include "asterorm/core/migration.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

namespace {
struct policy_state {
    int connects{}, capacity{}, profiles{}, locks{}, executes{};
};
struct policy_result {
    [[nodiscard]] std::size_t rows() const {
        return 0;
    }
};
struct policy_connection {
    static constexpr bool supports_transaction_pool_profile = true;
    policy_state* state;
    [[nodiscard]] bool is_open() const {
        return true;
    }
    void close() noexcept {}
    void set_max_prepared_statements(std::size_t maximum) const {
        state->capacity = static_cast<int>(maximum);
    }
    void set_transaction_pool_profile(bool enabled) const noexcept {
        state->profiles += enabled;
    }
    [[nodiscard]] asterorm::result<void> advisory_lock([[maybe_unused]] std::int64_t key) const {
        ++state->locks;
        return {};
    }
    [[nodiscard]] asterorm::result<void> advisory_unlock([[maybe_unused]] std::int64_t key) const {
        ++state->locks;
        return {};
    }
    [[nodiscard]] asterorm::result<policy_result>
    execute([[maybe_unused]] std::string_view sql) const {
        ++state->executes;
        return policy_result{};
    }
    [[nodiscard]] asterorm::result<policy_result>
    execute_prepared([[maybe_unused]] std::string_view sql,
                     [[maybe_unused]] const asterorm::query_parameters& parameters) const {
        ++state->executes;
        return policy_result{};
    }
};
struct policy_driver {
    policy_state* state;
    [[nodiscard]] asterorm::result<policy_connection>
    connect([[maybe_unused]] const std::string& settings) const {
        ++state->connects;
        return policy_connection{state};
    }
};
asterorm::pool_config policy_config() {
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    return config;
}
} // namespace
TEST_CASE("Pool policy: invalid combinations reject before warm or lazy connect", "[pool-policy]") {
    policy_state state;
    auto config = policy_config();
    config.min_size = GENERATE(0, 1);
    switch (GENERATE(0, 1, 2, 3)) {
    case 0:
        // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) -- deliberate invalid
        // config.
        config.statement_caching = static_cast<asterorm::statement_cache_policy>(99);
        break;
    case 1:
        // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) -- deliberate invalid
        // config.
        config.profile = static_cast<asterorm::pool_profile>(99);
        break;
    case 2:
        config.profile = asterorm::pool_profile::transaction_pool;
        break;
    default:
        config.profile = asterorm::pool_profile::transaction_pool;
        config.statement_caching = asterorm::statement_cache_policy::off;
        config.statement_timeout = std::chrono::milliseconds{0};
    }
    asterorm::connection_pool<policy_driver> pool{policy_driver{&state}, config};
    REQUIRE(pool.warmup_error());
    REQUIRE(pool.warmup_error()->kind == asterorm::db_error_kind::connection_failed);
    REQUIRE_FALSE(pool.acquire());
    REQUIRE(state.connects == 0);
}
TEST_CASE("Pool policy: explicit off and zero capacity preserve effective cache semantics",
          "[pool-policy]") {
    policy_state state;
    auto config = policy_config();
    config.min_size = GENERATE(0, 1);
    const bool off = GENERATE(false, true);
    config.statement_caching = off ? asterorm::statement_cache_policy::off
                                   : asterorm::statement_cache_policy::per_connection;
    config.prepared_statement_cache_size = GENERATE(0, 4);
    const bool profile = GENERATE(false, true);
    if (profile) {
        config.profile = asterorm::pool_profile::transaction_pool;
        config.statement_caching = asterorm::statement_cache_policy::off;
    }
    asterorm::connection_pool<policy_driver> pool{policy_driver{&state}, config};
    REQUIRE_FALSE(pool.warmup_error());
    REQUIRE(pool.acquire());
    REQUIRE(state.capacity == (config.statement_caching == asterorm::statement_cache_policy::off
                                   ? 0
                                   : static_cast<int>(config.prepared_statement_cache_size)));
    REQUIRE(state.profiles == (profile ? 1 : 0));
    REQUIRE(pool.allows_session_advisory_locks() == !profile);
}
TEST_CASE("Pool policy: advisory admission blocks before acquisition or callback",
          "[pool-policy]") {
    policy_state state;
    auto config = policy_config();
    config.profile = asterorm::pool_profile::transaction_pool;
    config.statement_caching = asterorm::statement_cache_policy::off;
    asterorm::connection_pool<policy_driver> pool{policy_driver{&state}, config};
    asterorm::session db{pool};
    int callbacks{};
    auto denied = db.with_advisory_lock(1, [&](auto&) -> asterorm::result<int> {
        ++callbacks;
        return 7;
    });
    REQUIRE_FALSE(denied);
    REQUIRE(denied.error().message.contains("transaction_pool"));
    REQUIRE(callbacks == 0);
    REQUIRE(state.connects == 0);
    REQUIRE(state.locks == 0);
    REQUIRE(state.executes == 0);
}
TEST_CASE("Pool policy: unsupported partial adapters reject explicit policy before dialing",
          "[pool-policy]") {
    struct connection {
        [[nodiscard]] bool is_open() const {
            return true;
        }
    };
    struct driver {
        int* connects;
        [[nodiscard]] asterorm::result<connection>
        connect([[maybe_unused]] const std::string& settings) const {
            ++*connects;
            return connection{};
        }
    };
    int connects{};
    auto config = policy_config();
    config.min_size = GENERATE(0, 1);
    if (GENERATE(false, true)) {
        config.statement_caching = asterorm::statement_cache_policy::off;
    } else {
        config.profile = asterorm::pool_profile::transaction_pool;
        config.prepared_statement_cache_size = 0;
    }
    asterorm::connection_pool<driver> pool{driver{&connects}, config};
    REQUIRE(pool.warmup_error());
    REQUIRE(pool.warmup_error()->kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(pool.acquire());
    REQUIRE(connects == 0);
}
