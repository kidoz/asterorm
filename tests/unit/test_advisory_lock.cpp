#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "asterorm/session.hpp"

namespace {
struct advisory_state {
    int connects{};
    int closes{};
    int queries{};
    int unlocks{};
    bool acquire_error{};
    bool acquire_throw{};
    bool unlock_error{};
    bool unlock_throw{};
    bool rollback_error{};
    bool begin_error{};
    std::map<std::int64_t, int> held;
    std::vector<int> used_connections;
};

asterorm::db_error advisory_error(std::string message) {
    return {
        .kind = asterorm::db_error_kind::query_failed,
        .sqlstate = "XX000",
        .message = std::move(message),
    };
}

struct advisory_result {
    int connection_id{};
    std::string tag;
    [[nodiscard]] int rows() const {
        return 1;
    }
    [[nodiscard]] int columns() const {
        return 1;
    }
    [[nodiscard]] std::string_view command_tag() const {
        return tag;
    }
    [[nodiscard]] std::optional<std::string> get_string([[maybe_unused]] int row,
                                                        [[maybe_unused]] int column) const {
        return std::to_string(connection_id);
    }
};

struct advisory_connection {
    advisory_state* state;
    int connection_id;
    bool open{true};
    [[nodiscard]] bool is_open() const {
        return open;
    }
    void close() noexcept {
        if (open) {
            ++state->closes;
            state->held.clear();
            open = false;
        }
    }
    asterorm::result<advisory_result> execute(std::string_view statement) {
        if (statement.starts_with("BEGIN") && state->begin_error) {
            return std::unexpected(advisory_error("begin failed"));
        }
        if (statement == "ROLLBACK" && state->rollback_error) {
            return std::unexpected(advisory_error("rollback failed"));
        }
        state->used_connections.push_back(connection_id);
        return advisory_result{.connection_id = connection_id, .tag = std::string{statement}};
    }
    asterorm::result<advisory_result>
    execute_prepared([[maybe_unused]] std::string_view statement,
                     [[maybe_unused]] const asterorm::query_parameters& params) {
        ++state->queries;
        state->used_connections.push_back(connection_id);
        return advisory_result{.connection_id = connection_id};
    }
    [[nodiscard]] asterorm::result<void> advisory_lock(std::int64_t key) const {
        ++state->held[key];
        if (state->acquire_throw) {
            throw std::runtime_error("acquire threw");
        }
        if (state->acquire_error) {
            return std::unexpected(advisory_error("acquire failed"));
        }
        return {};
    }
    [[nodiscard]] asterorm::result<void> advisory_unlock(std::int64_t key) const {
        ++state->unlocks;
        if (state->unlock_throw) {
            throw std::runtime_error("unlock threw");
        }
        if (state->unlock_error) {
            return std::unexpected(advisory_error("unlock failed"));
        }
        if (--state->held[key] == 0) {
            state->held.erase(key);
        }
        return {};
    }
};

struct advisory_driver {
    advisory_state* state;
    [[nodiscard]] asterorm::result<advisory_connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        return advisory_connection{.state = state, .connection_id = ++state->connects};
    }
};

asterorm::pool_config single_connection() {
    asterorm::pool_config config;
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds{50};
    return config;
}
} // namespace

TEST_CASE("Advisory scope: nesting, callbacks and transactions share one lease", "[advisory]") {
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto result = db.with_advisory_lock(-7, [&](auto& locked) -> asterorm::result<int> {
        REQUIRE(pool.stats().in_use == 1);
        REQUIRE(state.held[-7] == 1);
        auto inner = locked.with_advisory_lock(-7, [&](auto& nested) -> asterorm::result<void> {
            REQUIRE(state.held[-7] == 2);
            auto transaction = nested.begin();
            REQUIRE(transaction.has_value());
            auto selected = nested.template native_scalar<int>("SELECT connection_id");
            REQUIRE(selected.has_value());
            REQUIRE(*selected == std::optional<int>{1});
            REQUIRE(transaction->commit().has_value());
            REQUIRE(pool.stats().in_use == 1);
            return {};
        });
        REQUIRE(inner.has_value());
        REQUIRE(state.held[-7] == 1);
        return 42;
    });
    REQUIRE(result.has_value());
    REQUIRE(*result == 42);
    REQUIRE(state.connects == 1);
    REQUIRE(state.unlocks == 2);
    REQUIRE(state.held.empty());
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Advisory scope: an existing transaction retains its lease", "[advisory]") {
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto transaction = db.begin();
    REQUIRE(transaction.has_value());
    REQUIRE(
        db.with_advisory_lock(9, [](auto&) -> asterorm::result<void> { return {}; }).has_value());
    REQUIRE(pool.stats().in_use == 1);
    REQUIRE(state.held.empty());
    REQUIRE(transaction->rollback().has_value());
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Advisory scope: callback failures and exceptions release the lock", "[advisory]") {
    const bool throws = GENERATE(false, true);
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto body = [&](auto&) -> asterorm::result<void> {
        if (throws) {
            throw std::runtime_error("body threw");
        }
        return std::unexpected(advisory_error("body failed"));
    };
    if (throws) {
        REQUIRE_THROWS_WITH(db.with_advisory_lock(3, body), "body threw");
    } else {
        auto result = db.with_advisory_lock(3, body);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message == "body failed");
    }
    REQUIRE(state.held.empty());
    REQUIRE(state.unlocks == 1);
    REQUIRE(state.closes == 0);
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Advisory scope: uncertain acquisition is discarded", "[advisory]") {
    const bool throws = GENERATE(false, true);
    advisory_state state;
    state.acquire_error = !throws;
    state.acquire_throw = throws;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    bool invoked = false;
    auto body = [&](auto&) -> asterorm::result<void> {
        invoked = true;
        return {};
    };
    if (throws) {
        REQUIRE_THROWS_WITH(db.with_advisory_lock(3, body), "acquire threw");
    } else {
        REQUIRE_FALSE(db.with_advisory_lock(3, body).has_value());
    }
    REQUIRE_FALSE(invoked);
    REQUIRE(state.held.empty());
    REQUIRE(state.closes == 1);
    REQUIRE(pool.stats().total == 0);
}

TEST_CASE("Advisory scope: failed nested cleanup poisons the outer scope", "[advisory]") {
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto result = db.with_advisory_lock(1, [&](auto& locked) -> asterorm::result<void> {
        advisory_connection* escaped = nullptr;
        auto captured = locked.with_connection([&](auto& connection) -> asterorm::result<void> {
            escaped = &connection;
            return {};
        });
        REQUIRE(captured.has_value());
        auto inner = locked.with_advisory_lock(2, [&](auto&) -> asterorm::result<void> {
            state.unlock_error = true;
            return std::unexpected(advisory_error("primary failure"));
        });
        REQUIRE_FALSE(inner.has_value());
        REQUIRE(inner.error().message == "primary failure");
        REQUIRE(inner.error().detail.has_value());
        REQUIRE(inner.error().detail->contains("cleanup"));
        REQUIRE_FALSE(locked.begin().has_value());
        REQUIRE_FALSE(locked.template native_scalar<int>("SELECT connection_id").has_value());
        bool callback_invoked = false;
        REQUIRE_THROWS_AS(locked.with_connection([&](auto&) { callback_invoked = true; }),
                          std::logic_error);
        REQUIRE_FALSE(callback_invoked);
        REQUIRE_FALSE(locked.observed_execute(*escaped, "SELECT connection_id", {}).has_value());
        return {}; // Ignoring the inner error must not turn the outer result into success.
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(state.queries == 0);
    REQUIRE(state.closes == 1);
    REQUIRE(state.unlocks == 1);
    REQUIRE(state.held.empty());
    REQUIRE(pool.stats().total == 0);
    state.unlock_error = false;
    REQUIRE(db.native_scalar<int>("SELECT connection_id").has_value());
    REQUIRE(state.connects == 2);
}

TEST_CASE("Advisory scope: cleanup exceptions preserve body exceptions", "[advisory]") {
    const bool throws = GENERATE(false, true);
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    REQUIRE_THROWS_WITH(db.with_advisory_lock(1,
                                              [&](auto&) -> asterorm::result<void> {
                                                  state.unlock_throw = throws;
                                                  state.unlock_error = !throws;
                                                  throw std::runtime_error(
                                                      "original body exception");
                                              }),
                        "original body exception");
    REQUIRE(state.closes == 1);
    REQUIRE(state.held.empty());
    REQUIRE(pool.stats().total == 0);
}

TEST_CASE("Advisory scope: cleanup throws after a successful callback", "[advisory]") {
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    REQUIRE_THROWS_WITH(db.with_advisory_lock(1,
                                              [&](auto&) -> asterorm::result<void> {
                                                  state.unlock_throw = true;
                                                  return {};
                                              }),
                        "unlock threw");
    REQUIRE(state.closes == 1);
    REQUIRE(state.held.empty());
    REQUIRE(pool.stats().total == 0);
    state.unlock_throw = false;
    REQUIRE(
        db.with_advisory_lock(1, [](auto&) -> asterorm::result<void> { return {}; }).has_value());
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Advisory scope: cleanup failure invalidates an existing transaction", "[advisory]") {
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto stale = db.begin();
    REQUIRE(stale.has_value());
    auto result = db.with_advisory_lock(1, [&](auto&) -> asterorm::result<void> {
        state.unlock_error = true;
        return {};
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message == "unlock failed");
    REQUIRE(state.closes == 1);
    REQUIRE_FALSE(stale->commit().has_value());
    REQUIRE(pool.stats().total == 0);
    auto next = db.begin();
    REQUIRE(next.has_value());
    REQUIRE_FALSE(stale->rollback().has_value());
    REQUIRE(next->commit().has_value());
}

TEST_CASE("Advisory scope: a returned transaction guard keeps its lease", "[advisory]") {
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto transaction = db.with_advisory_lock(5, [](auto& locked) { return locked.begin(); });
    REQUIRE(transaction.has_value());
    REQUIRE(state.held.empty());
    REQUIRE(pool.stats().in_use == 1);
    REQUIRE(transaction->commit().has_value());
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Advisory scope: failed BEGIN retains the scope pin for cleanup", "[advisory]") {
    advisory_state state;
    state.begin_error = true;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto result = db.with_advisory_lock(1, [&](auto& locked) -> asterorm::result<void> {
        REQUIRE_FALSE(locked.begin().has_value());
        REQUIRE(state.held[1] == 1);
        REQUIRE(pool.stats().in_use == 1);
        return {};
    });
    REQUIRE(result.has_value());
    REQUIRE(state.held.empty());
    REQUIRE(state.unlocks == 1);
    REQUIRE(state.closes == 0);
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Advisory scope: unsupported adapters reject before connecting", "[advisory]") {
    struct unsupported_connection {
        [[nodiscard]] bool is_open() const {
            return true;
        }
    };
    struct unsupported_driver {
        int* connects;
        [[nodiscard]] asterorm::result<unsupported_connection>
        connect([[maybe_unused]] const std::string& conninfo) const {
            ++*connects;
            return unsupported_connection{};
        }
    };
    int connects = 0;
    auto config = single_connection();
    config.min_size = 0;
    asterorm::connection_pool<unsupported_driver> pool{unsupported_driver{.connects = &connects},
                                                       config};
    asterorm::session db{pool};
    bool called = false;
    auto result = db.with_advisory_lock(1, [&](auto&) -> asterorm::result<void> {
        called = true;
        return {};
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE_FALSE(called);
    REQUIRE(connects == 0);
    REQUIRE(pool.stats().total == 0);
}

TEST_CASE("Advisory scope: rollback failure discards the scope connection", "[advisory]") {
    advisory_state state;
    asterorm::connection_pool<advisory_driver> pool{advisory_driver{.state = &state},
                                                    single_connection()};
    asterorm::session db{pool};
    auto result = db.with_advisory_lock(1, [&](auto& locked) -> asterorm::result<void> {
        auto transaction = locked.begin();
        REQUIRE(transaction.has_value());
        state.rollback_error = true;
        REQUIRE_FALSE(transaction->rollback().has_value());
        REQUIRE_FALSE(locked.template native_scalar<int>("SELECT connection_id").has_value());
        return {};
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(state.closes == 1);
    REQUIRE(state.held.empty());
    REQUIRE(pool.stats().total == 0);
}
