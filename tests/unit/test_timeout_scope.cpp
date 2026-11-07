#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "asterorm/session.hpp"

namespace {
using std::chrono::milliseconds;
asterorm::db_error timeout_error(std::string message) {
    return {
        .kind = asterorm::db_error_kind::query_failed,
        .sqlstate = "XX000",
        .message = std::move(message),
    };
}
struct scope_state {
    int connects{};
    int closes{};
    int reads{};
    int sets{};
    int queries{};
    int held{};
    int fail_set{};
    int throw_set{};
    bool fail_read{};
    bool throw_read{};
    bool fail_unlock{};
    bool fail_rollback{};
    bool rollback_on_commit{};
    milliseconds value{500};
    std::vector<milliseconds> values;
    std::vector<std::string> commands;
};
struct scope_result {
    int value{};
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
        return std::to_string(value);
    }
};
struct scope_connection {
    scope_state* state;
    bool open{true};
    bool transaction{};
    [[nodiscard]] bool is_open() const {
        return open;
    }
    [[nodiscard]] bool is_transaction_idle() const noexcept {
        return !transaction;
    }
    void close() noexcept {
        if (open) {
            ++state->closes;
            state->value = milliseconds{500};
            state->held = 0;
            transaction = false;
            open = false;
        }
    }
    [[nodiscard]] asterorm::result<milliseconds> statement_timeout() const {
        ++state->reads;
        if (state->throw_read) {
            throw std::runtime_error("snapshot threw");
        }
        if (state->fail_read) {
            return std::unexpected(timeout_error("snapshot failed"));
        }
        return state->value;
    }
    [[nodiscard]] asterorm::result<void> set_statement_timeout(milliseconds duration) const {
        state->value = duration;
        return {};
    }
    [[nodiscard]] asterorm::result<void> set_local_statement_timeout(milliseconds duration) const {
        ++state->sets;
        state->value = duration;
        state->values.push_back(duration);
        if (state->sets == state->throw_set) {
            throw std::runtime_error("setting threw");
        }
        if (state->sets == state->fail_set) {
            return std::unexpected(timeout_error("setting failed"));
        }
        return {};
    }
    [[nodiscard]] asterorm::result<void> advisory_lock([[maybe_unused]] std::int64_t key) const {
        ++state->held;
        return {};
    }
    [[nodiscard]] asterorm::result<void> advisory_unlock([[maybe_unused]] std::int64_t key) const {
        if (state->fail_unlock) {
            return std::unexpected(timeout_error("unlock failed"));
        }
        --state->held;
        return {};
    }
    asterorm::result<scope_result> execute(std::string_view sql) {
        state->commands.emplace_back(sql);
        if (sql.starts_with("BEGIN")) {
            transaction = true;
        }
        if (sql == "ROLLBACK" && state->fail_rollback) {
            return std::unexpected(timeout_error("rollback failed"));
        }
        if (sql == "COMMIT" || sql == "ROLLBACK") {
            transaction = false;
            state->value = milliseconds{500};
        }
        return scope_result{
            .tag = sql == "COMMIT" && state->rollback_on_commit ? "ROLLBACK" : std::string{sql},
        };
    }
    [[nodiscard]] asterorm::result<scope_result>
    execute_prepared(std::string_view sql,
                     [[maybe_unused]] const asterorm::query_parameters& parameters) const {
        ++state->queries;
        if (sql == "invalidated") {
            auto error = timeout_error("deadlock");
            error.sqlstate = "40P01";
            error.kind = asterorm::db_error_kind::deadlock_detected;
            return std::unexpected(std::move(error));
        }
        return scope_result{.value = static_cast<int>(state->value.count())};
    }
};
struct scope_driver {
    scope_state* state;
    [[nodiscard]] asterorm::result<scope_connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        ++state->connects;
        return scope_connection{.state = state};
    }
};
struct scope_fixture {
    scope_state state;
    asterorm::pool_config config = [] {
        asterorm::pool_config value;
        value.min_size = 0;
        value.max_size = 1;
        value.acquire_timeout = milliseconds{50};
        return value;
    }();
    asterorm::connection_pool<scope_driver> pool{scope_driver{&state}, config};
    asterorm::session<decltype(pool)> db{pool};
};
} // namespace

TEST_CASE("Timeout scope: validation and autocommit admission do not lease", "[timeout-scope]") {
    scope_fixture fixture;
    bool called = false;
    auto callback = [&](auto&) -> asterorm::result<void> {
        called = true;
        return {};
    };
    for (auto count : {-1LL, 2147483648LL, 100LL}) {
        REQUIRE_FALSE(fixture.db.with_statement_timeout(milliseconds{count}, callback).has_value());
    }
    REQUIRE_FALSE(called);
    REQUIRE(fixture.state.connects == 0);
    REQUIRE(fixture.state.reads == 0);
}

TEST_CASE("Timeout scope: nested values restore actual prior values and return move-only results",
          "[timeout-scope]") {
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    fixture.state.value = milliseconds{250};
    int events = 0;
    fixture.db.set_observer([&](const auto&) { ++events; });
    auto result = fixture.db.with_statement_timeout(
        milliseconds{2147483647}, [&](auto& outer) -> asterorm::result<std::unique_ptr<int>> {
            REQUIRE(fixture.state.value == milliseconds{2147483647});
            auto nested = outer.with_statement_timeout(
                milliseconds{0}, [&](auto& inner) -> asterorm::result<void> {
                    REQUIRE(inner.template native_scalar<int>("timeout")->value() == 0);
                    return {};
                });
            REQUIRE(nested.has_value());
            REQUIRE(fixture.state.value == milliseconds{2147483647});
            return std::make_unique<int>(42);
        });
    REQUIRE(result.has_value());
    REQUIRE(**result == 42);
    REQUIRE(fixture.state.value == milliseconds{250});
    REQUIRE(fixture.state.values ==
            std::vector<milliseconds>{milliseconds{2147483647}, milliseconds{0},
                                      milliseconds{2147483647}, milliseconds{250}});
    REQUIRE(events == 1);
    REQUIRE(fixture.pool.stats().in_use == 1);
    REQUIRE(transaction->rollback().has_value());
    REQUIRE(fixture.pool.stats().in_use == 0);
}

TEST_CASE("Timeout scope: callback errors and exceptions restore without ending the transaction",
          "[timeout-scope]") {
    const bool throws = GENERATE(false, true);
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    auto body = [&](auto&) -> asterorm::result<void> {
        if (throws) {
            throw std::runtime_error("callback threw");
        }
        return std::unexpected(timeout_error("callback failed"));
    };
    if (throws) {
        REQUIRE_THROWS_WITH(fixture.db.with_statement_timeout(milliseconds{100}, body),
                            "callback threw");
    } else {
        auto result = fixture.db.with_statement_timeout(milliseconds{100}, body);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message == "callback failed");
    }
    REQUIRE(fixture.state.value == milliseconds{500});
    REQUIRE(fixture.db.has_active_transaction());
    REQUIRE(fixture.state.closes == 0);
    REQUIRE(transaction->commit().has_value());
}

TEST_CASE("Timeout scope: snapshot failure leaves transaction ownership with the caller",
          "[timeout-scope]") {
    const bool throws = GENERATE(false, true);
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    fixture.state.fail_read = !throws;
    fixture.state.throw_read = throws;
    bool called = false;
    auto callback = [&](auto&) -> asterorm::result<void> {
        called = true;
        return {};
    };
    if (throws) {
        REQUIRE_THROWS_WITH(fixture.db.with_statement_timeout(milliseconds{100}, callback),
                            "snapshot threw");
    } else {
        REQUIRE_FALSE(fixture.db.with_statement_timeout(milliseconds{100}, callback).has_value());
    }
    REQUIRE_FALSE(called);
    REQUIRE(fixture.state.sets == 0);
    REQUIRE(fixture.state.closes == 0);
    REQUIRE(fixture.db.has_active_transaction());
    REQUIRE(transaction->rollback().has_value());
}

TEST_CASE("Timeout scope: uncertain application discards before callback", "[timeout-scope]") {
    const bool throws = GENERATE(false, true);
    scope_fixture fixture;
    auto stale = fixture.db.begin();
    REQUIRE(stale.has_value());
    fixture.state.fail_set = throws ? 0 : 1;
    fixture.state.throw_set = throws ? 1 : 0;
    bool called = false;
    auto callback = [&](auto&) -> asterorm::result<void> {
        called = true;
        return {};
    };
    if (throws) {
        REQUIRE_THROWS_WITH(fixture.db.with_statement_timeout(milliseconds{100}, callback),
                            "setting threw");
    } else {
        REQUIRE_FALSE(fixture.db.with_statement_timeout(milliseconds{100}, callback).has_value());
    }
    REQUIRE_FALSE(called);
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.pool.stats().total == 0);
    fixture.state.fail_set = fixture.state.throw_set = 0;
    auto fresh = fixture.db.begin();
    REQUIRE(fresh.has_value());
    REQUIRE_FALSE(stale->rollback().has_value());
    REQUIRE(fresh->commit().has_value());
}

TEST_CASE("Timeout scope: failed nested restoration poisons all frames and preserves diagnostics",
          "[timeout-scope]") {
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    auto outer = fixture.db.with_statement_timeout(
        milliseconds{200}, [&](auto& session) -> asterorm::result<void> {
            auto inner = session.with_statement_timeout(
                milliseconds{100}, [&](auto&) -> asterorm::result<void> {
                    fixture.state.fail_set = fixture.state.sets + 1;
                    auto error = timeout_error("primary failure");
                    error.detail = "original detail";
                    return std::unexpected(std::move(error));
                });
            REQUIRE_FALSE(inner.has_value());
            REQUIRE(inner.error().message == "primary failure");
            REQUIRE(inner.error().sqlstate == "XX000");
            REQUIRE(inner.error().detail->starts_with("original detail"));
            REQUIRE(inner.error().detail->contains("cleanup"));
            REQUIRE_FALSE(session.begin().has_value());
            REQUIRE_FALSE(session.template native_scalar<int>("timeout").has_value());
            return {}; // Ignoring the inner error cannot produce success.
        });
    REQUIRE_FALSE(outer.has_value());
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.state.sets == 3);
    REQUIRE(fixture.pool.stats().total == 0);
    REQUIRE(fixture.db.begin().has_value());
}

TEST_CASE("Timeout scope: secondary cleanup failure preserves callback exception",
          "[timeout-scope]") {
    const bool throws = GENERATE(false, true);
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    REQUIRE_THROWS_WITH(
        fixture.db.with_statement_timeout(milliseconds{100},
                                          [&](auto&) -> asterorm::result<void> {
                                              fixture.state.fail_set = throws ? 0 : 2;
                                              fixture.state.throw_set = throws ? 2 : 0;
                                              throw std::runtime_error("original exception");
                                          }),
        "original exception");
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.pool.stats().total == 0);
}

TEST_CASE("Timeout scope: transaction completion retains pins and blocks further work",
          "[timeout-scope]") {
    const bool rollback = GENERATE(false, true);
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    auto result = fixture.db.with_statement_timeout(
        milliseconds{200}, [&](auto& outer) -> asterorm::result<void> {
            auto nested = outer.with_statement_timeout(
                milliseconds{100}, [&](auto& inner) -> asterorm::result<void> {
                    scope_connection* captured = nullptr;
                    REQUIRE(inner
                                .with_connection([&](auto& connection) -> asterorm::result<void> {
                                    captured = &connection;
                                    return {};
                                })
                                .has_value());
                    REQUIRE(
                        (rollback ? transaction->rollback() : transaction->commit()).has_value());
                    REQUIRE(fixture.pool.stats().in_use == 1);
                    REQUIRE_FALSE(inner.begin().has_value());
                    REQUIRE_FALSE(inner.template native_scalar<int>("timeout").has_value());
                    REQUIRE_FALSE(inner.observed_execute(*captured, "timeout", {}).has_value());
                    REQUIRE_THROWS_AS(inner.with_connection([](auto&) {}), std::logic_error);
                    REQUIRE_FALSE(inner
                                      .with_advisory_lock(1,
                                                          [](auto&) -> asterorm::result<void> {
                                                              FAIL("advisory callback invoked");
                                                              return {};
                                                          })
                                      .has_value());
                    REQUIRE_FALSE(inner
                                      .with_statement_timeout(milliseconds{50},
                                                              [](auto&) -> asterorm::result<void> {
                                                                  FAIL("timeout callback invoked");
                                                                  return {};
                                                              })
                                      .has_value());
                    return {};
                });
            REQUIRE(nested.has_value());
            REQUIRE_FALSE(outer.begin().has_value());
            return {};
        });
    REQUIRE(result.has_value());
    REQUIRE(fixture.state.sets == 2);
    REQUIRE(fixture.state.value == milliseconds{500});
    REQUIRE(fixture.state.held == 0);
    REQUIRE(fixture.pool.stats().in_use == 0);
    REQUIRE(fixture.db.native_scalar<int>("timeout")->value() == 500);
}

TEST_CASE("Timeout scope: savepoint rollback is rejected without poisoning a healthy transaction",
          "[timeout-scope]") {
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    REQUIRE(fixture.db.savepoint("before").has_value());
    auto result = fixture.db.with_statement_timeout(
        milliseconds{100}, [&](auto& session) -> asterorm::result<void> {
            REQUIRE(session.savepoint("inside").has_value());
            REQUIRE(session.release_savepoint("inside").has_value());
            auto commands = fixture.state.commands.size();
            REQUIRE_FALSE(session.rollback_to_savepoint("before").has_value());
            REQUIRE(fixture.state.commands.size() == commands);
            REQUIRE(session.template native_scalar<int>("timeout")->value() == 100);
            return {};
        });
    REQUIRE(result.has_value());
    REQUIRE(fixture.db.rollback_to_savepoint("before").has_value());
    REQUIRE(transaction->commit().has_value());
}

TEST_CASE("Timeout scope: advisory and timeout cleanup failures share poisoning",
          "[timeout-scope]") {
    const bool advisory_outer = GENERATE(false, true);
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    asterorm::result<void> result;
    if (advisory_outer) {
        result = fixture.db.with_advisory_lock(1, [&](auto& session) -> asterorm::result<void> {
            auto inner = session.with_statement_timeout(milliseconds{100},
                                                        [&](auto&) -> asterorm::result<void> {
                                                            fixture.state.fail_set = 2;
                                                            return {};
                                                        });
            REQUIRE_FALSE(inner.has_value());
            REQUIRE_FALSE(session.begin().has_value());
            return {};
        });
    } else {
        result = fixture.db.with_statement_timeout(
            milliseconds{100}, [&](auto& session) -> asterorm::result<void> {
                auto inner = session.with_advisory_lock(1, [&](auto&) -> asterorm::result<void> {
                    fixture.state.fail_unlock = true;
                    return {};
                });
                REQUIRE_FALSE(inner.has_value());
                REQUIRE_FALSE(session.begin().has_value());
                return {};
            });
    }
    REQUIRE_FALSE(result.has_value());
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.state.held == 0);
    REQUIRE(fixture.pool.stats().total == 0);
}

TEST_CASE("Timeout scope: unsupported adapters reject before connection creation",
          "[timeout-scope]") {
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
    asterorm::pool_config config;
    config.min_size = 0;
    asterorm::connection_pool<unsupported_driver> pool{unsupported_driver{&connects}, config};
    asterorm::session db{pool};
    auto result = db.with_statement_timeout(milliseconds{100}, [](auto&) -> asterorm::result<void> {
        FAIL("unsupported callback invoked");
        return {};
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.contains("does not support"));
    REQUIRE(connects == 0);
}

TEST_CASE("Timeout scope: invalidated transactions reject before snapshot", "[timeout-scope]") {
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    REQUIRE_FALSE(fixture.db.native_scalar<int>("invalidated").has_value());
    REQUIRE_FALSE(fixture.db
                      .with_statement_timeout(milliseconds{100},
                                              [](auto&) -> asterorm::result<void> {
                                                  FAIL("invalidated callback invoked");
                                                  return {};
                                              })
                      .has_value());
    REQUIRE(fixture.state.reads == 0);
    REQUIRE(transaction->rollback().has_value());
}

TEST_CASE("Timeout scope: restoration errors and exceptions after successful callback discard",
          "[timeout-scope]") {
    const bool throws = GENERATE(false, true);
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    fixture.state.fail_set = throws ? 0 : 2;
    fixture.state.throw_set = throws ? 2 : 0;
    auto body = [](auto&) -> asterorm::result<void> { return {}; };
    if (throws) {
        REQUIRE_THROWS_WITH(fixture.db.with_statement_timeout(milliseconds{100}, body),
                            "setting threw");
    } else {
        auto result = fixture.db.with_statement_timeout(milliseconds{100}, body);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message == "setting failed");
    }
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.pool.stats().total == 0);
}

TEST_CASE("Timeout scope: failed rollback poisons cleanup", "[timeout-scope]") {
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    auto result =
        fixture.db.with_statement_timeout(milliseconds{100}, [&](auto&) -> asterorm::result<void> {
            fixture.state.fail_rollback = true;
            REQUIRE_FALSE(transaction->rollback().has_value());
            return {};
        });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.state.sets == 1);
    REQUIRE(fixture.pool.stats().total == 0);
}

TEST_CASE("Timeout scope: commit reported as rollback is preserved without replaying settings",
          "[timeout-scope]") {
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    fixture.state.rollback_on_commit = true;
    auto result = fixture.db.with_statement_timeout(milliseconds{100},
                                                    [&](auto&) { return transaction->commit(); });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.contains("rolled back instead"));
    REQUIRE(fixture.state.sets == 1);
    REQUIRE(fixture.state.closes == 0);
    REQUIRE(fixture.pool.stats().in_use == 0);
}

TEST_CASE("Timeout scope: returning a transaction guard retains transaction ownership",
          "[timeout-scope]") {
    scope_fixture fixture;
    auto started = fixture.db.begin();
    REQUIRE(started.has_value());
    auto transaction = std::move(*started);
    auto result = fixture.db.with_statement_timeout(
        milliseconds{100},
        [&](auto&) -> asterorm::result<decltype(transaction)> { return std::move(transaction); });
    REQUIRE(result.has_value());
    REQUIRE(fixture.state.value == milliseconds{500});
    REQUIRE(fixture.pool.stats().in_use == 1);
    REQUIRE(result->commit().has_value());
    REQUIRE(fixture.pool.stats().in_use == 0);
}

TEST_CASE("Timeout scope: existing advisory cleanup can run after transaction completion",
          "[timeout-scope]") {
    scope_fixture fixture;
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    auto result = fixture.db.with_statement_timeout(milliseconds{100}, [&](auto& session) {
        return session.with_advisory_lock(1, [&](auto&) { return transaction->commit(); });
    });
    REQUIRE(result.has_value());
    REQUIRE(fixture.state.held == 0);
    REQUIRE(fixture.state.sets == 1);
    REQUIRE(fixture.pool.stats().in_use == 0);
}
