#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <string>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"

namespace {
template <typename Value> Value advisory_take(asterorm::result<Value> result) {
    INFO((result ? "success" : result.error().message));
    REQUIRE(result.has_value());
    return std::move(*result);
}

std::string advisory_conninfo() {
    const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    return configured ? configured
                      : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                        "password=orm_test sslmode=disable";
}

asterorm::pool_config advisory_config() {
    asterorm::pool_config config;
    config.conninfo = advisory_conninfo();
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds{100};
    return config;
}

struct advisory_fixture {
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, advisory_config()};
    asterorm::session<decltype(pool)> db{pool};
    asterorm::pg::connection peer =
        advisory_take(asterorm::pg::driver{}.connect(advisory_conninfo()));
    std::int64_t key{-std::chrono::steady_clock::now().time_since_epoch().count()};

    advisory_fixture() {
        auto setup = db.with_connection(
            [](auto& connection) { return connection.execute("SET statement_timeout = '3s'"); });
        REQUIRE(setup.has_value());
        advisory_take(peer.execute("SET statement_timeout = '3s'"));
    }
    bool available() {
        auto attempt = advisory_take(peer.execute_params("SELECT pg_try_advisory_lock($1::bigint)",
                                                         {asterorm::encode_parameter(key)}));
        const bool acquired = attempt.get_value(0, 0) == "t";
        if (acquired) {
            auto released = advisory_take(peer.execute_params(
                "SELECT pg_advisory_unlock($1::bigint)", {asterorm::encode_parameter(key)}));
            REQUIRE(released.get_value(0, 0) == "t");
        }
        return acquired;
    }
    void acquire_after_disconnect() {
        // A bounded blocking request also waits for the old backend to process
        // its disconnect; there is no sleep/poll race with server cleanup.
        advisory_take(peer.execute_params("SELECT pg_advisory_lock($1::bigint)",
                                          {asterorm::encode_parameter(key)}));
        auto released = advisory_take(peer.execute_params("SELECT pg_advisory_unlock($1::bigint)",
                                                          {asterorm::encode_parameter(key)}));
        REQUIRE(released.get_value(0, 0) == "t");
    }
};
} // namespace

TEST_CASE("PG Integration: advisory scopes pin and release on all callback exits",
          "[pg][advisory]") {
    const int outcome = GENERATE(0, 1, 2);
    advisory_fixture fixture;
    auto original_pid = advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()"));
    auto body = [&](auto& locked) -> asterorm::result<int> {
        REQUIRE_FALSE(fixture.available());
        REQUIRE(advisory_take(locked.template native_scalar<int>("SELECT pg_backend_pid()")) ==
                original_pid);
        auto nested = locked.with_advisory_lock(fixture.key, [&](auto&) -> asterorm::result<void> {
            REQUIRE_FALSE(fixture.available());
            return {};
        });
        REQUIRE(nested.has_value());
        REQUIRE_FALSE(fixture.available());
        auto transaction = locked.begin();
        REQUIRE(transaction.has_value());
        REQUIRE(advisory_take(locked.template native_scalar<int>("SELECT pg_backend_pid()")) ==
                original_pid);
        REQUIRE(transaction->commit().has_value());
        REQUIRE_FALSE(fixture.available());
        REQUIRE(fixture.pool.stats().in_use == 1);
        if (outcome == 1) {
            asterorm::db_error error;
            error.kind = asterorm::db_error_kind::query_failed;
            error.message = "callback failed";
            return std::unexpected(std::move(error));
        }
        if (outcome == 2) {
            throw std::runtime_error("callback threw");
        }
        return 42;
    };
    if (outcome == 2) {
        REQUIRE_THROWS_WITH(fixture.db.with_advisory_lock(fixture.key, body), "callback threw");
    } else {
        auto result = fixture.db.with_advisory_lock(fixture.key, body);
        if (outcome == 0) {
            REQUIRE(result.has_value());
            REQUIRE(*result == 42);
        } else {
            REQUIRE_FALSE(result.has_value());
            REQUIRE(result.error().message == "callback failed");
        }
    }
    REQUIRE(fixture.available());
    REQUIRE(fixture.pool.stats().in_use == 0);
    REQUIRE(advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()")) ==
            original_pid);
}

TEST_CASE("PG Integration: advisory scopes reuse caller transactions", "[pg][advisory]") {
    const int completion = GENERATE(0, 1, 2);
    advisory_fixture fixture;
    auto original_pid = advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()"));
    auto transaction = fixture.db.begin();
    REQUIRE(transaction.has_value());
    auto result =
        fixture.db.with_advisory_lock(fixture.key, [&](auto& locked) -> asterorm::result<void> {
            REQUIRE_FALSE(fixture.available());
            if (completion == 1) {
                REQUIRE(locked.commit_transaction().has_value());
            } else if (completion == 2) {
                REQUIRE(locked.rollback_transaction().has_value());
            }
            REQUIRE_FALSE(fixture.available());
            REQUIRE(advisory_take(locked.template native_scalar<int>("SELECT pg_backend_pid()")) ==
                    original_pid);
            return {};
        });
    REQUIRE(result.has_value());
    REQUIRE(fixture.available());
    if (completion == 0) {
        REQUIRE(fixture.pool.stats().in_use == 1);
        REQUIRE(transaction->rollback().has_value());
    }
    REQUIRE(fixture.pool.stats().in_use == 0);
    REQUIRE(advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()")) ==
            original_pid);
}

TEST_CASE("PG Integration: aborted transactions cannot leak advisory locks", "[pg][advisory]") {
    const bool throws = GENERATE(false, true);
    advisory_fixture fixture;
    auto original_pid = advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()"));
    auto stale = fixture.db.begin();
    REQUIRE(stale.has_value());
    auto body = [&](auto& locked) -> asterorm::result<void> {
        auto failed = locked.template native_scalar<int>("SELECT 1/0");
        REQUIRE_FALSE(failed.has_value());
        REQUIRE(failed.error().sqlstate == "22012");
        if (throws) {
            throw std::runtime_error("original callback exception");
        }
        return std::unexpected(failed.error());
    };
    if (throws) {
        REQUIRE_THROWS_WITH(fixture.db.with_advisory_lock(fixture.key, body),
                            "original callback exception");
    } else {
        auto result = fixture.db.with_advisory_lock(fixture.key, body);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().sqlstate == "22012");
        REQUIRE(result.error().detail.has_value());
        REQUIRE(result.error().detail->contains("cleanup"));
    }
    REQUIRE(fixture.pool.stats().total == 0);
    REQUIRE_FALSE(stale->commit().has_value());
    fixture.acquire_after_disconnect();
    REQUIRE(advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()")) !=
            original_pid);
    auto fresh = fixture.db.begin();
    REQUIRE(fresh.has_value());
    REQUIRE_FALSE(stale->rollback().has_value());
    REQUIRE(fresh->commit().has_value());
}

TEST_CASE("PG Integration: lost advisory ownership fails and discards", "[pg][advisory]") {
    advisory_fixture fixture;
    auto result =
        fixture.db.with_advisory_lock(fixture.key, [&](auto& locked) -> asterorm::result<void> {
            auto unlocked = locked.template native_scalar<bool>(
                "SELECT pg_advisory_unlock($1::bigint)", fixture.key);
            REQUIRE(unlocked.has_value());
            REQUIRE(*unlocked == std::optional<bool>{true});
            return {};
        });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE(fixture.pool.stats().total == 0);
    fixture.acquire_after_disconnect();
}

TEST_CASE("PG Integration: advisory acquisition cancellation skips the callback",
          "[pg][advisory]") {
    advisory_fixture fixture;
    auto original_pid = advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()"));
    advisory_take(fixture.peer.execute_params("SELECT pg_advisory_lock($1::bigint)",
                                              {asterorm::encode_parameter(fixture.key)}));
    auto timeout = fixture.db.with_connection(
        [](auto& connection) { return connection.execute("SET statement_timeout = '50ms'"); });
    REQUIRE(timeout.has_value());
    bool invoked = false;
    auto result = fixture.db.with_advisory_lock(fixture.key, [&](auto&) -> asterorm::result<void> {
        invoked = true;
        return {};
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().sqlstate == "57014");
    REQUIRE(result.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(invoked);
    REQUIRE(fixture.pool.stats().total == 0);
    advisory_take(fixture.peer.execute_params("SELECT pg_advisory_unlock($1::bigint)",
                                              {asterorm::encode_parameter(fixture.key)}));
    REQUIRE(advisory_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()")) !=
            original_pid);
}
