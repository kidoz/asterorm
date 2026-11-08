#include <catch2/catch_test_macros.hpp>
#include <chrono>

#include "asterorm/core/statement_timeout.hpp"
#include "asterorm/postgres/connection.hpp"

TEST_CASE("PostgreSQL timeout adapter rejects invalid state without a server",
          "[pg-timeout-unit]") {
    using std::chrono::milliseconds;
    STATIC_REQUIRE(asterorm::detail::has_statement_timeout<asterorm::pg::connection>);
    STATIC_REQUIRE(asterorm::pg::connection::sql_dialect == asterorm::sql::dialect::postgres);
    asterorm::pg::connection connection{nullptr};
    REQUIRE_FALSE(connection.is_open());
    REQUIRE_FALSE(connection.is_transaction_idle());
    auto native = connection.execute_params("SELECT 1", {});
    REQUIRE_FALSE(native.has_value());
    auto snapshot = connection.statement_timeout();
    REQUIRE_FALSE(snapshot.has_value());
    // Control helpers preserve the native missing-result diagnostic.
    REQUIRE(snapshot.error().kind == native.error().kind);
    REQUIRE(snapshot.error().sqlstate == native.error().sqlstate);
    REQUIRE(snapshot.error().message == native.error().message);

    for (auto count : {-1LL, 2147483648LL}) {
        auto session = connection.set_statement_timeout(milliseconds{count});
        auto local = connection.set_local_statement_timeout(milliseconds{count});
        REQUIRE_FALSE(session.has_value());
        REQUIRE_FALSE(local.has_value());
        REQUIRE(session.error().kind == asterorm::db_error_kind::query_failed);
        REQUIRE(local.error().kind == asterorm::db_error_kind::query_failed);
        REQUIRE(session.error().message.contains("between"));
        REQUIRE(local.error().message.contains("between"));
    }
    for (auto count : {0LL, 100LL, 2147483647LL}) {
        auto session = connection.set_statement_timeout(milliseconds{count});
        auto local = connection.set_local_statement_timeout(milliseconds{count});
        REQUIRE_FALSE(session.has_value());
        REQUIRE_FALSE(local.has_value());
        REQUIRE(session.error().message.contains("idle connection"));
        REQUIRE(local.error().message.contains("healthy active transaction"));
    }
    connection.close();
    REQUIRE_FALSE(connection.is_transaction_idle());
}

TEST_CASE("PostgreSQL advisory adapter reports unavailable connections without a server",
          "[pg-advisory-unit]") {
    asterorm::pg::connection connection{nullptr};
    auto native = connection.execute_params("SELECT 1", {});
    REQUIRE_FALSE(native.has_value());
    for (auto key : {0LL, -1LL, 9223372036854775807LL, -9223372036854775807LL - 1}) {
        auto acquired = connection.advisory_lock(key);
        auto released = connection.advisory_unlock(key);
        REQUIRE_FALSE(acquired.has_value());
        REQUIRE_FALSE(released.has_value());
        REQUIRE(acquired.error().kind == native.error().kind);
        REQUIRE(released.error().kind == native.error().kind);
        REQUIRE(acquired.error().sqlstate == native.error().sqlstate);
        REQUIRE(released.error().sqlstate == native.error().sqlstate);
        REQUIRE(acquired.error().message == native.error().message);
        REQUIRE(released.error().message == native.error().message);
    }
}

TEST_CASE("PostgreSQL checked results own native handles and preserve move state",
          "[pg][row-count]") {
    auto result = asterorm::pg::result::from_native(PQmakeEmptyPGresult(nullptr, PGRES_COMMAND_OK));
    REQUIRE(result.has_value());
    STATIC_REQUIRE(std::is_same_v<decltype(result->affected_rows()), std::uint64_t>);
    REQUIRE(result->affected_rows() == 0);
    auto moved = std::move(*result);
    REQUIRE(result->affected_rows() == 0);
    REQUIRE(moved.affected_rows() == 0);
    asterorm::pg::result assigned{nullptr};
    assigned = std::move(moved);
    REQUIRE(assigned.affected_rows() == 0);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- moved-from results are explicitly empty.
    REQUIRE(moved.rows() == 0);
    REQUIRE_FALSE(asterorm::pg::result::from_native(nullptr).has_value());
    REQUIRE_FALSE(asterorm::pg::result::from_native(PQmakeEmptyPGresult(nullptr, PGRES_FATAL_ERROR))
                      .has_value());
}

TEST_CASE("PostgreSQL transaction profile survives moves without a server", "[pool-policy]") {
    asterorm::pg::connection original{nullptr};
    original.set_transaction_pool_profile(true);
    auto moved = std::move(original);
    asterorm::pg::connection assigned{nullptr};
    assigned = std::move(moved);
    for (auto outcome : {
             assigned.advisory_lock(1),
             assigned.advisory_unlock(1),
             assigned.set_statement_timeout(std::chrono::milliseconds{0}),
         }) {
        REQUIRE_FALSE(outcome);
        REQUIRE(outcome.error().kind == asterorm::db_error_kind::query_failed);
        REQUIRE(outcome.error().message.contains("transaction_pool"));
    }
    assigned.set_transaction_pool_profile(false);
    auto ordinary = assigned.execute_params("SELECT 1", {});
    REQUIRE_FALSE(ordinary);
    REQUIRE(assigned.advisory_lock(1).error().message == ordinary.error().message);
}
