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
