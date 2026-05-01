#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

#include "asterorm/core/session.hpp"

using namespace asterorm;

TEST_CASE("Transactions: BEGIN SQL assembly", "[core][tx]") {
    REQUIRE(detail::build_begin_sql({}) == "BEGIN");
    REQUIRE(detail::build_begin_sql({.isolation = isolation_level::serializable}) ==
            "BEGIN ISOLATION LEVEL SERIALIZABLE");
    REQUIRE(detail::build_begin_sql(
                {.isolation = isolation_level::repeatable_read, .read_only = true}) ==
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    REQUIRE(detail::build_begin_sql({.read_only = true, .deferrable = true}) ==
            "BEGIN READ ONLY DEFERRABLE");
}

TEST_CASE("Transactions: savepoint name quoting", "[core][tx]") {
    using detail::quote_savepoint;
    REQUIRE(quote_savepoint("sp1").value() == R"("sp1")");
    REQUIRE(quote_savepoint("with space").value() == R"("with space")");
    REQUIRE(!quote_savepoint("").has_value());
    REQUIRE(!quote_savepoint(R"(bad"name)").has_value());
    REQUIRE(!quote_savepoint(std::string_view("with\0null", 9)).has_value());
}

struct observer_result {
    int rows() const {
        return 0;
    }
};

struct observer_connection {
    bool open{true};

    bool is_open() const {
        return open;
    }

    void close() {
        open = false;
    }

    asterorm::result<observer_result> execute(std::string_view) {
        return observer_result{};
    }

    asterorm::result<observer_result>
    execute_prepared(std::string_view, const std::vector<std::optional<std::string>>&) {
        return observer_result{};
    }
};

struct observer_driver {
    asterorm::result<observer_connection> connect(const std::string&) const {
        return observer_connection{};
    }
};

TEST_CASE("Session: observer may reenter session APIs", "[core][observer]") {
    pool_config cfg;
    cfg.min_size = 0;
    cfg.max_size = 1;

    connection_pool<observer_driver> pool{observer_driver{}, cfg};
    session db{pool};
    bool observed = false;

    db.set_observer([&](const query_event&) {
        observed = true;
        db.set_observer({});
    });

    auto result = db.with_connection([&](auto& conn) {
        return db.observed_execute(conn, "SELECT 1", std::vector<std::optional<std::string>>{});
    });

    REQUIRE(result.has_value());
    REQUIRE(observed);
}

TEST_CASE("Session: transaction helper commits successful callback", "[core][tx]") {
    pool_config cfg;
    cfg.min_size = 0;
    cfg.max_size = 1;

    connection_pool<observer_driver> pool{observer_driver{}, cfg};
    session db{pool};

    auto result = db.transact([](auto&) -> asterorm::result<int> { return 42; });
    REQUIRE(result.has_value());
    REQUIRE(*result == 42);
}

TEST_CASE("Session: retry_transaction retries serialization failures", "[core][tx]") {
    pool_config cfg;
    cfg.min_size = 0;
    cfg.max_size = 1;

    connection_pool<observer_driver> pool{observer_driver{}, cfg};
    session db{pool};
    int attempts = 0;

    auto result = db.retry_transaction(
        [&](auto&) -> asterorm::result<int> {
            ++attempts;
            if (attempts == 1) {
                db_error err;
                err.kind = db_error_kind::serialization_failure;
                err.sqlstate = "40001";
                err.message = "retry me";
                return std::unexpected(err);
            }
            return 7;
        },
        2);

    REQUIRE(result.has_value());
    REQUIRE(*result == 7);
    REQUIRE(attempts == 2);
}
