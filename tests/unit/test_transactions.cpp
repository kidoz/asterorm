#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <stdexcept>
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
    std::string_view tag{"COMMIT"};

    int rows() const {
        return 0;
    }

    std::string_view command_tag() const {
        return tag;
    }
};

struct observer_connection {
    bool open{true};
    std::vector<std::string>* statements{nullptr};
    std::string_view commit_tag{"COMMIT"};
    std::optional<db_error> query_error;

    bool is_open() const {
        return open;
    }

    void close() {
        open = false;
    }

    asterorm::result<observer_result> execute(std::string_view sql) {
        if (statements)
            statements->push_back(std::string{sql});
        if (sql == "COMMIT")
            return observer_result{.tag = commit_tag};
        if (query_error && sql != "BEGIN" && sql != "ROLLBACK")
            return std::unexpected(*query_error);
        return observer_result{};
    }

    asterorm::result<observer_result> execute_prepared(std::string_view sql,
                                                       const query_parameters&) {
        if (statements)
            statements->push_back(std::string{sql});
        if (query_error)
            return std::unexpected(*query_error);
        return observer_result{};
    }
};

struct observer_driver {
    std::vector<std::string>* statements{nullptr};
    std::string_view commit_tag{"COMMIT"};
    std::optional<db_error> query_error;

    asterorm::result<observer_connection> connect(const std::string&) const {
        observer_connection connection;
        connection.statements = statements;
        connection.commit_tag = commit_tag;
        connection.query_error = query_error;
        return connection;
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

    auto result = db.with_connection(
        [&](auto& conn) { return db.observed_execute(conn, "SELECT 1", query_parameters{}); });

    REQUIRE(result.has_value());
    REQUIRE(observed);
}

TEST_CASE("Session: stale guard cannot finish a newer transaction", "[core][tx]") {
    pool_config cfg;
    cfg.min_size = 0;
    cfg.max_size = 1;
    std::vector<std::string> statements;

    connection_pool<observer_driver> pool{observer_driver{.statements = &statements}, cfg};
    session db{pool};

    auto first = db.begin();
    REQUIRE(first.has_value());
    REQUIRE(db.commit_transaction().has_value());
    auto second = db.begin();
    REQUIRE(second.has_value());

    REQUIRE_FALSE(first->rollback().has_value());
    REQUIRE(second->commit().has_value());
    REQUIRE(statements == std::vector<std::string>{"BEGIN", "COMMIT", "BEGIN", "COMMIT"});
}

TEST_CASE("Session: rollback command tag is not a successful commit", "[core][tx]") {
    pool_config cfg;
    cfg.min_size = 0;
    cfg.max_size = 1;
    std::vector<std::string> statements;

    connection_pool<observer_driver> pool{
        observer_driver{.statements = &statements, .commit_tag = "ROLLBACK"}, cfg};
    session db{pool};
    auto transaction = db.begin();
    REQUIRE(transaction.has_value());

    REQUIRE_FALSE(transaction->commit().has_value());
    auto next = db.begin();
    REQUIRE(next.has_value());
    REQUIRE(next->rollback().has_value());
}

TEST_CASE("Session: invalidated transaction rejects work until rollback", "[core][tx]") {
    pool_config cfg;
    cfg.min_size = 0;
    cfg.max_size = 1;
    std::vector<std::string> statements;
    db_error deadlock;
    deadlock.kind = db_error_kind::deadlock_detected;
    deadlock.sqlstate = "40001";
    deadlock.message = "deadlock victim";

    connection_pool<observer_driver> pool{
        observer_driver{.statements = &statements, .query_error = deadlock}, cfg};
    session db{pool};
    auto transaction = db.begin();
    REQUIRE(transaction.has_value());
    observer_connection* pinned_connection = nullptr;
    auto query = db.with_connection([&](auto& conn) {
        pinned_connection = &conn;
        return conn.execute("UPDATE contended_row SET value = 1");
    });
    REQUIRE_FALSE(query);

    SECTION("Connection callbacks are not invoked") {
        bool callback_invoked = false;
        auto rejected = db.with_connection([&](auto& conn) {
            callback_invoked = true;
            return conn.execute("INSERT INTO escaped_writes VALUES (1)");
        });
        REQUIRE_FALSE(rejected);
        REQUIRE(rejected.error().message.contains("invalidated"));
        REQUIRE_FALSE(callback_invoked);
    }

    SECTION("Void callbacks cannot bypass invalidation") {
        bool callback_invoked = false;
        REQUIRE_THROWS_AS(db.with_connection([&](auto&) { callback_invoked = true; }),
                          std::logic_error);
        REQUIRE_FALSE(callback_invoked);
    }

    SECTION("Direct observed execution is rejected before notifying observers") {
        bool observer_invoked = false;
        db.set_observer([&](const query_event&) { observer_invoked = true; });
        auto rejected =
            db.observed_execute(*pinned_connection, "INSERT INTO escaped_writes VALUES (1)", {});
        REQUIRE_FALSE(rejected);
        REQUIRE(rejected.error().message.contains("invalidated"));
        REQUIRE_FALSE(observer_invoked);
        db.set_observer({});
    }

    SECTION("Savepoints cannot recover an invalidated transaction") {
        REQUIRE_FALSE(db.savepoint("blocked"));
        REQUIRE_FALSE(db.rollback_to_savepoint("blocked"));
        REQUIRE_FALSE(db.release_savepoint("blocked"));
    }

    auto committed = transaction->commit();
    REQUIRE_FALSE(committed);
    REQUIRE(committed.error().message.contains("invalidated"));
    REQUIRE(statements == std::vector<std::string>{"BEGIN", "UPDATE contended_row SET value = 1"});
    REQUIRE(transaction->rollback());

    auto recovered = db.with_connection([](auto& conn) {
        conn.query_error.reset();
        return conn.execute("INSERT INTO recovered_writes VALUES (1)");
    });
    REQUIRE(recovered);
    auto next_transaction = db.begin();
    REQUIRE(next_transaction);
    REQUIRE(next_transaction->commit());
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
