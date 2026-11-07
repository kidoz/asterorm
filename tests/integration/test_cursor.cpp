#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "asterorm/postgres/driver.hpp"

namespace {
template <typename Value> Value cursor_take(asterorm::result<Value> outcome) {
    REQUIRE(outcome.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}
struct cursor_connection {
    PGconn* native;
    asterorm::pg::connection connection;
    cursor_connection()
        : native(PQconnectdb(std::getenv("ASTERORM_TEST_CONNINFO")
                                 ? std::getenv("ASTERORM_TEST_CONNINFO")
                                 : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                   "password=orm_test sslmode=disable")),
          connection(native) {
        REQUIRE(connection.is_open());
    }
    // Deliberate test-only bypass to inspect portals or inject external interference.
    asterorm::pg::result inspect(const char* sql) const {
        return cursor_take(asterorm::pg::result::from_native(PQexec(native, sql)));
    }
    [[nodiscard]] std::int64_t portals() const {
        return inspect("SELECT count(*) FROM pg_cursors").get_int64(0, 0).value();
    }
};
} // namespace

TEST_CASE("PG cursor batches are parameterized bounded and close at EOF", "[pg][cursor]") {
    const auto chunk = GENERATE(1U, 2U, 3U, 7U, 256U, 65536U);
    cursor_connection test;
    const auto pid = test.inspect("SELECT pg_backend_pid()").get_int64(0, 0);
    cursor_take(test.connection.execute_prepared("SELECT 1", {}));
    const auto cached = test.connection.prepared_statement_count();
    cursor_take(test.connection.execute("BEGIN"));
    auto reader = cursor_take(test.connection.open_cursor(
        "SELECT value AS id,$2::text AS note,$3::text AS missing FROM generate_series(1,$1::int) "
        "value",
        {
            asterorm::encode_parameter(7),
            asterorm::encode_parameter("quote' ; --"),
            asterorm::encode_parameter(std::optional<std::string>{}),
        },
        chunk));
    auto portal = test.inspect("SELECT is_holdable,is_scrollable,is_binary FROM pg_cursors");
    REQUIRE(portal.rows() == 1);
    REQUIRE(portal.get_string(0, 0) == "f");
    REQUIRE(portal.get_string(0, 1) == "f");
    REQUIRE(portal.get_string(0, 2) == "f");
    REQUIRE_FALSE(test.connection.is_pool_reusable());
    REQUIRE_FALSE(test.connection.cursor_ready());
    REQUIRE_FALSE(test.connection.open_cursor("SELECT 1", {}, 1).has_value());
    REQUIRE_FALSE(test.connection.open_single_row("SELECT 1").has_value());
    REQUIRE_FALSE(test.connection.execute("COMMIT").has_value());
    REQUIRE_FALSE(test.connection.execute_params("SELECT 1", {}).has_value());
    REQUIRE_FALSE(test.connection.execute_prepared("SELECT 1", {}).has_value());
    REQUIRE_FALSE(test.connection.copy_out("COPY (SELECT 1) TO STDOUT").has_value());
    REQUIRE_THROWS_AS(test.connection.clear_prepared_statement_cache(), std::logic_error);
    int expected = 1;
    std::vector<int> batches;
    while (auto batch = cursor_take(reader.next())) {
        REQUIRE(batch->rows() <= static_cast<int>(chunk));
        batches.push_back(batch->rows());
        for (int row = 0; row < batch->rows(); ++row) {
            REQUIRE(batch->get_int64(row, 0) == expected++);
            REQUIRE(batch->get_string(row, 1) == "quote' ; --");
            REQUIRE(batch->is_null(row, 2));
        }
    }
    REQUIRE(expected == 8);
    REQUIRE(std::cmp_equal(batches.back(), 7U % chunk == 0U ? chunk : 7U % chunk));
    REQUIRE_FALSE(cursor_take(reader.next()).has_value());
    REQUIRE(test.portals() == 0);
    REQUIRE(test.connection.cursor_ready());
    REQUIRE(test.connection.is_pool_reusable());
    REQUIRE(test.connection.prepared_statement_count() == cached);
    REQUIRE(test.inspect("SELECT pg_backend_pid()").get_int64(0, 0) == pid);
    auto newer = cursor_take(test.connection.open_cursor("SELECT 8", {}, 1));
    cursor_take(reader.close());
    reader.abandon();
    REQUIRE(cursor_take(newer.next())->get_int64(0, 0) == 8);
    cursor_take(newer.close());
    cursor_take(test.connection.execute("COMMIT"));
}

TEST_CASE("PG cursor admission leaves rollback ownership with its caller", "[pg][cursor]") {
    cursor_connection test;
    REQUIRE_FALSE(test.connection.open_cursor("SELECT 1", {}, 2).has_value());
    REQUIRE(test.connection.is_transaction_idle());
    cursor_take(test.connection.execute("BEGIN"));
    REQUIRE_FALSE(test.connection.execute("SELECT 1/0").has_value());
    REQUIRE_FALSE(test.connection.open_cursor("SELECT 1", {}, 2).has_value());
    REQUIRE(test.connection.is_open());
    REQUIRE(PQtransactionStatus(test.native) == PQTRANS_INERROR);
    cursor_take(test.connection.execute("ROLLBACK"));
}

TEST_CASE("PG cursor explicit close retains transaction and owned batch", "[pg][cursor]") {
    cursor_connection test;
    cursor_take(test.connection.execute("BEGIN"));
    auto reader = cursor_take(test.connection.open_cursor("SELECT generate_series(1,10)", {}, 3));
    auto first = cursor_take(reader.next());
    REQUIRE(first->rows() == 3);
    cursor_take(reader.close());
    cursor_take(reader.close());
    REQUIRE_FALSE(cursor_take(reader.next()).has_value());
    REQUIRE(test.portals() == 0);
    REQUIRE(test.connection.cursor_ready());
    REQUIRE(first->get_int64(2, 0) == 3);
    cursor_take(test.connection.execute("COMMIT"));
}

TEST_CASE("PG cursor errors after delivered batches preserve diagnostics and discard",
          "[pg][cursor]") {
    cursor_connection test;
    cursor_take(test.connection.execute("BEGIN"));
    auto reader = cursor_take(
        test.connection.open_cursor("SELECT CASE WHEN value=5 THEN 1/(value-value) ELSE value END "
                                    "FROM generate_series(1,8) value",
                                    {}, 2));
    auto first = cursor_take(reader.next());
    REQUIRE(first->get_int64(0, 0) == 1);
    REQUIRE(cursor_take(reader.next())->get_int64(1, 0) == 4);
    const auto failed = reader.next();
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().sqlstate == "22012");
    REQUIRE(reader.next().error().message == failed.error().message);
    REQUIRE(reader.close().error().sqlstate == "22012");
    REQUIRE_FALSE(test.connection.is_open());
    REQUIRE(first->get_int64(1, 0) == 2);
}

TEST_CASE("PG cursor CLOSE failure discards without replacing its diagnostic", "[pg][cursor]") {
    cursor_connection test;
    cursor_take(test.connection.execute("BEGIN"));
    auto reader = cursor_take(test.connection.open_cursor("SELECT 1", {}, 1));
    test.inspect("CLOSE ALL");
    const auto failed = reader.close();
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().sqlstate == "34000");
    REQUIRE(reader.next().error().sqlstate == "34000");
    REQUIRE_FALSE(test.connection.is_open());
}

TEST_CASE("PG cursor moves track ownership and unfinished destruction discards", "[pg][cursor]") {
    cursor_connection test;
    cursor_take(test.connection.execute("BEGIN"));
    auto reader = cursor_take(test.connection.open_cursor("SELECT generate_series(1,3)", {}, 1));
    auto moved_connection = std::move(test.connection);
    auto moved_reader = std::move(reader);
    // NOLINTNEXTLINE(bugprone-use-after-move): the moved-from handle must reject next().
    REQUIRE_FALSE(reader.next().has_value());
    REQUIRE(cursor_take(moved_reader.next())->get_int64(0, 0) == 1);
    {
        asterorm::pg::cursor_reader replacement;
        replacement = std::move(moved_reader);
    }
    REQUIRE_FALSE(moved_connection.is_open());
    // NOLINTNEXTLINE(bugprone-use-after-move): verify the defined moved-from state.
    REQUIRE_FALSE(moved_reader.next().has_value());
}

TEST_CASE("PG cursor empty SELECT closes without exposing a batch", "[pg][cursor]") {
    cursor_connection test;
    cursor_take(test.connection.execute("BEGIN"));
    auto reader = cursor_take(test.connection.open_cursor("SELECT 1 WHERE FALSE", {}, 4));
    REQUIRE_FALSE(cursor_take(reader.next()).has_value());
    REQUIRE(test.portals() == 0);
    cursor_take(test.connection.execute("ROLLBACK"));
}

TEST_CASE("PG cursor declaration errors preserve server diagnostics and discard", "[pg][cursor]") {
    cursor_connection test;
    cursor_take(test.connection.execute("BEGIN"));
    const auto rejected = test.connection.open_cursor("SELECT * FROM missing_cursor_source", {}, 2);
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(rejected.error().sqlstate == "42P01");
    REQUIRE_FALSE(test.connection.is_open());
}

TEST_CASE("PG cursor loss invalidates retained handles safely", "[pg][cursor]") {
    asterorm::pg::cursor_reader reader;
    {
        cursor_connection test;
        cursor_take(test.connection.execute("BEGIN"));
        reader = cursor_take(test.connection.open_cursor("SELECT 1", {}, 1));
    }
    REQUIRE_FALSE(reader.next().has_value());
    REQUIRE_FALSE(reader.close().has_value());
    reader.abandon();
}

TEST_CASE("PG cursor detects backend termination on FETCH", "[pg][cursor]") {
    cursor_connection test;
    const auto pid = test.inspect("SELECT pg_backend_pid()").get_int64(0, 0).value();
    cursor_take(test.connection.execute("BEGIN"));
    auto reader = cursor_take(test.connection.open_cursor("SELECT generate_series(1,3)", {}, 1));
    REQUIRE(cursor_take(reader.next())->get_int64(0, 0) == 1);
    cursor_connection killer;
    cursor_take(killer.connection.execute_params("SELECT pg_terminate_backend($1::int)",
                                                 {asterorm::encode_parameter(pid)}));
    const auto failed = reader.next();
    REQUIRE_FALSE(failed.has_value());
    // libpq may detect the dead socket before receiving the server's FATAL result.
    REQUIRE((failed.error().sqlstate.empty() || failed.error().sqlstate == "57P01"));
    REQUIRE_FALSE(failed.error().message.empty());
    REQUIRE_FALSE(test.connection.is_open());
}
