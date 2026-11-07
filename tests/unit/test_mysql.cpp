#include <catch2/catch_test_macros.hpp>
#include <limits>

#include "asterorm/mysql/detail/params.hpp"
#include "asterorm/mysql/driver.hpp"

TEST_CASE("MySQL placeholders preserve literals and reorder repeated parameters", "[mysql]") {
    using asterorm::mysql::detail::prepare_query;
    auto query = prepare_query("SELECT $2, '$1', `a$2`, \"b$1\", $1, $2 /* $8 */ -- $9\n# $7\n",
                               {"first", "second"});
    REQUIRE(query.has_value());
    REQUIRE(query->sql == "SELECT ?, '$1', `a$2`, \"b$1\", ?, ? /* $8 */ -- $9\n# $7\n");
    REQUIRE(query->params == asterorm::query_parameters{"second", "first", "second"});
    REQUIRE(prepare_query("SELECT '?', ?", {"hello"}).has_value());
    auto identifier = prepare_query("SELECT column$1, $1suffix", {});
    REQUIRE(identifier.has_value());
    REQUIRE(identifier->sql == "SELECT column$1, $1suffix");
    REQUIRE(prepare_query("SELECT 'it''s $1', $1", {"hello"}).has_value());
    for (const auto* sql : {
             "SELECT $0",
             "SELECT $2",
             "SELECT $999999999999999999999999",
             "SELECT $1, ?",
             "SELECT 1",
             "SELECT 'unterminated",
             "SELECT 1 /*",
             "SELECT /*! $1 */ 1",
         }) {
        INFO(sql);
        REQUIRE_FALSE(prepare_query(sql, {"value"}));
    }
    asterorm::query_parameter failed;
    failed.error = asterorm::mysql::detail::error("encoding failed");
    auto rejected = prepare_query("SELECT $1", {failed});
    REQUIRE_FALSE(rejected);
    REQUIRE(rejected.error().message == "encoding failed");
}

TEST_CASE("MySQL rejects invalid connection options without connecting", "[mysql]") {
    asterorm::mysql::driver driver;
    for (const auto* options : {
             "port=0",
             "port=65536",
             "port=abc",
             "connect_timeout=-1",
             "sslmode=disable",
             "password='unterminated",
             "user=one user=two",
             "host",
             "user='x'garbage",
         }) {
        INFO(options);
        REQUIRE_FALSE(driver.connect(options));
    }
}

TEST_CASE("MySQL closed and moved connections fail safely", "[mysql]") {
    asterorm::mysql::connection connection;
    REQUIRE_FALSE(connection.is_open());
    REQUIRE_FALSE(connection.execute("SELECT 1"));
    REQUIRE_FALSE(connection.execute_params("SELECT ?", {"x"}));
    REQUIRE_FALSE(connection.execute_prepared("SELECT 1", {}));
    REQUIRE_FALSE(connection.copy_in("COPY", {}));
    REQUIRE_FALSE(connection.copy_out("COPY"));
    auto moved = std::move(connection);
    moved.close();
    moved.close();
    REQUIRE(moved.prepared_statement_count() == 0);
}

TEST_CASE("MySQL results preserve NULL and validate integers", "[mysql]") {
    asterorm::mysql::result rows{{"id", "name", "invalid"}, {{"42", std::nullopt, "12x"}}, 3, 42};
    REQUIRE(rows.rows() == 1);
    REQUIRE(rows.columns() == 3);
    REQUIRE(rows.affected_rows() == 3);
    REQUIRE(rows.last_insert_id() == 42);
    REQUIRE(rows.get_int64(0, 0) == 42);
    REQUIRE(rows.is_null(0, 1));
    REQUIRE_FALSE(rows.get_int64(0, 2));
    REQUIRE(rows.is_null(-1, 0));
    REQUIRE(rows.get_value(5, 5).empty());
}

TEST_CASE("MySQL result counts retain 64-bit values without a server", "[mysql][row-count]") {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    asterorm::mysql::result result{{}, {}, maximum};
    STATIC_REQUIRE(std::is_same_v<decltype(result.affected_rows()), std::uint64_t>);
    REQUIRE(result.affected_rows() == maximum);
    auto moved = std::move(result);
    REQUIRE(moved.affected_rows() == maximum);
}
