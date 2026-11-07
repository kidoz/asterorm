#include <catch2/catch_test_macros.hpp>
#include <string>
#include <type_traits>

#include "asterorm/postgres/connection.hpp"

namespace {
PGresult* native_tuple(ExecStatusType status, int rows) {
    auto* native = PQmakeEmptyPGresult(nullptr, status);
    PGresAttDesc attribute{};
    std::string name{"value"};
    attribute.name = name.data();
    attribute.typid = 25;
    attribute.typlen = -1;
    attribute.atttypmod = -1;
    REQUIRE(PQsetResultAttrs(native, 1, &attribute) == 1);
    for (int row = 0; row < rows; ++row) {
        std::string value{"owned text"};
        REQUIRE(PQsetvalue(native, row, 0, value.data(), static_cast<int>(value.size())) == 1);
    }
    return native;
}
} // namespace

TEST_CASE("PostgreSQL single-row admission validates without a server", "[pg-single-row-unit]") {
    asterorm::pg::connection connection{nullptr};
    STATIC_REQUIRE(asterorm::pg::connection::supports_single_row_stream);
    REQUIRE_FALSE(connection.single_row_ready());
    REQUIRE_FALSE(connection.is_pool_reusable());
    auto missing = connection.open_single_row("SELECT 1");
    REQUIRE_FALSE(missing.has_value());
    REQUIRE(missing.error().message.contains("healthy"));
    for (const auto& text : {std::string{}, std::string{"SELECT\0 1", 9}}) {
        auto invalid = connection.open_single_row(text);
        REQUIRE_FALSE(invalid.has_value());
        REQUIRE(invalid.error().kind == asterorm::db_error_kind::parse_failed);
        REQUIRE(invalid.error().sqlstate.empty());
    }
    auto parameter = asterorm::encode_parameter("value");
    parameter.error = asterorm::db_error{
        .kind = asterorm::db_error_kind::parse_failed,
        .message = "encoding identity",
        .detail = "encoding detail",
    };
    auto encoded = connection.open_single_row("SELECT $1::text", {parameter});
    REQUIRE_FALSE(encoded.has_value());
    REQUIRE(encoded.error().message == "encoding identity");
    REQUIRE(encoded.error().detail == "encoding detail");
    auto embedded = connection.open_single_row(
        "SELECT $1::text", {asterorm::encode_parameter(std::string{"a\0b", 3})});
    REQUIRE_FALSE(embedded.has_value());
    REQUIRE(embedded.error().message.contains("NUL"));
}

TEST_CASE("PostgreSQL single-tuple factory owns only one-row results", "[pg-single-row-unit]") {
    auto owned = asterorm::pg::result::from_single_tuple(native_tuple(PGRES_SINGLE_TUPLE, 1));
    REQUIRE(owned.has_value());
    REQUIRE(owned->rows() == 1);
    REQUIRE(owned->columns() == 1);
    REQUIRE(owned->column_name(0) == "value");
    REQUIRE(owned->get_string(0, 0) == "owned text");
    REQUIRE(owned->affected_rows() == 0);
    REQUIRE_FALSE(asterorm::pg::result::from_single_tuple(nullptr).has_value());
    for (const int rows : {0, 2}) {
        REQUIRE_FALSE(
            asterorm::pg::result::from_single_tuple(native_tuple(PGRES_SINGLE_TUPLE, rows))
                .has_value());
    }
    REQUIRE_FALSE(
        asterorm::pg::result::from_single_tuple(native_tuple(PGRES_TUPLES_OK, 1)).has_value());
    REQUIRE_FALSE(
        asterorm::pg::result::from_native(native_tuple(PGRES_SINGLE_TUPLE, 1)).has_value());
}

TEST_CASE("PostgreSQL empty readers have safe move and close behavior", "[pg-single-row-unit]") {
    using asterorm::pg::single_row_reader;
    STATIC_REQUIRE(!std::is_copy_constructible_v<single_row_reader>);
    STATIC_REQUIRE(std::is_nothrow_move_constructible_v<single_row_reader>);
    STATIC_REQUIRE(std::is_nothrow_move_assignable_v<single_row_reader>);
    single_row_reader empty;
    empty.close();
    auto moved = std::move(empty);
    auto inactive = moved.next();
    REQUIRE_FALSE(inactive.has_value());
    REQUIRE(inactive.error().kind == asterorm::db_error_kind::query_failed);
    single_row_reader assigned;
    assigned = std::move(moved);
    assigned.close();
    assigned.close();
    REQUIRE_FALSE(assigned.next().has_value());
}

TEST_CASE("PostgreSQL cursor admission validates without a server", "[pg-cursor-unit]") {
    asterorm::pg::connection connection{nullptr};
    STATIC_REQUIRE(asterorm::pg::connection::supports_cursor_stream);
    STATIC_REQUIRE(!std::is_copy_constructible_v<asterorm::pg::cursor_reader>);
    STATIC_REQUIRE(std::is_nothrow_move_constructible_v<asterorm::pg::cursor_reader>);
    STATIC_REQUIRE(std::is_nothrow_move_assignable_v<asterorm::pg::cursor_reader>);
    REQUIRE_FALSE(connection.cursor_ready());
    for (const auto chunk : {0U, 65537U}) {
        auto invalid = connection.open_cursor("SELECT 1", {}, chunk);
        REQUIRE_FALSE(invalid.has_value());
        REQUIRE(invalid.error().kind == asterorm::db_error_kind::parse_failed);
    }
    for (const auto& sql : {std::string{}, std::string{"SELECT\0 1", 9}}) {
        auto invalid = connection.open_cursor(sql, {}, 1);
        REQUIRE_FALSE(invalid.has_value());
        REQUIRE(invalid.error().kind == asterorm::db_error_kind::parse_failed);
    }
    auto encoded = asterorm::encode_parameter(1);
    encoded.error = asterorm::db_error{
        .kind = asterorm::db_error_kind::parse_failed,
        .message = "cursor codec identity",
    };
    REQUIRE(connection.open_cursor("SELECT $1", {encoded}, 1).error().message ==
            "cursor codec identity");
    REQUIRE(
        connection.open_cursor("SELECT $1", {asterorm::encode_parameter(std::string{"a\0b", 3})}, 1)
            .error()
            .message.contains("NUL"));
    REQUIRE_FALSE(connection.open_cursor("SELECT 1", {}, 1).has_value());
    asterorm::pg::cursor_reader reader;
    REQUIRE(reader.close().has_value());
    REQUIRE_FALSE(reader.next().has_value());
    auto moved = std::move(reader);
    REQUIRE(moved.close().has_value());
    moved.abandon();
}
