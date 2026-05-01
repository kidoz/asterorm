#include <catch2/catch_test_macros.hpp>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/block.h>
#include <clickhouse/columns/numeric.h>
#pragma GCC diagnostic pop

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "asterorm/clickhouse/detail/params.hpp"
#include "asterorm/clickhouse/result.hpp"

TEST_CASE("ClickHouse: parameter interpolation skips literals and comments", "[clickhouse]") {
    std::vector<std::optional<std::string>> parameters{"value", "second"};

    auto interpolated_sql = asterorm::ch::detail::interpolate_params(
        "SELECT '$1', \"$2\", $1, -- $2\n $2, /* $1 */ '$2'", parameters);

    REQUIRE(interpolated_sql.has_value());
    REQUIRE(*interpolated_sql == "SELECT '$1', \"$2\", 'value', -- $2\n 'second', /* $1 */ '$2'");
}

TEST_CASE("ClickHouse: parameter interpolation rejects unsafe bytes", "[clickhouse]") {
    std::vector<std::optional<std::string>> parameters{std::string("bad\0value", 9)};

    auto interpolated_sql = asterorm::ch::detail::interpolate_params("SELECT $1", parameters);

    REQUIRE_FALSE(interpolated_sql.has_value());
    REQUIRE(interpolated_sql.error().kind == asterorm::db_error_kind::parse_failed);
}

TEST_CASE("ClickHouse: result converts numeric columns to strings", "[clickhouse]") {
    auto answer_column = std::make_shared<clickhouse::ColumnInt64>();
    answer_column->Append(42);

    auto unsigned_column = std::make_shared<clickhouse::ColumnUInt8>();
    unsigned_column->Append(7);

    clickhouse::Block block;
    block.AppendColumn("answer", answer_column);
    block.AppendColumn("small_number", unsigned_column);

    asterorm::ch::result query_result;
    query_result.add_block(block);

    REQUIRE(query_result.rows() == 1);
    REQUIRE(query_result.get_string(0, 0) == std::optional<std::string>{"42"});
    REQUIRE(query_result.get_int64(0, 0) == std::optional<std::int64_t>{42});
    REQUIRE(query_result.get_string(0, 1) == std::optional<std::string>{"7"});
}
