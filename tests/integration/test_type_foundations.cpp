#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"
#include <array>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <limits>

struct type_foundation_row {
    int id{};
    asterorm::interval span;
    asterorm::network host;
    asterorm::cidr subnet;
    asterorm::macaddr hardware;
    asterorm::numeric amount;
    std::chrono::system_clock::time_point instant;
    std::optional<asterorm::interval> absent;
};

template <> struct asterorm::entity_traits<type_foundation_row> {
    static constexpr const char* table = "type_foundation_rows";
    static constexpr auto primary_key = asterorm::pk<&type_foundation_row::id>("id");
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&type_foundation_row::id>("id"),
                        asterorm::column<&type_foundation_row::span>("span"),
                        asterorm::column<&type_foundation_row::host>("host"),
                        asterorm::column<&type_foundation_row::subnet>("subnet"),
                        asterorm::column<&type_foundation_row::hardware>("hardware"),
                        asterorm::column<&type_foundation_row::amount>("amount"),
                        asterorm::column<&type_foundation_row::instant>("instant"),
                        asterorm::column<&type_foundation_row::absent>("absent"));
};
namespace {
std::string connection_settings() {
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    return configured ? configured
                      : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test "
                        "sslmode=disable";
}

template <typename Value> Value take(asterorm::result<Value> outcome) {
    if (!outcome) {
        INFO(outcome.error().message);
        REQUIRE(outcome);
    }
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}

template <typename Value> Value parse(const std::optional<std::string>& text) {
    Value output{};
    take(asterorm::decode(text, output));
    return output;
}
} // namespace

TEST_CASE("PG type foundations: all native interval styles and component limits",
          "[pg][type-foundations]") {
    auto connection = take(asterorm::pg::driver{}.connect(connection_settings()));
    take(connection.execute("BEGIN"));
    // Connection ownership guarantees rollback even on a failed assertion.
    const asterorm::interval zero;
    const std::array intervals{
        zero,
        asterorm::interval{.months = 14, .days = -3, .microseconds = 14706000007},
        asterorm::interval{.months = -14, .days = 3, .microseconds = -14706000007},
        asterorm::interval{.months = -14, .days = -3, .microseconds = -14706000007},
        asterorm::interval{.microseconds = -1},
        asterorm::interval{.months = 1},
        asterorm::interval{.days = 30},
        asterorm::interval{
            .months = std::numeric_limits<std::int32_t>::min(),
            .days = std::numeric_limits<std::int32_t>::min(),
            .microseconds = std::numeric_limits<std::int64_t>::min(),
        },
        asterorm::interval{
            .months = std::numeric_limits<std::int32_t>::max(),
            .days = std::numeric_limits<std::int32_t>::max(),
            .microseconds = std::numeric_limits<std::int64_t>::max(),
        },
    };
    for (const auto* style : {"postgres", "postgres_verbose", "sql_standard", "iso_8601"}) {
        INFO(style);
        take(connection.execute("SET LOCAL IntervalStyle = '" + std::string{style} + "'"));
        for (const auto& expected : intervals) {
            auto rows = take(connection.execute_params("SELECT $1::interval",
                                                       {asterorm::encode_parameter(expected)}));
            auto wire = rows.get_string(0, 0);
            INFO(wire.value_or("NULL"));
            REQUIRE(parse<asterorm::interval>(wire) == expected);
        }
    }
    const auto calendar =
        take(connection.execute("SELECT interval '1 month' = interval '30 days', "
                                "(date '2024-01-31' + interval '1 month')::date::text, "
                                "(date '2024-01-31' + interval '30 days')::date::text"));
    REQUIRE(calendar.get_value(0, 0) == "t");
    REQUIRE(calendar.get_value(0, 1) == "2024-02-29");
    REQUIRE(calendar.get_value(0, 2) == "2024-03-01");
    take(connection.execute("ROLLBACK"));
}

TEST_CASE("PG type foundations: scalar parameters, native canonicalization and arrays",
          "[pg][type-foundations]") {
    auto connection = take(asterorm::pg::driver{}.connect(connection_settings()));
    for (const auto* text : {
             "192.0.2.9/24",
             "255.255.255.255/32",
             "::/0",
             "::1/128",
             "2001:db8::1/64",
             "::ffff:192.0.2.9/120",
         }) {
        const auto expected = parse<asterorm::network>(std::string{text});
        auto rows = take(
            connection.execute_params("SELECT $1::inet", {asterorm::encode_parameter(expected)}));
        REQUIRE(parse<asterorm::network>(rows.get_string(0, 0)) == expected);
    }
    for (const auto* text :
         {"0.0.0.0/0", "192.0.2.0/24", "2001:db8::/64", "::ffff:192.0.2.0/120"}) {
        const auto expected = parse<asterorm::cidr>(std::string{text});
        auto rows = take(
            connection.execute_params("SELECT $1::cidr", {asterorm::encode_parameter(expected)}));
        REQUIRE(parse<asterorm::cidr>(rows.get_string(0, 0)) == expected);
    }
    const auto hardware = parse<asterorm::macaddr>(std::string{"08:00:2B:01:02:03"});
    auto mac_rows = take(
        connection.execute_params("SELECT $1::macaddr", {asterorm::encode_parameter(hardware)}));
    REQUIRE(parse<asterorm::macaddr>(mac_rows.get_string(0, 0)) == hardware);
    for (const auto* text :
         {"NaN", "Infinity", "-Infinity", "123456789012345678901234567890.012300"}) {
        const asterorm::numeric expected{.value = text};
        auto rows = take(connection.execute_params("SELECT $1::numeric",
                                                   {asterorm::encode_parameter(expected)}));
        REQUIRE(parse<asterorm::numeric>(rows.get_string(0, 0)) == expected);
    }
    const std::vector<asterorm::jsonb> documents{{.value = R"({"a":1})"}, {.value = "null"}};
    auto documents_rows = take(
        connection.execute_params("SELECT $1::jsonb[]", {asterorm::encode_parameter(documents)}));
    const auto decoded_documents =
        parse<std::vector<asterorm::jsonb>>(documents_rows.get_string(0, 0));
    REQUIRE(decoded_documents.size() == 2);
    REQUIRE(decoded_documents[0].value == R"({"a": 1})");
    REQUIRE(decoded_documents[1].value == "null");
    const std::vector<std::optional<asterorm::interval>> spans{
        asterorm::interval{.months = 14, .days = -3},
        std::nullopt,
    };
    auto spans_rows = take(
        connection.execute_params("SELECT $1::interval[]", {asterorm::encode_parameter(spans)}));
    REQUIRE(parse<std::vector<std::optional<asterorm::interval>>>(spans_rows.get_string(0, 0)) ==
            spans);
    const auto offset_rows =
        take(connection.execute("SELECT '2026-01-01 01:02:03.123456+01:02:03'::timestamptz"));
    REQUIRE(
        parse<std::chrono::system_clock::time_point>(offset_rows.get_string(0, 0)) ==
        parse<std::chrono::system_clock::time_point>(std::string{"2026-01-01 00:00:00.123456Z"}));
}

TEST_CASE("PG type foundations: repository mapping and local encoding errors",
          "[pg][type-foundations]") {
    asterorm::connection_pool<asterorm::pg::driver> pool{
        asterorm::pg::driver{}, {.min_size = 0, .max_size = 1, .conninfo = connection_settings()}};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    auto transaction = take(db.begin());
    take(db.with_connection([](auto& connection) {
        return connection.execute("CREATE TEMP TABLE type_foundation_rows (id integer primary key, "
                                  "span interval, host inet, "
                                  "subnet cidr, hardware macaddr, amount numeric, instant "
                                  "timestamptz, absent interval) ON COMMIT DROP");
    }));
    type_foundation_row row{
        .id = 1,
        .span = {.months = 14, .days = -3, .microseconds = -1},
        .host = parse<asterorm::network>(std::string{"192.0.2.9/24"}),
        .subnet = parse<asterorm::cidr>(std::string{"2001:db8::/64"}),
        .hardware = parse<asterorm::macaddr>(std::string{"08:00:2b:01:02:03"}),
        .amount = {.value = "NaN"},
        .instant = parse<std::chrono::system_clock::time_point>(
            std::string{"1969-12-31 23:59:59.999999Z"}),
    };
    take(repo.insert(row));
    const auto loaded = take(repo.find<type_foundation_row>(1));
    REQUIRE(loaded.span == row.span);
    REQUIRE(loaded.host == row.host);
    REQUIRE(loaded.subnet == row.subnet);
    REQUIRE(loaded.hardware == row.hardware);
    REQUIRE(loaded.amount == row.amount);
    REQUIRE(loaded.instant == row.instant);
    REQUIRE_FALSE(loaded.absent);
    row.id = 2;
    row.subnet.value = row.host; // Host bits cannot be silently masked.
    const auto invalid = repo.insert(row);
    REQUIRE_FALSE(invalid);
    REQUIRE(invalid.error().kind == asterorm::db_error_kind::parse_failed);
    REQUIRE(take(db.native_scalar<int>("SELECT count(*)::int FROM type_foundation_rows")) == 1);
    take(transaction.rollback());
}
