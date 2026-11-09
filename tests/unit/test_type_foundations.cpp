#include "asterorm/core/codecs.hpp"
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <limits>

namespace {
template <typename Value> Value parse(std::string text) {
    Value output{};
    INFO(text);
    REQUIRE(asterorm::decode(std::optional{text}, output));
    return output;
}

template <typename Value> void rejects(const std::string& text, const Value& sentinel) {
    auto output = sentinel;
    INFO(text);
    auto decoded = asterorm::decode(std::optional{text}, output);
    REQUIRE_FALSE(decoded);
    REQUIRE(decoded.error().kind == asterorm::db_error_kind::parse_failed);
    REQUIRE(output == sentinel);
}

template <typename Value> void round_trip(const Value& value) {
    auto encoded = asterorm::encode(value);
    REQUIRE(encoded);
    REQUIRE(parse<Value>(*encoded) == value);
}
} // namespace

TEST_CASE("Type foundations: intervals retain independent signed components",
          "[core][codec][type-foundations]") {
    using asterorm::interval;
    const interval mixed{.months = 14, .days = -3, .microseconds = 14706000007};
    for (const auto* text : {
             "1 year 2 mons -3 days +04:05:06.000007",
             "@ 1 year 2 mons -3 days 4 hours 5 mins 6.000007 secs",
             "+1-2 -3 +4:05:06.000007",
             "P1Y2M-3DT4H5M6.000007S",
         }) {
        REQUIRE(parse<interval>(text) == mixed);
    }
    REQUIRE(parse<interval>("@ 1 year 2 mons 3 days 4 hours 5 mins 6.000007 secs ago") ==
            interval{.months = -14, .days = -3, .microseconds = -14706000007});
    REQUIRE(parse<interval>("-1-2 3 4:05:06.000007") ==
            interval{.months = -14, .days = -3, .microseconds = -14706000007});
    REQUIRE(parse<interval>("-1-2 +3 +4:05:06.000007") ==
            interval{.months = -14, .days = 3, .microseconds = 14706000007});
    for (const auto* text : {"00:00:00", "@ 0", "0", "P0D", "PT0S"}) {
        REQUIRE(parse<interval>(text) == interval{});
    }
    const interval zero;
    for (const interval value : {
             mixed,
             zero,
             interval{.microseconds = -1},
             interval{
                 .months = std::numeric_limits<std::int32_t>::min(),
                 .days = std::numeric_limits<std::int32_t>::min(),
                 .microseconds = std::numeric_limits<std::int64_t>::min(),
             },
             interval{
                 .months = std::numeric_limits<std::int32_t>::max(),
                 .days = std::numeric_limits<std::int32_t>::max(),
                 .microseconds = std::numeric_limits<std::int64_t>::max(),
             },
         }) {
        round_trip(value);
    }
    REQUIRE(parse<interval>("@ 2562047788 hours 54.775808 secs ago").microseconds ==
            std::numeric_limits<std::int64_t>::min());
    REQUIRE(parse<interval>("-2562047788:00:54.775808").microseconds ==
            std::numeric_limits<std::int64_t>::min());
    for (const auto* text : {
             "",
             "P",
             "PT",
             "P1DT",
             "P1Y1Y",
             "P1D1Y",
             "P1W",
             "1 frogs",
             "1 day junk",
             "1 day 2 days",
             "2147483648 days",
             "178956971 years",
             "9223372036854.775808 secs",
             "-9223372036854.775809 secs",
             "2562047788:00:54.775808",
             "1:60:00",
             "1:00:60",
             "1:00:00junk",
             "1.0000001 secs",
             "@ 1 day ago junk",
             "1-12",
             "1-2 3 4",
         }) {
        rejects(text, mixed);
    }
    rejects(std::string{"P1D\0junk", 8}, mixed);
}

TEST_CASE("Type foundations: numeric network addresses and strict CIDR",
          "[core][codec][type-foundations]") {
    using asterorm::network;
    const auto host = parse<network>("192.0.2.9/24");
    REQUIRE(host.family == asterorm::address_family::ipv4);
    REQUIRE(host.prefix_length == 24);
    REQUIRE(host.address[0] == 192);
    REQUIRE(host.address[3] == 9);
    REQUIRE(asterorm::encode(host) == "192.0.2.9/24");
    const auto subnet = parse<asterorm::cidr>("192.0.2.0/24");
    rejects("192.0.2.9/24", subnet);
    REQUIRE(asterorm::encode_parameter(asterorm::cidr{.value = host}).error);
    for (const auto* text : {
             "0.0.0.0/0",
             "255.255.255.255",
             "::",
             "::1/128",
             "2001:db8::1/64",
             "1:0:0:2:0:0:3:4",
             "::ffff:192.0.2.9/120",
             "::192.0.2.1",
             "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
         }) {
        round_trip(parse<network>(text));
    }
    REQUIRE(parse<network>("::ffff:192.0.2.9").family == asterorm::address_family::ipv6);
    REQUIRE(asterorm::encode(parse<network>("1:0:0:2:0:0:3:4")) == "1::2:0:0:3:4/128");
    round_trip(parse<asterorm::cidr>("::ffff:192.0.2.0/120"));
    rejects("2001:db8::1/64", parse<asterorm::cidr>("2001:db8::/64"));
    for (const auto* text : {
             "",
             "localhost",
             "127.1",
             "1.2.3.256",
             "01.2.3.4",
             "1.2.3.4/33",
             "1.2.3.4/-1",
             "1.2.3.4/24junk",
             "1.2.3.4/24/1",
             "fe80::1%eth0",
             "2001:db8::1/129",
             "1::2::3",
             ":::1",
             "1:",
             ":1",
             "1:2:3:4:5:6:7",
             "1:2:3:4:5:6:7:8:9",
             "12345::",
             "gg::",
             "::1.2.3.4:1",
             "::ffff:192.0.2.256",
             "[::1]",
             " ::1",
             "::1 ",
             "+1::",
         }) {
        rejects(text, host);
    }
    auto invalid = host;
    invalid.address[4] = 1;
    REQUIRE(asterorm::encode_parameter(invalid).error);
    invalid = host;
    // Exercise a deliberately invalid wire family.
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    invalid.family = static_cast<asterorm::address_family>(42);
    REQUIRE(asterorm::encode_parameter(invalid).error);
    invalid = host;
    invalid.prefix_length = 129;
    REQUIRE(asterorm::encode_parameter(invalid).error);
}

TEST_CASE("Type foundations: MAC and nullable wrappers", "[core][codec][type-foundations]") {
    const auto hardware = parse<asterorm::macaddr>("08:00:2B:01:02:ff");
    REQUIRE(asterorm::encode(hardware) == "08:00:2b:01:02:ff");
    round_trip(hardware);
    for (const auto* text : {
             "08:00:2b:01:02",
             "08:00:2b:01:02:03:04:05",
             "08:00:2g:01:02:03",
             "08002b010203",
             "08-00-2b-01-02-03",
             "+8:00:2b:01:02:03",
         }) {
        rejects(text, hardware);
    }
    std::optional<asterorm::interval> nullable{asterorm::interval{.months = 1}};
    REQUIRE(asterorm::decode(std::nullopt, nullable));
    REQUIRE_FALSE(nullable);
    auto null_parameter = asterorm::encode_parameter(nullable);
    REQUIRE_FALSE(null_parameter.value);
    REQUIRE_FALSE(null_parameter.error);
    rejects("bad", std::optional{hardware});
}

TEST_CASE("Type foundations: finite temporal and exact numeric prerequisites",
          "[core][codec][type-foundations]") {
    using namespace std::chrono;
    using asterorm::date;
    const date leap{.year = 2024, .month = 2, .day = 29};
    for (const auto* text : {
             "0000-01-01",
             "-001-01-01",
             "65537-01-01",
             "32768-01-01",
             "2024-257-01",
             "2024-02-30",
             "2023-02-29",
             "2024-02-29 BC",
             "2024-2-29",
             "infinity",
         }) {
        rejects(text, leap);
    }
    round_trip(date{.year = 32767, .month = 12, .day = 31});
    REQUIRE(asterorm::encode_parameter(date{.year = 0, .month = 1, .day = 1}).error);
    const auto instant = parse<system_clock::time_point>("2026-01-01 00:00:00.123456Z");
    REQUIRE(parse<system_clock::time_point>("2026-01-01 01:02:03.123456+01:02:03") == instant);
    REQUIRE(parse<system_clock::time_point>("2025-12-31 22:57:57.123456-01:02:03") == instant);
    REQUIRE(parse<system_clock::time_point>("1969-12-31 23:59:59Z") ==
            system_clock::time_point{seconds{-1}});
    REQUIRE(asterorm::encode(system_clock::time_point{microseconds{-1}}) ==
            "1969-12-31 23:59:59.999999+00:00");
    for (const auto* text : {
             "2026-02-30 25:99:00+00junk",
             "2026-02-30 00:00:00Z",
             "2026-01-01 24:00:00Z",
             "2026-01-01 00:00:60Z",
             "2026-01-01 00:00:00+16",
             "2026-01-01 00:00:00+01:60",
             "2026-01-01 00:00:00+01:00:60",
             "2026-01-01 00:00:00.Z",
             "2026-01-01 00:00:00.1234567Z",
             "2026-01-01 00:00:00Zjunk",
             "2026-01-01 00:00:00+01:00junk",
             "40000-12-31 00:00:00Z",
             "0000-01-01 00:00:00Z",
             "infinity",
         }) {
        rejects(text, instant);
    }
    for (const auto boundary : {system_clock::time_point::min(), system_clock::time_point::max()}) {
        const auto encoded = asterorm::encode(boundary);
        // A coarse clock with dates outside the AD model may reject encoding.
        if (encoded) {
            const auto decoded = parse<system_clock::time_point>(*encoded);
            REQUIRE(duration_cast<microseconds>(decoded.time_since_epoch()) ==
                    duration_cast<microseconds>(boundary.time_since_epoch()));
        }
    }
    const asterorm::numeric exact{.value = "123456789012345678901234567890.012300"};
    round_trip(exact);
    for (const auto* text : {"NaN", "Infinity", "-Infinity", "+1.25e-123", ".5", "1."}) {
        round_trip(asterorm::numeric{.value = text});
    }
    for (const auto* text : {"", "nan", "+Infinity", "1e", "1.2x", "1 2", " 1", "1\n"}) {
        rejects(text, exact);
        REQUIRE(asterorm::encode_parameter(asterorm::numeric{.value = text}).error);
    }
}

TEST_CASE("Type foundations: flat array grammar is atomic and rejects nesting",
          "[core][codec][type-foundations]") {
    const std::vector<int> sentinel{9, 8};
    for (const auto* text : {
             "{1,2junk}",
             "{1,}",
             "{,1}",
             "{1,,2}",
             "{{1,2},{3,4}}",
             "[0:1]={1,2}",
             "[1:2]={1,2}",
             "{1,\"2\"junk}",
             "{\"1}",
             "{1,2\\}",
             "{1,2147483648}",
         }) {
        rejects(text, sentinel);
    }
    const std::vector<std::vector<int>> nested{{1}};
    REQUIRE(asterorm::encode_parameter(nested).error);
    rejects("{}", nested);
    const std::vector<std::optional<std::vector<int>>> nullable_nested{std::nullopt};
    REQUIRE(asterorm::encode_parameter(nullable_nested).error);
    rejects("{NULL}", nullable_nested);
    REQUIRE(parse<std::vector<int>>("{ 1 , 2 }") == std::vector<int>{1, 2});
    REQUIRE(parse<std::vector<int>>("{ }").empty());
    round_trip(std::vector<std::optional<std::string>>{
        "",
        "NuLl",
        "a{b}",
        "a,b",
        "a\\b",
        "a\"b",
        " a ",
        std::nullopt,
    });
    round_trip(std::vector<bool>{true, false});
    round_trip(std::vector<asterorm::jsonb>{{.value = R"({"a":1})"}, {.value = "null"}});
    round_trip(std::vector<asterorm::interval>{{.months = 1}, {.microseconds = -1}});
    round_trip(std::vector<asterorm::network>{parse<asterorm::network>("::1/128")});
    REQUIRE(asterorm::encode_parameter(std::vector<std::string>{std::string{"a\0b", 3}}).error);
    int unchanged = 9;
    REQUIRE_FALSE(asterorm::decode(std::optional<std::string>{"12x"}, unchanged));
    REQUIRE(unchanged == 9);
}
