#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <optional>
#include <string_view>

#include "asterorm/core/codecs.hpp"
#include "asterorm/postgres/copy.hpp"

using namespace asterorm;

TEST_CASE("Codecs: float / double round-trip", "[core][codec]") {
    double d = 3.141592653589793;
    auto encoded = encode(d);
    REQUIRE(encoded.has_value());
    double out = 0;
    REQUIRE(decode(encoded, out).has_value());
    REQUIRE(out == d);

    float f = 1.25f;
    auto fenc = encode(f);
    float fout = 0;
    REQUIRE(decode(fenc, fout).has_value());
    REQUIRE(fout == f);
}

TEST_CASE("Codecs: reject malformed scalar values", "[core][codec]") {
    int i = 0;
    REQUIRE(!decode(std::optional<std::string>{}, i).has_value());
    REQUIRE(!decode(std::optional<std::string>{"12x"}, i).has_value());

    double d = 0;
    REQUIRE(!decode(std::optional<std::string>{"1.5x"}, d).has_value());

    bool b = false;
    REQUIRE(!decode(std::optional<std::string>{"yes"}, b).has_value());
    REQUIRE(decode(std::optional<std::string>{"false"}, b).has_value());
    REQUIRE_FALSE(b);
}

TEST_CASE("Codecs: bytea hex round-trip", "[core][codec]") {
    bytea b{.data = {0xde, 0xad, 0xbe, 0xef, 0x00, 0xff}};
    auto enc = encode(b);
    REQUIRE(enc.has_value());
    REQUIRE(*enc == "\\xdeadbeef00ff");

    bytea out;
    REQUIRE(decode(enc, out).has_value());
    REQUIRE(out == b);

    bytea bad;
    REQUIRE(!decode(std::optional<std::string>{"deadbeef"}, bad).has_value());
    REQUIRE(!decode(std::optional<std::string>{"\\xdeadbeex0"}, bad).has_value());
}

TEST_CASE("Codecs: uuid round-trip", "[core][codec]") {
    uuid u;
    for (int i = 0; i < 16; ++i)
        u.bytes[i] = static_cast<std::uint8_t>(i * 0x11);
    auto enc = encode(u);
    REQUIRE(enc.has_value());
    REQUIRE(enc->size() == 36);

    uuid out;
    REQUIRE(decode(enc, out).has_value());
    REQUIRE(out == u);

    uuid bad;
    REQUIRE(!decode(std::optional<std::string>{"not-a-uuid"}, bad).has_value());
    REQUIRE(!decode(std::optional<std::string>{"00000000-0000-0000-0000-00000000000X"}, bad)
                 .has_value());
}

TEST_CASE("Codecs: timestamp round-trip", "[core][codec]") {
    using namespace std::chrono;
    auto tp = system_clock::time_point{seconds{1609459200} + microseconds{123456}}; // 2021-01-01
    auto enc = encode(tp);
    REQUIRE(enc.has_value());

    system_clock::time_point out;
    REQUIRE(decode(enc, out).has_value());
    REQUIRE(duration_cast<microseconds>(out - tp).count() == 0);

    // PG-style with -05 offset should normalize to the same instant.
    system_clock::time_point offset_tp;
    REQUIRE(
        decode(std::optional<std::string>{"2021-01-01 00:00:00.123456-05"}, offset_tp).has_value());
    auto expected = system_clock::time_point{seconds{1609459200 + 5 * 3600} + microseconds{123456}};
    REQUIRE(offset_tp == expected);
}

TEST_CASE("Codecs: date, time, and numeric wrappers", "[core][codec]") {
    date d{.year = 2026, .month = 4, .day = 24};
    auto denc = encode(d);
    REQUIRE(denc.has_value());
    REQUIRE(*denc == "2026-04-24");
    date dout;
    REQUIRE(decode(denc, dout).has_value());
    REQUIRE(dout == d);
    REQUIRE(!decode(std::optional<std::string>{"2026-02-30"}, dout).has_value());

    time_of_day t{.hour = 9, .minute = 5, .second = 7, .microsecond = 123};
    auto tenc = encode(t);
    REQUIRE(tenc.has_value());
    REQUIRE(*tenc == "09:05:07.000123");
    time_of_day tout;
    REQUIRE(decode(std::optional<std::string>{"09:05:07.123"}, tout).has_value());
    REQUIRE(tout.microsecond == 123000);
    REQUIRE(!decode(std::optional<std::string>{"24:00:00"}, tout).has_value());

    numeric n{.value = "-1234567890.012300"};
    auto nenc = encode(n);
    REQUIRE(nenc.has_value());
    REQUIRE(*nenc == "-1234567890.012300");
    numeric nout;
    REQUIRE(decode(nenc, nout).has_value());
    REQUIRE(nout == n);
    REQUIRE(decode(std::optional<std::string>{"1.25e3"}, nout).has_value());
    REQUIRE(!decode(std::optional<std::string>{"12.3.4"}, nout).has_value());
}

enum class codec_status { active, archived };

template <> struct asterorm::enum_traits<codec_status> {
    static std::string_view to_db(codec_status s) {
        switch (s) {
        case codec_status::active:
            return "active";
        case codec_status::archived:
            return "archived";
        }
        return "active";
    }

    static std::optional<codec_status> from_db(std::string_view s) {
        if (s == "active")
            return codec_status::active;
        if (s == "archived")
            return codec_status::archived;
        return std::nullopt;
    }
};

TEST_CASE("Codecs: enum label traits", "[core][codec]") {
    auto encoded = encode(codec_status::archived);
    REQUIRE(encoded.has_value());
    REQUIRE(*encoded == "archived");

    codec_status parsed = codec_status::active;
    REQUIRE(decode(encoded, parsed).has_value());
    REQUIRE(parsed == codec_status::archived);
    REQUIRE(!decode(std::optional<std::string>{"deleted"}, parsed).has_value());
}

TEST_CASE("PostgreSQL COPY: structured text row escaping", "[core][copy]") {
    asterorm::pg::copy_row row{
        std::string{"plain"},        std::nullopt,
        std::string{"with\ttab"},    std::string{"line\nbreak"},
        std::string{"slash\\value"}, std::string{"\\N"},
    };

    auto encoded = asterorm::pg::encode_copy_row(row);
    REQUIRE(encoded == R"(plain	\N	with\ttab	line\nbreak	slash\\value	\\N)");

    auto decoded = asterorm::pg::decode_copy_row(encoded);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->size() == row.size());
    REQUIRE((*decoded)[0] == "plain");
    REQUIRE_FALSE((*decoded)[1].has_value());
    REQUIRE((*decoded)[2] == "with\ttab");
    REQUIRE((*decoded)[3] == "line\nbreak");
    REQUIRE((*decoded)[4] == "slash\\value");
    REQUIRE((*decoded)[5] == "\\N");

    auto bad = asterorm::pg::decode_copy_row("bad\\");
    REQUIRE_FALSE(bad.has_value());
}

struct my_money {
    std::int64_t cents{0};
    bool operator==(const my_money&) const = default;
};

template <> struct asterorm::codec<my_money> {
    static std::optional<std::string> encode(const my_money& m) {
        return std::to_string(m.cents);
    }
    static asterorm::result<void> decode(const std::optional<std::string>& s, my_money& out) {
        if (!s)
            return {};
        out.cents = std::stoll(*s);
        return {};
    }
};

TEST_CASE("Codecs: user-defined codec<T> specialization", "[core][codec]") {
    my_money m{.cents = 4200};
    auto enc = encode(m);
    REQUIRE(enc.has_value());
    REQUIRE(*enc == "4200");

    my_money out;
    REQUIRE(decode(enc, out).has_value());
    REQUIRE(out == m);
}
