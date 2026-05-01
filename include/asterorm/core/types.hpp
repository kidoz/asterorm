#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace asterorm {

// Wraps a JSONB value as its PostgreSQL text representation. Encoders pass the
// raw text through; decoders store whatever PG returns (including whitespace).
struct jsonb {
    std::string value;

    bool operator==(const jsonb& other) const {
        return value == other.value;
    }
};

// Raw byte payload. Round-trips via PostgreSQL hex bytea format ("\x..").
struct bytea {
    std::vector<std::uint8_t> data;

    bool operator==(const bytea& other) const {
        return data == other.data;
    }
};

// Canonical 16-byte UUID, formatted as RFC 4122 xxxxxxxx-xxxx-... hex groups.
struct uuid {
    std::array<std::uint8_t, 16> bytes{};

    bool operator==(const uuid& other) const {
        return bytes == other.bytes;
    }
};

struct date {
    int year{};
    unsigned month{};
    unsigned day{};

    bool operator==(const date& other) const = default;
};

struct time_of_day {
    unsigned hour{};
    unsigned minute{};
    unsigned second{};
    unsigned microsecond{};

    bool operator==(const time_of_day& other) const = default;
};

// Arbitrary-precision PostgreSQL NUMERIC/DECIMAL text. AsterORM keeps the exact
// textual value instead of rounding through binary floating point.
struct numeric {
    std::string value;

    bool operator==(const numeric& other) const {
        return value == other.value;
    }
};

// Optional enum-label mapping hook:
//
// template <> struct asterorm::enum_traits<MyEnum> {
//     static std::string_view to_db(MyEnum);
//     static std::optional<MyEnum> from_db(std::string_view);
// };
template <typename E> struct enum_traits;

} // namespace asterorm
