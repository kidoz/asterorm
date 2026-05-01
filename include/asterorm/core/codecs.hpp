#pragma once
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "asterorm/core/error.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/core/types.hpp"

namespace asterorm {

template <typename T> struct is_optional : std::false_type {};

template <typename T> struct is_optional<std::optional<T>> : std::true_type {};

template <typename T> inline constexpr bool is_optional_v = is_optional<T>::value;

template <typename T> struct is_vector : std::false_type {};

template <typename T, typename A> struct is_vector<std::vector<T, A>> : std::true_type {};

template <typename T> inline constexpr bool is_vector_v = is_vector<T>::value;

// Customization point: specialize `codec<T>` with static `encode` / `decode`
// to teach asterorm about a user-defined type.
//
//   template <> struct asterorm::codec<MyType> {
//       static std::optional<std::string> encode(const MyType&);
//       static asterorm::result<void> decode(const std::optional<std::string>&, MyType&);
//   };
template <typename T> struct codec; // primary template intentionally undefined

namespace detail {
template <typename T, typename = void> struct has_custom_codec : std::false_type {};

template <typename T>
struct has_custom_codec<
    T, std::void_t<decltype(codec<T>::encode(std::declval<const T&>())),
                   decltype(codec<T>::decode(std::declval<const std::optional<std::string>&>(),
                                             std::declval<T&>()))>> : std::true_type {};

template <typename T, typename = void> struct has_enum_traits : std::false_type {};

template <typename T>
struct has_enum_traits<
    T, std::void_t<decltype(enum_traits<T>::to_db(std::declval<T>())),
                   decltype(enum_traits<T>::from_db(std::declval<std::string_view>()))>>
    : std::true_type {};

inline char nibble_to_hex(std::uint8_t n) {
    return static_cast<char>(n < 10 ? '0' + n : 'a' + (n - 10));
}

inline std::optional<std::uint8_t> hex_to_nibble(char c) {
    if (c >= '0' && c <= '9')
        return static_cast<std::uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f')
        return static_cast<std::uint8_t>(10 + (c - 'a'));
    if (c >= 'A' && c <= 'F')
        return static_cast<std::uint8_t>(10 + (c - 'A'));
    return std::nullopt;
}

inline std::string encode_bytea(const bytea& b) {
    std::string out = "\\x";
    out.reserve(2 + b.data.size() * 2);
    for (auto byte : b.data) {
        out += nibble_to_hex(static_cast<std::uint8_t>(byte >> 4));
        out += nibble_to_hex(static_cast<std::uint8_t>(byte & 0x0f));
    }
    return out;
}

inline result<void> decode_bytea(std::string_view s, bytea& out) {
    auto err = [](std::string msg) {
        db_error e;
        e.kind = db_error_kind::parse_failed;
        e.message = std::move(msg);
        return e;
    };
    if (s.size() < 2 || s[0] != '\\' || s[1] != 'x')
        return std::unexpected(err("bytea must start with \\x"));
    std::string_view hex = s.substr(2);
    if (hex.size() % 2 != 0)
        return std::unexpected(err("bytea hex length must be even"));
    out.data.clear();
    out.data.reserve(hex.size() / 2);
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        auto hi = hex_to_nibble(hex[i]);
        auto lo = hex_to_nibble(hex[i + 1]);
        if (!hi || !lo)
            return std::unexpected(err("bytea contains non-hex character"));
        out.data.push_back(static_cast<std::uint8_t>((*hi << 4) | *lo));
    }
    return {};
}

inline std::string encode_uuid(const uuid& u) {
    // 8-4-4-4-12 hex groups (36 chars).
    std::string out;
    out.reserve(36);
    constexpr std::array<int, 5> groups{4, 2, 2, 2, 6};
    std::size_t byte = 0;
    bool first_group = true;
    for (int g : groups) {
        if (!first_group)
            out += '-';
        first_group = false;
        for (int i = 0; i < g; ++i, ++byte) {
            out += nibble_to_hex(static_cast<std::uint8_t>(u.bytes[byte] >> 4));
            out += nibble_to_hex(static_cast<std::uint8_t>(u.bytes[byte] & 0x0f));
        }
    }
    return out;
}

inline result<void> decode_uuid(std::string_view s, uuid& out) {
    auto err = [](std::string msg) {
        db_error e;
        e.kind = db_error_kind::parse_failed;
        e.message = std::move(msg);
        return e;
    };
    if (s.size() != 36)
        return std::unexpected(err("uuid must be 36 characters"));
    std::size_t byte = 0;
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (s[i] != '-')
                return std::unexpected(err("uuid missing dash separator"));
            continue;
        }
        auto hi = hex_to_nibble(s[i]);
        auto lo = hex_to_nibble(s[i + 1]);
        if (!hi || !lo)
            return std::unexpected(err("uuid contains non-hex character"));
        out.bytes[byte++] = static_cast<std::uint8_t>((*hi << 4) | *lo);
        ++i; // consumed second nibble
    }
    return {};
}

inline std::string encode_timestamp(std::chrono::system_clock::time_point tp) {
    using namespace std::chrono;
    const auto secs = time_point_cast<seconds>(tp);
    const auto us = duration_cast<microseconds>(tp - secs).count();
    const std::time_t t = system_clock::to_time_t(secs);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[64];
    int n = std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06lld+00",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                          tm.tm_sec, static_cast<long long>(us));
    return std::string(buf, buf + (n > 0 ? n : 0));
}

inline result<void> decode_timestamp(std::string_view s,
                                     std::chrono::system_clock::time_point& out) {
    // Accepts "YYYY-MM-DD HH:MM:SS[.ffffff][+HH[:MM]|Z]" (PG timestamptz text).
    auto err = [](std::string msg) {
        db_error e;
        e.kind = db_error_kind::parse_failed;
        e.message = std::move(msg);
        return e;
    };
    int y{}, mo{}, d{}, h{}, mi{}, se{};
    long long us = 0;
    int tz_h = 0, tz_m = 0;
    char tz_sign = '+';
    // Fast parse with sscanf — sufficient for libpq text output.
    char rest[32] = {};
    int consumed = 0;
    int fields = std::sscanf(std::string(s).c_str(), "%d-%d-%d %d:%d:%d%31s%n", &y, &mo, &d, &h,
                             &mi, &se, rest, &consumed);
    if (fields < 6)
        return std::unexpected(err("malformed timestamp"));
    std::string_view tail{rest};
    if (!tail.empty() && tail.front() == '.') {
        std::size_t i = 1;
        long long frac = 0;
        int digits = 0;
        while (i < tail.size() && tail[i] >= '0' && tail[i] <= '9' && digits < 6) {
            frac = frac * 10 + (tail[i] - '0');
            ++i;
            ++digits;
        }
        while (digits < 6) {
            frac *= 10;
            ++digits;
        }
        us = frac;
        tail = tail.substr(i);
    }
    if (!tail.empty() && (tail.front() == '+' || tail.front() == '-')) {
        tz_sign = tail.front();
        tail = tail.substr(1);
        int parsed = std::sscanf(std::string(tail).c_str(), "%d:%d", &tz_h, &tz_m);
        if (parsed < 1)
            return std::unexpected(err("malformed timezone"));
    }
    std::tm tm{};
    tm.tm_year = y - 1900;
    tm.tm_mon = mo - 1;
    tm.tm_mday = d;
    tm.tm_hour = h;
    tm.tm_min = mi;
    tm.tm_sec = se;
    // timegm is POSIX; use _mkgmtime on Windows.
#if defined(_WIN32)
    std::time_t tt = _mkgmtime(&tm);
#else
    std::time_t tt = timegm(&tm);
#endif
    if (tt == -1)
        return std::unexpected(err("invalid calendar date"));
    auto tp = std::chrono::system_clock::from_time_t(tt);
    tp += std::chrono::microseconds{us};
    int offset_seconds = (tz_h * 3600 + tz_m * 60) * (tz_sign == '-' ? -1 : 1);
    tp -= std::chrono::seconds{offset_seconds};
    out = tp;
    return {};
}

inline bool is_valid_numeric(std::string_view s) {
    if (s.empty())
        return false;

    std::size_t i = 0;
    if (s[i] == '+' || s[i] == '-')
        ++i;

    bool saw_digit = false;
    while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        saw_digit = true;
        ++i;
    }

    if (i < s.size() && s[i] == '.') {
        ++i;
        while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
            saw_digit = true;
            ++i;
        }
    }

    if (!saw_digit)
        return false;

    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
        ++i;
        if (i < s.size() && (s[i] == '+' || s[i] == '-'))
            ++i;
        bool saw_exponent_digit = false;
        while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
            saw_exponent_digit = true;
            ++i;
        }
        if (!saw_exponent_digit)
            return false;
    }

    return i == s.size();
}

inline result<void> decode_date(std::string_view s, date& out) {
    auto err = [](std::string msg) {
        db_error e;
        e.kind = db_error_kind::parse_failed;
        e.message = std::move(msg);
        return e;
    };

    int y = 0;
    unsigned mo = 0;
    unsigned d = 0;
    char tail = '\0';
    if (std::sscanf(std::string(s).c_str(), "%d-%u-%u%c", &y, &mo, &d, &tail) != 3)
        return std::unexpected(err("malformed date"));

    using namespace std::chrono;
    year_month_day ymd{year{y}, month{mo}, day{d}};
    if (!ymd.ok())
        return std::unexpected(err("invalid calendar date"));

    out = date{.year = y, .month = mo, .day = d};
    return {};
}

inline result<void> decode_time(std::string_view s, time_of_day& out) {
    auto err = [](std::string msg) {
        db_error e;
        e.kind = db_error_kind::parse_failed;
        e.message = std::move(msg);
        return e;
    };

    int h = 0;
    int mi = 0;
    int se = 0;
    int consumed = 0;
    if (std::sscanf(std::string(s).c_str(), "%d:%d:%d%n", &h, &mi, &se, &consumed) != 3)
        return std::unexpected(err("malformed time"));

    if (h < 0 || h > 23 || mi < 0 || mi > 59 || se < 0 || se > 59)
        return std::unexpected(err("invalid time"));

    std::size_t i = static_cast<std::size_t>(consumed);
    unsigned us = 0;
    if (i < s.size() && s[i] == '.') {
        ++i;
        unsigned digits = 0;
        while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
            if (digits < 6) {
                us = us * 10 + static_cast<unsigned>(s[i] - '0');
                ++digits;
            }
            ++i;
        }
        if (digits == 0)
            return std::unexpected(err("malformed time fraction"));
        while (digits < 6) {
            us *= 10;
            ++digits;
        }
    }

    if (i != s.size())
        return std::unexpected(err("malformed time"));

    out = time_of_day{.hour = static_cast<unsigned>(h),
                      .minute = static_cast<unsigned>(mi),
                      .second = static_cast<unsigned>(se),
                      .microsecond = us};
    return {};
}

inline result<void> decode_numeric(std::string_view s, numeric& out) {
    if (!is_valid_numeric(s)) {
        db_error err;
        err.kind = db_error_kind::parse_failed;
        err.message = "malformed numeric";
        return std::unexpected(err);
    }
    out.value = std::string{s};
    return {};
}
} // namespace detail

template <typename T> std::optional<std::string> encode(const T& val) {
    using DecayedT = std::decay_t<T>;
    if constexpr (is_optional_v<DecayedT>) {
        if (!val)
            return std::nullopt;
        return encode(*val);
    } else if constexpr (detail::has_custom_codec<DecayedT>::value) {
        return codec<DecayedT>::encode(val);
    } else if constexpr (std::is_enum_v<DecayedT> && detail::has_enum_traits<DecayedT>::value) {
        return std::string(enum_traits<DecayedT>::to_db(val));
    } else if constexpr (std::is_enum_v<DecayedT>) {
        return std::to_string(static_cast<std::underlying_type_t<DecayedT>>(val));
    } else if constexpr (std::is_convertible_v<T, std::string_view>) {
        return std::string(std::string_view(val));
    } else if constexpr (std::is_same_v<DecayedT, bool>) {
        return val ? "t" : "f";
    } else if constexpr (std::is_integral_v<DecayedT>) {
        return std::to_string(val);
    } else if constexpr (std::is_floating_point_v<DecayedT>) {
        // %.17g round-trips IEEE 754 doubles; narrower for float.
        char buf[64];
        int n = std::snprintf(buf, sizeof(buf), std::is_same_v<DecayedT, float> ? "%.9g" : "%.17g",
                              static_cast<double>(val));
        return std::string(buf, buf + (n > 0 ? n : 0));
    } else if constexpr (std::is_same_v<DecayedT, asterorm::jsonb>) {
        return val.value;
    } else if constexpr (std::is_same_v<DecayedT, asterorm::bytea>) {
        return detail::encode_bytea(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::uuid>) {
        return detail::encode_uuid(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::date>) {
        char buf[16];
        int n = std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u", val.year, val.month, val.day);
        return std::string(buf, buf + (n > 0 ? n : 0));
    } else if constexpr (std::is_same_v<DecayedT, asterorm::time_of_day>) {
        char buf[32];
        int n = std::snprintf(buf, sizeof(buf), "%02u:%02u:%02u.%06u", val.hour, val.minute,
                              val.second, val.microsecond);
        return std::string(buf, buf + (n > 0 ? n : 0));
    } else if constexpr (std::is_same_v<DecayedT, asterorm::numeric>) {
        return val.value;
    } else if constexpr (std::is_same_v<DecayedT, std::chrono::system_clock::time_point>) {
        return detail::encode_timestamp(val);
    } else if constexpr (is_vector_v<DecayedT>) {
        std::string res = "{";
        for (size_t i = 0; i < val.size(); ++i) {
            auto enc = encode(val[i]);
            if (enc) {
                using ElemT = typename DecayedT::value_type;
                if constexpr (std::is_convertible_v<ElemT, std::string_view>) {
                    std::string escaped = "\"";
                    for (char c : *enc) {
                        if (c == '"' || c == '\\')
                            escaped += '\\';
                        escaped += c;
                    }
                    escaped += "\"";
                    res += escaped;
                } else {
                    res += *enc;
                }
            } else {
                res += "NULL";
            }
            if (i < val.size() - 1) {
                res += ",";
            }
        }
        res += "}";
        return res;
    } else {
        static_assert(sizeof(T) == 0, "Unsupported type for encoding");
    }
}

template <typename T> std::optional<std::string> encode(const std::optional<T>& val) {
    if (!val) {
        return std::nullopt;
    }
    return encode(*val);
}

template <typename T> asterorm::result<void> decode(const std::optional<std::string>& str, T& val) {
    if constexpr (is_optional_v<T>) {
        if (!str) {
            val = std::nullopt;
            return {};
        }
        typename T::value_type temp{};
        auto res = decode(str, temp);
        if (res)
            val = std::move(temp);
        return res;
    } else {
        if (!str) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "Cannot decode SQL NULL into a non-optional value";
            return std::unexpected(err);
        }

        if constexpr (detail::has_custom_codec<T>::value) {
            return codec<T>::decode(str, val);
        } else if constexpr (std::is_enum_v<T> && detail::has_enum_traits<T>::value) {
            auto parsed = enum_traits<T>::from_db(*str);
            if (!parsed) {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "Failed to parse enum value";
                return std::unexpected(err);
            }
            val = *parsed;
            return {};
        } else if constexpr (std::is_enum_v<T>) {
            std::underlying_type_t<T> raw{};
            auto decoded = decode(str, raw);
            if (!decoded)
                return std::unexpected(decoded.error());
            val = static_cast<T>(raw);
            return {};
        } else if constexpr (std::is_same_v<T, std::string>) {
            val = *str;
            return {};
        } else if constexpr (std::is_same_v<T, bool>) {
            if (*str == "t" || *str == "true" || *str == "1") {
                val = true;
                return {};
            }
            if (*str == "f" || *str == "false" || *str == "0") {
                val = false;
                return {};
            }
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "Failed to parse boolean value";
            return std::unexpected(err);
        } else if constexpr (std::is_same_v<T, char>) {
            if (str->size() != 1) {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "Failed to parse char value";
                return std::unexpected(err);
            }
            val = (*str)[0];
            return {};
        } else if constexpr (std::is_integral_v<T>) {
            auto [ptr, ec] = std::from_chars(str->data(), str->data() + str->size(), val);
            if (ec != std::errc{} || ptr != str->data() + str->size()) {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "Failed to parse numeric value";
                return std::unexpected(err);
            }
            return {};
        } else if constexpr (std::is_floating_point_v<T>) {
            // libc++ lacks std::from_chars for float/double; fall back to strtod.
            const char* begin = str->c_str();
            char* end = nullptr;
            errno = 0;
            double v = std::strtod(begin, &end);
            if (end == begin || errno == ERANGE || end != begin + str->size()) {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "Failed to parse floating-point value";
                return std::unexpected(err);
            }
            val = static_cast<T>(v);
            return {};
        } else if constexpr (std::is_same_v<T, asterorm::jsonb>) {
            val.value = *str;
            return {};
        } else if constexpr (std::is_same_v<T, asterorm::bytea>) {
            return detail::decode_bytea(*str, val);
        } else if constexpr (std::is_same_v<T, asterorm::uuid>) {
            return detail::decode_uuid(*str, val);
        } else if constexpr (std::is_same_v<T, asterorm::date>) {
            return detail::decode_date(*str, val);
        } else if constexpr (std::is_same_v<T, asterorm::time_of_day>) {
            return detail::decode_time(*str, val);
        } else if constexpr (std::is_same_v<T, asterorm::numeric>) {
            return detail::decode_numeric(*str, val);
        } else if constexpr (std::is_same_v<T, std::chrono::system_clock::time_point>) {
            return detail::decode_timestamp(*str, val);
        } else if constexpr (is_vector_v<T>) {
            val.clear();
            const std::string& s = *str;
            if (s.size() < 2 || s.front() != '{' || s.back() != '}') {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "Invalid array format";
                return std::unexpected(err);
            }

            if (s.size() == 2) {
                return {};
            } // empty array "{}"

            using ElemT = typename T::value_type;
            std::string current;
            bool in_quotes = false;
            bool escaped = false;

            auto push_item = [&]() -> asterorm::result<void> {
                ElemT item{};
                std::optional<std::string> item_str;
                if (!in_quotes && current == "NULL") {
                    item_str = std::nullopt;
                } else {
                    item_str = current;
                }
                auto res = decode(item_str, item);
                if (!res) {
                    return res;
                }
                val.push_back(std::move(item));
                current.clear();
                return {};
            };

            for (size_t i = 1; i < s.size() - 1; ++i) {
                char c = s[i];
                if (escaped) {
                    current += c;
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    in_quotes = !in_quotes;
                } else if (c == ',' && !in_quotes) {
                    auto res = push_item();
                    if (!res) {
                        return res;
                    }
                } else {
                    current += c;
                }
            }

            auto res = push_item();
            if (!res) {
                return res;
            }

            return {};
        } else {
            static_assert(sizeof(T) == 0, "Unsupported type for decoding");
        }
    }
}

template <typename T>
asterorm::result<void> decode(const std::optional<std::string>& str, std::optional<T>& val) {
    if (!str) {
        val = std::nullopt;
        return {};
    } else {
        T temp{};
        auto res = decode(str, temp);
        if (res) {
            val = std::move(temp);
        }
        return res;
    }
}

} // namespace asterorm
