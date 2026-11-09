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

#include "asterorm/core/detail/text_codecs.hpp"
#include "asterorm/core/error.hpp"
#include "asterorm/core/parameter.hpp"
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

template <typename T>
inline constexpr bool is_string_array_element_v = std::is_convertible_v<T, std::string_view>;

template <typename T>
inline constexpr bool is_string_array_element_v<std::optional<T>> =
    std::is_convertible_v<T, std::string_view>;

template <typename T> inline constexpr bool is_nested_array_v = is_vector_v<T>;
template <typename T>
inline constexpr bool is_nested_array_v<std::optional<T>> = is_nested_array_v<T>;

template <std::size_t Size>
std::optional<std::string> formatted_string(const char (&buffer)[Size], int length) {
    if (length < 0 || static_cast<std::size_t>(length) >= Size)
        return std::nullopt;
    return std::string(buffer, static_cast<std::size_t>(length));
}

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
        return detail::formatted_string(buf, n);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::jsonb>) {
        return val.value;
    } else if constexpr (std::is_same_v<DecayedT, asterorm::bytea>) {
        return detail::encode_bytea(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::uuid>) {
        return detail::encode_uuid(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::date>) {
        using namespace std::chrono;
        if (val.year < 1 || val.year > static_cast<int>(year::max()))
            return std::nullopt;
        if (val.month < 1 || val.month > 12 || val.day < 1 || val.day > 31)
            return std::nullopt;
        const year_month_day calendar_date{year{val.year}, month{val.month}, day{val.day}};
        if (!calendar_date.ok())
            return std::nullopt;
        char buf[16];
        int n = std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u", val.year, val.month, val.day);
        return detail::formatted_string(buf, n);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::time_of_day>) {
        if (val.hour > 23 || val.minute > 59 || val.second > 59 || val.microsecond > 999999)
            return std::nullopt;
        char buf[32];
        int n = std::snprintf(buf, sizeof(buf), "%02u:%02u:%02u.%06u", val.hour, val.minute,
                              val.second, val.microsecond);
        return detail::formatted_string(buf, n);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::interval>) {
        return detail::encode_interval(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::network>) {
        return detail::encode_network(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::cidr>) {
        if (!detail::has_zero_host_bits(val.value))
            return std::nullopt;
        return detail::encode_network(val.value);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::macaddr>) {
        return detail::encode_macaddr(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::numeric>) {
        if (!detail::is_valid_numeric(val.value))
            return std::nullopt;
        return val.value;
    } else if constexpr (std::is_same_v<DecayedT, std::chrono::system_clock::time_point>) {
        return detail::encode_timestamp(val);
    } else if constexpr (is_vector_v<DecayedT>) {
        using Element = typename DecayedT::value_type;
        if constexpr (detail::is_nested_array_v<Element>) {
            return std::nullopt; // Multidimensional arrays are explicitly unsupported.
        } else {
            std::string text = "{";
            for (std::size_t index = 0; index < val.size(); ++index) {
                if (index != 0)
                    text += ',';
                // vector<bool> returns a proxy rather than a bool reference.
                const auto encoded = [&] {
                    if constexpr (std::is_same_v<Element, bool>)
                        return encode(static_cast<bool>(val[index]));
                    else
                        return encode(val[index]);
                }();
                if (!encoded) {
                    bool is_null = false;
                    if constexpr (is_optional_v<Element>)
                        is_null = !val[index].has_value();
                    if (!is_null)
                        return std::nullopt;
                    text += "NULL";
                } else {
                    if (encoded->find('\0') != std::string::npos)
                        return std::nullopt;
                    const bool quote =
                        detail::is_string_array_element_v<Element> || encoded->empty() ||
                        (encoded->size() == 4 && ((*encoded)[0] == 'N' || (*encoded)[0] == 'n') &&
                         ((*encoded)[1] == 'U' || (*encoded)[1] == 'u') &&
                         ((*encoded)[2] == 'L' || (*encoded)[2] == 'l') &&
                         ((*encoded)[3] == 'L' || (*encoded)[3] == 'l')) ||
                        encoded->find_first_of(",{}\"\\ \t\r\n\f\v") != std::string::npos;
                    if (quote)
                        text += '"';
                    for (char character : *encoded) {
                        if (character == '"' || character == '\\')
                            text += '\\';
                        text += character;
                    }
                    if (quote)
                        text += '"';
                }
            }
            text += '}';
            return text;
        }
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
            T decoded{};
            auto [ptr, ec] = std::from_chars(str->data(), str->data() + str->size(), decoded);
            if (ec != std::errc{} || ptr != str->data() + str->size()) {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "Failed to parse numeric value";
                return std::unexpected(err);
            }
            val = decoded;
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
        } else if constexpr (std::is_same_v<T, asterorm::interval>) {
            return detail::decode_interval(*str, val);
        } else if constexpr (std::is_same_v<T, asterorm::network>) {
            return detail::decode_network(*str, val);
        } else if constexpr (std::is_same_v<T, asterorm::cidr>) {
            network decoded;
            auto status = detail::decode_network(*str, decoded);
            if (!status)
                return status;
            if (!detail::has_zero_host_bits(decoded))
                return std::unexpected(db_error{.kind = db_error_kind::parse_failed,
                                                .message = "CIDR contains host bits"});
            val.value = decoded;
            return {};
        } else if constexpr (std::is_same_v<T, asterorm::macaddr>) {
            return detail::decode_macaddr(*str, val);
        } else if constexpr (std::is_same_v<T, asterorm::numeric>) {
            return detail::decode_numeric(*str, val);
        } else if constexpr (std::is_same_v<T, std::chrono::system_clock::time_point>) {
            return detail::decode_timestamp(*str, val);
        } else if constexpr (is_vector_v<T>) {
            using Element = typename T::value_type;
            if constexpr (detail::is_nested_array_v<Element>) {
                return std::unexpected(
                    db_error{.kind = db_error_kind::parse_failed,
                             .message = "Multidimensional arrays are unsupported"});
            } else {
                auto elements = detail::parse_flat_array(*str);
                if (!elements)
                    return std::unexpected(elements.error());
                T decoded{val.get_allocator()};
                decoded.reserve(elements->size());
                for (const auto& element : *elements) {
                    Element item{};
                    auto status = decode(element, item);
                    if (!status)
                        return status;
                    decoded.push_back(std::move(item));
                }
                val = std::move(decoded);
                return {};
            }
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

template <typename T> query_parameter encode_parameter(const T& val) {
    using DecayedT = std::decay_t<T>;
    if constexpr (is_optional_v<DecayedT>) {
        using ValueT = typename DecayedT::value_type;
        constexpr auto type =
            std::is_same_v<ValueT, bool> ? parameter_type::boolean : parameter_type::text;
        query_parameter parameter{encode(val), type};
        if (!parameter.value && val.has_value()) {
            parameter.error = db_error{.kind = db_error_kind::parse_failed,
                                       .message = "Failed to encode query parameter"};
        }
        return parameter;
    } else {
        constexpr auto type =
            std::is_same_v<DecayedT, bool> ? parameter_type::boolean : parameter_type::text;
        query_parameter parameter{encode(val), type};
        if (!parameter.value) {
            parameter.error = db_error{.kind = db_error_kind::parse_failed,
                                       .message = "Failed to encode query parameter"};
        }
        return parameter;
    }
}

} // namespace asterorm
