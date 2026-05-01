#pragma once
#include <charconv>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "asterorm/core/error.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/postgres/types.hpp"

namespace asterorm {

template <typename T> struct is_optional : std::false_type {};

template <typename T> struct is_optional<std::optional<T>> : std::true_type {};

template <typename T> inline constexpr bool is_optional_v = is_optional<T>::value;

template <typename T> struct is_vector : std::false_type {};

template <typename T, typename A> struct is_vector<std::vector<T, A>> : std::true_type {};

template <typename T> inline constexpr bool is_vector_v = is_vector<T>::value;

template <typename T> std::optional<std::string> encode(const T& val) {
    using DecayedT = std::decay_t<T>;
    if constexpr (std::is_convertible_v<T, std::string_view>) { // NOLINT(bugprone-branch-clone)
        return std::string(std::string_view(val));
    } else if constexpr (std::is_same_v<DecayedT, bool>) {
        return val ? "t" : "f";
    } else if constexpr (std::is_integral_v<DecayedT>) {
        return std::to_string(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::pg::jsonb>) {
        return val.value;
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
    if (!str) {
        return {}; // leave as default or unassigned if null
    }

    if constexpr (std::is_same_v<T, std::string>) {
        val = *str;
        return {};
    } else if constexpr (std::is_same_v<T, bool>) {
        val = (*str == "t" || *str == "true" || *str == "1");
        return {};
    } else if constexpr (std::is_integral_v<T>) {
        auto [ptr, ec] = std::from_chars(str->data(), str->data() + str->size(), val);
        if (ec != std::errc{}) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "Failed to parse numeric value";
            return std::unexpected(err);
        }
        return {};
    } else if constexpr (std::is_same_v<T, asterorm::pg::jsonb>) {
        val.value = *str;
        return {};
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