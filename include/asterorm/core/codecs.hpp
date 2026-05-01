#pragma once
#include <charconv>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "asterorm/postgres/types.hpp"

namespace asterorm {

template <typename T>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<std::optional<T>> : std::true_type {};

template <typename T>
inline constexpr bool is_optional_v = is_optional<T>::value;

template <typename T>
std::optional<std::string> encode(const T& val) {
    using DecayedT = std::decay_t<T>;
    if constexpr (std::is_convertible_v<T, std::string_view>) { // NOLINT(bugprone-branch-clone)
        return std::string(std::string_view(val));
    } else if constexpr (std::is_same_v<DecayedT, bool>) {
        return val ? "t" : "f";
    } else if constexpr (std::is_integral_v<DecayedT>) {
        return std::to_string(val);
    } else if constexpr (std::is_same_v<DecayedT, asterorm::pg::jsonb>) {
        return val.value;
    } else {
        static_assert(sizeof(T) == 0, "Unsupported type for encoding");
    }
    }

    template <typename T>
    std::optional<std::string> encode(const std::optional<T>& val) {
    if (!val) {
        return std::nullopt;
    }
    return encode(*val);
    }

    template <typename T>
    void decode(const std::optional<std::string>& str, T& val) {
    if (!str) {
        return;  // leave as default or unassigned if null
    }

    if constexpr (std::is_same_v<T, std::string>) {
        val = *str;
    } else if constexpr (std::is_same_v<T, bool>) {
        val = (*str == "t" || *str == "true" || *str == "1");
    } else if constexpr (std::is_integral_v<T>) {
        std::from_chars(str->data(), str->data() + str->size(), val);
    } else if constexpr (std::is_same_v<T, asterorm::pg::jsonb>) {
        val.value = *str;
    } else {
        static_assert(sizeof(T) == 0, "Unsupported type for decoding");
    }
}

template <typename T>
void decode(const std::optional<std::string>& str, std::optional<T>& val) {
    if (!str) {
        val = std::nullopt;
    } else {
        T temp;
        decode(str, temp);
        val = temp;
    }
}

}  // namespace asterorm