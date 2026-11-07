#pragma once
#include <charconv>
#include <concepts>
#include <cstdint>
#include <string_view>
#include <utility>

#include "asterorm/core/result.hpp"

namespace asterorm::detail {
inline db_error invalid_row_count() {
    db_error error;
    error.kind = db_error_kind::query_failed;
    error.message = "Invalid or unrepresentable affected-row count";
    return error;
}

inline result<std::uint64_t> parse_row_count(std::string_view text) {
    if (text.empty()) {
        return std::unexpected(invalid_row_count());
    }
    std::uint64_t count{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), count);
    if (error != std::errc{} || end != text.data() + text.size()) {
        return std::unexpected(invalid_row_count());
    }
    return count;
}

template <std::unsigned_integral Destination, std::integral Source>
result<Destination> checked_row_count(Source count) {
    if (!std::in_range<Destination>(count)) {
        return std::unexpected(invalid_row_count());
    }
    return static_cast<Destination>(count);
}
} // namespace asterorm::detail
