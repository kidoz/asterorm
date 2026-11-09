#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "asterorm/core/result.hpp"
#include "asterorm/core/types.hpp"

namespace asterorm::detail {
// Internal text grammar entry points. Each decoder commits only after success.
std::optional<std::string> encode_timestamp(std::chrono::system_clock::time_point timestamp);
result<void> decode_timestamp(std::string_view text, std::chrono::system_clock::time_point& output);
result<void> decode_date(std::string_view text, date& output);
bool is_valid_numeric(std::string_view text);
std::optional<std::string> encode_interval(const interval& value);
result<void> decode_interval(std::string_view text, interval& output);
std::optional<std::string> encode_network(const network& value);
result<void> decode_network(std::string_view text, network& output);
bool has_zero_host_bits(const network& value);
std::string encode_macaddr(const macaddr& value);
result<void> decode_macaddr(std::string_view text, macaddr& output);
result<std::vector<std::optional<std::string>>> parse_flat_array(std::string_view text);
} // namespace asterorm::detail
