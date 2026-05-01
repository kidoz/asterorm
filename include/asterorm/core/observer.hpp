#pragma once
#include <chrono>
#include <functional>
#include <string_view>

#include "asterorm/core/error.hpp"

namespace asterorm {

// Structured event emitted for every query the session executes.
// `elapsed` covers round-trip from dispatch to response (not including result decode).
// `error` is non-null iff the query failed.
struct query_event {
    std::string_view sql;
    std::size_t param_count{0};
    std::chrono::nanoseconds elapsed{};
    const db_error* error{nullptr};
};

using query_observer = std::function<void(const query_event&)>;

} // namespace asterorm
