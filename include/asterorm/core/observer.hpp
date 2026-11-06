#pragma once
#include <chrono>
#include <functional>
#include <string>
#include <string_view>

#include "asterorm/core/error.hpp"

namespace asterorm {

// Structured event emitted for every query the session executes.
// `elapsed` covers round-trip from dispatch to response (not including result decode).
// `error` is non-null iff the query failed.
// `sql` is owned by the event and stays valid for as long as the event does.
// The callback runs while the session still holds the connection lease. Do not
// call `connection_pool::close` from an observer: close waits for outstanding
// leases and will stall until its timeout.
struct query_event {
    std::string sql;
    std::size_t param_count{0};
    std::chrono::nanoseconds elapsed{};
    const db_error* error{nullptr};
};

using query_observer = std::function<void(const query_event&)>;

} // namespace asterorm
