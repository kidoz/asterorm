#pragma once
#include <chrono>
#include <functional>
#include <string>
#include <string_view>

#include "asterorm/core/error.hpp"

namespace asterorm {

// Structured event emitted for every query the session executes.
// `elapsed` covers round-trip from dispatch to response (not including result decode).
// Row streams instead accumulate adapter open/read/close time, excluding hydration
// and consumer pauses, and emit once on EOF/error/explicit close. Abandonment emits
// no event. The observer is snapshotted when a stream opens.
// `error` is non-null iff the query failed.
// Its pointee is valid during the callback; copy diagnostics that must outlive it.
// `sql` is owned by the event and stays valid for as long as the event does.
// The callback runs while the session still holds the connection lease, except
// a stream invalidated earlier by its parent guard reports after losing its pin.
// Do not call `connection_pool::close` from an observer: close waits for outstanding
// leases and will stall until its timeout.
struct query_event {
    std::string sql;
    std::size_t param_count{0};
    std::chrono::nanoseconds elapsed{};
    const db_error* error{nullptr};
};

using query_observer = std::function<void(const query_event&)>;

} // namespace asterorm
