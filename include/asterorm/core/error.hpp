#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace asterorm {

enum class db_error_kind : std::uint8_t {
    connection_failed,
    query_failed,
    parse_failed,
    constraint_violation,
    serialization_failure,
    deadlock_detected,
    stale_write,
    timeout,
    unknown
};

struct db_error {
    db_error_kind kind{db_error_kind::unknown};
    std::string sqlstate;
    std::string message;
    std::optional<std::string> detail;
    std::optional<std::string> hint;

    std::optional<std::string> table;
    std::optional<std::string> column;
    std::optional<std::string> constraint;
};

// Map a SQLSTATE (5-char code) to a db_error_kind. Defaults to query_failed
// when the state is non-empty but not classified, or unknown for empty input.
// See https://www.postgresql.org/docs/current/errcodes-appendix.html
db_error_kind classify_sqlstate(std::string_view sqlstate) noexcept;

} // namespace asterorm
