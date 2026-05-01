#pragma once
#include <optional>
#include <string>

namespace asterorm {

enum class db_error_kind : std::uint8_t {
    connection_failed,
    query_failed,
    parse_failed,
    constraint_violation,
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

} // namespace asterorm
