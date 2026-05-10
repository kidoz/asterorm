#include "asterorm/core/error.hpp"

namespace asterorm {

db_error_kind classify_sqlstate(std::string_view s) noexcept {
    if (s.empty())
        return db_error_kind::unknown;

    // Specific codes first.
    if (s == "40001")
        return db_error_kind::serialization_failure;
    if (s == "40P01")
        return db_error_kind::deadlock_detected;

    // Class-level mapping (first 2 chars). Use starts_with so this stays
    // noexcept; substr() can throw std::out_of_range and triggers
    // bugprone-exception-escape under clang-tidy.
    if (s.starts_with("08"))
        return db_error_kind::connection_failed;
    if (s.starts_with("23"))
        return db_error_kind::constraint_violation;
    if (s.starts_with("22"))
        return db_error_kind::parse_failed;
    if (s.starts_with("57"))
        return db_error_kind::connection_failed; // operator intervention

    return db_error_kind::query_failed;
}

} // namespace asterorm
