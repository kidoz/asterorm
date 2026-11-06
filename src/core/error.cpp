#include "asterorm/core/error.hpp"

namespace asterorm {

db_error_kind classify_sqlstate(std::string_view sqlstate) noexcept {
    if (sqlstate.empty()) {
        return db_error_kind::unknown;
    }

    // Specific codes first.
    if (sqlstate == "40001") {
        return db_error_kind::serialization_failure;
    }
    if (sqlstate == "40P01") {
        return db_error_kind::deadlock_detected;
    }
    // Query cancellation does not imply a lost connection.
    if (sqlstate == "57014") {
        return db_error_kind::query_failed;
    }

    // Class-level mapping (first 2 chars). Use starts_with so this stays
    // noexcept; substr() can throw std::out_of_range and triggers
    // bugprone-exception-escape under clang-tidy.
    if (sqlstate.starts_with("08")) {
        return db_error_kind::connection_failed;
    }
    if (sqlstate.starts_with("23")) {
        return db_error_kind::constraint_violation;
    }
    if (sqlstate.starts_with("22")) {
        return db_error_kind::parse_failed;
    }
    if (sqlstate.starts_with("57")) {
        return db_error_kind::connection_failed; // operator intervention
    }

    return db_error_kind::query_failed;
}

} // namespace asterorm
