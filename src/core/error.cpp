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

    // Class-level mapping (first 2 chars).
    if (s.size() >= 2) {
        const auto cls = s.substr(0, 2);
        if (cls == "08")
            return db_error_kind::connection_failed;
        if (cls == "23")
            return db_error_kind::constraint_violation;
        if (cls == "22")
            return db_error_kind::parse_failed;
        if (cls == "57")
            return db_error_kind::connection_failed; // operator intervention
    }

    return db_error_kind::query_failed;
}

} // namespace asterorm
