#pragma once

#include <chrono>
#include <concepts>
#include <utility>

#include "asterorm/core/result.hpp"

namespace asterorm::detail {

inline db_error unsupported_statement_timeout() {
    db_error error;
    error.kind = db_error_kind::query_failed;
    error.message = "This backend does not support statement timeout configuration";
    return error;
}

inline asterorm::result<void> validate_statement_timeout(std::chrono::milliseconds duration) {
    if (duration.count() < 0 || duration.count() > 2147483647) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Statement timeout must be between 0 and 2147483647 milliseconds";
        return std::unexpected(std::move(error));
    }
    return {};
}

template <typename Connection>
concept has_statement_timeout = requires(Connection& connection, const Connection& constant,
                                         std::chrono::milliseconds duration) {
    { connection.statement_timeout() } -> std::same_as<asterorm::result<std::chrono::milliseconds>>;
    { connection.set_statement_timeout(duration) } -> std::same_as<asterorm::result<void>>;
    { connection.set_local_statement_timeout(duration) } -> std::same_as<asterorm::result<void>>;
    { constant.is_transaction_idle() } noexcept -> std::same_as<bool>;
    { connection.close() } noexcept -> std::same_as<void>;
};

} // namespace asterorm::detail
