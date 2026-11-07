#pragma once
#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "asterorm/core/observer.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/sql/compiler.hpp"

namespace asterorm::detail {
inline constexpr std::string_view testing_savepoint_prefix = "_asterorm_test_";

inline db_error testing_scope_error(const char* message) {
    db_error error;
    error.kind = db_error_kind::query_failed;
    error.message = message;
    return error;
}

template <typename Connection> constexpr bool supports_testing_scope() {
    if constexpr (requires(Connection& connection) {
                      Connection::supports_transactional_test_scope;
                      requires std::same_as<decltype(Connection::supports_transactional_test_scope),
                                            const bool>;
                      Connection::sql_dialect;
                      { connection.close() } noexcept -> std::same_as<void>;
                      { connection.is_transaction_idle() } noexcept -> std::same_as<bool>;
                  }) {
        return Connection::supports_transactional_test_scope &&
               (Connection::sql_dialect == sql::dialect::postgres ||
                Connection::sql_dialect == sql::dialect::sqlite);
    }
    return false;
}

// Shared only to keep ownership alive across observer destruction/reentrant cleanup.
// Session/pool remain borrowed and must outlive the public testing scope.
struct testing_scope_state {
    std::uint64_t generation{};
    std::uint64_t transaction_id{};
    std::string begin_sql;
    std::string savepoint_sql;
    std::string rollback_sql;
    std::string release_sql;
    query_observer observer;
    std::optional<db_error> failure;
    db_error exception_error{
        testing_scope_error("Transactional test scope terminated by an exception")};
    db_error lost_error{
        testing_scope_error("Transactional test scope lost its transaction or connection")};
    db_error abandoned_error{
        testing_scope_error("Transactional test scope abandoned during an active callback")};
    bool done{};
    bool finishing{};
    bool observer_restored{};
};

struct testing_session_access {
    template <typename Session>
    static result<std::shared_ptr<testing_scope_state>> create(Session& owner) {
        if constexpr (requires {
                          {
                              owner.create_testing_scope()
                          } -> std::same_as<result<std::shared_ptr<testing_scope_state>>>;
                      }) {
            return owner.create_testing_scope();
        } else {
            return std::unexpected(
                testing_scope_error("This session does not support transactional test scopes"));
        }
    }
    template <typename Session>
    static result<void> finish(Session& owner, const std::shared_ptr<testing_scope_state>& state) {
        if constexpr (requires {
                          { owner.finish_testing_scope(state) } -> std::same_as<result<void>>;
                      }) {
            return owner.finish_testing_scope(state);
        } else {
            return std::unexpected(testing_scope_error("Unsupported transactional test scope"));
        }
    }
    template <typename Session>
    static void abandon(Session& owner,
                        const std::shared_ptr<testing_scope_state>& state) noexcept {
        if constexpr (requires {
                          { owner.abandon_testing_scope(state) } noexcept -> std::same_as<void>;
                      }) {
            owner.abandon_testing_scope(state);
        }
    }
    template <typename Session>
    static bool active(Session& owner, const std::shared_ptr<testing_scope_state>& state) {
        if constexpr (requires {
                          { owner.testing_scope_active(state) } -> std::same_as<bool>;
                      }) {
            return owner.testing_scope_active(state);
        } else {
            return false;
        }
    }
};
} // namespace asterorm::detail
