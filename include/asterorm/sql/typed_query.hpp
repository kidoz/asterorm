#pragma once
#include <concepts>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/core/error.hpp"
#include "asterorm/core/hydration.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/sql/compiler.hpp"
#include "asterorm/sql/typed.hpp"
#include "asterorm/sql/window.hpp"

// Bridge between the typed DSL (sql/typed.hpp) and a session/connection. A
// free function instead of a repository method so users opt in by including
// this header; the heavy template work doesn't bleed into core repository
// translation units.

namespace asterorm::sql::typed {

namespace detail {

template <typename Session> constexpr bool window_session() {
    if constexpr (requires { typename Session::connection_type; }) {
        using connection_type = typename Session::connection_type;
        if constexpr (requires { connection_type::sql_dialect; }) {
            if constexpr (std::same_as<std::remove_cv_t<decltype(connection_type::sql_dialect)>,
                                       dialect>) {
                return (connection_type::sql_dialect == dialect::postgres ||
                        connection_type::sql_dialect == dialect::sqlite ||
                        connection_type::sql_dialect == dialect::mysql) &&
                    requires(Session & owner, connection_type & connection,
                             const compiled_query & statement)
                {
                    {owner.observed_execute(connection, statement.sql, statement.params)}
                        ->std::same_as<decltype(connection.execute_prepared(statement.sql,
                                                                            statement.params))>;
                };
            }
        }
    }
    return false;
}
template <typename Session, typename Connection>
auto execute_select(Session& session, Connection& connection, const compiled_query& statement) {
    if constexpr (requires {
                      {
                          session.observed_execute(connection, statement.sql, statement.params)
                      } -> std::same_as<decltype(connection.execute_prepared(statement.sql,
                                                                             statement.params))>;
                  }) {
        return session.observed_execute(connection, statement.sql, statement.params);
    } else {
        return connection.execute_prepared(statement.sql, statement.params);
    }
}

template <typename Entity, typename Result, typename Res>
asterorm::result<void> hydrate_whole_entity(const Res& result_set, int row, Result& out) {
    using traits = entity_traits<Entity>;
    return ::asterorm::detail::hydrate_entity(
        result_set, row, out,
        std::make_index_sequence<std::tuple_size_v<std::decay_t<decltype(traits::columns)>>>{});
}

template <typename DTO, typename... Items, typename Res>
asterorm::result<DTO> hydrate_dto(const Res& result_set, int row) {
    std::tuple<typename Items::projected_type...> field_values{};
    asterorm::result<void> first_decode_error;

    auto decode_at = [&]<std::size_t Index>(auto& destination) {
        if (!first_decode_error) {
            return;
        }
        auto raw_value = result_set.get_string(row, static_cast<int>(Index));
        auto step_error = asterorm::decode(raw_value, destination);
        if (!step_error) {
            first_decode_error = std::move(step_error);
        }
    };

    [&]<std::size_t... Indices>(std::index_sequence<Indices...>) {
        (decode_at.template operator()<Indices>(std::get<Indices>(field_values)), ...);
    }(std::make_index_sequence<sizeof...(Items)>{});

    if (!first_decode_error) {
        return std::unexpected(first_decode_error.error());
    }
    return std::apply(
        [](auto&&... values) { return DTO{std::forward<decltype(values)>(values)...}; },
        std::move(field_values));
}

} // namespace detail

// Execute a typed SELECT against the session and hydrate the rows into the
// builder's Result type. Whole-entity builders (sql::typed::select<Entity>())
// hydrate via entity_traits<Entity>; projection builders (select_cols<DTO> or
// inferred select_cols) initialise the DTO or tuple from each row positionally
// using the typelist of projected items.
// Accept the builder by either rvalue (temporary chain) or lvalue (named).
// The lvalue overload forwards to the rvalue one; both branch on the typelist
// to pick whole-entity vs. DTO hydration.
// Prefer a compatible session observed_execute helper for query events.
// Custom facades without that helper retain direct
// prepared execution through their with_connection callback.
template <typename Session, typename Entity, typename Result, typename... Items>
asterorm::result<std::vector<Result>>
query(Session& session, typed_select_builder<Entity, Result, Items...>& builder) {
    return query(session, std::move(builder));
}

template <typename Session, typename Entity, typename Result, typename... Items>
asterorm::result<std::vector<Result>>
query(Session& session, typed_select_builder<Entity, Result, Items...>&& builder) {
    auto select_ast_value = builder.build();
    if ((select_ast_value.distinct_on || window_detail::contains_window(select_ast_value)) &&
        !detail::window_session<Session>()) {
        db_error error;
        error.kind = db_error_kind::parse_failed;
        error.message = "Structured aggregates/DISTINCT ON require an explicit supported dialect "
                        "and compatible observed_execute";
        return std::unexpected(error);
    }
    if ((select_ast_value.locking.strength != select_lock_strength::none ||
         select_ast_value.locking.wait != select_lock_wait::block) &&
        !asterorm::sql::session_supports_select_locking<Session>()) {
        db_error error;
        error.kind = db_error_kind::parse_failed;
        error.message = "SELECT locking requires an explicit supported connection SQL dialect";
        return std::unexpected(std::move(error));
    }
    asterorm::sql::compiler statement_compiler{asterorm::sql::dialect_for_session<Session>()};
    auto compiled_query = statement_compiler.compile(select_ast_value);
    if (!compiled_query)
        return std::unexpected(compiled_query.error());

    auto execution_result = session.with_connection([&](auto& connection) {
        return detail::execute_select(session, connection, *compiled_query);
    });
    if (!execution_result) {
        return std::unexpected(execution_result.error());
    }

    std::vector<Result> rows;
    rows.reserve(static_cast<std::size_t>(execution_result->rows()));
    for (int row_index = 0; row_index < execution_result->rows(); ++row_index) {
        if constexpr (sizeof...(Items) == 0) {
            Result entity_row{};
            auto hydration_error =
                detail::hydrate_whole_entity<Entity>(*execution_result, row_index, entity_row);
            if (!hydration_error) {
                return std::unexpected(hydration_error.error());
            }
            rows.push_back(std::move(entity_row));
        } else {
            auto dto_or_error = detail::hydrate_dto<Result, Items...>(*execution_result, row_index);
            if (!dto_or_error) {
                return std::unexpected(dto_or_error.error());
            }
            rows.push_back(std::move(*dto_or_error));
        }
    }
    return rows;
}

} // namespace asterorm::sql::typed
