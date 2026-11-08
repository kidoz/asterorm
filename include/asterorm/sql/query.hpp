#pragma once

#include <algorithm>
#include <array>
#include <concepts>
#include <tuple>
#include <type_traits>
#include <utility>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/sql/compiler.hpp"

namespace asterorm::sql {
namespace query_detail {
inline db_error invalid_mapping(std::string message) {
    db_error diagnostic;
    diagnostic.kind = db_error_kind::parse_failed;
    diagnostic.message = std::move(message);
    return diagnostic;
}

template <typename Session> constexpr bool explicit_dialect() {
    if constexpr (requires { typename Session::connection_type; }) {
        using connection_type = typename Session::connection_type;
        if constexpr (requires { connection_type::sql_dialect; }) {
            return std::same_as<std::remove_cv_t<decltype(connection_type::sql_dialect)>, dialect>;
        }
    }
    return false;
}
} // namespace query_detail

// Materializes explicitly named trait columns. Table metadata is not required.
// A compatible observer helper is mandatory for advanced-query session facades.
template <typename Row, typename Session>
result<std::vector<Row>> query_as(Session& owner, const query_ast& ast,
                                  query_compile_options options = {}) {
    static_assert(std::default_initializable<Row>,
                  "query_as<Row> requires a default-constructible Row");
    static_assert(
        requires { entity_traits<Row>::columns; },
        "query_as<Row> requires entity_traits<Row>::columns");
    if constexpr (!requires { entity_traits<Row>::columns; }) {
        return std::unexpected(query_detail::invalid_mapping("query_as requires mapped columns"));
    } else if constexpr (!query_detail::explicit_dialect<Session>()) {
        return std::unexpected(
            query_detail::invalid_mapping("query_as requires an explicit SQL dialect"));
    } else {
        using connection_type = typename Session::connection_type;
        if constexpr (!requires(Session& session, connection_type& connection,
                                const compiled_query& statement) {
                          {
                              session.observed_execute(connection, statement.sql, statement.params)
                          } -> std::same_as<decltype(connection.execute_prepared(
                              statement.sql, statement.params))>;
                      }) {
            return std::unexpected(query_detail::invalid_mapping(
                "query_as requires a compatible observed_execute helper"));
        } else {
            auto statement = compiler{connection_type::sql_dialect, options}.compile(ast);
            if (!statement) {
                return std::unexpected(statement.error());
            }
            constexpr auto column_count = std::tuple_size_v<decltype(entity_traits<Row>::columns)>;
            std::array<std::string_view, column_count> mapped_names{};
            std::apply([&](const auto&... columns) { mapped_names = {columns.name...}; },
                       entity_traits<Row>::columns);
            if constexpr (column_count == 0) {
                return std::unexpected(
                    query_detail::invalid_mapping("query_as requires mapped columns"));
            }
            for (std::size_t index = 0; index < mapped_names.size(); ++index) {
                const auto name = mapped_names[index];
                if (name.empty() ||
                    std::find(mapped_names.begin(), mapped_names.begin() + index, name) !=
                        mapped_names.begin() + index ||
                    std::find(statement->result_columns.begin(), statement->result_columns.end(),
                              name) == statement->result_columns.end()) {
                    return std::unexpected(query_detail::invalid_mapping(
                        "query_as has a missing/duplicate mapped output label: " +
                        std::string{name}));
                }
            }
            auto native_result = owner.with_connection([&](auto& connection) {
                return owner.observed_execute(connection, statement->sql, statement->params);
            });
            if (!native_result) {
                return std::unexpected(native_result.error());
            }
            // Check metadata even for an empty result, before decoding any rows.
            if (native_result->columns() < 0 ||
                static_cast<std::size_t>(native_result->columns()) !=
                    statement->result_columns.size()) {
                return std::unexpected(query_detail::invalid_mapping(
                    "query_as result width differs from the validated projection"));
            }
            for (int index = 0; index < native_result->columns(); ++index) {
                if (native_result->column_name(index) !=
                    statement->result_columns[static_cast<std::size_t>(index)]) {
                    return std::unexpected(query_detail::invalid_mapping(
                        "query_as result labels differ from the validated projection"));
                }
            }
            std::array<int, column_count> positions{};
            for (std::size_t index = 0; index < column_count; ++index) {
                positions[index] = static_cast<int>(std::find(statement->result_columns.begin(),
                                                              statement->result_columns.end(),
                                                              mapped_names[index]) -
                                                    statement->result_columns.begin());
            }
            std::vector<Row> rows;
            for (int row_index = 0; row_index < native_result->rows(); ++row_index) {
                Row row{};
                result<void> decoded;
                [&]<std::size_t... Indices>(std::index_sequence<Indices...>) {
                    (
                        [&] {
                            if (decoded) {
                                const auto& mapping =
                                    std::get<Indices>(entity_traits<Row>::columns);
                                decoded = asterorm::decode(
                                    native_result->get_string(row_index, positions[Indices]),
                                    row.*(mapping.member_ptr));
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<column_count>{});
                if (!decoded) {
                    return std::unexpected(decoded.error());
                }
                rows.push_back(std::move(row));
            }
            return rows;
        }
    }
}
} // namespace asterorm::sql
