#pragma once
#include <optional>
#include <string>
#include <vector>

#include "asterorm/core/result.hpp"
#include "asterorm/sql/ast.hpp"

namespace asterorm::sql {

enum class dialect { postgres, mysql, sqlite, clickhouse };

[[nodiscard]] constexpr bool supports_select_locking(dialect target) {
    return target == dialect::postgres || target == dialect::mysql;
}

// Locking is opt-in for connection types; the legacy PostgreSQL-compatible
// fallback for ordinary SQL is not evidence of row-lock support.
template <typename Session> [[nodiscard]] constexpr bool session_supports_select_locking() {
    if constexpr (requires { typename Session::connection_type; }) {
        using connection_type = typename Session::connection_type;
        if constexpr (requires { connection_type::sql_dialect; }) {
            return supports_select_locking(connection_type::sql_dialect);
        }
    }
    return false;
}

template <typename Connection> constexpr dialect dialect_for() {
    if constexpr (requires { Connection::sql_dialect; })
        return Connection::sql_dialect;
    return dialect::postgres;
}

template <typename Session> constexpr dialect dialect_for_session() {
    if constexpr (requires { typename Session::connection_type; })
        return dialect_for<typename Session::connection_type>();
    return dialect::postgres;
}

struct compiled_query {
    std::string sql;
    query_parameters params;
};

class compiler {
  public:
    explicit compiler(dialect target = dialect::postgres) : dialect_(target) {}

    // Identifiers are quoted for `dialect`. Empty names and embedded NUL bytes
    // are rejected so a C-string client API cannot truncate the statement.
    [[nodiscard]] asterorm::result<std::string> quote_ident(std::string_view ident) const;
    [[nodiscard]] asterorm::result<std::string> quote_qualified_ident(std::string_view ident) const;

    [[nodiscard]] asterorm::result<compiled_query> compile(const select_ast& ast);
    [[nodiscard]] asterorm::result<compiled_query> compile(const insert_ast& ast);
    [[nodiscard]] asterorm::result<compiled_query> compile(const insert_select_ast& ast);
    [[nodiscard]] asterorm::result<compiled_query> compile(const update_ast& ast);
    [[nodiscard]] asterorm::result<compiled_query> compile(const delete_ast& ast);
    [[nodiscard]] asterorm::result<std::string>
    upsert_clause(std::string_view table, const std::vector<std::string>& columns,
                  const std::vector<std::string>& primary_keys,
                  std::optional<std::string_view> version_column = std::nullopt) const;

  private:
    dialect dialect_;
    [[nodiscard]] asterorm::result<std::string> render_column(std::string_view ident) const;
    [[nodiscard]] std::string placeholder(std::size_t index) const;
    [[nodiscard]] asterorm::result<std::string> compile_predicate(const predicate_ast& pred,
                                                                  query_parameters& params);
    [[nodiscard]] asterorm::result<std::string> compile_expr(const expr_ast& expr,
                                                             query_parameters& params);
    [[nodiscard]] asterorm::result<std::string> compile_value(const value_source& src,
                                                              query_parameters& params);
    [[nodiscard]] asterorm::result<std::string>
    compile_arithmetic(const arithmetic_operand& operand, query_parameters& params,
                       std::size_t& visited_nodes, std::size_t depth = 0);
    [[nodiscard]] static std::string op_to_string(op_kind op);
};

} // namespace asterorm::sql
