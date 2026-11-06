#pragma once
#include <optional>
#include <string>
#include <vector>

#include "asterorm/sql/ast.hpp"

namespace asterorm::sql {

enum class dialect { postgres, mysql };

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

    compiled_query compile(const select_ast& ast);
    compiled_query compile(const insert_ast& ast);
    compiled_query compile(const update_ast& ast);
    compiled_query compile(const delete_ast& ast);
    std::string upsert_clause(std::string_view table, const std::vector<std::string>& columns,
                              const std::vector<std::string>& primary_keys,
                              std::optional<std::string_view> version_column = std::nullopt) const;

  private:
    dialect dialect_;
    std::string quote_ident(std::string_view ident) const;
    std::string quote_qualified_ident(std::string_view ident) const;
    std::string render_column(std::string_view ident) const;
    std::string placeholder(std::size_t index) const;
    std::string compile_predicate(const predicate_ast& pred, query_parameters& params);
    std::string compile_expr(const expr_ast& expr, query_parameters& params);
    std::string compile_value(const value_source& src, query_parameters& params);
    std::string op_to_string(op_kind op);
};

} // namespace asterorm::sql
