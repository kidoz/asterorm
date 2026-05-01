#pragma once
#include <optional>
#include <string>
#include <vector>

#include "asterorm/sql/ast.hpp"

namespace asterorm::sql {

struct compiled_query {
    std::string sql;
    std::vector<std::optional<std::string>> params;
};

class compiler {
  public:
    compiled_query compile(const select_ast& ast);
    compiled_query compile(const insert_ast& ast);
    compiled_query compile(const update_ast& ast);
    compiled_query compile(const delete_ast& ast);

  private:
    std::string compile_predicate(const predicate_ast& pred,
                                  std::vector<std::optional<std::string>>& params);
    std::string compile_expr(const expr_ast& expr, std::vector<std::optional<std::string>>& params);
    std::string compile_value(const value_source& src,
                              std::vector<std::optional<std::string>>& params);
    std::string op_to_string(op_kind op);
};

} // namespace asterorm::sql
