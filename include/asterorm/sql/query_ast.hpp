#pragma once
#include "asterorm/sql/ast.hpp"

namespace asterorm::sql {
// Syntax budgets count occurrences across every CTE and the main SELECT.
// Root depth is zero; shared function DAGs count again at each use.
inline constexpr std::size_t max_query_depth = 64;
inline constexpr std::size_t max_query_nodes = 4096;
inline constexpr std::size_t max_query_ctes = 64;

enum class mysql_query_profile { baseline_8_0_19, set_operations_8_0_31 };
struct query_compile_options {
    mysql_query_profile mysql_profile{mysql_query_profile::baseline_8_0_19};
};

struct query_ast;
struct cte_ast {
    std::string name;
    std::vector<std::string> columns;
    std::unique_ptr<query_ast> body;
};
enum class set_operator { union_, intersect_, except_ };
enum class set_quantifier { distinct, all };
// Representation reserved for the next delivery; compilation currently rejects it.
struct compound_ast {
    set_operator operation{set_operator::union_};
    set_quantifier quantifier{set_quantifier::distinct};
    std::unique_ptr<query_ast> left;
    std::unique_ptr<query_ast> right;
    std::vector<order_by_ast> order_bys;
    std::optional<std::size_t> limit_val;
    std::optional<std::size_t> offset_val;
};
struct query_ast {
    std::vector<cte_ast> ctes;
    bool recursive{};
    std::variant<select_ast, compound_ast> body;
};

class query_builder {
  public:
    explicit query_builder(select_ast leaf) {
        ast_.body = std::move(leaf);
    }
    query_builder& with_cte(std::string name, query_ast body,
                            std::vector<std::string> columns = {}) {
        ast_.ctes.push_back({.name = std::move(name),
                             .columns = std::move(columns),
                             .body = std::make_unique<query_ast>(std::move(body))});
        return *this;
    }
    query_builder& recursive(bool enabled = true) {
        ast_.recursive = enabled;
        return *this;
    }
    query_ast build() {
        return std::move(ast_);
    }

  private:
    query_ast ast_;
};
inline query_builder query(select_ast leaf) {
    return query_builder{std::move(leaf)};
}
} // namespace asterorm::sql
