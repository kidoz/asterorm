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
// Owned binary grouping; tails belong to this full compound expression.
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
    explicit query_builder(compound_ast compound) {
        ast_.body = std::move(compound);
    }
    query_builder& order_by(std::string column, bool ascending = true) {
        std::visit(
            [&](auto& body) {
                body.order_bys.push_back({.column = std::move(column), .ascending = ascending});
            },
            ast_.body);
        return *this;
    }
    query_builder& limit(std::size_t count) {
        std::visit([&](auto& body) { body.limit_val = count; }, ast_.body);
        return *this;
    }
    query_builder& offset(std::size_t count) {
        std::visit([&](auto& body) { body.offset_val = count; }, ast_.body);
        return *this;
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
namespace query_ast_detail {
inline query_builder combine(query_ast left, query_ast right, set_operator operation,
                             set_quantifier quantifier) {
    return query_builder{compound_ast{
        .operation = operation,
        .quantifier = quantifier,
        .left = std::make_unique<query_ast>(std::move(left)),
        .right = std::make_unique<query_ast>(std::move(right)),
        .order_bys = {},
        .limit_val = std::nullopt,
        .offset_val = std::nullopt,
    }};
}
} // namespace query_ast_detail

inline query_builder union_(query_ast left, query_ast right) {
    return query_ast_detail::combine(std::move(left), std::move(right), set_operator::union_,
                                     set_quantifier::distinct);
}
inline query_builder union_all(query_ast left, query_ast right) {
    return query_ast_detail::combine(std::move(left), std::move(right), set_operator::union_,
                                     set_quantifier::all);
}
inline query_builder intersect_(query_ast left, query_ast right) {
    return query_ast_detail::combine(std::move(left), std::move(right), set_operator::intersect_,
                                     set_quantifier::distinct);
}
inline query_builder intersect_all(query_ast left, query_ast right) {
    return query_ast_detail::combine(std::move(left), std::move(right), set_operator::intersect_,
                                     set_quantifier::all);
}
inline query_builder except_(query_ast left, query_ast right) {
    return query_ast_detail::combine(std::move(left), std::move(right), set_operator::except_,
                                     set_quantifier::distinct);
}
inline query_builder except_all(query_ast left, query_ast right) {
    return query_ast_detail::combine(std::move(left), std::move(right), set_operator::except_,
                                     set_quantifier::all);
}
} // namespace asterorm::sql
