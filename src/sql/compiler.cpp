#include "asterorm/sql/compiler.hpp"

#include <stdexcept>
#include <string_view>

namespace asterorm::sql {

static std::string quote_ident(std::string_view ident) {
    std::string res;
    res.reserve(ident.size() + 2);
    res += '"';
    for (char c : ident) {
        if (c == '"') {
            res += "\"\"";
        } else {
            res += c;
        }
    }
    res += '"';
    return res;
}

std::string compiler::op_to_string(op_kind op) {
    switch (op) {
    case op_kind::eq:
        return "=";
    case op_kind::neq:
        return "!=";
    case op_kind::lt:
        return "<";
    case op_kind::gt:
        return ">";
    case op_kind::le:
        return "<=";
    case op_kind::ge:
        return ">=";
    case op_kind::and_:
        return "AND";
    case op_kind::or_:
        return "OR";
    }
    return "";
}

std::string compiler::compile_expr(const expr_ast& expr,
                                   std::vector<std::optional<std::string>>& params) { // NOLINT
    if (std::holds_alternative<column_expr>(expr)) {
        return quote_ident(std::get<column_expr>(expr).name);
    }
    params.push_back(std::get<param_expr>(expr).encoded_value);
    return "$" + std::to_string(params.size());
}

std::string compiler::compile_predicate(const predicate_ast& pred,
                                        std::vector<std::optional<std::string>>& params) { // NOLINT
    if (std::holds_alternative<comparison_predicate>(pred.val)) {
        const auto& comp = std::get<comparison_predicate>(pred.val);
        std::string left = compile_expr(comp.left, params);
        std::string right = compile_expr(comp.right, params);
        return left + " " + op_to_string(comp.op) + " " + right;
    }
    const auto& bin = std::get<binary_predicate>(pred.val);
    std::string left = compile_predicate(*bin.left, params);
    std::string right = compile_predicate(*bin.right, params);
    return "(" + left + " " + op_to_string(bin.op) + " " + right + ")";
}

compiled_query compiler::compile(const select_ast& ast) { // NOLINT
    compiled_query q;

    q.sql = "SELECT ";
    if (ast.columns.empty()) {
        q.sql += "*";
    } else {
        for (size_t i = 0; i < ast.columns.size(); ++i) {
            q.sql += quote_ident(ast.columns[i]);
            if (i < ast.columns.size() - 1) {
                q.sql += ", ";
            }
        }
    }

    q.sql += " FROM " + quote_ident(ast.table);

    if (ast.where_clause) {
        q.sql += " WHERE " + compile_predicate(*ast.where_clause, q.params);
    }

    if (!ast.order_bys.empty()) {
        q.sql += " ORDER BY ";
        for (size_t i = 0; i < ast.order_bys.size(); ++i) {
            q.sql += quote_ident(ast.order_bys[i].column) +
                     (ast.order_bys[i].ascending ? " ASC" : " DESC");
            if (i < ast.order_bys.size() - 1) {
                q.sql += ", ";
            }
        }
    }

    if (ast.limit_val) {
        q.sql += " LIMIT " + std::to_string(*ast.limit_val);
    }

    return q;
}

} // namespace asterorm::sql