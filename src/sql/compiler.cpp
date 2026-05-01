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

static std::string quote_qualified_ident(std::string_view ident) {
    std::string res;
    std::size_t start = 0;
    while (start <= ident.size()) {
        const auto dot = ident.find('.', start);
        const auto end = dot == std::string_view::npos ? ident.size() : dot;
        if (!res.empty())
            res += '.';
        res += quote_ident(ident.substr(start, end - start));
        if (dot == std::string_view::npos)
            break;
        start = dot + 1;
    }
    return res;
}

static std::string render_column(std::string_view ident) {
    return (ident == "*") ? std::string{"*"} : quote_qualified_ident(ident);
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
        return quote_qualified_ident(std::get<column_expr>(expr).name);
    }
    if (std::holds_alternative<param_expr>(expr)) {
        params.push_back(std::get<param_expr>(expr).encoded_value);
        return "$" + std::to_string(params.size());
    }
    const auto& fn = *std::get<std::shared_ptr<func_expr>>(expr);
    std::string out = fn.name;
    out += '(';
    if (fn.distinct)
        out += "DISTINCT ";
    if (fn.raw_arg) {
        out += *fn.raw_arg;
    } else {
        for (std::size_t i = 0; i < fn.args.size(); ++i) {
            out += compile_expr(fn.args[i], params);
            if (i + 1 < fn.args.size())
                out += ", ";
        }
    }
    out += ')';
    if (fn.alias) {
        out += " AS " + quote_ident(*fn.alias);
    }
    return out;
}

std::string compiler::compile_value(const value_source& src,
                                    std::vector<std::optional<std::string>>& params) {
    if (std::holds_alternative<raw_expr>(src)) {
        // raw_expr is verbatim; the builder author is responsible for safety.
        return std::get<raw_expr>(src).fragment;
    }
    params.push_back(std::get<param_expr>(src).encoded_value);
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
    if (!ast.items.empty()) {
        for (size_t i = 0; i < ast.items.size(); ++i) {
            const auto& it = ast.items[i];
            if (it.fn) {
                expr_ast wrapped = it.fn;
                q.sql += compile_expr(wrapped, q.params);
            } else if (it.column_name) {
                q.sql += render_column(*it.column_name);
            }
            if (it.alias && !it.fn) {
                q.sql += " AS " + quote_ident(*it.alias);
            }
            if (i + 1 < ast.items.size())
                q.sql += ", ";
        }
    } else if (ast.columns.empty()) {
        q.sql += "*";
    } else {
        for (size_t i = 0; i < ast.columns.size(); ++i) {
            q.sql += render_column(ast.columns[i]);
            if (i + 1 < ast.columns.size())
                q.sql += ", ";
        }
    }

    q.sql += " FROM " + quote_qualified_ident(ast.table);
    if (ast.table_alias) {
        q.sql += " AS " + quote_ident(*ast.table_alias);
    }

    for (const auto& j : ast.joins) {
        switch (j.kind) {
        case join_kind::inner:
            q.sql += " INNER JOIN ";
            break;
        case join_kind::left:
            q.sql += " LEFT JOIN ";
            break;
        case join_kind::right:
            q.sql += " RIGHT JOIN ";
            break;
        case join_kind::full:
            q.sql += " FULL JOIN ";
            break;
        case join_kind::cross:
            q.sql += " CROSS JOIN ";
            break;
        }
        q.sql += quote_qualified_ident(j.table);
        if (j.alias)
            q.sql += " AS " + quote_ident(*j.alias);
        if (j.on_clause) {
            q.sql += " ON " + compile_predicate(*j.on_clause, q.params);
        }
    }

    if (ast.where_clause) {
        q.sql += " WHERE " + compile_predicate(*ast.where_clause, q.params);
    }

    if (!ast.group_bys.empty()) {
        q.sql += " GROUP BY ";
        for (size_t i = 0; i < ast.group_bys.size(); ++i) {
            q.sql += quote_qualified_ident(ast.group_bys[i]);
            if (i + 1 < ast.group_bys.size())
                q.sql += ", ";
        }
    }

    if (ast.having_clause) {
        q.sql += " HAVING " + compile_predicate(*ast.having_clause, q.params);
    }

    if (!ast.order_bys.empty()) {
        q.sql += " ORDER BY ";
        for (size_t i = 0; i < ast.order_bys.size(); ++i) {
            q.sql += quote_qualified_ident(ast.order_bys[i].column) +
                     (ast.order_bys[i].ascending ? " ASC" : " DESC");
            if (i < ast.order_bys.size() - 1) {
                q.sql += ", ";
            }
        }
    }

    if (ast.limit_val) {
        q.sql += " LIMIT " + std::to_string(*ast.limit_val);
    }
    if (ast.offset_val) {
        q.sql += " OFFSET " + std::to_string(*ast.offset_val);
    }

    return q;
}

compiled_query compiler::compile(const insert_ast& ast) {
    compiled_query q;
    if (ast.rows.empty())
        throw std::invalid_argument("insert_ast requires at least one row");

    q.sql = "INSERT INTO " + quote_qualified_ident(ast.table);
    if (!ast.columns.empty()) {
        q.sql += " (";
        for (size_t i = 0; i < ast.columns.size(); ++i) {
            q.sql += quote_qualified_ident(ast.columns[i]);
            if (i + 1 < ast.columns.size())
                q.sql += ", ";
        }
        q.sql += ")";
    }

    q.sql += " VALUES ";
    for (size_t r = 0; r < ast.rows.size(); ++r) {
        q.sql += "(";
        const auto& row = ast.rows[r];
        for (size_t i = 0; i < row.size(); ++i) {
            q.sql += compile_value(row[i], q.params);
            if (i + 1 < row.size())
                q.sql += ", ";
        }
        q.sql += ")";
        if (r + 1 < ast.rows.size())
            q.sql += ", ";
    }

    if (ast.on_conflict) {
        const auto& oc = *ast.on_conflict;
        q.sql += " ON CONFLICT";
        if (!oc.target_columns.empty()) {
            q.sql += " (";
            for (size_t i = 0; i < oc.target_columns.size(); ++i) {
                q.sql += quote_qualified_ident(oc.target_columns[i]);
                if (i + 1 < oc.target_columns.size())
                    q.sql += ", ";
            }
            q.sql += ")";
        }
        if (oc.do_nothing) {
            q.sql += " DO NOTHING";
        } else {
            q.sql += " DO UPDATE SET ";
            for (size_t i = 0; i < oc.updates.size(); ++i) {
                q.sql += quote_qualified_ident(oc.updates[i].first);
                q.sql += " = ";
                q.sql += compile_value(oc.updates[i].second, q.params);
                if (i + 1 < oc.updates.size())
                    q.sql += ", ";
            }
            if (oc.where_clause) {
                q.sql += " WHERE " + compile_predicate(*oc.where_clause, q.params);
            }
        }
    }

    if (!ast.returning.empty()) {
        q.sql += " RETURNING ";
        for (size_t i = 0; i < ast.returning.size(); ++i) {
            q.sql += render_column(ast.returning[i]);
            if (i + 1 < ast.returning.size())
                q.sql += ", ";
        }
    }

    return q;
}

compiled_query compiler::compile(const update_ast& ast) {
    compiled_query q;
    q.sql = "UPDATE " + quote_qualified_ident(ast.table) + " SET ";
    for (size_t i = 0; i < ast.assignments.size(); ++i) {
        q.sql += quote_qualified_ident(ast.assignments[i].first);
        q.sql += " = ";
        q.sql += compile_value(ast.assignments[i].second, q.params);
        if (i + 1 < ast.assignments.size())
            q.sql += ", ";
    }
    if (ast.where_clause) {
        q.sql += " WHERE " + compile_predicate(*ast.where_clause, q.params);
    }
    if (!ast.returning.empty()) {
        q.sql += " RETURNING ";
        for (size_t i = 0; i < ast.returning.size(); ++i) {
            q.sql += render_column(ast.returning[i]);
            if (i + 1 < ast.returning.size())
                q.sql += ", ";
        }
    }
    return q;
}

compiled_query compiler::compile(const delete_ast& ast) {
    compiled_query q;
    q.sql = "DELETE FROM " + quote_qualified_ident(ast.table);
    if (ast.where_clause) {
        q.sql += " WHERE " + compile_predicate(*ast.where_clause, q.params);
    }
    if (!ast.returning.empty()) {
        q.sql += " RETURNING ";
        for (size_t i = 0; i < ast.returning.size(); ++i) {
            q.sql += render_column(ast.returning[i]);
            if (i + 1 < ast.returning.size())
                q.sql += ", ";
        }
    }
    return q;
}

} // namespace asterorm::sql
