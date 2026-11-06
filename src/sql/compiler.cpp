#include "asterorm/sql/compiler.hpp"

#include <algorithm>
#include <cctype>
#include <string_view>

namespace asterorm::sql {
namespace {

db_error invalid_sql(std::string message) {
    db_error error;
    error.kind = db_error_kind::parse_failed;
    error.message = std::move(message);
    return error;
}

asterorm::result<std::string> compile_select_lock(const select_ast& ast, dialect target) {
    const auto& locking = ast.locking;
    std::string suffix;
    switch (locking.strength) {
    case select_lock_strength::none:
        if (locking.wait != select_lock_wait::block) {
            return std::unexpected(
                invalid_sql("SELECT lock wait policy requires FOR UPDATE or FOR SHARE"));
        }
        return suffix;
    case select_lock_strength::update:
        suffix = " FOR UPDATE";
        break;
    case select_lock_strength::share:
        suffix = " FOR SHARE";
        break;
    default:
        return std::unexpected(invalid_sql("Invalid SELECT lock strength"));
    }
    if (!supports_select_locking(target)) {
        return std::unexpected(invalid_sql("This SQL dialect does not support SELECT locking"));
    }
    if (!ast.joins.empty() || !ast.group_bys.empty() || ast.having_clause ||
        std::ranges::any_of(ast.items,
                            [](const select_item& item) { return item.fn != nullptr; })) {
        return std::unexpected(invalid_sql("SELECT locking requires a single table and column-only "
                                           "projections without GROUP BY or HAVING"));
    }
    switch (locking.wait) {
    case select_lock_wait::block:
        break;
    case select_lock_wait::nowait:
        suffix += " NOWAIT";
        break;
    case select_lock_wait::skip_locked:
        suffix += " SKIP LOCKED";
        break;
    default:
        return std::unexpected(invalid_sql("Invalid SELECT lock wait policy"));
    }
    return suffix;
}

bool bare_function_name(std::string_view name) {
    if (name.empty())
        return false;
    const auto first = static_cast<unsigned char>(name.front());
    if (std::isalpha(first) == 0 && name.front() != '_')
        return false;
    for (char character : name) {
        const auto byte = static_cast<unsigned char>(character);
        if (std::isalnum(byte) == 0 && character != '_')
            return false;
    }
    return true;
}

bool contains_nul(std::string_view text) {
    return text.find('\0') != std::string_view::npos;
}

} // namespace

asterorm::result<std::string> compiler::quote_ident(std::string_view ident) const {
    if (ident.empty() || contains_nul(ident))
        return std::unexpected(invalid_sql("Invalid SQL identifier"));

    const char quote = dialect_ == dialect::mysql ? '`' : '"';
    std::string quoted;
    quoted.reserve(ident.size() + 2);
    quoted += quote;
    for (char character : ident) {
        if (character == quote) {
            quoted += quote;
            quoted += quote;
        } else {
            quoted += character;
        }
    }
    quoted += quote;
    return quoted;
}

asterorm::result<std::string> compiler::quote_qualified_ident(std::string_view ident) const {
    if (ident.empty())
        return std::unexpected(invalid_sql("Invalid SQL identifier"));

    std::string quoted;
    std::size_t start = 0;
    while (start <= ident.size()) {
        const auto dot = ident.find('.', start);
        const auto end = dot == std::string_view::npos ? ident.size() : dot;
        if (end == start)
            return std::unexpected(invalid_sql("Invalid SQL identifier"));
        auto part = quote_ident(ident.substr(start, end - start));
        if (!part)
            return std::unexpected(part.error());
        if (!quoted.empty())
            quoted += '.';
        quoted += *part;
        if (dot == std::string_view::npos)
            break;
        start = dot + 1;
    }
    return quoted;
}

asterorm::result<std::string> compiler::render_column(std::string_view ident) const {
    if (ident == "*")
        return std::string{"*"};
    return quote_qualified_ident(ident);
}

std::string compiler::placeholder(std::size_t index) const {
    return dialect_ == dialect::mysql ? "?" : "$" + std::to_string(index);
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

asterorm::result<std::string> compiler::compile_expr(const expr_ast& expr,
                                                     query_parameters& params) { // NOLINT
    if (std::holds_alternative<column_expr>(expr))
        return quote_qualified_ident(std::get<column_expr>(expr).name);
    if (std::holds_alternative<param_expr>(expr)) {
        params.push_back(std::get<param_expr>(expr).encoded_value);
        return placeholder(params.size());
    }
    const auto& fn = *std::get<std::shared_ptr<func_expr>>(expr);
    if (!bare_function_name(fn.name))
        return std::unexpected(invalid_sql("Invalid SQL function name"));
    if (fn.raw_arg && contains_nul(*fn.raw_arg))
        return std::unexpected(invalid_sql("SQL fragment contains a NUL byte"));

    std::string out = fn.name;
    out += '(';
    if (fn.distinct)
        out += "DISTINCT ";
    if (fn.raw_arg) {
        out += *fn.raw_arg;
    } else {
        for (std::size_t i = 0; i < fn.args.size(); ++i) {
            auto argument = compile_expr(fn.args[i], params);
            if (!argument)
                return std::unexpected(argument.error());
            out += *argument;
            if (i + 1 < fn.args.size())
                out += ", ";
        }
    }
    out += ')';
    if (fn.alias) {
        auto alias = quote_ident(*fn.alias);
        if (!alias)
            return std::unexpected(alias.error());
        out += " AS " + *alias;
    }
    return out;
}

asterorm::result<std::string> compiler::compile_value(const value_source& src,
                                                      query_parameters& params) {
    if (std::holds_alternative<raw_expr>(src)) {
        // raw_expr is verbatim; the builder author is responsible for safety.
        const auto& fragment = std::get<raw_expr>(src).fragment;
        if (contains_nul(fragment))
            return std::unexpected(invalid_sql("SQL fragment contains a NUL byte"));
        return fragment;
    }
    params.push_back(std::get<param_expr>(src).encoded_value);
    return placeholder(params.size());
}

asterorm::result<std::string> compiler::compile_predicate(const predicate_ast& pred,
                                                          query_parameters& params) { // NOLINT
    if (std::holds_alternative<comparison_predicate>(pred.val)) {
        const auto& comp = std::get<comparison_predicate>(pred.val);
        auto left = compile_expr(comp.left, params);
        if (!left)
            return std::unexpected(left.error());
        auto right = compile_expr(comp.right, params);
        if (!right)
            return std::unexpected(right.error());
        return *left + " " + op_to_string(comp.op) + " " + *right;
    }
    if (std::holds_alternative<unary_predicate>(pred.val)) {
        const auto& un = std::get<unary_predicate>(pred.val);
        auto operand = compile_expr(un.operand, params);
        if (!operand)
            return std::unexpected(operand.error());
        return *operand + (un.op == unary_op_kind::is_null ? " IS NULL" : " IS NOT NULL");
    }
    const auto& bin = std::get<binary_predicate>(pred.val);
    auto left = compile_predicate(*bin.left, params);
    if (!left)
        return std::unexpected(left.error());
    auto right = compile_predicate(*bin.right, params);
    if (!right)
        return std::unexpected(right.error());
    return "(" + *left + " " + op_to_string(bin.op) + " " + *right + ")";
}

asterorm::result<std::string>
compiler::upsert_clause(std::string_view table, const std::vector<std::string>& columns,
                        const std::vector<std::string>& primary_keys,
                        std::optional<std::string_view> version_column) const {
    std::string clause;
    if (dialect_ == dialect::mysql) {
        clause = " AS asterorm_new ON DUPLICATE KEY UPDATE ";
    } else {
        clause = " ON CONFLICT (";
        for (std::size_t index = 0; index < primary_keys.size(); ++index) {
            if (index != 0)
                clause += ", ";
            auto quoted = quote_ident(primary_keys[index]);
            if (!quoted)
                return std::unexpected(quoted.error());
            clause += *quoted;
        }
        clause += ") DO UPDATE SET ";
    }
    bool first = true;
    for (const auto& column : columns) {
        if (std::ranges::find(primary_keys, column) != primary_keys.end())
            continue;
        if (!first)
            clause += ", ";
        first = false;
        auto quoted_column = quote_ident(column);
        if (!quoted_column)
            return std::unexpected(quoted_column.error());
        clause += *quoted_column + " = ";
        if (version_column && *version_column == column) {
            auto quoted_table = quote_qualified_ident(table);
            if (!quoted_table)
                return std::unexpected(quoted_table.error());
            clause += *quoted_table + "." + *quoted_column + " + 1";
        } else {
            clause += (dialect_ == dialect::mysql ? "asterorm_new." : "EXCLUDED.") + *quoted_column;
        }
    }
    if (first)
        return std::unexpected(invalid_sql("upsert requires at least one updatable column"));
    return clause;
}

asterorm::result<compiled_query> compiler::compile(const select_ast& ast) { // NOLINT
    auto locking = compile_select_lock(ast, dialect_);
    if (!locking) {
        return std::unexpected(locking.error());
    }
    compiled_query query;

    auto append_qualified = [&](std::string_view ident) -> asterorm::result<void> {
        auto quoted = quote_qualified_ident(ident);
        if (!quoted)
            return std::unexpected(quoted.error());
        query.sql += *quoted;
        return {};
    };
    auto append_ident = [&](std::string_view ident) -> asterorm::result<void> {
        auto quoted = quote_ident(ident);
        if (!quoted)
            return std::unexpected(quoted.error());
        query.sql += *quoted;
        return {};
    };

    query.sql = "SELECT ";
    if (!ast.items.empty()) {
        for (size_t i = 0; i < ast.items.size(); ++i) {
            const auto& it = ast.items[i];
            if (it.fn) {
                expr_ast wrapped = it.fn;
                auto rendered = compile_expr(wrapped, query.params);
                if (!rendered)
                    return std::unexpected(rendered.error());
                query.sql += *rendered;
            } else if (it.column_name) {
                auto rendered = render_column(*it.column_name);
                if (!rendered)
                    return std::unexpected(rendered.error());
                query.sql += *rendered;
            }
            if (it.alias && !it.fn) {
                query.sql += " AS ";
                auto appended = append_ident(*it.alias);
                if (!appended)
                    return std::unexpected(appended.error());
            }
            if (i + 1 < ast.items.size())
                query.sql += ", ";
        }
    } else if (ast.columns.empty()) {
        query.sql += "*";
    } else {
        for (size_t i = 0; i < ast.columns.size(); ++i) {
            auto rendered = render_column(ast.columns[i]);
            if (!rendered)
                return std::unexpected(rendered.error());
            query.sql += *rendered;
            if (i + 1 < ast.columns.size())
                query.sql += ", ";
        }
    }

    query.sql += " FROM ";
    if (auto appended = append_qualified(ast.table); !appended)
        return std::unexpected(appended.error());
    if (ast.table_alias) {
        query.sql += " AS ";
        if (auto appended = append_ident(*ast.table_alias); !appended)
            return std::unexpected(appended.error());
    }

    for (const auto& join : ast.joins) {
        switch (join.kind) {
        case join_kind::inner:
            query.sql += " INNER JOIN ";
            break;
        case join_kind::left:
            query.sql += " LEFT JOIN ";
            break;
        case join_kind::right:
            query.sql += " RIGHT JOIN ";
            break;
        case join_kind::full:
            if (dialect_ == dialect::mysql)
                return std::unexpected(invalid_sql("MySQL does not support FULL JOIN"));
            query.sql += " FULL JOIN ";
            break;
        case join_kind::cross:
            query.sql += " CROSS JOIN ";
            break;
        }
        if (auto appended = append_qualified(join.table); !appended)
            return std::unexpected(appended.error());
        if (join.alias) {
            query.sql += " AS ";
            if (auto appended = append_ident(*join.alias); !appended)
                return std::unexpected(appended.error());
        }
        if (join.on_clause) {
            auto predicate = compile_predicate(*join.on_clause, query.params);
            if (!predicate)
                return std::unexpected(predicate.error());
            query.sql += " ON " + *predicate;
        }
    }

    if (ast.where_clause) {
        auto predicate = compile_predicate(*ast.where_clause, query.params);
        if (!predicate)
            return std::unexpected(predicate.error());
        query.sql += " WHERE " + *predicate;
    }

    if (!ast.group_bys.empty()) {
        query.sql += " GROUP BY ";
        for (size_t i = 0; i < ast.group_bys.size(); ++i) {
            if (auto appended = append_qualified(ast.group_bys[i]); !appended)
                return std::unexpected(appended.error());
            if (i + 1 < ast.group_bys.size())
                query.sql += ", ";
        }
    }

    if (ast.having_clause) {
        auto predicate = compile_predicate(*ast.having_clause, query.params);
        if (!predicate)
            return std::unexpected(predicate.error());
        query.sql += " HAVING " + *predicate;
    }

    if (!ast.order_bys.empty()) {
        query.sql += " ORDER BY ";
        for (size_t i = 0; i < ast.order_bys.size(); ++i) {
            if (auto appended = append_qualified(ast.order_bys[i].column); !appended)
                return std::unexpected(appended.error());
            query.sql += ast.order_bys[i].ascending ? " ASC" : " DESC";
            if (i < ast.order_bys.size() - 1)
                query.sql += ", ";
        }
    }

    if (ast.limit_val)
        query.sql += " LIMIT " + std::to_string(*ast.limit_val);
    if (dialect_ == dialect::mysql && ast.offset_val && !ast.limit_val)
        query.sql += " LIMIT 18446744073709551615";
    if (ast.offset_val)
        query.sql += " OFFSET " + std::to_string(*ast.offset_val);

    query.sql += *locking;

    return query;
}

asterorm::result<compiled_query> compiler::compile(const insert_ast& ast) {
    compiled_query query;
    if (ast.rows.empty())
        return std::unexpected(invalid_sql("insert_ast requires at least one row"));

    auto table = quote_qualified_ident(ast.table);
    if (!table)
        return std::unexpected(table.error());
    query.sql = "INSERT INTO " + *table;
    if (!ast.columns.empty()) {
        query.sql += " (";
        for (size_t i = 0; i < ast.columns.size(); ++i) {
            auto column = quote_qualified_ident(ast.columns[i]);
            if (!column)
                return std::unexpected(column.error());
            query.sql += *column;
            if (i + 1 < ast.columns.size())
                query.sql += ", ";
        }
        query.sql += ")";
    }

    query.sql += " VALUES ";
    for (size_t row_index = 0; row_index < ast.rows.size(); ++row_index) {
        query.sql += "(";
        const auto& row = ast.rows[row_index];
        for (size_t i = 0; i < row.size(); ++i) {
            auto value = compile_value(row[i], query.params);
            if (!value)
                return std::unexpected(value.error());
            query.sql += *value;
            if (i + 1 < row.size())
                query.sql += ", ";
        }
        query.sql += ")";
        if (row_index + 1 < ast.rows.size())
            query.sql += ", ";
    }

    if (ast.on_conflict) {
        if (dialect_ == dialect::mysql)
            return std::unexpected(
                invalid_sql("MySQL does not support targeted ON CONFLICT; use repository::upsert"));
        const auto& conflict = *ast.on_conflict;
        query.sql += " ON CONFLICT";
        if (!conflict.target_columns.empty()) {
            query.sql += " (";
            for (size_t i = 0; i < conflict.target_columns.size(); ++i) {
                auto column = quote_qualified_ident(conflict.target_columns[i]);
                if (!column)
                    return std::unexpected(column.error());
                query.sql += *column;
                if (i + 1 < conflict.target_columns.size())
                    query.sql += ", ";
            }
            query.sql += ")";
        }
        if (conflict.do_nothing) {
            query.sql += " DO NOTHING";
        } else {
            query.sql += " DO UPDATE SET ";
            for (size_t i = 0; i < conflict.updates.size(); ++i) {
                auto column = quote_qualified_ident(conflict.updates[i].first);
                if (!column)
                    return std::unexpected(column.error());
                auto value = compile_value(conflict.updates[i].second, query.params);
                if (!value)
                    return std::unexpected(value.error());
                query.sql += *column + " = " + *value;
                if (i + 1 < conflict.updates.size())
                    query.sql += ", ";
            }
            if (conflict.where_clause) {
                auto predicate = compile_predicate(*conflict.where_clause, query.params);
                if (!predicate)
                    return std::unexpected(predicate.error());
                query.sql += " WHERE " + *predicate;
            }
        }
    }

    if (!ast.returning.empty()) {
        if (dialect_ == dialect::mysql)
            return std::unexpected(invalid_sql("MySQL does not support RETURNING"));
        query.sql += " RETURNING ";
        for (size_t i = 0; i < ast.returning.size(); ++i) {
            auto column = render_column(ast.returning[i]);
            if (!column)
                return std::unexpected(column.error());
            query.sql += *column;
            if (i + 1 < ast.returning.size())
                query.sql += ", ";
        }
    }

    return query;
}

asterorm::result<compiled_query> compiler::compile(const update_ast& ast) {
    compiled_query query;
    auto table = quote_qualified_ident(ast.table);
    if (!table)
        return std::unexpected(table.error());
    query.sql = "UPDATE " + *table + " SET ";
    for (size_t i = 0; i < ast.assignments.size(); ++i) {
        auto column = quote_qualified_ident(ast.assignments[i].first);
        if (!column)
            return std::unexpected(column.error());
        auto value = compile_value(ast.assignments[i].second, query.params);
        if (!value)
            return std::unexpected(value.error());
        query.sql += *column + " = " + *value;
        if (i + 1 < ast.assignments.size())
            query.sql += ", ";
    }
    if (ast.where_clause) {
        auto predicate = compile_predicate(*ast.where_clause, query.params);
        if (!predicate)
            return std::unexpected(predicate.error());
        query.sql += " WHERE " + *predicate;
    }
    if (!ast.returning.empty()) {
        if (dialect_ == dialect::mysql)
            return std::unexpected(invalid_sql("MySQL does not support RETURNING"));
        query.sql += " RETURNING ";
        for (size_t i = 0; i < ast.returning.size(); ++i) {
            auto column = render_column(ast.returning[i]);
            if (!column)
                return std::unexpected(column.error());
            query.sql += *column;
            if (i + 1 < ast.returning.size())
                query.sql += ", ";
        }
    }
    return query;
}

asterorm::result<compiled_query> compiler::compile(const delete_ast& ast) {
    compiled_query query;
    auto table = quote_qualified_ident(ast.table);
    if (!table)
        return std::unexpected(table.error());
    query.sql = "DELETE FROM " + *table;
    if (ast.where_clause) {
        auto predicate = compile_predicate(*ast.where_clause, query.params);
        if (!predicate)
            return std::unexpected(predicate.error());
        query.sql += " WHERE " + *predicate;
    }
    if (!ast.returning.empty()) {
        if (dialect_ == dialect::mysql)
            return std::unexpected(invalid_sql("MySQL does not support RETURNING"));
        query.sql += " RETURNING ";
        for (size_t i = 0; i < ast.returning.size(); ++i) {
            auto column = render_column(ast.returning[i]);
            if (!column)
                return std::unexpected(column.error());
            query.sql += *column;
            if (i + 1 < ast.returning.size())
                query.sql += ", ";
        }
    }
    return query;
}

} // namespace asterorm::sql
