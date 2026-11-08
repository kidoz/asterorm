#include "asterorm/sql/compiler.hpp"
#include "window_validation.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
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

constexpr std::size_t max_insert_predicate_depth = 64;
constexpr std::size_t max_insert_predicate_nodes = 1024;

result<void> insert_source_column(const compiler& compiler, std::string_view name) {
    if (name.contains('*')) {
        return std::unexpected(invalid_sql("INSERT SELECT requires explicit column names"));
    }
    auto quoted = compiler.quote_qualified_ident(name);
    if (!quoted) {
        return std::unexpected(quoted.error());
    }
    return {};
}

result<void> insert_source_operand(const compiler& compiler, const expr_ast& operand) {
    if (const auto* column = std::get_if<column_expr>(&operand)) {
        return insert_source_column(compiler, column->name);
    }
    if (const auto* parameter = std::get_if<param_expr>(&operand)) {
        if (parameter->encoded_value.error) {
            return std::unexpected(*parameter->encoded_value.error);
        }
        return {};
    }
    return std::unexpected(
        invalid_sql("INSERT SELECT predicates require column/parameter operands"));
}

result<void> insert_source_predicate(const compiler& compiler, const predicate_ast& predicate,
                                     std::size_t& visited, std::size_t depth = 0) {
    if (depth > max_insert_predicate_depth || ++visited > max_insert_predicate_nodes) {
        return std::unexpected(invalid_sql("INSERT SELECT predicate exceeds its depth/node limit"));
    }
    if (const auto* comparison = std::get_if<comparison_predicate>(&predicate.val)) {
        switch (comparison->op) {
        case op_kind::eq:
        case op_kind::neq:
        case op_kind::lt:
        case op_kind::gt:
        case op_kind::le:
        case op_kind::ge:
            break;
        default:
            return std::unexpected(invalid_sql("Invalid INSERT SELECT comparison operator"));
        }
        auto left = insert_source_operand(compiler, comparison->left);
        if (!left) {
            return left;
        }
        return insert_source_operand(compiler, comparison->right);
    }
    if (const auto* unary = std::get_if<unary_predicate>(&predicate.val)) {
        if (unary->op != unary_op_kind::is_null && unary->op != unary_op_kind::is_not_null) {
            return std::unexpected(invalid_sql("Invalid INSERT SELECT NULL operator"));
        }
        return insert_source_operand(compiler, unary->operand);
    }
    const auto& binary = std::get<binary_predicate>(predicate.val);
    if (!binary.left || !binary.right ||
        (binary.op != op_kind::and_ && binary.op != op_kind::or_)) {
        return std::unexpected(invalid_sql("Invalid INSERT SELECT logical predicate"));
    }
    auto left = insert_source_predicate(compiler, *binary.left, visited, depth + 1);
    if (!left) {
        return left;
    }
    return insert_source_predicate(compiler, *binary.right, visited, depth + 1);
}

bool same_mysql_table_name(std::string_view left, std::string_view right) {
    left.remove_prefix(left.rfind('.') == std::string_view::npos ? 0 : left.rfind('.') + 1);
    right.remove_prefix(right.rfind('.') == std::string_view::npos ? 0 : right.rfind('.') + 1);
    if (left.size() != right.size()) {
        return false;
    }
    auto ascii_lower = [](unsigned char character) {
        return character >= 'A' && character <= 'Z' ? character + ('a' - 'A') : character;
    };
    for (std::size_t index = 0; index < left.size(); ++index) {
        if (ascii_lower(static_cast<unsigned char>(left[index])) !=
            ascii_lower(static_cast<unsigned char>(right[index]))) {
            return false;
        }
    }
    return true;
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

asterorm::result<std::string> compiler::compile_expr(const expr_ast& expr, query_parameters& params,
                                                     bool projection_root, bool aggregates_allowed,
                                                     bool filter_predicate) { // NOLINT
    if (std::holds_alternative<column_expr>(expr)) {
        return quote_qualified_ident(std::get<column_expr>(expr).name);
    }
    if (std::holds_alternative<param_expr>(expr)) {
        const auto& parameter = std::get<param_expr>(expr).encoded_value;
        if (parameter.error) {
            return std::unexpected(*parameter.error);
        }
        if (parameter.value && contains_nul(*parameter.value)) {
            return std::unexpected(invalid_sql("SQL parameter contains a NUL byte"));
        }
        params.push_back(parameter);
        auto token = placeholder(params.size());
        const auto kind = std::get<param_expr>(expr).kind;
        if (kind != parameter_kind::other && kind != parameter_kind::signed_integer &&
            kind != parameter_kind::unsigned_integer && kind != parameter_kind::real &&
            kind != parameter_kind::boolean) {
            return std::unexpected(invalid_sql("Invalid AST parameter kind"));
        }
        if (cast_window_parameters_ && kind != parameter_kind::other &&
            dialect_ != dialect::postgres) {
            if (dialect_ == dialect::sqlite && kind == parameter_kind::unsigned_integer &&
                parameter.value) {
                std::uint64_t value{};
                auto parsed =
                    std::from_chars(parameter.value->data(),
                                    parameter.value->data() + parameter.value->size(), value);
                if (parsed.ec != std::errc{} ||
                    parsed.ptr != parameter.value->data() + parameter.value->size() ||
                    value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
                    return std::unexpected(invalid_sql(
                        "SQLite window numeric parameters require signed 64-bit range"));
                }
            }
            const auto type =
                kind == parameter_kind::real
                    ? (dialect_ == dialect::mysql ? "DOUBLE" : "REAL")
                    : (dialect_ == dialect::sqlite
                           ? "INTEGER"
                           : (kind == parameter_kind::unsigned_integer ? "UNSIGNED" : "SIGNED"));
            return "CAST(" + token + " AS " + type + ')';
        }
        return token;
    }
    const auto& pointer = std::get<std::shared_ptr<func_expr>>(expr);
    if (!pointer) {
        return std::unexpected(invalid_sql("SQL function requires a non-null node"));
    }
    const auto& function = *pointer;
    if (function.alias && !projection_root) {
        return std::unexpected(invalid_sql("SQL function aliases belong in the projection"));
    }
    if (!bare_function_name(function.name)) {
        return std::unexpected(invalid_sql("Invalid SQL function name"));
    }
    if (function.raw_arg && contains_nul(*function.raw_arg)) {
        return std::unexpected(invalid_sql("SQL fragment contains a NUL byte"));
    }

    auto window_valid = window_detail::validate_function(function, dialect_);
    if (!window_valid) {
        return std::unexpected(window_valid.error());
    }
    if (function.over && !projection_root) {
        return std::unexpected(invalid_sql("Windows are only allowed at projection boundaries"));
    }
    if (function.filter && !projection_root && !aggregates_allowed) {
        return std::unexpected(
            invalid_sql("Filtered aggregates require projection or HAVING context"));
    }
    if (filter_predicate &&
        (function.over || function.filter || window_detail::is_aggregate(function.name))) {
        return std::unexpected(
            invalid_sql("FILTER predicates cannot contain aggregates or windows"));
    }
    std::string out = function.name;
    out += '(';
    if (function.distinct) {
        out += "DISTINCT ";
    }
    if (function.raw_arg) {
        out += *function.raw_arg;
    } else {
        for (std::size_t i = 0; i < function.args.size(); ++i) {
            auto argument = compile_expr(function.args[i], params, false,
                                         aggregates_allowed || projection_root, filter_predicate);
            if (!argument) {
                return std::unexpected(argument.error());
            }
            out += *argument;
            if (i == 0 && function.window_position) {
                out += ", " + std::to_string(*function.window_position);
            }
            if (i + 1 < function.args.size()) {
                out += ", ";
            }
        }
    }
    out += ')';
    if (function.filter) {
        auto condition = compile_predicate(*function.filter, params, false, true);
        if (!condition) {
            return std::unexpected(condition.error());
        }
        out += " FILTER (WHERE " + *condition + ')';
    }
    if (function.over) {
        auto clause = compile_over(*function.over);
        if (!clause) {
            return std::unexpected(clause.error());
        }
        out += *clause;
    }
    return out;
}

asterorm::result<std::string> compiler::compile_arithmetic(const arithmetic_operand& operand,
                                                           query_parameters& params,
                                                           std::size_t& visited_nodes,
                                                           std::size_t depth) {
    if (depth > max_arithmetic_depth || ++visited_nodes > max_arithmetic_nodes) {
        return std::unexpected(invalid_sql("Assignment arithmetic exceeds the depth/node limit"));
    }
    if (const auto* column = std::get_if<column_expr>(&operand)) {
        return compile_expr(expr_ast{*column}, params);
    }
    if (const auto* parameter = std::get_if<param_expr>(&operand)) {
        if (parameter->encoded_value.error) {
            return std::unexpected(*parameter->encoded_value.error);
        }
        return compile_expr(expr_ast{*parameter}, params);
    }
    const auto& expression = std::get<arithmetic_ptr>(operand);
    if (!expression) {
        return std::unexpected(invalid_sql("Assignment arithmetic requires a non-null node"));
    }
    std::string_view operation;
    switch (expression->op) {
    case arithmetic_op::add:
        operation = " + ";
        break;
    case arithmetic_op::multiply:
        operation = " * ";
        break;
    default:
        return std::unexpected(invalid_sql("Invalid assignment arithmetic operator"));
    }
    auto left = compile_arithmetic(expression->left, params, visited_nodes, depth + 1);
    if (!left) {
        return std::unexpected(left.error());
    }
    auto right = compile_arithmetic(expression->right, params, visited_nodes, depth + 1);
    if (!right) {
        return std::unexpected(right.error());
    }
    return "(" + *left + std::string{operation} + *right + ")";
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
    if (const auto* arithmetic = std::get_if<arithmetic_ptr>(&src)) {
        std::size_t visited_nodes = 0;
        return compile_arithmetic(arithmetic_operand{*arithmetic}, params, visited_nodes);
    }
    params.push_back(std::get<param_expr>(src).encoded_value);
    return placeholder(params.size());
}

asterorm::result<std::string> compiler::compile_predicate(const predicate_ast& pred,
                                                          query_parameters& params,
                                                          bool aggregates_allowed,
                                                          bool filter_predicate) { // NOLINT
    if (std::holds_alternative<comparison_predicate>(pred.val)) {
        const auto& comp = std::get<comparison_predicate>(pred.val);
        auto left = compile_expr(comp.left, params, false, aggregates_allowed, filter_predicate);
        if (!left)
            return std::unexpected(left.error());
        auto right = compile_expr(comp.right, params, false, aggregates_allowed, filter_predicate);
        if (!right)
            return std::unexpected(right.error());
        return *left + " " + op_to_string(comp.op) + " " + *right;
    }
    if (std::holds_alternative<unary_predicate>(pred.val)) {
        const auto& un = std::get<unary_predicate>(pred.val);
        auto operand =
            compile_expr(un.operand, params, false, aggregates_allowed, filter_predicate);
        if (!operand)
            return std::unexpected(operand.error());
        return *operand + (un.op == unary_op_kind::is_null ? " IS NULL" : " IS NOT NULL");
    }
    const auto& bin = std::get<binary_predicate>(pred.val);
    auto left = compile_predicate(*bin.left, params, aggregates_allowed, filter_predicate);
    if (!left)
        return std::unexpected(left.error());
    auto right = compile_predicate(*bin.right, params, aggregates_allowed, filter_predicate);
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

asterorm::result<compiled_query> compiler::compile(const select_ast& ast) {
    window_detail::parameter_scope parameter_scope{cast_window_parameters_,
                                                   window_detail::contains_window(ast)};
    compiled_query query;
    auto rendered = compile_select(ast, query.params);
    if (!rendered) {
        return std::unexpected(rendered.error());
    }
    query.sql = std::move(*rendered);
    return query;
}

asterorm::result<std::string> compiler::compile_select(const select_ast& ast,
                                                       query_parameters& params) { // NOLINT
    auto windows_valid = validate_select_windows(ast);
    if (!windows_valid) {
        return std::unexpected(windows_valid.error());
    }
    auto locking = compile_select_lock(ast, dialect_);
    if (!locking) {
        return std::unexpected(locking.error());
    }
    std::string query;

    auto append_qualified = [&](std::string_view ident) -> asterorm::result<void> {
        auto quoted = quote_qualified_ident(ident);
        if (!quoted) {
            return std::unexpected(quoted.error());
        }
        query += *quoted;
        return {};
    };
    auto append_ident = [&](std::string_view ident) -> asterorm::result<void> {
        auto quoted = quote_ident(ident);
        if (!quoted) {
            return std::unexpected(quoted.error());
        }
        query += *quoted;
        return {};
    };

    query = "SELECT ";
    if (ast.distinct_on) {
        query += "DISTINCT ON (";
        for (std::size_t index = 0; index < ast.distinct_on->columns.size(); ++index) {
            if (index != 0) {
                query += ", ";
            }
            auto appended = append_qualified(ast.distinct_on->columns[index]);
            if (!appended) {
                return std::unexpected(appended.error());
            }
        }
        query += ") ";
    }
    if (!ast.items.empty()) {
        for (size_t i = 0; i < ast.items.size(); ++i) {
            const auto& item = ast.items[i];
            if (item.fn) {
                expr_ast wrapped = item.fn;
                auto rendered = compile_expr(wrapped, params, true);
                if (!rendered) {
                    return std::unexpected(rendered.error());
                }
                query += *rendered;
            } else if (item.column_name) {
                auto rendered = render_column(*item.column_name);
                if (!rendered) {
                    return std::unexpected(rendered.error());
                }
                query += *rendered;
            }
            if (item.fn && item.fn->alias && item.alias && item.fn->alias != item.alias) {
                return std::unexpected(invalid_sql("Conflicting projection aliases"));
            }
            const auto& alias = item.fn && !item.alias ? item.fn->alias : item.alias;
            if (alias) {
                query += " AS ";
                auto appended = append_ident(*alias);
                if (!appended) {
                    return std::unexpected(appended.error());
                }
            }
            if (i + 1 < ast.items.size()) {
                query += ", ";
            }
        }
    } else if (ast.columns.empty()) {
        query += '*';
    } else {
        for (size_t i = 0; i < ast.columns.size(); ++i) {
            auto rendered = render_column(ast.columns[i]);
            if (!rendered) {
                return std::unexpected(rendered.error());
            }
            query += *rendered;
            if (i + 1 < ast.columns.size()) {
                query += ", ";
            }
        }
    }

    query += " FROM ";
    if (auto appended = append_qualified(ast.table); !appended) {
        return std::unexpected(appended.error());
    }
    if (ast.table_alias) {
        query += " AS ";
        if (auto appended = append_ident(*ast.table_alias); !appended) {
            return std::unexpected(appended.error());
        }
    }

    for (const auto& join : ast.joins) {
        switch (join.kind) {
        case join_kind::inner:
            query += " INNER JOIN ";
            break;
        case join_kind::left:
            query += " LEFT JOIN ";
            break;
        case join_kind::right:
            query += " RIGHT JOIN ";
            break;
        case join_kind::full:
            if (dialect_ == dialect::mysql) {
                return std::unexpected(invalid_sql("MySQL does not support FULL JOIN"));
            }
            query += " FULL JOIN ";
            break;
        case join_kind::cross:
            query += " CROSS JOIN ";
            break;
        }
        if (auto appended = append_qualified(join.table); !appended) {
            return std::unexpected(appended.error());
        }
        if (join.alias) {
            query += " AS ";
            if (auto appended = append_ident(*join.alias); !appended) {
                return std::unexpected(appended.error());
            }
        }
        if (join.on_clause) {
            auto predicate = compile_predicate(*join.on_clause, params);
            if (!predicate) {
                return std::unexpected(predicate.error());
            }
            query += " ON " + *predicate;
        }
    }

    if (ast.where_clause) {
        auto predicate = compile_predicate(*ast.where_clause, params);
        if (!predicate) {
            return std::unexpected(predicate.error());
        }
        query += " WHERE " + *predicate;
    }

    if (!ast.group_bys.empty()) {
        query += " GROUP BY ";
        for (size_t i = 0; i < ast.group_bys.size(); ++i) {
            if (auto appended = append_qualified(ast.group_bys[i]); !appended) {
                return std::unexpected(appended.error());
            }
            if (i + 1 < ast.group_bys.size()) {
                query += ", ";
            }
        }
    }

    if (ast.having_clause) {
        auto predicate = compile_predicate(*ast.having_clause, params, true);
        if (!predicate) {
            return std::unexpected(predicate.error());
        }
        query += " HAVING " + *predicate;
    }

    if (!ast.order_bys.empty()) {
        query += " ORDER BY ";
        for (size_t i = 0; i < ast.order_bys.size(); ++i) {
            if (auto appended = append_qualified(ast.order_bys[i].column); !appended) {
                return std::unexpected(appended.error());
            }
            query += ast.order_bys[i].ascending ? " ASC" : " DESC";
            if (i < ast.order_bys.size() - 1) {
                query += ", ";
            }
        }
    }

    if (ast.limit_val) {
        query += " LIMIT " + std::to_string(*ast.limit_val);
    }
    if (dialect_ == dialect::mysql && ast.offset_val && !ast.limit_val) {
        query += " LIMIT 18446744073709551615";
    }
    if (dialect_ == dialect::sqlite && ast.offset_val && !ast.limit_val) {
        query += " LIMIT -1";
    }
    if (ast.offset_val) {
        query += " OFFSET " + std::to_string(*ast.offset_val);
    }

    query += *locking;

    return query;
}

asterorm::result<compiled_query> compiler::compile(const insert_select_ast& ast) {
    if (dialect_ != dialect::postgres && dialect_ != dialect::sqlite &&
        dialect_ != dialect::mysql) {
        return std::unexpected(invalid_sql("This dialect does not support INSERT SELECT"));
    }
    const auto& source = ast.source;
    if (ast.columns.empty() || (!source.columns.empty() && !source.items.empty()) ||
        (source.columns.empty() && source.items.empty()) || !source.joins.empty() ||
        !source.group_bys.empty() || source.having_clause || source.distinct_on ||
        source.locking.strength != select_lock_strength::none ||
        source.locking.wait != select_lock_wait::block ||
        (source.offset_val && !source.limit_val)) {
        return std::unexpected(
            invalid_sql("INSERT SELECT requires a column-only single-table source and explicit "
                        "destinations; OFFSET requires LIMIT"));
    }
    const auto width = source.items.empty() ? source.columns.size() : source.items.size();
    if (width != ast.columns.size()) {
        return std::unexpected(invalid_sql("INSERT SELECT projection/destination arity mismatch"));
    }
    auto target = quote_qualified_ident(ast.table);
    if (!target) {
        return std::unexpected(target.error());
    }
    auto source_table = quote_qualified_ident(source.table);
    if (!source_table) {
        return std::unexpected(source_table.error());
    }
    if (dialect_ == dialect::mysql && same_mysql_table_name(ast.table, source.table)) {
        return std::unexpected(
            invalid_sql("MySQL INSERT SELECT rejects matching source/destination table names"));
    }
    std::string prefix = "INSERT INTO " + *target + " (";
    for (std::size_t index = 0; index < ast.columns.size(); ++index) {
        const auto& name = ast.columns[index];
        if (std::find(ast.columns.begin(), ast.columns.begin() + static_cast<std::ptrdiff_t>(index),
                      name) != ast.columns.begin() + static_cast<std::ptrdiff_t>(index)) {
            return std::unexpected(invalid_sql("Duplicate INSERT SELECT destination column"));
        }
        auto quoted = quote_ident(name);
        if (!quoted) {
            return std::unexpected(quoted.error());
        }
        if (index != 0) {
            prefix += ", ";
        }
        prefix += *quoted;
    }
    prefix += ") ";
    for (const auto& name : source.columns) {
        auto valid = insert_source_column(*this, name);
        if (!valid) {
            return std::unexpected(valid.error());
        }
    }
    for (const auto& item : source.items) {
        if (item.fn || !item.column_name) {
            return std::unexpected(invalid_sql("INSERT SELECT requires column-only projections"));
        }
        auto valid = insert_source_column(*this, *item.column_name);
        if (!valid) {
            return std::unexpected(valid.error());
        }
    }
    for (const auto& order : source.order_bys) {
        auto valid = insert_source_column(*this, order.column);
        if (!valid) {
            return std::unexpected(valid.error());
        }
    }
    if (source.where_clause) {
        std::size_t visited = 0;
        auto valid = insert_source_predicate(*this, *source.where_clause, visited);
        if (!valid) {
            return std::unexpected(valid.error());
        }
    }
    auto selected = compile(source);
    if (!selected) {
        return std::unexpected(selected.error());
    }
    selected->sql = prefix + selected->sql;
    return selected;
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
