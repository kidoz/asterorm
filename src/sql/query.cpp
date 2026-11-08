#include "asterorm/sql/compiler.hpp"

#include <algorithm>
#include <unordered_set>

namespace asterorm::sql {
namespace {

std::string folded(std::string_view name) {
    std::string result{name};
    for (auto& character : result) {
        if (character >= 'A' && character <= 'Z') {
            character = static_cast<char>(character + ('a' - 'A'));
        }
    }
    return result;
}

bool simple_name(std::string_view name) {
    if (name.empty() || name.size() > 63) {
        return false;
    }
    auto letter = [](char character) {
        return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
               character == '_';
    };
    if (!letter(name.front())) {
        return false;
    }
    return std::ranges::all_of(name, [&](char character) {
        return letter(character) || (character >= '0' && character <= '9');
    });
}

// Validation walks occurrences, not distinct pointers: sharing a function is
// valid, but every emitted occurrence consumes budget and binds its arguments.
class query_validator {
  public:
    explicit query_validator(const compiler& renderer) : renderer_(renderer) {}

    result<std::vector<std::string>> validate(const query_ast& root) {
        if (root.ctes.size() > max_query_ctes) {
            return std::unexpected(error("Query exceeds the CTE limit"));
        }
        std::unordered_set<std::string> names;
        for (const auto& definition : root.ctes) {
            if (!simple_name(definition.name) || !names.insert(folded(definition.name)).second) {
                return std::unexpected(error("Invalid or duplicate CTE name"));
            }
            cte_names_.push_back(definition.name);
        }
        if (!visit(0) || !envelope(root)) {
            return std::unexpected(*failure_);
        }
        for (std::size_t index = 0; index < root.ctes.size(); ++index) {
            const auto& definition = root.ctes[index];
            path_ = "cte[" + std::to_string(index) + "]:" + definition.name;
            if (!definition.body) {
                return std::unexpected(error("CTE requires a non-null query body"));
            }
            if (!definition.body->ctes.empty()) {
                return std::unexpected(error("Nested WITH is not supported"));
            }
            if (!visit(1) || !envelope(*definition.body)) {
                return std::unexpected(*failure_);
            }
            auto labels = select(std::get<select_ast>(definition.body->body), index, 2);
            if (!labels) {
                return std::unexpected(labels.error());
            }
            if (!definition.columns.empty()) {
                if (definition.columns.size() != labels->size()) {
                    return std::unexpected(error("CTE column list/projection arity mismatch"));
                }
                if (!unique_labels(definition.columns)) {
                    return std::unexpected(*failure_);
                }
            }
        }
        path_ = "root";
        return select(std::get<select_ast>(root.body), root.ctes.size(), 1);
    }

  private:
    const compiler& renderer_;
    std::size_t visited_{};
    std::string path_{"root"};
    std::optional<db_error> failure_;
    std::vector<std::string> cte_names_;
    std::unordered_set<const func_expr*> active_functions_;

    [[nodiscard]] db_error error(std::string message) const {
        db_error diagnostic;
        diagnostic.kind = db_error_kind::parse_failed;
        diagnostic.message = std::move(message);
        diagnostic.detail = path_;
        return diagnostic;
    }
    bool fail(std::string message) {
        failure_ = error(std::move(message));
        return false;
    }
    bool visit(std::size_t depth) {
        return (depth <= max_query_depth && ++visited_ <= max_query_nodes) ||
               fail("Query exceeds the depth/node limit");
    }
    bool envelope(const query_ast& query) {
        if (query.recursive) {
            return fail("Recursive queries are not supported yet");
        }
        if (!std::holds_alternative<select_ast>(query.body)) {
            return fail("Set expressions are not supported yet");
        }
        return true;
    }
    bool identifier(std::string_view name, bool qualified = true) {
        auto quoted =
            qualified ? renderer_.quote_qualified_ident(name) : renderer_.quote_ident(name);
        if (!quoted) {
            failure_ = quoted.error();
            failure_->detail = path_;
            return false;
        }
        return true;
    }
    bool column(std::string_view name, std::size_t depth) {
        return visit(depth) && ((!name.contains('*') && identifier(name)) ||
                                fail("Advanced queries require explicit columns"));
    }
    bool relation(std::string_view name, std::size_t visible, std::size_t depth) {
        if (!visit(depth) || !identifier(name)) {
            return false;
        }
        if (name.contains('.')) {
            return true;
        }
        for (std::size_t index = 0; index < cte_names_.size(); ++index) {
            if (folded(name) != folded(cte_names_[index])) {
                continue;
            }
            if (name != cte_names_[index]) {
                return fail("CTE reference spelling must match its declaration");
            }
            if (index >= visible) {
                return fail("Self/forward CTE references require unsupported recursion");
            }
        }
        return true;
    }
    bool unique_labels(const std::vector<std::string>& labels) {
        std::unordered_set<std::string> seen;
        for (const auto& label : labels) {
            if (!simple_name(label) || !seen.insert(folded(label)).second) {
                return fail("Invalid or duplicate output label");
            }
        }
        return true;
    }
    bool expression(const expr_ast& operand, std::size_t depth, bool projection = false) {
        if (!visit(depth)) {
            return false;
        }
        if (const auto* source = std::get_if<column_expr>(&operand)) {
            return (!source->name.contains('*') && identifier(source->name)) ||
                   fail("Advanced queries require explicit columns");
        }
        if (const auto* parameter = std::get_if<param_expr>(&operand)) {
            if (parameter->encoded_value.error) {
                failure_ = *parameter->encoded_value.error;
                return false;
            }
            return !parameter->encoded_value.value ||
                   !parameter->encoded_value.value->contains('\0') ||
                   fail("SQL parameter contains a NUL byte");
        }
        const auto* function = std::get_if<std::shared_ptr<func_expr>>(&operand);
        if (!function || !*function) {
            return fail("Expression requires a non-null function node");
        }
        if (!active_functions_.insert(function->get()).second) {
            return fail("Function graph contains a cycle");
        }
        const auto& node = **function;
        if (node.alias && !projection) {
            return fail("Function aliases belong in the projection");
        }
        if (node.raw_arg && (folded(node.name) != "count" || *node.raw_arg != "*" ||
                             !node.args.empty() || node.distinct)) {
            return fail("Advanced queries only allow the structured COUNT(*) raw argument");
        }
        for (const auto& argument : node.args) {
            if (!expression(argument, depth + 1)) {
                return false;
            }
        }
        active_functions_.erase(function->get());
        return true;
    }
    bool predicate(const predicate_ast& condition, std::size_t depth) {
        if (!visit(depth)) {
            return false;
        }
        if (const auto* comparison = std::get_if<comparison_predicate>(&condition.val)) {
            switch (comparison->op) {
            case op_kind::eq:
            case op_kind::neq:
            case op_kind::lt:
            case op_kind::gt:
            case op_kind::le:
            case op_kind::ge:
                return expression(comparison->left, depth + 1) &&
                       expression(comparison->right, depth + 1);
            default:
                return fail("Invalid comparison operator");
            }
        }
        if (const auto* unary = std::get_if<unary_predicate>(&condition.val)) {
            if (unary->op != unary_op_kind::is_null && unary->op != unary_op_kind::is_not_null) {
                return fail("Invalid unary operator");
            }
            return expression(unary->operand, depth + 1);
        }
        const auto* binary = std::get_if<binary_predicate>(&condition.val);
        if (!binary || !binary->left || !binary->right ||
            (binary->op != op_kind::and_ && binary->op != op_kind::or_)) {
            return fail("Invalid binary predicate");
        }
        return predicate(*binary->left, depth + 1) && predicate(*binary->right, depth + 1);
    }
    result<std::vector<std::string>> select(const select_ast& leaf, std::size_t visible,
                                            std::size_t depth) {
        if (!visit(depth)) {
            return std::unexpected(*failure_);
        }
        if (leaf.locking.strength != select_lock_strength::none ||
            leaf.locking.wait != select_lock_wait::block) {
            return std::unexpected(error("Advanced queries do not support locking"));
        }
        if ((!leaf.columns.empty() && !leaf.items.empty()) ||
            (leaf.columns.empty() && leaf.items.empty())) {
            return std::unexpected(error("Advanced queries require one explicit projection list"));
        }
        std::vector<std::string> labels;
        for (const auto& name : leaf.columns) {
            if (!column(name, depth + 1)) {
                return std::unexpected(*failure_);
            }
            labels.push_back(name.substr(name.find_last_of('.') + 1));
        }
        for (const auto& item : leaf.items) {
            if (item.column_name.has_value() == static_cast<bool>(item.fn)) {
                return std::unexpected(error("Projection requires exactly one column/function"));
            }
            auto alias = item.alias;
            if (item.fn) {
                if (alias && item.fn->alias && alias != item.fn->alias) {
                    return std::unexpected(error("Conflicting projection aliases"));
                }
                if (!alias) {
                    alias = item.fn->alias;
                }
                if (!alias) {
                    return std::unexpected(error("Function projection requires an output alias"));
                }
                if (!expression(expr_ast{item.fn}, depth + 1, true)) {
                    return std::unexpected(*failure_);
                }
            } else if (!column(*item.column_name, depth + 1)) {
                return std::unexpected(*failure_);
            }
            labels.push_back(
                alias ? *alias : item.column_name->substr(item.column_name->find_last_of('.') + 1));
        }
        if (!unique_labels(labels) || !relation(leaf.table, visible, depth + 1) ||
            (leaf.table_alias && !identifier(*leaf.table_alias, false))) {
            return std::unexpected(*failure_);
        }
        for (const auto& join : leaf.joins) {
            switch (join.kind) {
            case join_kind::inner:
            case join_kind::left:
            case join_kind::right:
            case join_kind::full:
                if (!join.on_clause) {
                    return std::unexpected(error("JOIN requires an ON predicate"));
                }
                break;
            case join_kind::cross:
                if (join.on_clause) {
                    return std::unexpected(error("CROSS JOIN cannot have an ON predicate"));
                }
                break;
            default:
                return std::unexpected(error("Invalid JOIN kind"));
            }
            if (!relation(join.table, visible, depth + 1) ||
                (join.alias && !identifier(*join.alias, false)) ||
                (join.on_clause && !predicate(*join.on_clause, depth + 1))) {
                return std::unexpected(*failure_);
            }
        }
        if (leaf.where_clause && !predicate(*leaf.where_clause, depth + 1)) {
            return std::unexpected(*failure_);
        }
        for (const auto& name : leaf.group_bys) {
            if (!column(name, depth + 1)) {
                return std::unexpected(*failure_);
            }
        }
        if (leaf.having_clause && !predicate(*leaf.having_clause, depth + 1)) {
            return std::unexpected(*failure_);
        }
        for (const auto& order : leaf.order_bys) {
            if (!column(order.column, depth + 1)) {
                return std::unexpected(*failure_);
            }
        }
        return labels;
    }
};
} // namespace

result<compiled_query> compiler::compile(const query_ast& ast) {
    if ((dialect_ != dialect::postgres && dialect_ != dialect::sqlite &&
         dialect_ != dialect::mysql) ||
        (query_options_.mysql_profile != mysql_query_profile::baseline_8_0_19 &&
         query_options_.mysql_profile != mysql_query_profile::set_operations_8_0_31)) {
        db_error diagnostic;
        diagnostic.kind = db_error_kind::parse_failed;
        diagnostic.message = "Unsupported advanced-query dialect/profile";
        return std::unexpected(diagnostic);
    }
    auto labels = query_validator{*this}.validate(ast);
    if (!labels) {
        return std::unexpected(labels.error());
    }
    compiled_query query;
    query.result_columns = std::move(*labels);
    if (!ast.ctes.empty()) {
        query.sql = "WITH ";
        for (std::size_t index = 0; index < ast.ctes.size(); ++index) {
            const auto& definition = ast.ctes[index];
            if (index != 0) {
                query.sql += ", ";
            }
            query.sql += *quote_ident(definition.name);
            if (!definition.columns.empty()) {
                query.sql += '(';
                for (std::size_t column_index = 0; column_index < definition.columns.size();
                     ++column_index) {
                    if (column_index != 0) {
                        query.sql += ", ";
                    }
                    query.sql += *quote_ident(definition.columns[column_index]);
                }
                query.sql += ')';
            }
            auto body = compile_select(std::get<select_ast>(definition.body->body), query.params);
            if (!body) {
                return std::unexpected(body.error());
            }
            query.sql += " AS (" + *body + ')';
        }
        query.sql += ' ';
    }
    auto body = compile_select(std::get<select_ast>(ast.body), query.params);
    if (!body) {
        return std::unexpected(body.error());
    }
    query.sql += *body;
    return query;
}
} // namespace asterorm::sql
