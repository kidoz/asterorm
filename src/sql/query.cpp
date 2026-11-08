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
    query_validator(const compiler& renderer, dialect target, query_compile_options options)
        : renderer_(renderer), target_(target), options_(options) {}

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
        if (!visit(0) || !envelope(root, true)) {
            return std::unexpected(*failure_);
        }
        for (std::size_t index = 0; index < root.ctes.size(); ++index) {
            const auto& definition = root.ctes[index];
            path_ = "cte[" + std::to_string(index) + "]:" + definition.name;
            if (!definition.body) {
                return std::unexpected(error("CTE requires a non-null query body"));
            }
            // Only a direct SELECT step can introduce the one admitted self-reference.
            const auto* compound = std::get_if<compound_ast>(&definition.body->body);
            const auto* step = compound && compound->right
                                   ? std::get_if<select_ast>(&compound->right->body)
                                   : nullptr;
            if (step && step->joins.size() > max_query_nodes) {
                return std::unexpected(error("Query exceeds the node limit"));
            }
            const bool self_step = root.recursive && step && references(*step, definition.name);
            auto labels = self_step ? recursive_definition(definition, index)
                                    : operand(*definition.body, index, 1);
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
        if (root.recursive && !recursive_definition_) {
            return std::unexpected(error("RECURSIVE requires exactly one self-referencing CTE"));
        }
        return body(root, root.ctes.size(), 1);
    }

    [[nodiscard]] std::optional<std::size_t> recursive_definition_index() const {
        return recursive_definition_;
    }

  private:
    const compiler& renderer_;
    dialect target_;
    query_compile_options options_;
    std::size_t visited_{};
    std::string path_{"root"};
    std::optional<db_error> failure_;
    std::vector<std::string> cte_names_;
    std::unordered_set<const func_expr*> active_functions_;
    std::optional<std::size_t> recursive_definition_;
    std::optional<std::size_t> allowed_self_;
    bool recursive_step_{};

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
    bool envelope(const query_ast& query, bool statement_root = false) {
        if (query.recursive && !statement_root) {
            return fail("RECURSIVE is only allowed at the statement root");
        }
        if (query.body.valueless_by_exception()) {
            return fail("Query requires a valid body variant");
        }
        return true;
    }
    static bool same_relation(std::string_view table, std::string_view name) {
        return !table.contains('.') && folded(table) == folded(name);
    }
    static bool references(const select_ast& leaf, std::string_view name) {
        return same_relation(leaf.table, name) ||
               std::ranges::any_of(
                   leaf.joins, [&](const auto& join) { return same_relation(join.table, name); });
    }
    static bool has_tail(const select_ast& leaf) {
        return !leaf.order_bys.empty() || leaf.limit_val || leaf.offset_val;
    }
    result<std::vector<std::string>> recursive_definition(const cte_ast& definition,
                                                          std::size_t index) {
        if (recursive_definition_) {
            return std::unexpected(error("Only one self-referencing CTE is supported"));
        }
        if (definition.columns.empty()) {
            return std::unexpected(error("A recursive CTE requires explicit output columns"));
        }
        const auto& query = *definition.body;
        if (!visit(1) || !envelope(query)) {
            return std::unexpected(*failure_);
        }
        if (!query.ctes.empty()) {
            return std::unexpected(error("Nested WITH is not supported"));
        }
        if (!visit(2)) {
            return std::unexpected(*failure_);
        }
        const auto& compound = std::get<compound_ast>(query.body);
        if (compound.operation != set_operator::union_ ||
            compound.quantifier != set_quantifier::all || !compound.left || !compound.right ||
            !compound.order_bys.empty() || compound.limit_val || compound.offset_val) {
            return std::unexpected(
                error("A recursive CTE requires a two-arm UNION ALL without local tails"));
        }
        const auto* anchor = std::get_if<select_ast>(&compound.left->body);
        const auto* step = std::get_if<select_ast>(&compound.right->body);
        if (!anchor || !step || has_tail(*anchor) || has_tail(*step) || !step->group_bys.empty() ||
            step->having_clause) {
            return std::unexpected(error("Recursive arms require plain SELECTs without local "
                                         "tails; step grouping is unsupported"));
        }
        std::size_t self_count = same_relation(step->table, definition.name) ? 1 : 0;
        for (const auto& join : step->joins) {
            if (join.kind != join_kind::inner && join.kind != join_kind::cross) {
                return std::unexpected(error("Recursive steps do not support outer/invalid JOINs"));
            }
            if (same_relation(join.table, definition.name)) {
                if (join.kind != join_kind::inner) {
                    return std::unexpected(error("A recursive self JOIN must be INNER"));
                }
                ++self_count;
            }
        }
        if (self_count != 1) {
            return std::unexpected(
                error("A recursive step requires exactly one top-level self relation"));
        }
        const auto parent_path = path_;
        path_ = parent_path + ".anchor";
        auto anchor_labels = operand(*compound.left, index, 3);
        if (!anchor_labels) {
            return std::unexpected(anchor_labels.error());
        }
        path_ = parent_path + ".step";
        allowed_self_ = index;
        recursive_step_ = true;
        auto step_labels = operand(*compound.right, index, 3);
        allowed_self_.reset();
        recursive_step_ = false;
        if (!step_labels) {
            return std::unexpected(step_labels.error());
        }
        path_ = parent_path;
        if (anchor_labels->size() != step_labels->size()) {
            return std::unexpected(error("Recursive anchor/step projection arity mismatch"));
        }
        recursive_definition_ = index;
        return anchor_labels;
    }
    result<std::vector<std::string>> operand(const query_ast& query, std::size_t visible,
                                             std::size_t depth) {
        if (!visit(depth) || !envelope(query)) {
            return std::unexpected(*failure_);
        }
        if (!query.ctes.empty()) {
            return std::unexpected(error("Nested WITH is not supported"));
        }
        return body(query, visible, depth + 1);
    }
    result<std::vector<std::string>> body(const query_ast& query, std::size_t visible,
                                          std::size_t depth) {
        if (const auto* leaf = std::get_if<select_ast>(&query.body)) {
            return select(*leaf, visible, depth);
        }
        if (!visit(depth)) {
            return std::unexpected(*failure_);
        }
        const auto& compound = std::get<compound_ast>(query.body);
        if (!compound.left || !compound.right) {
            return std::unexpected(error("Set expressions require two non-null operands"));
        }
        switch (compound.operation) {
        case set_operator::union_:
        case set_operator::intersect_:
        case set_operator::except_:
            break;
        default:
            return std::unexpected(error("Invalid set operator"));
        }
        if (compound.quantifier != set_quantifier::distinct &&
            compound.quantifier != set_quantifier::all) {
            return std::unexpected(error("Invalid set quantifier"));
        }
        if (compound.operation != set_operator::union_) {
            if (target_ == dialect::mysql &&
                options_.mysql_profile != mysql_query_profile::set_operations_8_0_31) {
                return std::unexpected(
                    error("MySQL INTERSECT/EXCEPT requires the 8.0.31+ profile"));
            }
            if (target_ == dialect::sqlite && compound.quantifier == set_quantifier::all) {
                return std::unexpected(error("SQLite does not support INTERSECT ALL/EXCEPT ALL"));
            }
        }
        const auto parent_path = path_;
        path_ = parent_path + ".left";
        auto left = operand(*compound.left, visible, depth + 1);
        if (!left) {
            return std::unexpected(left.error());
        }
        path_ = parent_path + ".right";
        auto right = operand(*compound.right, visible, depth + 1);
        if (!right) {
            return std::unexpected(right.error());
        }
        path_ = parent_path;
        if (left->empty() || left->size() != right->size()) {
            return std::unexpected(error("Set operands require equal nonzero projection arity"));
        }
        for (const auto& order : compound.order_bys) {
            if (!visit(depth + 1)) {
                return std::unexpected(*failure_);
            }
            if (!simple_name(order.column) ||
                std::ranges::find(*left, order.column) == left->end()) {
                return std::unexpected(error("Compound ORDER BY requires a left output label"));
            }
        }
        return left;
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
            if (index >= visible && allowed_self_ != index) {
                return fail("Self/forward CTE reference is outside the admitted recursive step");
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
        if (recursive_step_) {
            return fail("Recursive steps support only column/parameter expressions");
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

result<std::string> compiler::compile_query_body(const query_ast& ast, query_parameters& params,
                                                 std::size_t& next_alias) {
    if (const auto* leaf = std::get_if<select_ast>(&ast.body)) {
        return compile_select(*leaf, params);
    }
    const auto& compound = std::get<compound_ast>(ast.body);
    auto arm = [&](const query_ast& child) -> result<std::string> {
        // Each alias is local to its wrapper, outside the child's name scope.
        auto alias = quote_ident("asterorm_set_" + std::to_string(next_alias++));
        auto rendered = compile_query_body(child, params, next_alias);
        if (!rendered) {
            return std::unexpected(rendered.error());
        }
        return "SELECT * FROM (" + *rendered + ") AS " + *alias;
    };
    auto left = arm(*compound.left);
    if (!left) {
        return std::unexpected(left.error());
    }
    auto right = arm(*compound.right);
    if (!right) {
        return std::unexpected(right.error());
    }
    std::string operation;
    switch (compound.operation) {
    case set_operator::union_:
        operation = " UNION";
        break;
    case set_operator::intersect_:
        operation = " INTERSECT";
        break;
    case set_operator::except_:
        operation = " EXCEPT";
        break;
    }
    if (compound.quantifier == set_quantifier::all) {
        operation += " ALL";
    }
    std::string rendered = *left + operation + ' ' + *right;
    for (std::size_t index = 0; index < compound.order_bys.size(); ++index) {
        const auto& order = compound.order_bys[index];
        rendered += index == 0 ? " ORDER BY " : ", ";
        rendered += *quote_ident(order.column);
        rendered += order.ascending ? " ASC" : " DESC";
    }
    if (compound.limit_val) {
        rendered += " LIMIT " + std::to_string(*compound.limit_val);
    } else if (compound.offset_val && dialect_ == dialect::sqlite) {
        rendered += " LIMIT -1";
    } else if (compound.offset_val && dialect_ == dialect::mysql) {
        rendered += " LIMIT 18446744073709551615";
    }
    if (compound.offset_val) {
        rendered += " OFFSET " + std::to_string(*compound.offset_val);
    }
    return rendered;
}

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
    query_validator validator{*this, dialect_, query_options_};
    auto labels = validator.validate(ast);
    if (!labels) {
        return std::unexpected(labels.error());
    }
    compiled_query query;
    query.result_columns = std::move(*labels);
    std::size_t next_alias = 0;
    if (!ast.ctes.empty()) {
        query.sql = ast.recursive ? "WITH RECURSIVE " : "WITH ";
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
            result<std::string> body;
            if (validator.recursive_definition_index() == index) {
                // A recursive reference must remain in the direct SELECT step.
                // Ordinary set wrappers would hide it inside a derived query.
                const auto& compound = std::get<compound_ast>(definition.body->body);
                auto anchor =
                    compile_select(std::get<select_ast>(compound.left->body), query.params);
                if (!anchor) {
                    return std::unexpected(anchor.error());
                }
                auto step =
                    compile_select(std::get<select_ast>(compound.right->body), query.params);
                if (!step) {
                    return std::unexpected(step.error());
                }
                body = *anchor + " UNION ALL " + *step;
            } else {
                body = compile_query_body(*definition.body, query.params, next_alias);
            }
            if (!body) {
                return std::unexpected(body.error());
            }
            query.sql += " AS (" + *body + ')';
        }
        query.sql += ' ';
    }
    auto body = compile_query_body(ast, query.params, next_alias);
    if (!body) {
        return std::unexpected(body.error());
    }
    query.sql += *body;
    return query;
}
} // namespace asterorm::sql
