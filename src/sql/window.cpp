#include "window_validation.hpp"

#include <algorithm>
#include <limits>
#include <unordered_set>

namespace asterorm::sql {
namespace {
db_error invalid_window(std::string message) {
    db_error diagnostic;
    diagnostic.kind = db_error_kind::parse_failed;
    diagnostic.message = std::move(message);
    return diagnostic;
}
std::string lower_name(std::string name) {
    for (auto& character : name) {
        if (character >= 'A' && character <= 'Z') {
            character = static_cast<char>(character + ('a' - 'A'));
        }
    }
    return name;
}
result<void> valid_frame(const rows_frame& frame) {
    auto bound = [](const frame_bound& edge) -> bool {
        switch (edge.kind) {
        case frame_bound_kind::preceding:
        case frame_bound_kind::following:
            return edge.offset >= 0;
        case frame_bound_kind::unbounded_preceding:
        case frame_bound_kind::current_row:
        case frame_bound_kind::unbounded_following:
            return edge.offset == 0;
        }
        return false;
    };
    if (!bound(frame.start) || !bound(frame.end) ||
        frame.start.kind == frame_bound_kind::unbounded_following ||
        frame.end.kind == frame_bound_kind::unbounded_preceding) {
        return std::unexpected(invalid_window("Invalid ROWS frame bound"));
    }
    auto coordinate = [](const frame_bound& edge) -> std::int64_t {
        switch (edge.kind) {
        case frame_bound_kind::unbounded_preceding:
            return std::numeric_limits<std::int64_t>::min();
        case frame_bound_kind::preceding:
            return -edge.offset;
        case frame_bound_kind::current_row:
            return 0;
        case frame_bound_kind::following:
            return edge.offset;
        case frame_bound_kind::unbounded_following:
            return std::numeric_limits<std::int64_t>::max();
        }
        return 0;
    };
    if (frame.start.kind > frame.end.kind || coordinate(frame.start) > coordinate(frame.end)) {
        return std::unexpected(invalid_window("ROWS frame start must not follow its end"));
    }
    return {};
}
} // namespace

bool window_detail::contains_window(const select_ast& ast) {
    // Presence detection is iterative and deduplicates shared functions. Full
    // validation later counts emitted occurrences and checks active-path cycles.
    std::vector<const predicate_ast*> predicates;
    std::vector<const func_expr*> functions;
    std::vector<const expr_ast*> expressions;
    std::unordered_set<const func_expr*> seen;
    for (const auto& item : ast.items) {
        if (item.fn) {
            functions.push_back(item.fn.get());
        }
    }
    if (ast.where_clause) {
        predicates.push_back(&*ast.where_clause);
    }
    if (ast.having_clause) {
        predicates.push_back(&*ast.having_clause);
    }
    for (const auto& join : ast.joins) {
        if (join.on_clause) {
            predicates.push_back(&*join.on_clause);
        }
    }
    while (!predicates.empty() || !functions.empty() || !expressions.empty()) {
        if (!predicates.empty()) {
            const auto* predicate = predicates.back();
            predicates.pop_back();
            if (const auto* comparison = std::get_if<comparison_predicate>(&predicate->val)) {
                expressions.push_back(&comparison->left);
                expressions.push_back(&comparison->right);
            } else if (const auto* unary = std::get_if<unary_predicate>(&predicate->val)) {
                expressions.push_back(&unary->operand);
            } else if (const auto* binary = std::get_if<binary_predicate>(&predicate->val)) {
                if (binary->left) {
                    predicates.push_back(binary->left.get());
                }
                if (binary->right) {
                    predicates.push_back(binary->right.get());
                }
            }
        } else if (!expressions.empty()) {
            const auto* expression = expressions.back();
            expressions.pop_back();
            if (const auto* function = std::get_if<std::shared_ptr<func_expr>>(expression);
                function && *function) {
                functions.push_back(function->get());
            }
        } else {
            const auto* function = functions.back();
            functions.pop_back();
            if (!seen.insert(function).second) {
                continue;
            }
            if (function->over || function->window_position || function->filter) {
                return true;
            }
            for (const auto& argument : function->args) {
                expressions.push_back(&argument);
            }
        }
    }
    return false;
}

bool window_detail::is_aggregate(std::string name) {
    name = lower_name(std::move(name));
    return name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max";
}
result<void> window_detail::validate_distinct(const select_ast& ast, dialect target) {
    if (!ast.distinct_on) {
        return {};
    }
    const auto& clause = *ast.distinct_on;
    if (target != dialect::postgres) {
        return std::unexpected(invalid_window("DISTINCT ON requires PostgreSQL"));
    }
    if (clause.columns.empty() || (clause.ties != distinct_on_ties::require_tiebreaker &&
                                   clause.ties != distinct_on_ties::allow_peers)) {
        return std::unexpected(
            invalid_window("DISTINCT ON requires keys and an explicit tie policy"));
    }
    if (ast.locking.strength != select_lock_strength::none ||
        ast.locking.wait != select_lock_wait::block) {
        return std::unexpected(invalid_window("DISTINCT ON does not support row locking"));
    }
    if (ast.order_bys.size() < clause.columns.size()) {
        return std::unexpected(invalid_window("DISTINCT ON keys must lead ORDER BY"));
    }
    for (std::size_t index = 0; index < clause.columns.size(); ++index) {
        const auto& key = clause.columns[index];
        if (key.empty() || key.contains('*') || key.contains('\0') ||
            key != ast.order_bys[index].column ||
            std::find(clause.columns.begin(),
                      clause.columns.begin() + static_cast<std::ptrdiff_t>(index),
                      key) != clause.columns.begin() + static_cast<std::ptrdiff_t>(index)) {
            return std::unexpected(invalid_window(
                "DISTINCT ON keys must be unique columns matching the ORDER BY prefix"));
        }
    }
    if (clause.ties == distinct_on_ties::require_tiebreaker &&
        std::none_of(ast.order_bys.begin() + static_cast<std::ptrdiff_t>(clause.columns.size()),
                     ast.order_bys.end(), [&](const auto& order) {
                         return std::find(clause.columns.begin(), clause.columns.end(),
                                          order.column) == clause.columns.end();
                     })) {
        return std::unexpected(
            invalid_window("DISTINCT ON tie policy requires an additional ORDER BY key"));
    }
    return {};
}

result<void> window_detail::validate_function(const func_expr& function, dialect target) {
    const auto name = lower_name(function.name);
    const bool ranking = name == "row_number" || name == "rank" || name == "dense_rank";
    const bool navigation = name == "lag" || name == "lead";
    const bool frame_value = name == "first_value" || name == "last_value" || name == "nth_value";
    const bool count = name == "count";
    const bool aggregate =
        count || name == "sum" || name == "avg" || name == "min" || name == "max";
    if (function.filter) {
        if (target != dialect::postgres && target != dialect::sqlite) {
            return std::unexpected(
                invalid_window("Aggregate FILTER requires PostgreSQL or SQLite"));
        }
        if (!aggregate) {
            return std::unexpected(invalid_window("FILTER requires COUNT/SUM/AVG/MIN/MAX"));
        }
        if (function.raw_arg &&
            (!count || *function.raw_arg != "*" || !function.args.empty() || function.distinct)) {
            return std::unexpected(
                invalid_window("FILTER only allows structured COUNT(*) raw arguments"));
        }
        if (count && function.raw_arg ? !function.args.empty() : function.args.size() != 1) {
            return std::unexpected(invalid_window("Invalid filtered aggregate arity"));
        }
    }
    if (!function.over) {
        if (ranking || navigation || frame_value || function.window_position) {
            return std::unexpected(invalid_window("Window functions require OVER"));
        }
        return {};
    }
    if (target != dialect::postgres && target != dialect::sqlite && target != dialect::mysql) {
        return std::unexpected(invalid_window("This dialect does not support structured windows"));
    }
    if (!ranking && !navigation && !frame_value && !aggregate) {
        return std::unexpected(invalid_window("Unsupported function with OVER"));
    }
    if (function.distinct) {
        return std::unexpected(invalid_window("DISTINCT window aggregates are unsupported"));
    }
    if (function.raw_arg && (!count || *function.raw_arg != "*" || !function.args.empty())) {
        return std::unexpected(
            invalid_window("Window functions only allow structured COUNT(*) raw arguments"));
    }
    const auto arity = function.args.size();
    if ((ranking && (arity != 0 || function.raw_arg)) ||
        (navigation && (arity < 1 || arity > 2 || function.raw_arg)) ||
        (frame_value && (arity != 1 || function.raw_arg)) ||
        (aggregate && (count && function.raw_arg ? arity != 0 : arity != 1))) {
        return std::unexpected(invalid_window("Invalid window function arity"));
    }
    if (navigation || name == "nth_value") {
        if (!function.window_position || *function.window_position < (navigation ? 0 : 1)) {
            return std::unexpected(invalid_window("Invalid window offset/position"));
        }
    } else if (function.window_position) {
        return std::unexpected(invalid_window("This window function does not take a position"));
    }
    const auto& clause = *function.over;
    if ((ranking || navigation || frame_value) && clause.order_bys.empty()) {
        return std::unexpected(invalid_window("This window function requires ORDER BY"));
    }
    if (frame_value && !clause.rows) {
        return std::unexpected(
            invalid_window("Frame-value functions require an explicit ROWS frame"));
    }
    if (clause.rows) {
        return valid_frame(*clause.rows);
    }
    return {};
}

result<std::string> compiler::compile_over(const over_clause& clause) const {
    std::string output = " OVER (";
    for (std::size_t index = 0; index < clause.partition_bys.size(); ++index) {
        auto column = quote_qualified_ident(clause.partition_bys[index]);
        if (!column) {
            return std::unexpected(column.error());
        }
        output += index == 0 ? "PARTITION BY " : ", ";
        output += *column;
    }
    for (std::size_t index = 0; index < clause.order_bys.size(); ++index) {
        auto column = quote_qualified_ident(clause.order_bys[index].column);
        if (!column) {
            return std::unexpected(column.error());
        }
        if (index == 0) {
            output += clause.partition_bys.empty() ? "ORDER BY " : " ORDER BY ";
        } else {
            output += ", ";
        }
        output += *column;
        output += clause.order_bys[index].ascending ? " ASC" : " DESC";
    }
    if (clause.rows) {
        auto render_bound = [](const frame_bound& edge) -> std::string {
            switch (edge.kind) {
            case frame_bound_kind::unbounded_preceding:
                return "UNBOUNDED PRECEDING";
            case frame_bound_kind::preceding:
                return std::to_string(edge.offset) + " PRECEDING";
            case frame_bound_kind::current_row:
                return "CURRENT ROW";
            case frame_bound_kind::following:
                return std::to_string(edge.offset) + " FOLLOWING";
            case frame_bound_kind::unbounded_following:
                return "UNBOUNDED FOLLOWING";
            }
            return {};
        };
        if (!clause.partition_bys.empty() || !clause.order_bys.empty()) {
            output += ' ';
        }
        output += "ROWS BETWEEN " + render_bound(clause.rows->start) + " AND " +
                  render_bound(clause.rows->end);
    }
    return output + ')';
}
} // namespace asterorm::sql
