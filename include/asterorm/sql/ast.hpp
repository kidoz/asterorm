#pragma once
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "asterorm/core/codecs.hpp"

namespace asterorm::sql {

struct column_expr {
    std::string name;
};

struct param_expr {
    std::optional<std::string> encoded_value;
};

// Function call such as COUNT(*), SUM(x), MAX(x). `raw_args` is emitted
// verbatim (no parameterization) and must be rendered by the caller (e.g.
// "*"); used to support DISTINCT and wildcard counts.
struct func_expr;

using expr_ast = std::variant<column_expr, param_expr, std::shared_ptr<func_expr>>;

struct func_expr {
    std::string name; // "count", "sum", "avg", "min", "max", ...
    std::vector<expr_ast> args;
    bool distinct{false};
    std::optional<std::string> raw_arg; // e.g. "*"
    std::optional<std::string> alias;   // rendered as AS "alias"
};

inline column_expr col(std::string name) {
    return {std::move(name)};
}

template <typename T> inline param_expr val(T&& v) {
    return {asterorm::encode(std::forward<T>(v))};
}

inline std::shared_ptr<func_expr> func(std::string name, std::vector<expr_ast> args = {}) {
    return std::make_shared<func_expr>(
        func_expr{std::move(name), std::move(args), false, std::nullopt, std::nullopt});
}

inline std::shared_ptr<func_expr> count_all() {
    return std::make_shared<func_expr>(
        func_expr{"count", {}, false, std::string{"*"}, std::nullopt});
}

inline std::shared_ptr<func_expr> count(expr_ast arg) {
    return func("count", {std::move(arg)});
}

inline std::shared_ptr<func_expr> sum(expr_ast arg) {
    return func("sum", {std::move(arg)});
}

inline std::shared_ptr<func_expr> avg(expr_ast arg) {
    return func("avg", {std::move(arg)});
}

inline std::shared_ptr<func_expr> min_(expr_ast arg) {
    return func("min", {std::move(arg)});
}

inline std::shared_ptr<func_expr> max_(expr_ast arg) {
    return func("max", {std::move(arg)});
}

inline std::shared_ptr<func_expr> as(std::shared_ptr<func_expr> f, std::string alias) {
    f->alias = std::move(alias);
    return f;
}

enum class op_kind : std::uint8_t { eq, neq, lt, gt, le, ge, and_, or_ };

struct predicate_ast;

struct comparison_predicate {
    op_kind op;
    expr_ast left;
    expr_ast right;
};

struct binary_predicate {
    op_kind op;
    std::unique_ptr<predicate_ast> left;
    std::unique_ptr<predicate_ast> right;
};

struct predicate_ast {
    std::variant<comparison_predicate, binary_predicate> val;
};

inline predicate_ast operator==(const expr_ast& lhs, const expr_ast& rhs) {
    return predicate_ast{comparison_predicate{op_kind::eq, lhs, rhs}};
}
inline predicate_ast operator!=(const expr_ast& lhs, const expr_ast& rhs) {
    return predicate_ast{comparison_predicate{op_kind::neq, lhs, rhs}};
}
inline predicate_ast operator<(const expr_ast& lhs, const expr_ast& rhs) {
    return predicate_ast{comparison_predicate{op_kind::lt, lhs, rhs}};
}
inline predicate_ast operator>(const expr_ast& lhs, const expr_ast& rhs) {
    return predicate_ast{comparison_predicate{op_kind::gt, lhs, rhs}};
}
inline predicate_ast operator<=(const expr_ast& lhs, const expr_ast& rhs) {
    return predicate_ast{comparison_predicate{op_kind::le, lhs, rhs}};
}
inline predicate_ast operator>=(const expr_ast& lhs, const expr_ast& rhs) {
    return predicate_ast{comparison_predicate{op_kind::ge, lhs, rhs}};
}

inline predicate_ast operator&&(predicate_ast lhs, predicate_ast rhs) {
    return predicate_ast{binary_predicate{op_kind::and_,
                                          std::make_unique<predicate_ast>(std::move(lhs)),
                                          std::make_unique<predicate_ast>(std::move(rhs))}};
}
inline predicate_ast operator||(predicate_ast lhs, predicate_ast rhs) {
    return predicate_ast{binary_predicate{op_kind::or_,
                                          std::make_unique<predicate_ast>(std::move(lhs)),
                                          std::make_unique<predicate_ast>(std::move(rhs))}};
}

struct order_by_ast {
    std::string column;
    bool ascending{true};
};

enum class join_kind : std::uint8_t { inner, left, right, full, cross };

struct join_ast {
    join_kind kind{join_kind::inner};
    std::string table;
    std::optional<std::string> alias;
    std::optional<predicate_ast> on_clause; // omitted for CROSS JOIN
};

// Rich select column: either a plain column name (legacy path), a function
// expression (aggregates), or a column with optional alias.
struct select_item {
    // If func is set, emit it; else emit column_name. `"*"` in column_name
    // is rendered as bare asterisk.
    std::optional<std::string> column_name;
    std::shared_ptr<func_expr> fn;
    std::optional<std::string> alias;
};

struct select_ast {
    std::string table;
    std::optional<std::string> table_alias;
    std::vector<std::string> columns; // legacy name-only column list
    std::vector<select_item> items;   // preferred when non-trivial
    std::vector<join_ast> joins;
    std::optional<predicate_ast> where_clause;
    std::vector<std::string> group_bys;
    std::optional<predicate_ast> having_clause;
    std::vector<order_by_ast> order_bys;
    std::optional<size_t> limit_val;
    std::optional<size_t> offset_val;
};

// Value source for an INSERT/UPDATE assignment: either a literal value or a
// raw expression snippet (e.g. "EXCLUDED.col", "DEFAULT"). Raw snippets are
// NOT parameterized — callers must ensure they are safe (column names are
// quoted via quote_ident internally).
struct raw_expr {
    std::string fragment;
};
using value_source = std::variant<param_expr, raw_expr>;

struct on_conflict_ast {
    std::vector<std::string> target_columns; // empty = ON CONFLICT with no target
    bool do_nothing{false};
    // When do_nothing is false, the assignments below build the DO UPDATE SET.
    std::vector<std::pair<std::string, value_source>> updates;
    std::optional<predicate_ast> where_clause;
};

struct insert_ast {
    std::string table;
    std::vector<std::string> columns;
    std::vector<std::vector<value_source>> rows; // at least one row
    std::optional<on_conflict_ast> on_conflict;
    std::vector<std::string> returning;
};

struct update_ast {
    std::string table;
    std::vector<std::pair<std::string, value_source>> assignments;
    std::optional<predicate_ast> where_clause;
    std::vector<std::string> returning;
};

struct delete_ast {
    std::string table;
    std::optional<predicate_ast> where_clause;
    std::vector<std::string> returning;
};

class select_builder {
  public:
    select_builder& select(std::vector<std::string> cols) {
        ast_.columns = std::move(cols);
        return *this;
    }

    select_builder& from(std::string table) {
        ast_.table = std::move(table);
        return *this;
    }

    select_builder& where(predicate_ast pred) {
        ast_.where_clause = std::move(pred);
        return *this;
    }

    select_builder& items(std::vector<select_item> its) {
        ast_.items = std::move(its);
        return *this;
    }

    select_builder& join(join_kind kind, std::string table, predicate_ast on_clause) {
        ast_.joins.push_back({kind, std::move(table), std::nullopt, std::move(on_clause)});
        return *this;
    }

    select_builder& inner_join(std::string table, predicate_ast on_clause) {
        return join(join_kind::inner, std::move(table), std::move(on_clause));
    }
    select_builder& left_join(std::string table, predicate_ast on_clause) {
        return join(join_kind::left, std::move(table), std::move(on_clause));
    }
    select_builder& right_join(std::string table, predicate_ast on_clause) {
        return join(join_kind::right, std::move(table), std::move(on_clause));
    }
    select_builder& full_join(std::string table, predicate_ast on_clause) {
        return join(join_kind::full, std::move(table), std::move(on_clause));
    }
    select_builder& cross_join(std::string table) {
        ast_.joins.push_back({join_kind::cross, std::move(table), std::nullopt, std::nullopt});
        return *this;
    }

    select_builder& group_by(std::vector<std::string> cols) {
        ast_.group_bys = std::move(cols);
        return *this;
    }

    select_builder& having(predicate_ast pred) {
        ast_.having_clause = std::move(pred);
        return *this;
    }

    select_builder& order_by(std::string column, bool ascending = true) {
        ast_.order_bys.push_back({std::move(column), ascending});
        return *this;
    }

    select_builder& limit(size_t n) {
        ast_.limit_val = n;
        return *this;
    }

    select_builder& offset(size_t n) {
        ast_.offset_val = n;
        return *this;
    }

    select_ast build() {
        return std::move(ast_);
    }

  private:
    select_ast ast_;
};

inline select_builder select(std::vector<std::string> cols) {
    select_builder b;
    b.select(std::move(cols));
    return b;
}

// -------- DML builders --------

class insert_builder {
  public:
    insert_builder& into(std::string table) {
        ast_.table = std::move(table);
        return *this;
    }
    insert_builder& columns(std::vector<std::string> cols) {
        ast_.columns = std::move(cols);
        return *this;
    }
    insert_builder& values(std::vector<value_source> row) {
        ast_.rows.push_back(std::move(row));
        return *this;
    }
    insert_builder& on_conflict(on_conflict_ast oc) {
        ast_.on_conflict = std::move(oc);
        return *this;
    }
    insert_builder& returning(std::vector<std::string> cols) {
        ast_.returning = std::move(cols);
        return *this;
    }
    insert_ast build() {
        return std::move(ast_);
    }

  private:
    insert_ast ast_;
};

class update_builder {
  public:
    update_builder& table(std::string t) {
        ast_.table = std::move(t);
        return *this;
    }
    update_builder& set(std::string column, value_source src) {
        ast_.assignments.emplace_back(std::move(column), std::move(src));
        return *this;
    }
    update_builder& where(predicate_ast pred) {
        ast_.where_clause = std::move(pred);
        return *this;
    }
    update_builder& returning(std::vector<std::string> cols) {
        ast_.returning = std::move(cols);
        return *this;
    }
    update_ast build() {
        return std::move(ast_);
    }

  private:
    update_ast ast_;
};

class delete_builder {
  public:
    delete_builder& from(std::string t) {
        ast_.table = std::move(t);
        return *this;
    }
    delete_builder& where(predicate_ast pred) {
        ast_.where_clause = std::move(pred);
        return *this;
    }
    delete_builder& returning(std::vector<std::string> cols) {
        ast_.returning = std::move(cols);
        return *this;
    }
    delete_ast build() {
        return std::move(ast_);
    }

  private:
    delete_ast ast_;
};

inline insert_builder insert_into(std::string table) {
    insert_builder b;
    b.into(std::move(table));
    return b;
}

inline update_builder update(std::string table) {
    update_builder b;
    b.table(std::move(table));
    return b;
}

inline delete_builder delete_from(std::string table) {
    delete_builder b;
    b.from(std::move(table));
    return b;
}

} // namespace asterorm::sql