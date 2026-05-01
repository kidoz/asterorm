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

using expr_ast = std::variant<column_expr, param_expr>;

inline column_expr col(std::string name) { return {std::move(name)}; }

template <typename T>
inline param_expr val(T&& v) {
    return {asterorm::encode(std::forward<T>(v))};
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
    return predicate_ast{binary_predicate{op_kind::and_, std::make_unique<predicate_ast>(std::move(lhs)),
                                          std::make_unique<predicate_ast>(std::move(rhs))}};
}
inline predicate_ast operator||(predicate_ast lhs, predicate_ast rhs) {
    return predicate_ast{binary_predicate{op_kind::or_, std::make_unique<predicate_ast>(std::move(lhs)),
                                          std::make_unique<predicate_ast>(std::move(rhs))}};
}

struct order_by_ast {
    std::string column;
    bool ascending{true};
};

struct select_ast {
    std::string table;
    std::vector<std::string> columns;
    std::optional<predicate_ast> where_clause;
    std::vector<order_by_ast> order_bys;
    std::optional<size_t> limit_val;
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

    select_builder& order_by(std::string column, bool ascending = true) {
        ast_.order_bys.push_back({std::move(column), ascending});
        return *this;
    }

    select_builder& limit(size_t n) {
        ast_.limit_val = n;
        return *this;
    }

    select_ast build() { return std::move(ast_); }

   private:
    select_ast ast_;
};

inline select_builder select(std::vector<std::string> cols) {
    select_builder b;
    b.select(std::move(cols));
    return b;
}

}  // namespace asterorm::sql