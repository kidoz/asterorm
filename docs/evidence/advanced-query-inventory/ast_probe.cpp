#include <iostream>
#include <stdexcept>
#include <type_traits>

#include "asterorm/sql/compiler.hpp"

template <typename Ast>
concept has_ctes = requires(Ast value) { value.ctes; };
template <typename Ast>
concept has_window = requires(Ast value) { value.over; };
template <typename Ast>
concept has_compound = requires(Ast value) { value.set_operation; };

int main() try {
    namespace sql = asterorm::sql;
    static_assert(!has_ctes<sql::select_ast>);
    static_assert(!has_window<sql::func_expr>);
    static_assert(!has_compound<sql::select_ast>);
    static_assert(!std::is_copy_constructible_v<sql::select_ast>);
    static_assert(std::variant_size_v<sql::expr_ast> == 3);
    sql::select_ast statement;
    statement.table = "items";
    statement.items.push_back(
        {.fn = sql::as(sql::func("coalesce", {sql::val(10), sql::val(20)}), "picked")});
    statement.joins.push_back({
        .kind = sql::join_kind::inner,
        .table = "other",
        .on_clause = sql::expr_ast{sql::col("items.id")} == sql::expr_ast{sql::val(30)},
    });
    statement.where_clause = sql::expr_ast{sql::col("items.id")} > sql::expr_ast{sql::val(40)};
    statement.having_clause = sql::expr_ast{sql::count_all()} > sql::expr_ast{sql::val(50)};
    sql::compiler compiler;
    auto compiled = compiler.compile(statement);
    if (!compiled || compiled->params.size() != 5) {
        throw std::runtime_error("Parameter traversal probe failed");
    }
    std::cout << "parameter_sql=" << compiled->sql << '\n';
    std::cout << "parameter_order=";
    for (std::size_t index = 0; index < compiled->params.size(); ++index) {
        if (compiled->params[index].value != std::to_string((index + 1) * 10)) {
            throw std::runtime_error("Unexpected parameter order");
        }
        if (index != 0) {
            std::cout << ',';
        }
        std::cout << *compiled->params[index].value;
    }
    std::cout << '\n';
    sql::select_ast alias;
    alias.table = "items";
    alias.items.push_back({.fn = sql::max_(sql::col("id")), .alias = "ignored_item_alias"});
    auto alias_result = compiler.compile(alias);
    if (!alias_result || alias_result->sql.contains("ignored_item_alias")) {
        throw std::runtime_error("Function/select-item alias behavior changed");
    }
    std::cout << "function_item_alias=" << alias_result->sql << '\n';
    sql::select_ast offset;
    offset.table = "items";
    offset.columns = {"id"};
    offset.offset_val = 1;
    auto offset_result = sql::compiler{sql::dialect::sqlite}.compile(offset);
    if (!offset_result || offset_result->sql != R"(SELECT "id" FROM "items" OFFSET 1)") {
        throw std::runtime_error("SQLite offset inventory changed");
    }
    std::cout << "sqlite_offset_sql=" << offset_result->sql << '\n';
    sql::select_ast invalid;
    invalid.table = "items";
    invalid.columns = {"id"};
    invalid.where_clause = sql::expr_ast{sql::col("id")} == sql::expr_ast{sql::val(7)};
    auto& right = std::get<sql::comparison_predicate>(invalid.where_clause->val).right;
    std::get<sql::param_expr>(right).encoded_value.error = asterorm::db_error{
        .kind = asterorm::db_error_kind::parse_failed,
        .message = "injected encoding diagnostic",
    };
    auto invalid_result = compiler.compile(invalid);
    if (!invalid_result || !invalid_result->params.front().error) {
        throw std::runtime_error("Encoded-error inventory changed");
    }
    std::cout << "encoded_error_deferred=1\n";
    std::cout << "structured_cte_window_compound=absent\n";
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
