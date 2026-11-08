#pragma once
#include "asterorm/sql/ast.hpp"
#include <concepts>

namespace asterorm::sql {
namespace window_detail {
[[nodiscard]] bool contains_window(const select_ast& ast);
} // namespace window_detail
inline frame_bound unbounded_preceding() {
    return {frame_bound_kind::unbounded_preceding, 0};
}
template <std::signed_integral Count> inline frame_bound preceding(Count count) {
    return {frame_bound_kind::preceding, count};
}
inline frame_bound current_row() {
    return {};
}
template <std::signed_integral Count> inline frame_bound following(Count count) {
    return {frame_bound_kind::following, count};
}
inline frame_bound unbounded_following() {
    return {frame_bound_kind::unbounded_following, 0};
}
inline rows_frame rows_between(frame_bound start, frame_bound end) {
    return {start, end};
}

// Decoration owns a copy of the function; existing shared arguments retain their ownership.
inline std::shared_ptr<func_expr> over(const std::shared_ptr<func_expr>& function,
                                       over_clause clause) {
    if (!function) {
        return {};
    }
    auto decorated = std::make_shared<func_expr>(*function);
    decorated->over = std::move(clause);
    return decorated;
}
inline std::shared_ptr<func_expr> row_number() {
    return func("row_number");
}
inline std::shared_ptr<func_expr> rank() {
    return func("rank");
}
inline std::shared_ptr<func_expr> dense_rank() {
    return func("dense_rank");
}
template <std::signed_integral Offset = std::int64_t>
inline std::shared_ptr<func_expr> lag(expr_ast value, Offset offset = 1,
                                      std::optional<expr_ast> fallback = std::nullopt) {
    auto function = func("lag", {std::move(value)});
    function->window_position = offset;
    if (fallback) {
        function->args.push_back(std::move(*fallback));
    }
    return function;
}
template <std::signed_integral Offset = std::int64_t>
inline std::shared_ptr<func_expr> lead(expr_ast value, Offset offset = 1,
                                       std::optional<expr_ast> fallback = std::nullopt) {
    auto function = lag(std::move(value), offset, std::move(fallback));
    function->name = "lead";
    return function;
}
inline std::shared_ptr<func_expr> first_value(expr_ast value) {
    return func("first_value", {std::move(value)});
}
inline std::shared_ptr<func_expr> last_value(expr_ast value) {
    return func("last_value", {std::move(value)});
}
template <std::signed_integral Position>
inline std::shared_ptr<func_expr> nth_value(expr_ast value, Position position) {
    auto function = func("nth_value", {std::move(value)});
    function->window_position = position;
    return function;
}
} // namespace asterorm::sql
