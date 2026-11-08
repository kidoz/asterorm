#pragma once
#include "asterorm/sql/typed.hpp"
#include "asterorm/sql/window.hpp"
#include <concepts>

namespace asterorm::sql::typed {
template <typename Entity = void, bool Ordered = false, bool Framed = false> struct window_spec {
    over_clause inner;
    template <auto... Members> auto partition_by(typed_col<Members>...) const {
        using entity = detail::pick_entity_t<Entity, detail::member_entity_t<Members>...>;
        static_assert(
            (detail::entity_compatible_v<entity, detail::member_entity_t<Members>> && ...) &&
                detail::entity_compatible_v<entity, Entity>,
            "asterorm: window keys must share one entity");
        window_spec<entity, Ordered, Framed> result{inner};
        (result.inner.partition_bys.emplace_back(typed_col<Members>::column_name()), ...);
        return result;
    }
    template <auto Member> auto order_by(typed_col<Member>, order_dir direction = asc) const {
        using entity = detail::pick_entity_t<Entity, detail::member_entity_t<Member>>;
        static_assert(detail::entity_compatible_v<Entity, entity> &&
                          detail::entity_compatible_v<detail::member_entity_t<Member>, entity>,
                      "asterorm: window keys must share one entity");
        window_spec<entity, true, Framed> result{inner};
        result.inner.order_bys.push_back({.column = std::string{typed_col<Member>::column_name()},
                                          .ascending = direction == asc});
        return result;
    }
    auto rows_between(frame_bound start, frame_bound end) const {
        window_spec<Entity, Ordered, true> result{inner};
        result.inner.rows = sql::rows_between(start, end);
        return result;
    }
};
inline window_spec<> window() {
    return {};
}

template <typename Value, typename Entity, bool FrameValue = false> struct window_function {
    using projected_type = Value;
    using entity_type = Entity;
    static constexpr bool frame_value = FrameValue;
    std::shared_ptr<func_expr> node;
};
inline auto row_number() {
    return window_function<std::int64_t, void>{sql::row_number()};
}
inline auto rank() {
    return window_function<std::int64_t, void>{sql::rank()};
}
inline auto dense_rank() {
    return window_function<std::int64_t, void>{sql::dense_rank()};
}

template <auto Member, typename Offset = std::int64_t>
auto lag(typed_col<Member>, Offset offset = 1) {
    static_assert(std::signed_integral<Offset>,
                  "asterorm: window offset requires a signed integer");
    using value = std::optional<detail::unwrap_optional_t<detail::member_field_t<Member>>>;
    return window_function<value, detail::member_entity_t<Member>>{
        sql::lag(column_expr{std::string{typed_col<Member>::column_name()}}, offset)};
}
template <auto Member, typename Offset, typename Default>
auto lag(typed_col<Member> column, Offset offset, typed_val<Default> fallback) {
    using field = detail::unwrap_optional_t<detail::member_field_t<Member>>;
    using fallback_type = detail::unwrap_optional_t<Default>;
    static_assert(std::same_as<field, fallback_type> ||
                      (std::same_as<field, std::string> &&
                       std::is_convertible_v<fallback_type, std::string_view>),
                  "asterorm: window default must match the column type");
    auto function = typed::lag(column, offset);
    function.node->args.emplace_back(sql::val(std::move(fallback.value)));
    return function;
}
template <auto Member, typename Offset = std::int64_t>
auto lead(typed_col<Member> column, Offset offset = 1) {
    auto function = typed::lag(column, offset);
    function.node->name = "lead";
    return function;
}
template <auto Member, typename Offset, typename Default>
auto lead(typed_col<Member> column, Offset offset, typed_val<Default> fallback) {
    auto function = typed::lag(column, offset, std::move(fallback));
    function.node->name = "lead";
    return function;
}
template <auto Member> auto first_value(typed_col<Member>) {
    using value = std::optional<detail::unwrap_optional_t<detail::member_field_t<Member>>>;
    return window_function<value, detail::member_entity_t<Member>, true>{
        sql::first_value(column_expr{std::string{typed_col<Member>::column_name()}})};
}
template <auto Member> auto last_value(typed_col<Member> column) {
    auto function = typed::first_value(column);
    function.node->name = "last_value";
    return function;
}
template <auto Member, typename Position>
auto nth_value(typed_col<Member> column, Position position) {
    static_assert(std::signed_integral<Position>,
                  "asterorm: window position requires a signed integer");
    auto function = typed::first_value(column);
    function.node->name = "nth_value";
    function.node->window_position = position;
    return function;
}

template <typename Value, typename Entity> struct window_item {
    using projected_type = Value;
    using entity_type = Entity;
    static constexpr bool uses_window = true;
    std::shared_ptr<func_expr> node;
    [[nodiscard]] select_item to_select_item() const {
        return {.column_name = std::nullopt,
                .fn = std::make_shared<func_expr>(*node),
                .alias = std::nullopt};
    }
    [[nodiscard]] window_item as(std::string alias) const {
        auto result = *this;
        result.node = std::make_shared<func_expr>(*node);
        result.node->alias = std::move(alias);
        return result;
    }
};

template <typename Value, typename Entity, bool FrameValue, typename Keys, bool Ordered,
          bool Framed>
auto over(window_function<Value, Entity, FrameValue> function,
          window_spec<Keys, Ordered, Framed> spec) {
    static_assert(detail::entity_compatible_v<Entity, Keys>,
                  "asterorm: window arguments and keys must share one entity");
    static_assert(Ordered, "asterorm: window function requires ORDER BY");
    static_assert(!FrameValue || Framed,
                  "asterorm: frame-value function requires an explicit ROWS frame");
    using entity = detail::pick_entity_t<Entity, Keys>;
    return window_item<Value, entity>{sql::over(function.node, std::move(spec.inner))};
}

namespace detail {
template <typename Item> struct window_aggregate {
    static constexpr bool supported = false;
};
template <> struct window_aggregate<count_all_t> {
    static constexpr bool supported = true;
    using type = std::int64_t;
};
template <auto Member>
struct window_aggregate<count_col_t<Member>> : window_aggregate<count_all_t> {};
template <auto Member> struct window_aggregate<sum_col_t<Member>> {
    static constexpr bool supported = true;
    using argument = unwrap_optional_t<member_field_t<Member>>;
    static constexpr bool numeric_argument =
        (std::is_arithmetic_v<argument> && !std::same_as<argument, bool>) ||
        std::same_as<argument, asterorm::numeric>;
    using type = std::optional<typename sum_col_t<Member>::projected_type>;
};
template <auto Member> struct window_aggregate<avg_col_t<Member>> {
    static constexpr bool supported = true;
    using argument = unwrap_optional_t<member_field_t<Member>>;
    static constexpr bool numeric_argument =
        (std::is_arithmetic_v<argument> && !std::same_as<argument, bool>) ||
        std::same_as<argument, asterorm::numeric>;
    using type = std::optional<typename avg_col_t<Member>::projected_type>;
};
template <auto Member> struct window_aggregate<min_col_t<Member>> {
    static constexpr bool supported = true;
    using type = std::optional<typename min_col_t<Member>::projected_type>;
};
template <auto Member> struct window_aggregate<max_col_t<Member>> {
    static constexpr bool supported = true;
    using type = std::optional<typename max_col_t<Member>::projected_type>;
};
} // namespace detail

template <selectable Aggregate, typename Keys, bool Ordered, bool Framed>
auto over(Aggregate aggregate, window_spec<Keys, Ordered, Framed> spec) {
    static_assert(detail::window_aggregate<Aggregate>::supported,
                  "asterorm: OVER requires a supported aggregate or window function");
    static_assert(detail::entity_compatible_v<typename Aggregate::entity_type, Keys>,
                  "asterorm: window arguments and keys must share one entity");
    using entity = detail::pick_entity_t<typename Aggregate::entity_type, Keys>;
    if constexpr (detail::window_aggregate<Aggregate>::supported) {
        if constexpr (requires { detail::window_aggregate<Aggregate>::numeric_argument; }) {
            static_assert(detail::window_aggregate<Aggregate>::numeric_argument,
                          "asterorm: window SUM/AVG requires a numeric non-boolean column");
        }
        using value = typename detail::window_aggregate<Aggregate>::type;
        return window_item<value, entity>{
            sql::over(aggregate.to_select_item().fn, std::move(spec.inner))};
    }
}
} // namespace asterorm::sql::typed
