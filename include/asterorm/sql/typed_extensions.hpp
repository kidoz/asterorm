#pragma once
#include "asterorm/sql/typed_window.hpp"

namespace asterorm::sql::typed {
template <typename Value, typename Entity> struct filtered_aggregate {
    using projected_type = Value;
    using entity_type = Entity;
    std::shared_ptr<func_expr> node;
    [[nodiscard]] select_item to_select_item() const {
        return {.column_name = std::nullopt,
                .fn = std::make_shared<func_expr>(*node),
                .alias = std::nullopt};
    }
    [[nodiscard]] filtered_aggregate as(std::string alias) const {
        auto result = *this;
        result.node = std::make_shared<func_expr>(*node);
        result.node->alias = std::move(alias);
        return result;
    }
};
template <selectable Aggregate, typename PredicateEntity>
auto filter(Aggregate aggregate, typed_predicate<PredicateEntity> predicate) {
    static_assert(detail::window_aggregate<Aggregate>::supported,
                  "asterorm: FILTER requires a supported aggregate before OVER");
    static_assert(detail::entity_compatible_v<typename Aggregate::entity_type, PredicateEntity>,
                  "asterorm: FILTER arguments and predicate must share one entity");
    if constexpr (detail::window_aggregate<Aggregate>::supported) {
        if constexpr (requires { detail::window_aggregate<Aggregate>::numeric_argument; }) {
            static_assert(detail::window_aggregate<Aggregate>::numeric_argument,
                          "asterorm: filtered SUM/AVG requires a numeric non-boolean column");
        }
        using entity = detail::pick_entity_t<typename Aggregate::entity_type, PredicateEntity>;
        using value = typename detail::window_aggregate<Aggregate>::type;
        return filtered_aggregate<value, entity>{
            sql::filter(aggregate.to_select_item().fn, std::move(predicate.inner))};
    }
}
template <typename Value, typename Source, typename Keys, bool Ordered, bool Framed>
auto over(filtered_aggregate<Value, Source> aggregate, window_spec<Keys, Ordered, Framed> spec) {
    static_assert(detail::entity_compatible_v<Source, Keys>,
                  "asterorm: FILTER window keys must share the aggregate entity");
    using entity = detail::pick_entity_t<Source, Keys>;
    return window_item<Value, entity>{sql::over(aggregate.node, std::move(spec.inner))};
}
} // namespace asterorm::sql::typed
