#pragma once
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/sql/ast.hpp"

// Compile-time-checked SQL DSL.
//
// Entry points live in `asterorm::sql::typed` so they coexist with the
// string-based `asterorm::sql::col(std::string)` and `sql::val(T)`. Bring
// them into scope with:
//
//     using namespace asterorm::sql::typed;
//
// The typed nodes lower into the existing `asterorm::sql::predicate_ast`
// and `asterorm::sql::select_ast`, so the runtime path is unchanged.

namespace asterorm::sql::typed {

// ---------------------------------------------------------------------------
// Member-pointer trait helpers
// ---------------------------------------------------------------------------
namespace detail {

template <typename T> struct member_ptr_traits;
template <typename Entity, typename Field> struct member_ptr_traits<Field Entity::*> {
    using entity_type = Entity;
    using field_type = Field;
};

template <auto MemberPtr>
using member_entity_t = typename member_ptr_traits<decltype(MemberPtr)>::entity_type;

template <auto MemberPtr>
using member_field_t = typename member_ptr_traits<decltype(MemberPtr)>::field_type;

template <typename T> struct unwrap_optional {
    using type = T;
};
template <typename T> struct unwrap_optional<std::optional<T>> {
    using type = T;
};
template <typename T> using unwrap_optional_t = typename unwrap_optional<T>::type;

// SFINAE-safe member-pointer equality. Two pointer-to-members of different
// (entity, field) types are statically not equal; same-typed ones use ==.
template <typename A, typename B>
constexpr bool member_ptr_equal(A pointer_a, B pointer_b) noexcept {
    if constexpr (std::is_same_v<A, B>) {
        return pointer_a == pointer_b;
    } else {
        return false;
    }
}

// Walk entity_traits<Entity>::columns at compile time and return the column
// name whose member_ptr matches MemberPtr. Empty string_view if not declared.
template <auto MemberPtr> constexpr std::string_view column_name_for() {
    using Entity = member_entity_t<MemberPtr>;
    constexpr const auto& cols = entity_traits<Entity>::columns;
    constexpr std::size_t column_count = std::tuple_size_v<std::decay_t<decltype(cols)>>;

    return [&]<std::size_t... Indices>(std::index_sequence<Indices...>) constexpr {
        std::string_view found{};
        [[maybe_unused]] const bool any_matched =
            ((member_ptr_equal(std::get<Indices>(cols).member_ptr, MemberPtr)
                  ? (found = std::get<Indices>(cols).name, true)
                  : false) ||
             ...);
        return found;
    }(std::make_index_sequence<column_count>{});
}

template <auto MemberPtr> constexpr bool column_declared_v = !column_name_for<MemberPtr>().empty();

// Operand compatibility for comparisons. Same decayed types match; arithmetic
// types interconvert except across the bool boundary; otherwise fall back to
// implicit-conversion (covers std::string from const char*, etc.).
template <typename Field, typename Other>
inline constexpr bool compatible_for_compare_v = []() constexpr {
    using F = std::decay_t<Field>;
    using O = std::decay_t<Other>;
    if constexpr (std::is_same_v<F, O>) {
        return true;
    } else if constexpr (std::is_arithmetic_v<F> && std::is_arithmetic_v<O>) {
        return std::is_same_v<F, bool> == std::is_same_v<O, bool>;
    } else {
        return std::is_convertible_v<O, F>;
    }
}();

// Cross-type predicate carrier (entity-tagged).
template <typename> inline constexpr bool always_false_v = false;

template <typename Entity, typename... Args>
concept aggregate_initable = requires(Args... values) { Entity{values...}; };

// First element of a non-type-template-parameter pack (no C++23 pack indexing).
template <auto First, auto... Rest> struct first_value {
    static constexpr auto value = First;
};
template <auto... Ptrs> inline constexpr auto first_value_v = first_value<Ptrs...>::value;

} // namespace detail

// ---------------------------------------------------------------------------
// Typed value and NULL sentinel
// ---------------------------------------------------------------------------

template <typename T> struct typed_val {
    T value;
};

template <typename T> constexpr typed_val<std::decay_t<T>> val(T&& value) {
    return typed_val<std::decay_t<T>>{std::forward<T>(value)};
}

struct null_t {};
inline constexpr null_t null{};

// ---------------------------------------------------------------------------
// Typed predicate (entity-tagged wrapper around predicate_ast)
// ---------------------------------------------------------------------------

template <typename Entity> struct typed_predicate {
    using entity_type = Entity;
    predicate_ast inner;
};

template <typename Entity>
constexpr typed_predicate<Entity> operator&&(typed_predicate<Entity> lhs,
                                             typed_predicate<Entity> rhs) {
    return typed_predicate<Entity>{predicate_ast{
        binary_predicate{op_kind::and_, std::make_unique<predicate_ast>(std::move(lhs.inner)),
                         std::make_unique<predicate_ast>(std::move(rhs.inner))}}};
}

template <typename Entity>
constexpr typed_predicate<Entity> operator||(typed_predicate<Entity> lhs,
                                             typed_predicate<Entity> rhs) {
    return typed_predicate<Entity>{predicate_ast{
        binary_predicate{op_kind::or_, std::make_unique<predicate_ast>(std::move(lhs.inner)),
                         std::make_unique<predicate_ast>(std::move(rhs.inner))}}};
}

// ---------------------------------------------------------------------------
// Selectable concept
// ---------------------------------------------------------------------------
//
// A "selectable" is anything that can appear in a select_cols(...) projection:
// a typed_col, or one of the aggregate function nodes below. Every selectable
// declares its projected C++ type, the entity it references (or `void` for
// entity-agnostic forms like COUNT(*)), and how to lower itself to a
// select_item AST node.

template <typename T>
concept selectable = requires {
    typename T::projected_type;
    typename T::entity_type;
    { T::to_select_item() } -> std::convertible_to<select_item>;
};

// ---------------------------------------------------------------------------
// Typed column reference
// ---------------------------------------------------------------------------

template <auto MemberPtr> struct typed_col {
    static_assert(detail::column_declared_v<MemberPtr>,
                  "asterorm: this member pointer is not declared in "
                  "entity_traits<T>::columns. Add it to the columns tuple.");

    using entity_type = detail::member_entity_t<MemberPtr>;
    using field_type = detail::member_field_t<MemberPtr>;
    using compare_type = detail::unwrap_optional_t<field_type>;
    // Selectable: a bare column projects into its declared field type.
    using projected_type = field_type;

    static constexpr std::string_view column_name() {
        return detail::column_name_for<MemberPtr>();
    }

    static select_item to_select_item() {
        return select_item{std::string{column_name()}, nullptr, std::nullopt};
    }

    // -- typed_col vs typed_val ------------------------------------------------
#define ASTERORM_TYPED_VAL_CMP(op_token, op_enum)                                                  \
    template <typename U>                                                                          \
        requires detail::compatible_for_compare_v<compare_type, U>                                 \
    friend typed_predicate<entity_type> operator op_token(typed_col, typed_val<U> right_value) {   \
        return typed_predicate<entity_type>{                                                       \
            predicate_ast{comparison_predicate{op_enum, column_expr{std::string{column_name()}},   \
                                               param_expr{asterorm::encode(right_value.value)}}}}; \
    }
    ASTERORM_TYPED_VAL_CMP(==, op_kind::eq)
    ASTERORM_TYPED_VAL_CMP(!=, op_kind::neq)
    ASTERORM_TYPED_VAL_CMP(<, op_kind::lt)
    ASTERORM_TYPED_VAL_CMP(>, op_kind::gt)
    ASTERORM_TYPED_VAL_CMP(<=, op_kind::le)
    ASTERORM_TYPED_VAL_CMP(>=, op_kind::ge)
#undef ASTERORM_TYPED_VAL_CMP

    // -- typed_col vs typed_col (same entity) ----------------------------------
#define ASTERORM_TYPED_COL_CMP(op_token, op_enum)                                                  \
    template <auto OtherPtr>                                                                       \
        requires std::is_same_v<entity_type, detail::member_entity_t<OtherPtr>> &&                 \
                 detail::compatible_for_compare_v<                                                 \
                     compare_type, detail::unwrap_optional_t<detail::member_field_t<OtherPtr>>>    \
    friend typed_predicate<entity_type> operator op_token(typed_col, typed_col<OtherPtr>) {        \
        return typed_predicate<entity_type>{predicate_ast{                                         \
            comparison_predicate{op_enum, column_expr{std::string{column_name()}},                 \
                                 column_expr{std::string{typed_col<OtherPtr>::column_name()}}}}};  \
    }
    ASTERORM_TYPED_COL_CMP(==, op_kind::eq)
    ASTERORM_TYPED_COL_CMP(!=, op_kind::neq)
    ASTERORM_TYPED_COL_CMP(<, op_kind::lt)
    ASTERORM_TYPED_COL_CMP(>, op_kind::gt)
    ASTERORM_TYPED_COL_CMP(<=, op_kind::le)
    ASTERORM_TYPED_COL_CMP(>=, op_kind::ge)
#undef ASTERORM_TYPED_COL_CMP

    // -- sql::null guard -------------------------------------------------------
    friend typed_predicate<entity_type> operator==(typed_col, null_t) {
        static_assert(detail::always_false_v<typed_col>,
                      "asterorm: NULL comparisons via '==' are not yet supported. "
                      "Use sql::typed::is_null(sql::typed::col<&T::field>) or "
                      "sql::typed::is_not_null(sql::typed::col<&T::field>) instead.");
        return {};
    }
    friend typed_predicate<entity_type> operator!=(typed_col, null_t) {
        static_assert(detail::always_false_v<typed_col>,
                      "asterorm: NULL comparisons via '!=' are not yet supported. "
                      "Use sql::typed::is_not_null(sql::typed::col<&T::field>) instead.");
        return {};
    }
};

template <auto MemberPtr> inline constexpr typed_col<MemberPtr> col{};

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL helpers
// ---------------------------------------------------------------------------

template <auto MemberPtr>
constexpr typed_predicate<detail::member_entity_t<MemberPtr>> is_null(typed_col<MemberPtr>) {
    return typed_predicate<detail::member_entity_t<MemberPtr>>{
        ::asterorm::sql::is_null(column_expr{std::string{typed_col<MemberPtr>::column_name()}})};
}

template <auto MemberPtr>
constexpr typed_predicate<detail::member_entity_t<MemberPtr>> is_not_null(typed_col<MemberPtr>) {
    return typed_predicate<detail::member_entity_t<MemberPtr>>{::asterorm::sql::is_not_null(
        column_expr{std::string{typed_col<MemberPtr>::column_name()}})};
}

// ---------------------------------------------------------------------------
// Typed aggregates
// ---------------------------------------------------------------------------
//
// Aggregate result-type rules follow what PostgreSQL/SQLite return through
// the text protocol:
//   COUNT(*) / COUNT(col)  → bigint  (mapped to std::int64_t)
//   SUM(int...)            → bigint  (mapped to std::int64_t)
//   SUM(float/double)      → double precision
//   SUM(numeric)           → numeric (preserved verbatim)
//   AVG(arithmetic)        → double precision
//   MIN/MAX(T)             → T
//
// PG actually returns AVG(int) as numeric; we project to double. For exact
// decimal averages, project the column as `numeric` upstream.

namespace detail {

template <typename T>
using sum_result_t = std::conditional_t<std::is_integral_v<T>, std::int64_t,
                                        std::conditional_t<std::is_floating_point_v<T>, double, T>>;

template <typename T> using avg_result_t = std::conditional_t<std::is_arithmetic_v<T>, double, T>;

} // namespace detail

struct count_all_t {
    using projected_type = std::int64_t;
    using entity_type = void;

    static select_item to_select_item() {
        return select_item{std::nullopt, ::asterorm::sql::count_all(), std::nullopt};
    }
};

constexpr count_all_t count_all() {
    return {};
}

template <auto MemberPtr> struct count_col_t {
    using projected_type = std::int64_t;
    using entity_type = detail::member_entity_t<MemberPtr>;

    static select_item to_select_item() {
        return select_item{
            std::nullopt,
            ::asterorm::sql::func("count",
                                  {column_expr{std::string{typed_col<MemberPtr>::column_name()}}}),
            std::nullopt};
    }
};

template <auto MemberPtr> constexpr count_col_t<MemberPtr> count(typed_col<MemberPtr>) {
    return {};
}

template <auto MemberPtr> struct sum_col_t {
    using projected_type =
        detail::sum_result_t<detail::unwrap_optional_t<detail::member_field_t<MemberPtr>>>;
    using entity_type = detail::member_entity_t<MemberPtr>;

    static select_item to_select_item() {
        return select_item{
            std::nullopt,
            ::asterorm::sql::sum(column_expr{std::string{typed_col<MemberPtr>::column_name()}}),
            std::nullopt};
    }
};

template <auto MemberPtr> constexpr sum_col_t<MemberPtr> sum(typed_col<MemberPtr>) {
    return {};
}

template <auto MemberPtr> struct avg_col_t {
    using projected_type =
        detail::avg_result_t<detail::unwrap_optional_t<detail::member_field_t<MemberPtr>>>;
    using entity_type = detail::member_entity_t<MemberPtr>;

    static select_item to_select_item() {
        return select_item{
            std::nullopt,
            ::asterorm::sql::avg(column_expr{std::string{typed_col<MemberPtr>::column_name()}}),
            std::nullopt};
    }
};

template <auto MemberPtr> constexpr avg_col_t<MemberPtr> avg(typed_col<MemberPtr>) {
    return {};
}

template <auto MemberPtr> struct min_col_t {
    using projected_type = detail::unwrap_optional_t<detail::member_field_t<MemberPtr>>;
    using entity_type = detail::member_entity_t<MemberPtr>;

    static select_item to_select_item() {
        return select_item{
            std::nullopt,
            ::asterorm::sql::min_(column_expr{std::string{typed_col<MemberPtr>::column_name()}}),
            std::nullopt};
    }
};

template <auto MemberPtr> constexpr min_col_t<MemberPtr> min(typed_col<MemberPtr>) {
    return {};
}

template <auto MemberPtr> struct max_col_t {
    using projected_type = detail::unwrap_optional_t<detail::member_field_t<MemberPtr>>;
    using entity_type = detail::member_entity_t<MemberPtr>;

    static select_item to_select_item() {
        return select_item{
            std::nullopt,
            ::asterorm::sql::max_(column_expr{std::string{typed_col<MemberPtr>::column_name()}}),
            std::nullopt};
    }
};

template <auto MemberPtr> constexpr max_col_t<MemberPtr> max(typed_col<MemberPtr>) {
    return {};
}

// ---------------------------------------------------------------------------
// Typed select builder
// ---------------------------------------------------------------------------

enum class order_dir : std::uint8_t { ascending, descending };
inline constexpr order_dir asc = order_dir::ascending;
inline constexpr order_dir desc = order_dir::descending;

namespace detail {

// Pick the first non-void entity from a pack, used to derive the entity tag
// for builders containing entity-agnostic items (count_all has no entity).
template <typename...> struct pick_entity {
    using type = void;
};
template <typename First, typename... Rest> struct pick_entity<First, Rest...> {
    using type =
        std::conditional_t<std::is_same_v<First, void>, typename pick_entity<Rest...>::type, First>;
};
template <typename... EntityTags> using pick_entity_t = typename pick_entity<EntityTags...>::type;

template <typename A, typename B>
inline constexpr bool entity_compatible_v =
    std::is_same_v<A, B> || std::is_same_v<A, void> || std::is_same_v<B, void>;

} // namespace detail

template <typename Entity, typename Result, typename... Items> class typed_select_builder {
  public:
    using entity_type = Entity;
    using result_type = Result;
    static constexpr std::size_t projection_size = sizeof...(Items);

    explicit typed_select_builder(select_ast initial_ast) : ast_(std::move(initial_ast)) {}

    typed_select_builder& where(typed_predicate<Entity> predicate) {
        ast_.where_clause = std::move(predicate.inner);
        return *this;
    }

    typed_select_builder& having(typed_predicate<Entity> predicate) {
        ast_.having_clause = std::move(predicate.inner);
        return *this;
    }

    template <auto... MemberPtrs>
        requires(sizeof...(MemberPtrs) > 0) &&
                ((std::is_same_v<Entity, detail::member_entity_t<MemberPtrs>>) && ...)
    typed_select_builder& group_by(typed_col<MemberPtrs>...) {
        (ast_.group_bys.emplace_back(typed_col<MemberPtrs>::column_name()), ...);
        return *this;
    }

    template <auto MemberPtr>
        requires std::is_same_v<Entity, detail::member_entity_t<MemberPtr>>
    typed_select_builder& order_by(typed_col<MemberPtr>, order_dir direction = asc) {
        ast_.order_bys.push_back(order_by_ast{std::string{typed_col<MemberPtr>::column_name()},
                                              direction == order_dir::ascending});
        return *this;
    }

    typed_select_builder& limit(std::size_t row_limit) {
        ast_.limit_val = row_limit;
        return *this;
    }

    typed_select_builder& offset(std::size_t row_offset) {
        ast_.offset_val = row_offset;
        return *this;
    }

    // Moves the underlying AST out — the builder is single-shot once built.
    select_ast build() {
        return std::move(ast_);
    }

  private:
    select_ast ast_;
};

namespace detail {

template <typename Entity> select_ast make_entity_select_ast() {
    using traits = entity_traits<Entity>;
    select_ast initial_ast;
    initial_ast.table = traits::table;

    auto append_columns = [&]<std::size_t... Indices>(std::index_sequence<Indices...>) {
        (initial_ast.columns.emplace_back(std::get<Indices>(traits::columns).name), ...);
    };
    append_columns(
        std::make_index_sequence<std::tuple_size_v<std::decay_t<decltype(traits::columns)>>>{});
    return initial_ast;
}

template <typename Entity, typename... Items> select_ast make_projection_select_ast() {
    select_ast initial_ast;
    initial_ast.table = entity_traits<Entity>::table;
    (initial_ast.items.emplace_back(Items::to_select_item()), ...);
    return initial_ast;
}

} // namespace detail

// Whole-entity SELECT: result hydrates as Entity using entity_traits<Entity>.
template <typename Entity> typed_select_builder<Entity, Entity> select() {
    return typed_select_builder<Entity, Entity>{detail::make_entity_select_ast<Entity>()};
}

// Mixed-projection SELECT into a user-declared DTO. Each Items must be a
// `selectable` (typed_col or one of the aggregate nodes); the DTO must be
// aggregate-initialisable from the projected types in declared order.
template <typename DTO, selectable... Items> auto select_cols(Items...) {
    static_assert(sizeof...(Items) > 0, "asterorm: select_cols requires at least one item");

    using entity = detail::pick_entity_t<typename Items::entity_type...>;
    static_assert(
        !std::is_same_v<entity, void>,
        "asterorm: select_cols requires at least one entity-bound column. Add a typed_col.");

    static_assert(
        ((detail::entity_compatible_v<typename Items::entity_type, entity>) && ...),
        "asterorm: select_cols requires every entity-bound item to share the same entity");

    static_assert(detail::aggregate_initable<DTO, typename Items::projected_type...>,
                  "asterorm: DTO field types do not match the projected items. "
                  "Each projected type must aggregate-initialize the DTO in declared order.");

    return typed_select_builder<entity, DTO, Items...>{
        detail::make_projection_select_ast<entity, Items...>()};
}

} // namespace asterorm::sql::typed
