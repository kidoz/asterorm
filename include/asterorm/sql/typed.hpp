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
// Typed column reference
// ---------------------------------------------------------------------------

template <auto MemberPtr> struct typed_col {
    static_assert(detail::column_declared_v<MemberPtr>,
                  "asterorm: this member pointer is not declared in "
                  "entity_traits<T>::columns. Add it to the columns tuple.");

    using entity_type = detail::member_entity_t<MemberPtr>;
    using field_type = detail::member_field_t<MemberPtr>;
    using compare_type = detail::unwrap_optional_t<field_type>;

    static constexpr std::string_view column_name() {
        return detail::column_name_for<MemberPtr>();
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
// Typed select builder
// ---------------------------------------------------------------------------

enum class order_dir : std::uint8_t { ascending, descending };
inline constexpr order_dir asc = order_dir::ascending;
inline constexpr order_dir desc = order_dir::descending;

template <typename Entity, typename Result, auto... ProjectionPtrs> class typed_select_builder {
  public:
    using entity_type = Entity;
    using result_type = Result;
    static constexpr std::size_t projection_size = sizeof...(ProjectionPtrs);

    explicit typed_select_builder(select_ast initial_ast) : ast_(std::move(initial_ast)) {}

    typed_select_builder& where(typed_predicate<Entity> predicate) {
        ast_.where_clause = std::move(predicate.inner);
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

template <typename Entity, auto... Ptrs> select_ast make_projection_select_ast() {
    select_ast initial_ast;
    initial_ast.table = entity_traits<Entity>::table;
    (initial_ast.columns.emplace_back(typed_col<Ptrs>::column_name()), ...);
    return initial_ast;
}

} // namespace detail

// Whole-entity SELECT: result hydrates as Entity using entity_traits<Entity>.
template <typename Entity> typed_select_builder<Entity, Entity> select() {
    return typed_select_builder<Entity, Entity>{detail::make_entity_select_ast<Entity>()};
}

// Tuple-of-columns SELECT into a user-declared DTO. The DTO must be
// aggregate-initialisable from the projected column types in declared order.
template <typename DTO, auto... MemberPtrs> auto select_cols(typed_col<MemberPtrs>...) {
    static_assert(sizeof...(MemberPtrs) > 0, "asterorm: select_cols requires at least one column");

    using first_entity = detail::member_entity_t<detail::first_value_v<MemberPtrs...>>;

    static_assert((std::is_same_v<detail::member_entity_t<MemberPtrs>, first_entity> && ...),
                  "asterorm: select_cols requires every column to belong to the same entity");

    static_assert(detail::aggregate_initable<DTO, detail::member_field_t<MemberPtrs>...>,
                  "asterorm: DTO field types do not match the projected columns. "
                  "Each projected column type must aggregate-initialize the DTO in "
                  "declared order.");

    return typed_select_builder<first_entity, DTO, MemberPtrs...>{
        detail::make_projection_select_ast<first_entity, MemberPtrs...>()};
}

} // namespace asterorm::sql::typed
