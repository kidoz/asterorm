#pragma once
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <tuple>
#include <type_traits>

namespace asterorm {

enum class generated : std::uint8_t { none, always, by_default };

template <auto MemberPtr> struct column_mapping {
    std::string_view name;
    generated gen;

    constexpr column_mapping(std::string_view n, generated g = generated::none) : name(n), gen(g) {}

    static constexpr auto member_ptr = MemberPtr;
};

template <auto MemberPtr>
constexpr auto column(std::string_view name, generated gen = generated::none) {
    return column_mapping<MemberPtr>{name, gen};
}

template <auto MemberPtr> struct pk_mapping {
    std::string_view name;

    constexpr pk_mapping(std::string_view n) : name(n) {}

    static constexpr auto member_ptr = MemberPtr;
};

template <auto MemberPtr> constexpr auto pk(std::string_view name) {
    return pk_mapping<MemberPtr>{name};
}

template <auto MemberPtr> struct version_mapping {
    std::string_view name;

    constexpr version_mapping(std::string_view n) : name(n) {}

    static constexpr auto member_ptr = MemberPtr;
};

template <auto MemberPtr> constexpr auto version(std::string_view name) {
    return version_mapping<MemberPtr>{name};
}

enum class relationship_kind : std::uint8_t { one_to_one, one_to_many, many_to_one };

template <relationship_kind Kind, typename RelatedEntity, auto LocalMemberPtr,
          auto RelatedMemberPtr>
struct relationship_mapping {
    std::string_view name;

    constexpr relationship_mapping(std::string_view relationship_name) : name(relationship_name) {}

    static constexpr relationship_kind kind = Kind;
    using related_entity = RelatedEntity;
    static constexpr auto local_member_ptr = LocalMemberPtr;
    static constexpr auto related_member_ptr = RelatedMemberPtr;
};

template <typename RelatedEntity, auto LocalMemberPtr, auto RelatedMemberPtr>
constexpr auto one_to_one(std::string_view name) {
    return relationship_mapping<relationship_kind::one_to_one, RelatedEntity, LocalMemberPtr,
                                RelatedMemberPtr>{name};
}

template <typename RelatedEntity, auto LocalMemberPtr, auto RelatedMemberPtr>
constexpr auto one_to_many(std::string_view name) {
    return relationship_mapping<relationship_kind::one_to_many, RelatedEntity, LocalMemberPtr,
                                RelatedMemberPtr>{name};
}

template <typename RelatedEntity, auto LocalMemberPtr, auto RelatedMemberPtr>
constexpr auto many_to_one(std::string_view name) {
    return relationship_mapping<relationship_kind::many_to_one, RelatedEntity, LocalMemberPtr,
                                RelatedMemberPtr>{name};
}

// Primary template to be specialized by users to map their entities
template <typename T> struct entity_traits;

template <typename T, typename = void> struct has_relationships : std::false_type {};

template <typename T>
struct has_relationships<T, std::void_t<decltype(entity_traits<T>::relationships)>>
    : std::true_type {};

template <typename T> inline constexpr bool has_relationships_v = has_relationships<T>::value;

template <typename T, typename = void>
struct relationship_count : std::integral_constant<std::size_t, 0> {};

template <typename T>
struct relationship_count<T, std::void_t<decltype(entity_traits<T>::relationships)>>
    : std::integral_constant<
          std::size_t, std::tuple_size_v<std::decay_t<decltype(entity_traits<T>::relationships)>>> {
};

template <typename T>
inline constexpr std::size_t relationship_count_v = relationship_count<T>::value;

} // namespace asterorm
