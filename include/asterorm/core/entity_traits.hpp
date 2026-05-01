#pragma once
#include <cstdint>
#include <string_view>
#include <tuple>

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

// Primary template to be specialized by users to map their entities
template <typename T> struct entity_traits;

} // namespace asterorm
