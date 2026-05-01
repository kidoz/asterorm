#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>

#include "asterorm/entity_traits.hpp"

struct test_user {
    std::optional<int> id;
    std::string name;
    bool active{true};
};

template <> struct asterorm::entity_traits<test_user> {
    static constexpr const char* table = "users";
    static constexpr auto primary_key = asterorm::pk<&test_user::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&test_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&test_user::name>("name"), asterorm::column<&test_user::active>("active"));
};

TEST_CASE("Core: Entity Traits Mapping", "[core][traits]") {
    REQUIRE(std::string_view(asterorm::entity_traits<test_user>::table) == "users");

    auto pk_map = asterorm::entity_traits<test_user>::primary_key;
    REQUIRE(pk_map.name == "id");

    auto cols = asterorm::entity_traits<test_user>::columns;
    REQUIRE(std::tuple_size_v<decltype(cols)> == 3);

    auto col0 = std::get<0>(cols);
    REQUIRE(col0.name == "id");
    REQUIRE(col0.gen == asterorm::generated::by_default);

    auto col1 = std::get<1>(cols);
    REQUIRE(col1.name == "name");
    REQUIRE(col1.gen == asterorm::generated::none);

    // Verify we can access the member pointer value at compile time
    REQUIRE(col1.member_ptr == &test_user::name);
}