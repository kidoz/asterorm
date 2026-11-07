#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <optional>
#include <string>

#include "asterorm/repository.hpp"
#include "asterorm/testing/catch2.hpp"
#include "support/sqlite_crud_fixture.hpp"

struct reference_user {
    std::optional<int> id;
    std::string name;
};
template <> struct asterorm::entity_traits<reference_user> {
    static constexpr const char* table = "reference_users";
    static constexpr auto primary_key = asterorm::pk<&reference_user::id>("id");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&reference_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&reference_user::name>("name"));
};

TEST_CASE("SQLite reference fixture: CRUD paths",
          "[sqlite][reference-crud][transactional:sqlite-crud]") {
    using asterorm::test::sqlite_crud_fixture;
    auto& database = asterorm::testing::catch2::session<sqlite_crud_fixture>();
    REQUIRE(sqlite_crud_fixture::instance != nullptr);
    auto& fixture = *sqlite_crud_fixture::instance;
    ++fixture.metrics.bodies;
    asterorm::repository repository{database};
    const auto value = GENERATE(2, 3);
    auto reference = repository.find<reference_user>(fixture.reference_user_id);
    REQUIRE(reference.has_value());
    REQUIRE(reference->name == "Reference");
    auto rows = database.native_scalar<int>("SELECT count(*) FROM temp.reference_users");
    REQUIRE(rows.has_value());
    REQUIRE(*rows == 1);
    SECTION("Update reference and insert a fresh object") {
        reference->name = "Mutation " + std::to_string(value);
        REQUIRE(repository.update(*reference).has_value());
        reference_user inserted{.name = "Fresh"};
        REQUIRE(repository.insert(inserted).has_value());
        REQUIRE(inserted.id.has_value());
        REQUIRE(*inserted.id != fixture.reference_user_id);
    }
    SECTION("Delete reference and create replacement data") {
        REQUIRE(repository.erase<reference_user>(fixture.reference_user_id).has_value());
        reference_user inserted{.name = "Replacement " + std::to_string(value)};
        REQUIRE(repository.insert(inserted).has_value());
        REQUIRE_FALSE(repository.find<reference_user>(fixture.reference_user_id).has_value());
    }
}
