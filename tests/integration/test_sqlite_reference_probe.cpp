#include "support/reference_fixture_cases.hpp"
#include "support/sqlite_crud_fixture.hpp"
#include <catch2/catch_test_macros.hpp>

TEST_CASE("Reference loss 1 initialize", "[.][reference-loss][transactional:sqlite-crud]") {
    asterorm::test::check_reference_initialized<asterorm::test::sqlite_crud_fixture>();
}
TEST_CASE("Reference loss 2 damage idle connection", "[.][reference-loss]") {
    asterorm::test::damage_idle_reference<asterorm::test::sqlite_crud_fixture>();
}
TEST_CASE("Reference loss 3 block access", "[.][reference-loss][transactional:sqlite-crud]") {
    asterorm::test::check_reference_protected<asterorm::test::sqlite_crud_fixture>();
}
TEST_CASE("Reference fixture untagged case stays lazy", "[.][reference-untagged]") {
    CHECK(asterorm::test::sqlite_crud_fixture::instance == nullptr);
}
