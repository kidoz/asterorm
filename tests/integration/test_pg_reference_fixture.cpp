#include "support/pg_crud_fixture.hpp"
#include "support/reference_fixture_cases.hpp"
#include <catch2/catch_test_macros.hpp>

using asterorm::test::pg_crud_fixture;

TEST_CASE("Reference loss 1 initialize", "[.][reference-loss][transactional:pg-crud]") {
    asterorm::test::check_reference_initialized<pg_crud_fixture>();
}
TEST_CASE("Reference loss 2 damage idle connection", "[.][reference-loss]") {
    if (asterorm::test::reference_damage_mode() == "terminate") {
        REQUIRE(pg_crud_fixture::instance != nullptr);
        auto& owner = pg_crud_fixture::instance->database;
        REQUIRE_FALSE(owner.has_active_transaction());
        auto pid = owner.native_scalar<int>("SELECT pg_backend_pid()");
        REQUIRE(pid.has_value());
        REQUIRE(pid->has_value());
        auto killer = asterorm::pg::driver{}.connect(asterorm::test::reference_pg_settings());
        REQUIRE(killer.has_value());
        REQUIRE(killer
                    ->execute_params("SELECT pg_terminate_backend($1::int)",
                                     {asterorm::encode_parameter(**pid)})
                    .has_value());
    } else {
        asterorm::test::damage_idle_reference<pg_crud_fixture>();
    }
}
TEST_CASE("Reference loss 3 block access", "[.][reference-loss][transactional:pg-crud]") {
    asterorm::test::check_reference_protected<pg_crud_fixture>();
}
TEST_CASE("Reference fixture untagged case stays lazy", "[.][reference-untagged]") {
    CHECK(pg_crud_fixture::instance == nullptr);
}
