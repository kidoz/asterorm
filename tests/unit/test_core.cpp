#include <catch2/catch_test_macros.hpp>

#include "asterorm/core/error.hpp"
#include "asterorm/core/result.hpp"

TEST_CASE("Core: db_error instantiation", "[core]") {
    asterorm::db_error err;
    err.kind = asterorm::db_error_kind::query_failed;
    err.message = "Test error";

    REQUIRE(err.kind == asterorm::db_error_kind::query_failed);
    REQUIRE(err.message == "Test error");
    REQUIRE(!err.detail.has_value());
}

TEST_CASE("Core: result type with expected semantics", "[core]") {
    asterorm::result<int> res = 42;
    REQUIRE(res.has_value());
    REQUIRE(res.value() == 42);

    asterorm::db_error err;
    err.message = "Failure";
    asterorm::result<int> bad_res = std::unexpected(err);

    REQUIRE(!bad_res.has_value());
    REQUIRE(bad_res.error().message == "Failure");
}
