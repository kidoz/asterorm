#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <stdexcept>
#include <thread>

#include "support/catch2_harness_fixture.hpp"

namespace {
void mutate_fixture() {
    auto& database = asterorm::testing::catch2::session<harness_test::fixture>();
    const auto count = database.native_scalar<int>("SELECT count(*) FROM harness_rows");
    REQUIRE(count.has_value());
    REQUIRE(*count == 1);
    REQUIRE(database
                .with_connection([](auto& native) {
                    return native.execute("INSERT INTO harness_rows VALUES(2)");
                })
                .has_value());
}
} // namespace

TEST_CASE("Harness section generator paths", "[flow][transactional:probe]") {
    harness_test::body_marker body;
    const auto value = GENERATE(1, 2);
    mutate_fixture();
    SECTION("first") {
        CHECK(value > 0);
    }
    SECTION("second") {
        CHECK(value < 3);
    }
}
TEST_CASE("Harness fatal assertion", "[fatal][transactional:probe]") {
    harness_test::body_marker body;
    mutate_fixture();
    REQUIRE(false);
}
TEST_CASE("Harness skip", "[skip][transactional:probe]") {
    harness_test::body_marker body;
    mutate_fixture();
    SKIP("intentional child skip");
}
TEST_CASE("Harness body exception", "[exception][transactional:probe]") {
    harness_test::body_marker body;
    mutate_fixture();
    throw std::runtime_error("intentional body exception");
}
TEST_CASE("Harness never accesses a session", "[noaccess][transactional:probe]") {
    harness_test::body_marker body;
    CHECK(true);
}
TEST_CASE("Harness repeats without accessing a session", "[repeat][transactional:probe]") {
    harness_test::body_marker body;
    const auto value = GENERATE(1, 2);
    CHECK(value > 0);
}
TEST_CASE("Harness untagged case", "[untagged]") {
    harness_test::body_marker body;
    CHECK(true);
}
TEST_CASE("Harness unknown provider", "[unknown][transactional:missing]") {
    CHECK(true); // Runner must fail even without an accessor.
}
TEST_CASE("Harness multiple providers", "[multiple][transactional:probe][transactional:missing]") {
    CHECK(true);
}
TEST_CASE("Harness empty provider", "[empty][transactional:]") {
    CHECK(true);
}
TEST_CASE("Harness missing provider separator", "[malformed][transactional]") {
    CHECK(true);
}
TEST_CASE("Harness unsupported session capability", "[unsupported][transactional:unsupported]") {
    CHECK(true);
}
TEST_CASE("Harness wrong fixture type", "[mismatch][transactional:probe]") {
    CHECK_THROWS_AS(asterorm::testing::catch2::session<harness_test::other_fixture>(),
                    std::logic_error);
}
TEST_CASE("Harness accessor without a tag", "[outside]") {
    CHECK_THROWS_AS(asterorm::testing::catch2::session<harness_test::fixture>(), std::logic_error);
}
TEST_CASE("Harness accessor on a worker thread", "[thread][transactional:probe]") {
    bool rejected = false;
    std::thread worker{[&] {
        try {
            (void)asterorm::testing::catch2::session<harness_test::fixture>();
        } catch (const std::logic_error&) {
            rejected = true;
        }
    }};
    worker.join();
    CHECK(rejected);
    mutate_fixture();
}
TEST_CASE("Harness tag case follows Catch2", "[casefold][TRANSACTIONAL:PROBE]") {
    harness_test::body_marker body;
    mutate_fixture();
}
TEST_CASE("Harness registration is frozen during execution", "[late][transactional:probe]") {
    CHECK_FALSE(asterorm::testing::catch2::register_fixture<harness_test::other_fixture>("late"));
}
TEST_CASE("Z Harness healthy provider survives another provider's failure",
          "[healthy][transactional:healthy]") {
    harness_test::body_marker body;
    auto& database = asterorm::testing::catch2::session<harness_test::other_fixture>();
    const auto count = database.native_scalar<int>("SELECT count(*) FROM harness_rows");
    REQUIRE(count.has_value());
    REQUIRE(*count == 1);
}
