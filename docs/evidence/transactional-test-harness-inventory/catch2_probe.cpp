#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include <iostream>

class inventory_listener : public Catch::EventListenerBase {
  public:
    using Catch::EventListenerBase::EventListenerBase;
    void testCaseStarting([[maybe_unused]] const Catch::TestCaseInfo& info) override {
        std::cout << "event=case_start\n";
    }
    void testCasePartialStarting([[maybe_unused]] const Catch::TestCaseInfo& info,
                                 [[maybe_unused]] std::uint64_t part_number) override {
        std::cout << "event=partial_start\n";
    }
    void testCasePartialEnded([[maybe_unused]] const Catch::TestCaseStats& stats,
                              [[maybe_unused]] std::uint64_t part_number) override {
        std::cout << "event=partial_end\n";
    }
    void testCaseEnded([[maybe_unused]] const Catch::TestCaseStats& stats) override {
        std::cout << "event=case_end\n";
    }
};
CATCH_REGISTER_LISTENER(inventory_listener)

TEST_CASE("Inventory section and generator lifecycle", "[inventory-flow]") {
    const auto value = GENERATE(1, 2);
    std::cout << "event=body\n";
    SECTION("first") {
        CHECK(value > 0);
    }
    SECTION("second") {
        CHECK(value > 0);
    }
}
TEST_CASE("Inventory fatal assertion unwinding", "[inventory-fatal]") {
    std::cout << "event=body\n";
    REQUIRE(false);
}
TEST_CASE("Inventory explicit skip unwinding", "[inventory-skip]") {
    std::cout << "event=body\n";
    SKIP("Intentional inventory skip");
}
