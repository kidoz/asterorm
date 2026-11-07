#include <cstdio>
#include <memory>
#include <stdexcept>

#include "catch2_harness_fixture.hpp"

int main(int argc, char* argv[]) try {
    namespace adapter = asterorm::testing::catch2;
    bool rejected = false;
    try {
        (void)adapter::session<harness_test::fixture>();
    } catch (const std::logic_error&) {
        rejected = true;
    }
    if (!rejected) {
        return 10;
    }
    const auto registered = adapter::register_fixture<harness_test::fixture>("probe", [] {
        ++harness_test::observed.factories;
        if (harness_test::fault_mode() == "factory_nonstd") {
            throw 17; // NOLINT(bugprone-std-exception-baseclass): verify catch-all callback safety.
        }
        if (harness_test::fault_mode() == "factory_throw") {
            throw std::runtime_error("injected factory exception");
        }
        return std::make_unique<harness_test::fixture>();
    });
    if (!registered) {
        return 9;
    }
    if (!adapter::register_fixture<harness_test::other_fixture>("healthy")) {
        return 9;
    }
    if (!adapter::register_fixture<harness_test::unsupported_fixture>("unsupported")) {
        return 9;
    }
    if (harness_test::fault_mode() == "duplicate_name") {
        (void)adapter::register_fixture<harness_test::other_fixture>("probe");
    }
    if (harness_test::fault_mode() == "duplicate_type") {
        (void)adapter::register_fixture<harness_test::fixture>("second");
    }
    if (harness_test::fault_mode() == "invalid_name") {
        (void)adapter::register_fixture<harness_test::other_fixture>("Bad Name");
    }
    auto status = adapter::run(argc, argv);
    if (harness_test::fault_mode() == "run_twice") {
        status = adapter::run(argc, argv);
    }
    const auto& counts = harness_test::observed;
    // C stdio keeps fault-injection receipts portable without requiring <print> support.
    // NOLINTNEXTLINE(modernize-use-std-print)
    std::printf("summary factories=%d constructed=%d prepares=%d connects=%d closes=%d begins=%d "
                "savepoints=%d "
                "rollback_to=%d releases=%d rollbacks=%d bodies=%d unwinds=%d\n",
                counts.factories, counts.constructed, counts.prepares, counts.connects,
                counts.closes, counts.begins, counts.savepoints, counts.rollback_to,
                counts.releases, counts.rollbacks, counts.bodies, counts.unwinds);
    return status;
} catch (...) {
    std::fputs("Unexpected exception in harness test main\n", stderr);
    return 11;
}
