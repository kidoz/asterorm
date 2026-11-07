#include "asterorm/testing/catch2.hpp"
#include "sqlite_crud_fixture.hpp"
#include <cstdio>
#include <cstdlib>

int main(int argc, char* argv[]) try {
    using asterorm::test::sqlite_crud_fixture;
    if (!asterorm::testing::catch2::register_fixture<sqlite_crud_fixture>("sqlite-crud")) {
        return 1;
    }
    const auto status = asterorm::testing::catch2::run(argc, argv);
    if (std::getenv("ASTERORM_REFERENCE_RECEIPT")) {
        asterorm::test::print_reference_receipt(
            "sqlite", sqlite_crud_fixture::instance ? sqlite_crud_fixture::instance->metrics
                                                    : asterorm::test::reference_metrics{});
    }
    return status;
} catch (...) {
    std::fputs("SQLite reference fixture initialization threw\n", stderr);
    return 1;
}
