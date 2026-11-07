#include "asterorm/testing/catch2.hpp"
#include "pg_crud_fixture.hpp"
#include <cstdio>

int main(int argc, char* argv[]) try {
    using asterorm::test::pg_crud_fixture;
    if (!asterorm::testing::catch2::register_fixture<pg_crud_fixture>("pg-crud")) {
        return 1;
    }
    const auto status = asterorm::testing::catch2::run(argc, argv);
    if (std::getenv("ASTERORM_REFERENCE_RECEIPT")) {
        asterorm::test::print_reference_receipt("pg", pg_crud_fixture::instance
                                                          ? pg_crud_fixture::instance->metrics
                                                          : asterorm::test::reference_metrics{});
    }
    return status;
} catch (...) {
    std::fputs("PostgreSQL reference fixture initialization threw\n", stderr);
    return 1;
}
