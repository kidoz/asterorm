#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string>

#include "asterorm/postgres/driver.hpp"
#include "support/transactional_session.hpp"

namespace {
std::string scope_settings() {
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    return configured ? configured
                      : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test "
                        "sslmode=disable";
}
} // namespace
TEST_CASE("PG transactional testing scope preserves fixtures and fails closed",
          "[pg][transactional-testing]") {
    asterorm::test::check_transactional_session(asterorm::pg::driver{}, scope_settings());
}
TEST_CASE("PG test scope gates streaming and owned scopes without dispatch",
          "[pg][transactional-testing]") {
    asterorm::pool_config config;
    config.conninfo = scope_settings();
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    using scope_type = asterorm::testing::transactional_session<decltype(db)>;
    using asterorm::test::scope_take;
    auto scope = scope_take(scope_type::create(db));
    bool called = false;
    REQUIRE_FALSE(db.with_advisory_lock(9, [&](auto&) -> asterorm::result<void> {
                        called = true;
                        return {};
                    }).has_value());
    REQUIRE_FALSE(db.with_statement_timeout(std::chrono::milliseconds{10},
                                            [&](auto&) -> asterorm::result<void> {
                                                called = true;
                                                return {};
                                            })
                      .has_value());
    const auto source = [] {
        return asterorm::sql::typed::select<transactional_scope_row>().build();
    };
    REQUIRE_FALSE(repo.stream<transactional_scope_row>(source()).has_value());
    REQUIRE_FALSE(
        repo.stream<transactional_scope_row>(source(), {.mode = asterorm::stream_mode::cursor})
            .has_value());
    REQUIRE_FALSE(called);
    scope_take(scope.finish());
    REQUIRE(scope_take(db.native_scalar<int>("SELECT pg_backend_pid()")) > 0);
}

TEST_CASE("PG transactional test scope discards a terminated backend and never reconnects silently",
          "[pg][transactional-testing]") {
    using asterorm::test::scope_take;
    asterorm::pool_config config;
    config.conninfo = scope_settings();
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    using scope_type = asterorm::testing::transactional_session<decltype(db)>;
    auto scope = scope_take(scope_type::create(db));
    const auto pid = scope_take(db.native_scalar<int>("SELECT pg_backend_pid()")).value();
    auto killer = scope_take(asterorm::pg::driver{}.connect(scope_settings()));
    scope_take(killer.execute_params("SELECT pg_terminate_backend($1::int)",
                                     {asterorm::encode_parameter(pid)}));
    REQUIRE_FALSE(db.native_scalar<int>("SELECT 1").has_value());
    const auto failed = scope.finish();
    REQUIRE_FALSE(failed.has_value());
    REQUIRE_FALSE(failed.error().message.empty());
    REQUIRE(scope.finish().error().message == failed.error().message);
    REQUIRE(pool.stats().total == 0);
    REQUIRE_FALSE(scope_type::create(db).has_value());
    REQUIRE_FALSE(db.native_scalar<int>("SELECT 1").has_value());
    asterorm::session independent{pool};
    REQUIRE(scope_take(independent.native_scalar<int>("SELECT 1")) == 1);
}
