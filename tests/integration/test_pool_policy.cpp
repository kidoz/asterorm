#include "../support/pool_policy.hpp"
#include "asterorm/core/migration.hpp"
#include "asterorm/postgres/driver.hpp"
#include <cstdlib>
#include <set>

namespace {
std::string direct_settings() {
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    return configured ? configured
                      : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test "
                        "sslmode=disable";
}
asterorm::pool_config profile_config(std::string settings) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 2;
    config.statement_caching = asterorm::statement_cache_policy::off;
    config.profile = asterorm::pool_profile::transaction_pool;
    return config;
}
template <typename Value> Value take(asterorm::result<Value> result) {
    REQUIRE(result);
    return std::move(*result);
}
void take(asterorm::result<void> result) {
    REQUIRE(result);
}
} // namespace
TEST_CASE("PG statement cache policy", "[pg][pool-policy]") {
    asterorm::test::check_pool_policy(asterorm::pg::driver{}, direct_settings());
}
TEST_CASE("PG transaction profile rejects session work before acquisition", "[pg][pool-policy]") {
    auto config = profile_config("host=invalid.invalid connect_timeout=1");
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const auto&) { ++events; });
    REQUIRE_FALSE(db.with_advisory_lock(1, [](auto&) -> asterorm::result<void> {
        FAIL("callback ran");
        return {};
    }));
    asterorm::schema_migrator migrator{db, "pool_profile_rejected_history"};
    REQUIRE_FALSE(migrator.current_version());
    REQUIRE_FALSE(migrator.apply(
        {{1, "rejected", "CREATE TABLE never_created(id INTEGER)", "DROP TABLE never_created"}}));
    REQUIRE_FALSE(migrator.rollback_to(0, {}));
    REQUIRE(pool.stats().acquires_succeeded == 0);
    REQUIRE(pool.stats().acquires_failed == 0);
    REQUIRE(events == 0);
}
TEST_CASE("PG native profile follows connection moves and blocks session APIs",
          "[pg][pool-policy]") {
    auto config = profile_config(direct_settings());
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    auto held = take(pool.acquire());
    auto moved = std::move(*held);
    REQUIRE_FALSE(moved.advisory_lock(1));
    REQUIRE_FALSE(moved.advisory_unlock(1));
    REQUIRE_FALSE(moved.set_statement_timeout(std::chrono::milliseconds{0}));
    moved.set_max_prepared_statements(8); // Profile still forces the unnamed execution path.
    REQUIRE(moved.execute_prepared("SELECT $1::integer", {"41"}));
    REQUIRE(moved.prepared_statement_count() == 0);
    auto assigned = take(asterorm::pg::driver{}.connect(direct_settings()));
    assigned = std::move(moved);
    REQUIRE_FALSE(assigned.advisory_lock(1));
    REQUIRE(assigned.execute_prepared("SELECT $1::integer", {"42"}));
    REQUIRE(assigned.prepared_statement_count() == 0);
}
TEST_CASE("PgBouncer transaction mode preserves supported profile operations", "[.][pgbouncer]") {
    const auto* settings = std::getenv("ASTERORM_PGBOUNCER_TEST_CONNINFO");
    REQUIRE(settings != nullptr);
    auto config = profile_config(settings);
    config.pre_ping = true;
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session first{pool};
    asterorm::session second{pool};
    auto created = first.with_connection([](auto& connection) {
        return connection.execute(
            "CREATE TABLE asterorm_pool_policy_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
    });
    REQUIRE(created);
    struct cleanup {
        std::string settings;
        ~cleanup() {
            try {
                auto connection = asterorm::pg::driver{}.connect(settings);
                if (!connection || !connection->execute("DROP TABLE asterorm_pool_policy_rows")) {
                    std::fputs("pool profile proxy cleanup failed\n", stderr);
                }
            } catch (...) {
                std::fputs("pool profile proxy cleanup threw\n", stderr);
            }
        }
    } drop{settings};
    std::set<int> backends;
    for (int round = 0; round < 6; ++round) {
        auto transaction = take(first.begin());
        auto other = take(second.begin());
        const auto first_pid = *take(first.native_scalar<int>("SELECT pg_backend_pid()"));
        const auto second_pid = *take(second.native_scalar<int>("SELECT pg_backend_pid()"));
        REQUIRE(first_pid != second_pid);
        backends.insert(first_pid);
        backends.insert(second_pid);
        asterorm::repository repo{first};
        pool_policy_row row{.id = round, .label = "inserted"};
        take(repo.insert(row));
        row.label = "updated";
        take(repo.update(row));
        REQUIRE(take(repo.find<pool_policy_row>(round)).label == "updated");
        auto typed = take(asterorm::sql::typed::query(
            first, asterorm::sql::typed::select<pool_policy_row>().where(
                       asterorm::sql::typed::col<&pool_policy_row::id> ==
                       asterorm::sql::typed::val(round))));
        REQUIRE(typed.size() == 1);
        take(first.with_statement_timeout(
            std::chrono::milliseconds{1000}, [&](auto& scoped) -> asterorm::result<void> {
                auto cursor = scoped.with_connection([](auto& connection) {
                    return connection.open_cursor("SELECT $1::integer", {"42"}, 1);
                });
                REQUIRE(cursor);
                auto batch = take(cursor->next());
                REQUIRE(batch);
                REQUIRE(batch->get_int64(0, 0) == 42);
                REQUIRE_FALSE(take(cursor->next()));
                return {};
            }));
        auto preparations =
            take(first.native_scalar<int>("SELECT count(*)::integer FROM pg_prepared_statements"));
        REQUIRE(preparations == 0);
        take(transaction.commit());
        take(other.commit());
        REQUIRE(take(second.native_scalar<int>(
                    "SELECT count(*)::integer FROM asterorm_pool_policy_rows WHERE id=$1",
                    round)) == 1);
        auto deleting = take(second.begin());
        asterorm::repository other_repo{second};
        take(other_repo.erase<pool_policy_row>(round));
        take(deleting.commit());
    }
    REQUIRE(backends.size() == 2);
    REQUIRE_FALSE(first.with_advisory_lock(7, [](auto&) -> asterorm::result<void> { return {}; }));
    asterorm::schema_migrator migrator{first, "proxy_rejected_history"};
    REQUIRE_FALSE(migrator.current_version());
    REQUIRE(first.with_connection([](auto& connection) -> asterorm::result<void> {
        REQUIRE(connection.prepared_statement_count() == 0);
        return {};
    }));
}
