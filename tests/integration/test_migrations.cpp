#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "asterorm/migration.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"

TEST_CASE("PG Integration: Schema migrations", "[pg][migrations]") {
    const char* env_conninfo = std::getenv("ASTERORM_TEST_CONNINFO");
    std::string conninfo = env_conninfo ? env_conninfo
                                        : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                          "password=orm_test sslmode=disable";

    asterorm::pool_config cfg;
    cfg.conninfo = conninfo;
    cfg.min_size = 1;
    cfg.max_size = 2;

    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};
    asterorm::session<decltype(pool)> db{pool};

    auto test_lease = pool.acquire();
    if (!test_lease.has_value()) {
        WARN("Could not connect to PostgreSQL. Is it running? Skipping test.");
        return;
    }

    (void)(*test_lease)->execute("DROP TABLE IF EXISTS migration_users;");
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS test_schema_migrations;");
    test_lease.value().release_to_pool();

    std::vector<asterorm::migration> migrations{
        {.version = 1,
         .name = "create users",
         .up_sql = "CREATE TABLE migration_users (id INT PRIMARY KEY)",
         .down_sql = "DROP TABLE migration_users"},
        {.version = 2,
         .name = "add email",
         .up_sql = "ALTER TABLE migration_users ADD COLUMN email TEXT",
         .down_sql = "ALTER TABLE migration_users DROP COLUMN email"},
    };

    asterorm::schema_migrator migrator{db, "test_schema_migrations"};

    auto initial_version = migrator.current_version();
    REQUIRE(initial_version.has_value());
    REQUIRE(*initial_version == 0);

    asterorm::schema_migrator invalid_migrator{db, "bad;schema_migrations"};
    auto invalid_apply = invalid_migrator.apply({});
    REQUIRE_FALSE(invalid_apply.has_value());

    auto applied = migrator.apply(migrations);
    REQUIRE(applied.has_value());
    REQUIRE(applied->current_version == 2);
    REQUIRE(applied->applied_count == 2);

    auto reapplied = migrator.apply(migrations);
    REQUIRE(reapplied.has_value());
    REQUIRE(reapplied->current_version == 2);
    REQUIRE(reapplied->applied_count == 0);

    auto rolled_back = migrator.rollback_to(0, migrations);
    REQUIRE(rolled_back.has_value());
    REQUIRE(rolled_back->current_version == 0);
    REQUIRE(rolled_back->applied_count == 2);

    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS migration_users;");
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS test_schema_migrations;");
}
