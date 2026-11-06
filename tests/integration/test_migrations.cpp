#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <type_traits>

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
    INFO("PostgreSQL integration tests require ASTERORM_TEST_CONNINFO or the default local test "
         "database");
    REQUIRE(test_lease.has_value());

    (void)(*test_lease)->execute("DROP TABLE IF EXISTS migration_users;");
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS test_schema_migrations;");
    test_lease.value().release_to_pool();

    std::vector<asterorm::migration> migrations{
        {
            .version = 1,
            .name = "create users",
            .up_sql = "CREATE TABLE migration_users (id INT PRIMARY KEY)",
            .down_sql = "DROP TABLE migration_users",
        },
        {
            .version = 2,
            .name = "add email",
            .up_sql = "ALTER TABLE migration_users ADD COLUMN email TEXT",
            .down_sql = "ALTER TABLE migration_users DROP COLUMN email",
        },
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

namespace {
template <typename Value> Value migration_take(asterorm::result<Value> result) {
    INFO((result ? "success" : result.error().message));
    REQUIRE(result.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*result);
    }
}

asterorm::pool_config migration_config() {
    const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    asterorm::pool_config config;
    config.conninfo = configured ? configured
                                 : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                   "password=orm_test sslmode=disable";
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds{100};
    return config;
}

struct migration_fixture {
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{},
                                                         migration_config()};
    asterorm::session<decltype(pool)> db{pool};
    asterorm::pg::connection peer =
        migration_take(asterorm::pg::driver{}.connect(migration_config().conninfo));
    std::string schema =
        "migration_failure_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    asterorm::schema_migrator<decltype(db)> migrator{db, schema + ".history"};

    migration_fixture() {
        migration_take(peer.execute("SET statement_timeout = '3s'"));
        migration_take(peer.execute("CREATE SCHEMA " + schema));
        migration_take(db.with_connection(
            [](auto& connection) { return connection.execute("SET statement_timeout = '3s'"); }));
    }
    ~migration_fixture() {
        try {
            (void)peer.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        } catch (...) {
            // Best-effort teardown must preserve the original test failure.
            peer.close();
        }
    }
    [[nodiscard]] std::vector<asterorm::migration> definitions() const {
        return {
            {
                .version = 1,
                .name = "create items",
                .up_sql = "CREATE TABLE " + schema + ".items (id INTEGER PRIMARY KEY)",
                .down_sql = "DROP TABLE " + schema + ".items",
            },
            {
                .version = 2,
                .name = "add value",
                .up_sql = "ALTER TABLE " + schema + ".items ADD COLUMN value TEXT",
                .down_sql = "ALTER TABLE " + schema + ".items DROP COLUMN value",
            },
        };
    }
    void require_released() {
        auto available = migration_take(
            peer.execute_params("SELECT pg_try_advisory_lock($1::bigint)",
                                {asterorm::encode_parameter(std::int64_t{0x41535445524f524d})}));
        REQUIRE(available.get_value(0, 0) == "t");
        migration_take(peer.advisory_unlock(0x41535445524f524d));
        REQUIRE(pool.stats().in_use == 0);
    }
};
} // namespace

TEST_CASE("PG Integration: migration failures retain earlier commits and release the lock",
          "[pg][migration-serialization]") {
    migration_fixture fixture;
    auto definitions = fixture.definitions();
    definitions[1].up_sql = "SELECT 1/0";
    auto applied = fixture.migrator.apply(definitions);
    REQUIRE_FALSE(applied.has_value());
    REQUIRE(applied.error().sqlstate == "22012");
    fixture.require_released();
    REQUIRE(migration_take(fixture.migrator.current_version()) == 1);
    auto retry = migration_take(fixture.migrator.apply(fixture.definitions()));
    REQUIRE(retry.current_version == 2);
    REQUIRE(retry.applied_count == 1);

    definitions = fixture.definitions();
    definitions[0].down_sql = "SELECT 1/0";
    auto rollback = fixture.migrator.rollback_to(0, definitions);
    REQUIRE_FALSE(rollback.has_value());
    REQUIRE(rollback.error().sqlstate == "22012");
    fixture.require_released();
    REQUIRE(migration_take(fixture.migrator.current_version()) == 1);
    REQUIRE(migration_take(fixture.migrator.rollback_to(0, fixture.definitions())).applied_count ==
            1);
    fixture.require_released();
}

TEST_CASE("PG Integration: migration observer exceptions roll back and release the lock",
          "[pg][migration-serialization]") {
    const bool rollback = GENERATE(false, true);
    migration_fixture fixture;
    if (rollback) {
        migration_take(fixture.migrator.apply(fixture.definitions()));
    }
    fixture.db.set_observer([&](const asterorm::query_event& event) {
        if (event.sql.starts_with(rollback ? "DELETE FROM" : "INSERT INTO")) {
            throw std::runtime_error("migration observer threw");
        }
    });
    if (rollback) {
        REQUIRE_THROWS_WITH(fixture.migrator.rollback_to(0, fixture.definitions()),
                            "migration observer threw");
    } else {
        REQUIRE_THROWS_WITH(fixture.migrator.apply(fixture.definitions()),
                            "migration observer threw");
    }
    fixture.db.set_observer({});
    fixture.require_released();
    REQUIRE(migration_take(fixture.migrator.current_version()) == (rollback ? 2 : 0));
    if (rollback) {
        REQUIRE(
            migration_take(fixture.migrator.rollback_to(0, fixture.definitions())).applied_count ==
            2);
    } else {
        REQUIRE(migration_take(fixture.migrator.apply(fixture.definitions())).applied_count == 2);
    }
}

TEST_CASE("PG Integration: migrations reject caller transactions before waiting or history setup",
          "[pg][migration-serialization]") {
    migration_fixture fixture;
    migration_take(fixture.peer.advisory_lock(0x41535445524f524d));
    auto transaction = migration_take(fixture.db.begin());
    REQUIRE(fixture.db.has_active_transaction());
    REQUIRE_FALSE(fixture.migrator.apply(fixture.definitions()).has_value());
    REQUIRE_FALSE(fixture.migrator.rollback_to(0, fixture.definitions()).has_value());
    REQUIRE_FALSE(fixture.migrator.current_version().has_value());
    REQUIRE(fixture.db.has_active_transaction());
    REQUIRE(migration_take(fixture.db.native_scalar<bool>("SELECT to_regclass($1) IS NULL",
                                                          fixture.schema + ".history")) == true);
    migration_take(transaction.commit());
    REQUIRE_FALSE(fixture.db.has_active_transaction());
    migration_take(fixture.peer.advisory_unlock(0x41535445524f524d));
    REQUIRE(migration_take(fixture.migrator.apply(fixture.definitions())).applied_count == 2);
    fixture.require_released();
}
