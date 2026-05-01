#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "asterorm/entity_traits.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"

struct native_user {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
};

template <>
struct asterorm::entity_traits<native_user> {
    static constexpr const char* table = "native_users";
    static constexpr auto primary_key = asterorm::pk<&native_user::id>("id");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&native_user::id>("id", asterorm::generated::by_default),
                        asterorm::column<&native_user::email>("email"), asterorm::column<&native_user::name>("name"),
                        asterorm::column<&native_user::active>("active"));
};

TEST_CASE("PG Integration: Native SQL", "[pg][sql]") {
    const char* env_conninfo = std::getenv("ASTERORM_TEST_CONNINFO");
    std::string conninfo =
        env_conninfo ? env_conninfo
                     : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test sslmode=disable";

    asterorm::pool_config cfg;
    cfg.conninfo = conninfo;
    cfg.min_size = 1;
    cfg.max_size = 2;

    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};
    asterorm::session<decltype(pool)> db{pool};

    // Fail fast if no DB
    auto test_lease = pool.acquire();
    if (!test_lease.has_value()) {
        WARN("Could not connect to PostgreSQL. Is it running? Skipping test.");
        return;
    }

    // Set up table
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS native_users;");
    (void)(*test_lease)
        ->execute("CREATE TABLE native_users (id SERIAL PRIMARY KEY, email TEXT, name TEXT, active BOOLEAN);");
    (void)(*test_lease)
        ->execute("INSERT INTO native_users (email, name, active) VALUES ('alice@example.com', 'Alice', true);");
    (void)(*test_lease)
        ->execute("INSERT INTO native_users (email, name, active) VALUES ('bob@example.com', 'Bob', false);");
    test_lease.value().release_to_pool();

    SECTION("Select mapped entities with parameters") {
        auto rows_res = db.native<native_user>(
            "SELECT id, email, name, active FROM native_users WHERE active = $1 ORDER BY id", true);
        REQUIRE(rows_res.has_value());

        auto& rows = rows_res.value();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0].name == "Alice");
        REQUIRE(rows[0].active == true);
    }

    SECTION("Select multiple entities") {
        auto rows_res = db.native<native_user>("SELECT id, email, name, active FROM native_users ORDER BY id");
        REQUIRE(rows_res.has_value());

        auto& rows = rows_res.value();
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0].name == "Alice");
        REQUIRE(rows[1].name == "Bob");
    }

    // Cleanup
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS native_users;");
}