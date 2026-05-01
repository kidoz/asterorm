#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"

struct crud_user {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
};

template <>
struct asterorm::entity_traits<crud_user> {
    static constexpr const char* table = "crud_users";
    static constexpr auto primary_key = asterorm::pk<&crud_user::id>("id");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&crud_user::id>("id", asterorm::generated::by_default),
                        asterorm::column<&crud_user::email>("email"), asterorm::column<&crud_user::name>("name"),
                        asterorm::column<&crud_user::active>("active"));
};

TEST_CASE("PG Integration: CRUD operations", "[pg][crud]") {
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
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS crud_users;");
    (void)(*test_lease)
        ->execute("CREATE TABLE crud_users (id SERIAL PRIMARY KEY, email TEXT, name TEXT, active BOOLEAN);");
    test_lease.value().release_to_pool();

    asterorm::repository repo{db};

    SECTION("Insert assigns ID and persists data") {
        crud_user u{.email = "alice@example.com", .name = "Alice", .active = true};
        auto insert_res = repo.insert(u);
        REQUIRE(insert_res.has_value());
        REQUIRE(u.id.has_value());
        REQUIRE(*u.id > 0);

        auto find_res = repo.find<crud_user>(*u.id);
        REQUIRE(find_res.has_value());
        auto loaded = find_res.value();
        REQUIRE(loaded.id == u.id);
        REQUIRE(loaded.email == "alice@example.com");
        REQUIRE(loaded.name == "Alice");
        REQUIRE(loaded.active == true);

        // Update
        loaded.name = "Alice Cooper";
        loaded.active = false;
        auto update_res = repo.update(loaded);
        REQUIRE(update_res.has_value());

        auto find_res2 = repo.find<crud_user>(*u.id);
        REQUIRE(find_res2.has_value());
        REQUIRE(find_res2.value().name == "Alice Cooper");
        REQUIRE(find_res2.value().active == false);

        // Delete
        auto erase_res = repo.erase<crud_user>(*u.id);
        REQUIRE(erase_res.has_value());

        auto find_res3 = repo.find<crud_user>(*u.id);
        REQUIRE(!find_res3.has_value());  // should fail
    }

    // Cleanup
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS crud_users;");
}