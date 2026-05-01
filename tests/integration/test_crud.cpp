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

struct generated_user {
    std::optional<int> id;
    std::string name;
    std::string slug;
};

struct locked_user {
    std::optional<int> id;
    std::string email;
    std::string name;
    int version{0};
};

struct order_line {
    int order_id{};
    int line_no{};
    std::string sku;
    int qty{};
};

template <> struct asterorm::entity_traits<crud_user> {
    static constexpr const char* table = "crud_users";
    static constexpr auto primary_key = asterorm::pk<&crud_user::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&crud_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&crud_user::email>("email"), asterorm::column<&crud_user::name>("name"),
        asterorm::column<&crud_user::active>("active"));
};

template <> struct asterorm::entity_traits<generated_user> {
    static constexpr const char* table = "generated_users";
    static constexpr auto primary_key = asterorm::pk<&generated_user::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&generated_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&generated_user::name>("name"),
        asterorm::column<&generated_user::slug>("slug", asterorm::generated::always));
};

template <> struct asterorm::entity_traits<locked_user> {
    static constexpr const char* table = "locked_users";
    static constexpr auto primary_key = asterorm::pk<&locked_user::id>("id");
    static constexpr auto version_column = asterorm::version<&locked_user::version>("version");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&locked_user::id>("id", asterorm::generated::by_default),
                        asterorm::column<&locked_user::email>("email"),
                        asterorm::column<&locked_user::name>("name"),
                        asterorm::column<&locked_user::version>("version"));
};

template <> struct asterorm::entity_traits<order_line> {
    static constexpr const char* table = "order_lines";
    static constexpr auto primary_key =
        std::make_tuple(asterorm::pk<&order_line::order_id>("order_id"),
                        asterorm::pk<&order_line::line_no>("line_no"));

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&order_line::order_id>("order_id"),
        asterorm::column<&order_line::line_no>("line_no"),
        asterorm::column<&order_line::sku>("sku"), asterorm::column<&order_line::qty>("qty"));
};

TEST_CASE("PG Integration: CRUD operations", "[pg][crud]") {
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

    // Fail fast if no DB
    auto test_lease = pool.acquire();
    if (!test_lease.has_value()) {
        WARN("Could not connect to PostgreSQL. Is it running? Skipping test.");
        return;
    }

    // Set up table
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS crud_users;");
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS generated_users;");
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS locked_users;");
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS order_lines;");
    (void)(*test_lease)
        ->execute("CREATE TABLE crud_users (id SERIAL PRIMARY KEY, email TEXT, name TEXT, active "
                  "BOOLEAN);");
    (void)(*test_lease)
        ->execute("CREATE TABLE generated_users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, slug "
                  "TEXT GENERATED ALWAYS AS (lower(name)) STORED);");
    (void)(*test_lease)
        ->execute("CREATE TABLE locked_users (id SERIAL PRIMARY KEY, email TEXT UNIQUE, name TEXT, "
                  "version INT NOT NULL DEFAULT 0);");
    (void)(*test_lease)
        ->execute("CREATE TABLE order_lines (order_id INT NOT NULL, line_no INT NOT NULL, sku TEXT "
                  "NOT NULL, qty INT NOT NULL, PRIMARY KEY (order_id, line_no));");
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
        REQUIRE(!find_res3.has_value()); // should fail
    }

    SECTION("Insert, upsert, and update refresh generated non-primary-key columns") {
        generated_user u{.name = "Alice Smith"};
        auto insert_res = repo.insert(u);
        REQUIRE(insert_res.has_value());
        REQUIRE(u.id.has_value());
        REQUIRE(u.slug == "alice smith");

        u.name = "Alice Cooper";
        auto update_res = repo.update(u);
        REQUIRE(update_res.has_value());
        REQUIRE(u.slug == "alice cooper");

        u.name = "Alice Delta";
        auto upsert_res = repo.upsert(u);
        REQUIRE(upsert_res.has_value());
        REQUIRE(u.slug == "alice delta");
    }

    SECTION("Bulk upsert refreshes input rows") {
        std::vector<crud_user> users{
            {.email = "bulk-a@example.com", .name = "Bulk A", .active = true},
            {.email = "bulk-b@example.com", .name = "Bulk B", .active = false},
        };

        auto inserted = repo.upsert_many(users);
        REQUIRE(inserted.has_value());
        REQUIRE(*inserted == 2);
        REQUIRE(users[0].id.has_value());
        REQUIRE(users[1].id.has_value());

        users[0].name = "Bulk A Updated";
        users[1].active = true;
        auto updated = repo.upsert_many(users);
        REQUIRE(updated.has_value());
        REQUIRE(*updated == 2);

        auto loaded = repo.find<crud_user>(*users[0].id);
        REQUIRE(loaded.has_value());
        REQUIRE(loaded->name == "Bulk A Updated");
    }

    SECTION("Version column provides optimistic locking") {
        locked_user first{.email = "locked@example.com", .name = "First"};
        auto insert_res = repo.insert(first);
        REQUIRE(insert_res.has_value());
        REQUIRE(first.id.has_value());
        REQUIRE(first.version == 0);

        auto loaded_res = repo.find<locked_user>(*first.id);
        REQUIRE(loaded_res.has_value());
        auto stale = loaded_res.value();

        first.name = "Second";
        auto update_res = repo.update(first);
        REQUIRE(update_res.has_value());
        REQUIRE(first.version == 1);

        stale.name = "Stale";
        auto stale_res = repo.update(stale);
        REQUIRE_FALSE(stale_res.has_value());
        REQUIRE(stale_res.error().kind == asterorm::db_error_kind::stale_write);
    }

    SECTION("Composite primary keys support CRUD and upsert") {
        order_line first{.order_id = 100, .line_no = 1, .sku = "ABC", .qty = 2};
        auto insert_res = repo.insert(first);
        REQUIRE(insert_res.has_value());

        auto found_res = repo.find<order_line>(std::make_tuple(100, 1));
        REQUIRE(found_res.has_value());
        REQUIRE(found_res->sku == "ABC");
        REQUIRE(found_res->qty == 2);

        found_res->qty = 5;
        auto update_res = repo.update(*found_res);
        REQUIRE(update_res.has_value());
        REQUIRE(found_res->qty == 5);

        order_line upserted{.order_id = 100, .line_no = 1, .sku = "XYZ", .qty = 9};
        auto upsert_res = repo.upsert(upserted);
        REQUIRE(upsert_res.has_value());
        REQUIRE(upserted.sku == "XYZ");

        std::vector<order_line> lines{
            {.order_id = 100, .line_no = 2, .sku = "DEF", .qty = 3},
            {.order_id = 100, .line_no = 1, .sku = "ABC-UPDATED", .qty = 11},
        };
        auto bulk_res = repo.upsert_many(lines);
        REQUIRE(bulk_res.has_value());
        REQUIRE(*bulk_res == 2);

        auto updated_res = repo.find<order_line>(std::make_tuple(100, 1));
        REQUIRE(updated_res.has_value());
        REQUIRE(updated_res->sku == "ABC-UPDATED");
        REQUIRE(updated_res->qty == 11);

        auto erase_res = repo.erase<order_line>(std::make_tuple(100, 2));
        REQUIRE(erase_res.has_value());
        auto erased_find = repo.find<order_line>(std::make_tuple(100, 2));
        REQUIRE_FALSE(erased_find.has_value());
    }

    // Cleanup
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS crud_users;");
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS generated_users;");
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS locked_users;");
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS order_lines;");
}
