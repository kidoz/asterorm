#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/postgres/types.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"

struct extra_model {
    std::optional<int> id;
    std::string key;
    asterorm::pg::jsonb data;
};

template <> struct asterorm::entity_traits<extra_model> {
    static constexpr const char* table = "extras_table";
    static constexpr auto primary_key = asterorm::pk<&extra_model::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&extra_model::id>("id", asterorm::generated::by_default),
        asterorm::column<&extra_model::key>("key"), asterorm::column<&extra_model::data>("data"));
};

TEST_CASE("PG Integration: Extras (JSONB & Upsert)", "[pg][extras]") {
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

    // Set up table with JSONB column
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS extras_table;");
    (void)(*test_lease)
        ->execute(
            "CREATE TABLE extras_table (id SERIAL PRIMARY KEY, key TEXT UNIQUE, data JSONB);");
    test_lease.value().release_to_pool();

    asterorm::repository repo{db};

    SECTION("JSONB Mapping") {
        extra_model m{.key = "config",
                      .data = asterorm::pg::jsonb{R"({"theme": "dark", "version": 2})"}};
        auto insert_res = repo.insert(m);
        REQUIRE(insert_res.has_value());
        REQUIRE(m.id.has_value());

        auto find_res = repo.find<extra_model>(*m.id);
        REQUIRE(find_res.has_value());

        // Let's remove whitespace for safe comparison since postgres might format the jsonb
        std::string found_json = find_res.value().data.value;
        REQUIRE(found_json.contains("\"theme\": \"dark\""));
        REQUIRE(found_json.contains("\"version\": 2"));
    }

    SECTION("Upsert (ON CONFLICT DO UPDATE)") {
        // First insert with ID provided explicitly
        extra_model m1{
            .id = 42, .key = "static", .data = asterorm::pg::jsonb{R"({"status": "initial"})"}};
        auto insert_res = repo.insert(m1);
        REQUIRE(insert_res.has_value());

        // Attempt upsert with SAME ID, but different data
        extra_model m2{.id = 42,
                       .key = "static_changed",
                       .data = asterorm::pg::jsonb{R"({"status": "updated"})"}};
        auto upsert_res = repo.upsert(m2);
        REQUIRE(upsert_res.has_value());

        // Verify it was updated instead of failing with primary key conflict
        auto find_res = repo.find<extra_model>(42);
        REQUIRE(find_res.has_value());
        REQUIRE(find_res.value().key == "static_changed");
        REQUIRE(find_res.value().data.value.contains("\"status\": \"updated\""));
    }
    // Cleanup
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS extras_table;");
}