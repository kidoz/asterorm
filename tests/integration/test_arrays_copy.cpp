#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string>
#include <vector>

struct array_model {
    std::optional<int> id;
    std::vector<std::string> tags;
    std::vector<int> nums;
};

template <> struct asterorm::entity_traits<array_model> {
    static constexpr const char* table = "extra_array_copy";
    static constexpr auto primary_key = asterorm::pk<&array_model::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&array_model::id>("id", asterorm::generated::by_default),
        asterorm::column<&array_model::tags>("tags"), asterorm::column<&array_model::nums>("nums"));
};

TEST_CASE("PG Integration: Arrays and COPY", "[pg][extras]") { // NOLINT
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
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS extra_array_copy;");
    (void)(*test_lease)
        ->execute(
            "CREATE TABLE extra_array_copy (id SERIAL PRIMARY KEY, tags TEXT[], nums INT[]);");
    test_lease.value().release_to_pool();

    asterorm::repository repo{db};

    SECTION("Arrays encode and decode") {
        array_model m;
        m.tags = {"c++", "postgres", "orm, with comma", "\"quoted\""};
        m.nums = {10, 20, 30};

        auto insert_res = repo.insert(m);
        REQUIRE(insert_res.has_value());
        REQUIRE(m.id.has_value());

        auto find_res = repo.find<array_model>(*m.id);
        REQUIRE(find_res.has_value());

        auto& loaded = find_res.value();
        REQUIRE(loaded.tags.size() == 4);
        REQUIRE(loaded.tags[0] == "c++");
        REQUIRE(loaded.tags[1] == "postgres");
        REQUIRE(loaded.tags[2] == "orm, with comma");
        REQUIRE(loaded.tags[3] == "\"quoted\"");

        REQUIRE(loaded.nums.size() == 3);
        REQUIRE(loaded.nums[0] == 10);
        REQUIRE(loaded.nums[1] == 20);
        REQUIRE(loaded.nums[2] == 30);
    }

    SECTION("COPY IN and COPY OUT") {
        std::vector<std::string> lines = {"100\t{tag1,tag2}\t{1,2}", "101\t{tag3}\t{3,4}"};
        auto copy_in_res = db.with_connection([&](auto& conn) {
            return conn.copy_in("COPY extra_array_copy (id, tags, nums) FROM STDIN", lines);
        });
        REQUIRE(copy_in_res.has_value());

        auto copy_out_res = db.with_connection([&](auto& conn) {
            return conn.copy_out("COPY extra_array_copy (id, tags, nums) TO STDOUT");
        });
        REQUIRE(copy_out_res.has_value());
        auto& out_lines = copy_out_res.value();
        REQUIRE(out_lines.size() >= 2);

        bool found_100 = false;
        for (const auto& line : out_lines) {
            if (line.contains("100\t{tag1,tag2}\t{1,2}")) {
                found_100 = true;
            }
        }
        REQUIRE(found_100);
    }

    // Cleanup
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS extra_array_copy;");
}