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

template <> struct asterorm::entity_traits<native_user> {
    static constexpr const char* table = "native_users";
    static constexpr auto primary_key = asterorm::pk<&native_user::id>("id");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&native_user::id>("id", asterorm::generated::by_default),
                        asterorm::column<&native_user::email>("email"),
                        asterorm::column<&native_user::name>("name"),
                        asterorm::column<&native_user::active>("active"));
};

TEST_CASE("PG Integration: Native SQL", "[pg][sql]") {
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

    // Set up table
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS native_users;");
    (void)(*test_lease)->execute("DROP TABLE IF EXISTS diagnostic_users;");
    (void)(*test_lease)
        ->execute("CREATE TABLE native_users (id SERIAL PRIMARY KEY, email TEXT, name TEXT, active "
                  "BOOLEAN);");
    (void)(*test_lease)
        ->execute("CREATE TABLE diagnostic_users (id SERIAL PRIMARY KEY, email TEXT NOT NULL, "
                  "CONSTRAINT diagnostic_users_email_key UNIQUE (email));");
    (void)(*test_lease)
        ->execute("INSERT INTO native_users (email, name, active) VALUES ('alice@example.com', "
                  "'Alice', true);");
    (void)(*test_lease)
        ->execute("INSERT INTO native_users (email, name, active) VALUES ('bob@example.com', "
                  "'Bob', false);");
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
        auto rows_res =
            db.native<native_user>("SELECT id, email, name, active FROM native_users ORDER BY id");
        REQUIRE(rows_res.has_value());

        auto& rows = rows_res.value();
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0].name == "Alice");
        REQUIRE(rows[1].name == "Bob");
    }

    SECTION("Mapped native SQL is strict by default") {
        auto rows_res = db.native<native_user>("SELECT id, email FROM native_users ORDER BY id");
        REQUIRE_FALSE(rows_res.has_value());
        REQUIRE(rows_res.error().message.contains("missing mapped column"));
    }

    SECTION("Mapped native SQL can be lenient") {
        auto rows_res = db.native_as<native_user>(asterorm::native_hydration::lenient,
                                                  "SELECT id, email FROM native_users ORDER BY id");
        REQUIRE(rows_res.has_value());
        REQUIRE(rows_res->size() == 2);
        REQUIRE((*rows_res)[0].email == "alice@example.com");
        REQUIRE((*rows_res)[0].name.empty());
    }

    SECTION("Native scalar query") {
        auto count_res =
            db.native_scalar<int>("SELECT count(*) FROM native_users WHERE active = $1", true);
        REQUIRE(count_res.has_value());
        REQUIRE(count_res->has_value());
        REQUIRE(**count_res == 1);
    }

    SECTION("Native row mapper query") {
        auto names_res = db.native_map("SELECT id, name FROM native_users ORDER BY id",
                                       [](const auto& result, int row) {
                                           return std::to_string(*result.get_int64(row, 0)) + ":" +
                                                  std::string{*result.get_string(row, 1)};
                                       });
        REQUIRE(names_res.has_value());
        REQUIRE(names_res->size() == 2);
        REQUIRE((*names_res)[0] == "1:Alice");
    }

    SECTION("Prepared statement cache has a bounded size") {
        auto cache_res = db.with_connection([](auto& conn) -> asterorm::result<std::size_t> {
            conn.set_max_prepared_statements(1);
            auto first = conn.execute_prepared("SELECT 1", {});
            if (!first)
                return std::unexpected(first.error());
            auto second = conn.execute_prepared("SELECT 2", {});
            if (!second)
                return std::unexpected(second.error());
            return conn.prepared_statement_count();
        });
        REQUIRE(cache_res.has_value());
        REQUIRE(*cache_res == 1);
    }

    SECTION("PostgreSQL errors expose structured diagnostics") {
        auto null_res = db.with_connection([](auto& conn) {
            return conn.execute("INSERT INTO diagnostic_users (email) VALUES (NULL)");
        });
        REQUIRE_FALSE(null_res.has_value());
        REQUIRE(null_res.error().kind == asterorm::db_error_kind::constraint_violation);
        REQUIRE(null_res.error().sqlstate == "23502");
        REQUIRE(null_res.error().table == "diagnostic_users");
        REQUIRE(null_res.error().column == "email");

        auto first = db.with_connection([](auto& conn) {
            return conn.execute(
                "INSERT INTO diagnostic_users (email) VALUES ('duplicate@example.com')");
        });
        REQUIRE(first.has_value());

        auto duplicate_res = db.with_connection([](auto& conn) {
            return conn.execute(
                "INSERT INTO diagnostic_users (email) VALUES ('duplicate@example.com')");
        });
        REQUIRE_FALSE(duplicate_res.has_value());
        REQUIRE(duplicate_res.error().kind == asterorm::db_error_kind::constraint_violation);
        REQUIRE(duplicate_res.error().sqlstate == "23505");
        REQUIRE(duplicate_res.error().constraint == "diagnostic_users_email_key");
        REQUIRE(duplicate_res.error().detail.has_value());
    }

    // Cleanup
    auto cleanup_lease = pool.acquire();
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS native_users;");
    (void)(*cleanup_lease)->execute("DROP TABLE IF EXISTS diagnostic_users;");
}
