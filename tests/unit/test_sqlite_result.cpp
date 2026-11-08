#include <catch2/catch_test_macros.hpp>
#include <limits>

#include "asterorm/sqlite/driver.hpp"
#include "asterorm/sqlite/result.hpp"

TEST_CASE("SQLite pool readiness rejects dirty transactions and busy native statements",
          "[sqlite][pool-health]") {
    sqlite3* native{};
    REQUIRE(sqlite3_open(":memory:", &native) == SQLITE_OK);
    asterorm::sqlite::connection connection{native};
    REQUIRE(connection.is_pool_reusable());
    sqlite3_stmt* raw{};
    REQUIRE(sqlite3_prepare_v2(native, "SELECT 1", -1, &raw, nullptr) == SQLITE_OK);
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement{raw, sqlite3_finalize};
    REQUIRE(sqlite3_step(statement.get()) == SQLITE_ROW);
    REQUIRE_FALSE(connection.is_pool_reusable());
    statement.reset();
    REQUIRE(connection.is_pool_reusable());
    REQUIRE(connection.execute("BEGIN"));
    REQUIRE_FALSE(connection.is_pool_reusable());
    REQUIRE(connection.execute("ROLLBACK"));
    REQUIRE(connection.is_pool_reusable());
}

TEST_CASE("SQLite result counts retain 64-bit values without a database", "[sqlite][row-count]") {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    asterorm::sqlite::result result{{}, {}, maximum};
    STATIC_REQUIRE(std::is_same_v<decltype(result.affected_rows()), std::uint64_t>);
    REQUIRE(result.affected_rows() == maximum);
    auto moved = std::move(result);
    REQUIRE(moved.affected_rows() == maximum);
    REQUIRE(moved.rows() == 0);
}

TEST_CASE("SQLite counts only direct statement changes, including RETURNING",
          "[sqlite][row-count]") {
    auto connected = asterorm::sqlite::driver{}.connect(":memory:");
    REQUIRE(connected.has_value());
    auto& connection = *connected;
    REQUIRE(connection.execute("CREATE TABLE counted (id INTEGER PRIMARY KEY, price INTEGER)")
                .has_value());
    auto inserted = connection.execute("INSERT INTO counted VALUES (1,10),(2,20)");
    REQUIRE(inserted.has_value());
    REQUIRE(inserted->affected_rows() == 2);
    auto selected = connection.execute("SELECT * FROM counted");
    REQUIRE(selected.has_value());
    REQUIRE(selected->affected_rows() == 0);
    auto created = connection.execute("CREATE TABLE audit (id INTEGER)");
    REQUIRE(created.has_value());
    REQUIRE(created->affected_rows() == 0);
    REQUIRE(connection
                .execute("CREATE TRIGGER counted_audit AFTER UPDATE ON counted BEGIN INSERT INTO "
                         "audit VALUES (NEW.id); END")
                .has_value());
    auto updated = connection.execute("UPDATE counted SET price=price RETURNING id");
    REQUIRE(updated.has_value());
    REQUIRE(updated->affected_rows() == 2);
    REQUIRE(updated->rows() == 2);
    auto absent = connection.execute("DELETE FROM counted WHERE id=99");
    REQUIRE(absent.has_value());
    REQUIRE(absent->affected_rows() == 0);
}
