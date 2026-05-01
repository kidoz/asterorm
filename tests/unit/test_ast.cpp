#include <catch2/catch_test_macros.hpp>

#include "asterorm/sql/ast.hpp"
#include "asterorm/sql/compiler.hpp"

using namespace asterorm::sql;

TEST_CASE("Core: SQL AST and Compiler", "[core][sql][ast]") {
    SECTION("Basic Select") {
        auto query = select({"id", "name"}).from("users").build();

        compiler c;
        auto compiled = c.compile(query);

        REQUIRE(compiled.sql == "SELECT id, name FROM users");
        REQUIRE(compiled.params.empty());
    }

    SECTION("Select with Where and Limit") {
        auto query = select({"*"})
                         .from("products")
                         .where(col("price") > val(100) && col("active") == val(true))
                         .order_by("price", false)
                         .limit(10)
                         .build();

        compiler c;
        auto compiled = c.compile(query);

        REQUIRE(compiled.sql ==
                "SELECT * FROM products WHERE (price > $1 AND active = $2) ORDER BY price DESC LIMIT 10");
        REQUIRE(compiled.params.size() == 2);
        REQUIRE(compiled.params[0] == "100");
        REQUIRE(compiled.params[1] == "t");
    }

    SECTION("Select with Complex Predicates") {
        auto query =
            select({"id"})
                .from("users")
                .where((col("role") == val("admin") || col("role") == val("moderator")) && col("banned") == val(false))
                .build();

        compiler c;
        auto compiled = c.compile(query);

        REQUIRE(compiled.sql == "SELECT id FROM users WHERE ((role = $1 OR role = $2) AND banned = $3)");
        REQUIRE(compiled.params.size() == 3);
        REQUIRE(compiled.params[0] == "admin");
        REQUIRE(compiled.params[1] == "moderator");
        REQUIRE(compiled.params[2] == "f");
    }
}