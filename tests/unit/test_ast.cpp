#include <catch2/catch_test_macros.hpp>

#include "asterorm/sql/ast.hpp"
#include "asterorm/sql/compiler.hpp"

using namespace asterorm::sql;

TEST_CASE("Core: SQL AST and Compiler", "[core][sql][ast]") {
    SECTION("Basic Select") {
        auto query = select({"id", "name"}).from("users").build();

        compiler c;
        auto compiled = c.compile(query);

        REQUIRE(compiled.sql == R"(SELECT "id", "name" FROM "users")");
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
                R"(SELECT * FROM "products" WHERE ("price" > $1 AND "active" = $2) ORDER )"
                R"(BY "price" DESC LIMIT 10)");
        REQUIRE(compiled.params.size() == 2);
        REQUIRE(compiled.params[0] == "100");
        REQUIRE(compiled.params[1] == "t");
    }

    SECTION("Select with Complex Predicates") {
        auto query = select({"id"})
                         .from("users")
                         .where((col("role") == val("admin") || col("role") == val("moderator")) &&
                                col("banned") == val(false))
                         .build();

        compiler c;
        auto compiled = c.compile(query);

        REQUIRE(
            compiled.sql ==
            R"(SELECT "id" FROM "users" WHERE (("role" = $1 OR "role" = $2) AND "banned" = $3))");
        REQUIRE(compiled.params.size() == 3);
        REQUIRE(compiled.params[0] == "admin");
        REQUIRE(compiled.params[1] == "moderator");
        REQUIRE(compiled.params[2] == "f");
    }

    SECTION("Select with Limit and Offset") {
        auto query = select({"id"}).from("users").limit(20).offset(40).build();
        compiler c;
        auto compiled = c.compile(query);
        REQUIRE(compiled.sql == R"(SELECT "id" FROM "users" LIMIT 20 OFFSET 40)");
    }
}

TEST_CASE("DML: insert / update / delete AST", "[core][sql][dml]") {
    SECTION("Insert with RETURNING") {
        auto q = insert_into("users")
                     .columns({"email", "name"})
                     .values({val(std::string{"a@x"}), val(std::string{"Alice"})})
                     .returning({"id"})
                     .build();
        compiler c;
        auto compiled = c.compile(q);
        REQUIRE(compiled.sql ==
                R"(INSERT INTO "users" ("email", "name") VALUES ($1, $2) RETURNING "id")");
        REQUIRE(compiled.params.size() == 2);
    }

    SECTION("Insert multi-row ON CONFLICT DO NOTHING") {
        auto q = insert_into("events")
                     .columns({"id", "name"})
                     .values({val(1), val(std::string{"a"})})
                     .values({val(2), val(std::string{"b"})})
                     .on_conflict({.target_columns = {"id"}, .do_nothing = true})
                     .build();
        compiler c;
        auto compiled = c.compile(q);
        REQUIRE(compiled.sql == R"(INSERT INTO "events" ("id", "name") VALUES ($1, $2), ($3, $4) )"
                                R"(ON CONFLICT ("id") DO NOTHING)");
        REQUIRE(compiled.params.size() == 4);
    }

    SECTION("Insert ON CONFLICT DO UPDATE with EXCLUDED") {
        auto q = insert_into("events")
                     .columns({"id", "name"})
                     .values({val(1), val(std::string{"n"})})
                     .on_conflict({.target_columns = {"id"},
                                   .updates = {{"name", raw_expr{R"("excluded"."name")"}}}})
                     .build();
        compiler c;
        auto compiled = c.compile(q);
        REQUIRE(compiled.sql == R"(INSERT INTO "events" ("id", "name") VALUES ($1, $2) )"
                                R"(ON CONFLICT ("id") DO UPDATE SET "name" = "excluded"."name")");
    }

    SECTION("Update with WHERE and RETURNING") {
        auto q = update("users")
                     .set("name", val(std::string{"Alice Cooper"}))
                     .where(col("id") == val(7))
                     .returning({"id", "name"})
                     .build();
        compiler c;
        auto compiled = c.compile(q);
        REQUIRE(compiled.sql ==
                R"(UPDATE "users" SET "name" = $1 WHERE "id" = $2 RETURNING "id", "name")");
        REQUIRE(compiled.params.size() == 2);
    }

    SECTION("Select with INNER JOIN, GROUP BY, HAVING, aggregate") {
        auto q = select({})
                     .items({{.fn = as(count_all(), "n")}, {.column_name = "users.role"}})
                     .from("users")
                     .inner_join("roles", col("users.role_id") == col("roles.id"))
                     .where(col("users.active") == val(true))
                     .group_by({"users.role"})
                     .having(count_all() > val(0)) // NOTE: requires comparator overload below
                     .build();
        asterorm::sql::compiler c;
        auto compiled = c.compile(q);
        REQUIRE(compiled.sql ==
                R"(SELECT count(*) AS "n", "users"."role" FROM "users" INNER JOIN "roles" ON )"
                R"("users"."role_id" = "roles"."id" WHERE "users"."active" = $1 GROUP BY )"
                R"("users"."role" HAVING count(*) > $2)");
        REQUIRE(compiled.params.size() == 2);
        REQUIRE(compiled.params[0] == "t");
        REQUIRE(compiled.params[1] == "0");
    }

    SECTION("Delete with RETURNING") {
        auto q = delete_from("users").where(col("id") == val(7)).returning({"*"}).build();
        compiler c;
        auto compiled = c.compile(q);
        REQUIRE(compiled.sql == R"(DELETE FROM "users" WHERE "id" = $1 RETURNING *)");
        REQUIRE(compiled.params.size() == 1);
    }
}
