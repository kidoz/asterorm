#include <catch2/catch_test_macros.hpp>

#include <string>

#include "asterorm/sql/ast.hpp"
#include "asterorm/sql/compiler.hpp"

using namespace asterorm::sql;

TEST_CASE("Assignment arithmetic quotes columns, binds values, and preserves nesting",
          "[core][sql][arithmetic]") {
    STATIC_REQUIRE(std::is_const_v<arithmetic_ptr::element_type>);
    STATIC_REQUIRE_FALSE(std::is_constructible_v<arithmetic_operand, raw_expr>);
    STATIC_REQUIRE_FALSE(std::is_constructible_v<arithmetic_operand, std::shared_ptr<func_expr>>);
    for (auto target : {dialect::postgres, dialect::sqlite, dialect::mysql}) {
        compiler compiler{target};
        int adjustment = 2;
        auto expression = multiply(add(col("price"), val(adjustment)), val(3));
        adjustment = 99;
        REQUIRE(adjustment == 99);
        auto query = compiler.compile(update("products")
                                          .set("active", val(false))
                                          .set("price", expression)
                                          .where(col("id") == val(7))
                                          .build());
        REQUIRE(query.has_value());
        REQUIRE(query->params.size() == 4);
        REQUIRE(query->params[0].type == asterorm::parameter_type::boolean);
        REQUIRE(query->params[1] == "2");
        REQUIRE(query->params[2] == "3");
        REQUIRE(query->params[3] == "7");
        if (target == dialect::mysql) {
            REQUIRE(
                query->sql ==
                "UPDATE `products` SET `active` = ?, `price` = ((`price` + ?) * ?) WHERE `id` = ?");
        } else {
            REQUIRE(
                query->sql ==
                R"(UPDATE "products" SET "active" = $1, "price" = (("price" + $2) * $3) WHERE "id" = $4)");
        }
        auto right_nested = compiler.compile(
            update("products").set("price", add(val(2), multiply(col("price"), val(3)))).build());
        REQUIRE(right_nested.has_value());
        REQUIRE(right_nested->params[0] == "2");
        REQUIRE(right_nested->params[1] == "3");
    }
    compiler compiler;
    auto quoted = compiler.compile(
        update("products")
            .set("price", add(col("odd\"name"), val(std::string{"'; DROP TABLE products; --"})))
            .build());
    REQUIRE(quoted.has_value());
    REQUIRE(quoted->sql == R"(UPDATE "products" SET "price" = ("odd""name" + $1))");
    REQUIRE(quoted->params[0] == "'; DROP TABLE products; --");
    auto inserted = compiler.compile(
        insert_into("products").columns({"price"}).values({add(val(2), val(3))}).build());
    REQUIRE(inserted.has_value());
    REQUIRE(inserted->sql == R"(INSERT INTO "products" ("price") VALUES (($1 + $2)))");
}

TEST_CASE("Assignment arithmetic rejects malformed nodes and retains encoding errors",
          "[core][sql][arithmetic]") {
    compiler compiler;
    auto empty = compiler.compile(update("products").set("price", arithmetic_ptr{}).build());
    REQUIRE_FALSE(empty.has_value());
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) -- malformed public AST input.
    const auto invalid_operator = static_cast<arithmetic_op>(255);
    auto malformed = std::make_shared<const arithmetic_expr>(arithmetic_expr{
        .op = invalid_operator,
        .left = col("price"),
        .right = val(1),
    });
    REQUIRE_FALSE(compiler.compile(update("products").set("price", malformed).build()).has_value());
    auto null_child = add(col("price"), arithmetic_ptr{});
    REQUIRE_FALSE(
        compiler.compile(update("products").set("price", null_child).build()).has_value());
    asterorm::query_parameter failed;
    failed.error = asterorm::db_error{
        .kind = asterorm::db_error_kind::parse_failed,
        .message = "arithmetic codec identity",
    };
    auto invalid_parameter = compiler.compile(
        update("products").set("price", multiply(col("price"), param_expr{failed})).build());
    REQUIRE_FALSE(invalid_parameter.has_value());
    REQUIRE(invalid_parameter.error().message == "arithmetic codec identity");
    arithmetic_ptr nested = add(col("price"), val(1));
    for (std::size_t depth = 1; depth < max_arithmetic_depth; ++depth) {
        nested = add(nested, val(1));
    }
    REQUIRE(compiler.compile(update("products").set("price", nested).build()).has_value());
    REQUIRE_FALSE(
        compiler.compile(update("products").set("price", add(nested, val(1))).build()).has_value());
    auto cycle = std::make_shared<arithmetic_expr>(arithmetic_expr{
        .op = arithmetic_op::add,
        .left = val(1),
        .right = val(2),
    });
    cycle->left = arithmetic_ptr{cycle};
    auto cyclic = compiler.compile(update("products").set("price", arithmetic_ptr{cycle}).build());
    cycle->left = val(0); // Break the deliberately malformed ownership cycle.
    REQUIRE_FALSE(cyclic.has_value());
    arithmetic_ptr wide = add(val(1), val(2));
    for (int expansion = 0; expansion < 8; ++expansion) {
        wide = add(wide, wide);
    }
    REQUIRE(compiler.compile(update("products").set("price", wide).build()).has_value());
    REQUIRE_FALSE(
        compiler.compile(update("products").set("price", add(wide, wide)).build()).has_value());
}

TEST_CASE("Core: SQL AST and Compiler", "[core][sql][ast]") {
    SECTION("Basic Select") {
        auto query = select({"id", "name"}).from("users").build();

        compiler c;
        auto compiled = c.compile(query).value();

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
        auto compiled = c.compile(query).value();

        REQUIRE(compiled.sql ==
                R"(SELECT * FROM "products" WHERE ("price" > $1 AND "active" = $2) ORDER )"
                R"(BY "price" DESC LIMIT 10)");
        REQUIRE(compiled.params.size() == 2);
        REQUIRE(compiled.params[0] == "100");
        REQUIRE(compiled.params[1] == "t");
        REQUIRE(compiled.params[1].type == asterorm::parameter_type::boolean);
        REQUIRE(asterorm::encode_parameter(std::string{"t"}).type ==
                asterorm::parameter_type::text);
    }

    SECTION("Select with Complex Predicates") {
        auto query = select({"id"})
                         .from("users")
                         .where((col("role") == val("admin") || col("role") == val("moderator")) &&
                                col("banned") == val(false))
                         .build();

        compiler c;
        auto compiled = c.compile(query).value();

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
        auto compiled = c.compile(query).value();
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
        auto compiled = c.compile(q).value();
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
        auto compiled = c.compile(q).value();
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
        auto compiled = c.compile(q).value();
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
        auto compiled = c.compile(q).value();
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
        auto compiled = c.compile(q).value();
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
        auto compiled = c.compile(q).value();
        REQUIRE(compiled.sql == R"(DELETE FROM "users" WHERE "id" = $1 RETURNING *)");
        REQUIRE(compiled.params.size() == 1);
    }
}

TEST_CASE("Compiler rejects identifiers and function names that can change the statement",
          "[core][sql][ast]") {
    compiler compiler;
    REQUIRE_FALSE(compiler.quote_ident(std::string_view("users\0drop", 10)).has_value());
    REQUIRE_FALSE(compiler.quote_qualified_ident("public..users").has_value());
    REQUIRE(compiler.quote_ident("na\"me").value() == R"("na""me")");

    auto injected = select({}).from("users").build();
    injected.items.push_back(select_item{.fn = func("count; delete from accounts", {})});
    REQUIRE_FALSE(compiler.compile(injected).has_value());

    insert_ast insert;
    insert.table = "events";
    insert.columns = {"id"};
    insert.rows = {{raw_expr{std::string("1\0; drop", 8)}}};
    REQUIRE_FALSE(compiler.compile(insert).has_value());
}
