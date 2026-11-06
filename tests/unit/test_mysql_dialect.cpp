#include <catch2/catch_test_macros.hpp>

#include "asterorm/sql/compiler.hpp"

TEST_CASE("MySQL dialect quotes identifiers and orders parameters", "[sql][mysql]") {
    using namespace asterorm::sql;
    compiler compiler{dialect::mysql};
    auto query = select({"group", "schema.a`b"})
                     .from("schema.order")
                     .where((col("id") == val(7)) && (col("active") == val(true)))
                     .offset(2)
                     .build();
    const auto compiled = compiler.compile(query);
    REQUIRE(compiled.has_value());
    REQUIRE(compiled->sql ==
            "SELECT `group`, `schema`.`a``b` FROM `schema`.`order` WHERE (`id` = ? "
            "AND `active` = ?) LIMIT 18446744073709551615 OFFSET 2");
    REQUIRE(compiled->params.size() == 2);
    REQUIRE(compiled->params[0] == "7");
    REQUIRE(compiled->params[1].type == asterorm::parameter_type::boolean);
    REQUIRE(compiler.compile(query).value().params == compiled->params);
}

TEST_CASE("MySQL rejects SQL features it cannot preserve", "[sql][mysql]") {
    using namespace asterorm::sql;
    compiler compiler{dialect::mysql};
    insert_ast insert;
    insert.table = "users";
    insert.columns = {"id"};
    insert.rows = {{val(1)}};
    insert.returning = {"id"};
    REQUIRE_FALSE(compiler.compile(insert).has_value());
    insert.returning.clear();
    REQUIRE(compiler.compile(insert).value().sql == "INSERT INTO `users` (`id`) VALUES (?)");
    insert.on_conflict = on_conflict_ast{};
    REQUIRE_FALSE(compiler.compile(insert).has_value());
    update_ast update;
    update.table = "users";
    update.assignments = {{"name", val("Alice")}};
    update.returning = {"id"};
    REQUIRE_FALSE(compiler.compile(update).has_value());
    delete_ast erase;
    erase.table = "users";
    erase.returning = {"id"};
    REQUIRE_FALSE(compiler.compile(erase).has_value());
}

TEST_CASE("Upserts render backend-specific incoming values", "[sql][mysql]") {
    using namespace asterorm::sql;
    compiler mysql_compiler{dialect::mysql};
    REQUIRE(mysql_compiler.upsert_clause("users", {"id", "name", "version"}, {"id"}, "version")
                .value() ==
            " AS asterorm_new ON DUPLICATE KEY UPDATE `name` = asterorm_new.`name`, `version` = "
            "`users`.`version` + 1");
    compiler default_compiler;
    REQUIRE(default_compiler.upsert_clause("users", {"id", "name"}, {"id"}).value() ==
            " ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"");
}
