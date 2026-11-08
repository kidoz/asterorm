#include "asterorm/sql/query.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

namespace {
namespace sql = asterorm::sql;
sql::query_ast ordinary(std::string table) {
    return sql::query(sql::select({"id"}).from(std::move(table)).build()).build();
}
sql::query_ast definition(std::string self = "walk") {
    auto anchor = sql::select({"id"}).from("roots").where(sql::col("id") == sql::val(1)).build();
    sql::select_ast step;
    step.table = "edges";
    step.table_alias = "child";
    step.columns = {"child.id"};
    step.joins.push_back({
        .kind = sql::join_kind::inner,
        .table = std::move(self),
        .alias = std::nullopt,
        .on_clause = sql::col("child.parent") == sql::col("walk.id"),
    });
    step.where_clause = sql::col("child.id") <= sql::val(4);
    return sql::union_all(sql::query(std::move(anchor)).build(),
                          sql::query(std::move(step)).build())
        .build();
}
sql::query_ast report() {
    return sql::query(sql::select({"id"})
                          .from("walk")
                          .where(sql::col("id") >= sql::val(1))
                          .order_by("id")
                          .build())
        .with_cte("walk", definition(), {"id"})
        .recursive()
        .build();
}
void reject(const sql::query_ast& query) {
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().kind == asterorm::db_error_kind::parse_failed);
}
} // namespace

TEST_CASE("Recursive CTE: direct arms compile with shared bindings on each dialect",
          "[recursive-query]") {
    auto query = report();
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    auto compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    std::string expected =
        "WITH RECURSIVE \"walk\"(\"id\") AS (SELECT \"id\" FROM \"roots\" WHERE \"id\" = $1 UNION "
        "ALL SELECT \"child\".\"id\" FROM \"edges\" AS \"child\" INNER JOIN \"walk\" ON "
        "\"child\".\"parent\" = \"walk\".\"id\" WHERE \"child\".\"id\" <= $2) SELECT \"id\" FROM "
        "\"walk\" WHERE \"id\" >= $3 ORDER BY \"id\" ASC";
    if (target == sql::dialect::mysql) {
        std::ranges::replace(expected, '"', '`');
        for (int index = 1; index <= 3; ++index) {
            expected.replace(expected.find("$" + std::to_string(index)), 2, "?");
        }
    }
    REQUIRE(compiled->sql == expected);
    REQUIRE(compiled->params == asterorm::query_parameters{"1", "4", "1"});
    REQUIRE(compiled->result_columns == std::vector<std::string>{"id"});
    REQUIRE(compiled->sql.find("asterorm_set_") == std::string::npos);
}

TEST_CASE("Recursive CTE: invalid shapes, references and step expressions reject",
          "[recursive-query]") {
    auto query = report();
    auto& cte = query.ctes[0];
    auto& compound = std::get<sql::compound_ast>(cte.body->body);
    auto& anchor = std::get<sql::select_ast>(compound.left->body);
    auto& step = std::get<sql::select_ast>(compound.right->body);
    SECTION("missing root flag") {
        query.recursive = false;
    }
    SECTION("unused root flag") {
        step.joins.clear();
    }
    SECTION("missing columns") {
        cte.columns.clear();
    }
    SECTION("wrong explicit arity") {
        cte.columns.emplace_back("extra");
    }
    SECTION("different arm arity") {
        step.columns.emplace_back("child.parent");
    }
    SECTION("distinct connector") {
        compound.quantifier = sql::set_quantifier::distinct;
    }
    SECTION("different connector") {
        compound.operation = sql::set_operator::except_;
    }
    SECTION("null anchor") {
        compound.left.reset();
    }
    SECTION("null step") {
        compound.right.reset();
    }
    SECTION("compound anchor") {
        compound.left->body = sql::compound_ast{};
    }
    SECTION("compound step") {
        compound.right->body = sql::compound_ast{};
    }
    SECTION("anchor self") {
        anchor.table = "walk";
    }
    SECTION("case-only self") {
        step.joins[0].table = "Walk";
    }
    SECTION("duplicate self") {
        step.table = "walk";
    }
    SECTION("self in CROSS JOIN") {
        step.joins[0].kind = sql::join_kind::cross;
        step.joins[0].on_clause.reset();
    }
    SECTION("outer join") {
        step.joins[0].kind = sql::join_kind::left;
    }
    SECTION("projection function") {
        step.columns.clear();
        step.items = {{.column_name = {}, .fn = sql::count_all(), .alias = "id"}};
    }
    SECTION("predicate function") {
        step.where_clause = sql::sum(sql::col("child.id")) > sql::val(1);
    }
    SECTION("join function") {
        step.joins[0].on_clause = sql::max_(sql::col("walk.id")) == sql::col("child.parent");
    }
    SECTION("grouping") {
        step.group_bys = {"child.id"};
    }
    SECTION("having") {
        step.having_clause = sql::col("child.id") > sql::val(1);
    }
    SECTION("step order") {
        step.order_bys = {{.column = "child.id"}};
    }
    SECTION("step limit") {
        step.limit_val = 1;
    }
    SECTION("step offset") {
        step.offset_val = 1;
    }
    SECTION("anchor order") {
        anchor.order_bys = {{.column = "id"}};
    }
    SECTION("anchor limit") {
        anchor.limit_val = 1;
    }
    SECTION("connector tail") {
        compound.limit_val = 1;
    }
    SECTION("nested WITH") {
        compound.right->ctes.push_back({
            .name = "hidden",
            .columns = {},
            .body = std::make_unique<sql::query_ast>(ordinary("base")),
        });
    }
    SECTION("nested recursive flag") {
        cte.body->recursive = true;
    }
    SECTION("recursive step flag") {
        compound.right->recursive = true;
    }
    SECTION("forward/mutual reference") {
        anchor.table = "later";
        query.ctes.push_back({
            .name = "later",
            .columns = {},
            .body = std::make_unique<sql::query_ast>(ordinary("walk")),
        });
    }
    SECTION("two self definitions") {
        query.ctes.push_back({
            .name = "second",
            .columns = {"id"},
            .body = std::make_unique<sql::query_ast>(definition("second")),
        });
    }
    SECTION("NUL step value") {
        step.where_clause = sql::col("child.id") == sql::val(std::string("bad\0value", 9));
    }
    SECTION("invalid predicate") {
        step.where_clause = sql::predicate_ast{
            sql::binary_predicate{.op = sql::op_kind::and_, .left = nullptr, .right = nullptr},
        };
    }
    reject(query);
    sql::compiler compiler;
    REQUIRE_FALSE(compiler.compile(query));
    auto fresh = compiler.compile(report());
    REQUIRE(fresh);
    REQUIRE(fresh->params == asterorm::query_parameters{"1", "4", "1"});
}

TEST_CASE("Recursive CTE: earlier and later ordinary definitions keep their visibility and binds",
          "[recursive-query]") {
    auto body = definition();
    std::get<sql::select_ast>(std::get<sql::compound_ast>(body.body).left->body).table = "seed";
    auto query =
        sql::union_(ordinary("filtered"), ordinary("other"))
            .with_cte(
                "seed",
                sql::query(
                    sql::select({"id"}).from("roots").where(sql::col("id") > sql::val(0)).build())
                    .build())
            .with_cte("walk", std::move(body), {"id"})
            .with_cte(
                "filtered",
                sql::query(
                    sql::select({"id"}).from("walk").where(sql::col("id") < sql::val(100)).build())
                    .build())
            .order_by("id")
            .recursive()
            .build();
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE(compiled);
    REQUIRE(compiled->params == asterorm::query_parameters{"0", "1", "4", "100"});
    REQUIRE(compiled->sql.starts_with("WITH RECURSIVE \"seed\""));
    REQUIRE(compiled->sql.find("asterorm_set_0") > compiled->sql.find("\"filtered\" AS"));
}

TEST_CASE("Recursive CTE: self FROM and SQL data cycles are distinct from AST cycles",
          "[recursive-query]") {
    auto query = report();
    auto& step = std::get<sql::select_ast>(
        std::get<sql::compound_ast>(query.ctes[0].body->body).right->body);
    step.table = "walk";
    step.table_alias.reset();
    step.columns = {"walk.id"};
    step.joins.clear();
    step.where_clause = sql::col("walk.id") > sql::val(0);
    // This echo step could run indefinitely: do not execute it. Syntax validation
    // neither proves data termination nor inserts hidden iteration limits.
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE(compiled);
    REQUIRE(compiled->params == asterorm::query_parameters{"1", "0", "1"});
}

TEST_CASE("Recursive CTE: budget boundaries include both direct arms", "[recursive-query]") {
    auto query = report();
    SECTION("nodes") {
        auto& consumer = std::get<sql::select_ast>(query.body);
        // 28 occurrences: root, recursive envelope/connector, both arms and consumer.
        consumer.group_bys.assign(sql::max_query_nodes - 28, "id");
        REQUIRE(sql::compiler{}.compile(query));
        consumer.group_bys.emplace_back("id");
        reject(query);
    }
    SECTION("depth") {
        auto nested = sql::func("abs", {sql::val(1)});
        for (int index = 1; index < 59; ++index) {
            nested = sql::func("abs", {nested});
        }
        auto& anchor = std::get<sql::select_ast>(
            std::get<sql::compound_ast>(query.ctes[0].body->body).left->body);
        anchor.columns.clear();
        anchor.items = {{.column_name = {}, .fn = nested, .alias = "id"}};
        REQUIRE(sql::compiler{}.compile(query));
        anchor.items[0].fn = sql::func("abs", {nested});
        reject(query);
    }
}

TEST_CASE("Recursive CTE: encoded step errors retain diagnostics and arm paths are structured",
          "[recursive-query]") {
    auto query = report();
    auto& step = std::get<sql::select_ast>(
        std::get<sql::compound_ast>(query.ctes[0].body->body).right->body);
    asterorm::query_parameter parameter;
    parameter.error = asterorm::db_error{
        .kind = asterorm::db_error_kind::parse_failed,
        .sqlstate = "22000",
        .message = "encoding identity",
        .detail = "original",
        .hint = {},
        .table = {},
        .column = {},
        .constraint = {},
    };
    step.where_clause =
        sql::expr_ast{sql::col("child.id")} == sql::expr_ast{sql::param_expr{parameter}};
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().sqlstate == "22000");
    REQUIRE(compiled.error().detail == "original");
    step.where_clause = sql::col("child.id") == sql::count_all();
    compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().detail == "cte[0]:walk.step");
}
