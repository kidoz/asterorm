#include "asterorm/sql/query.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

namespace {
namespace sql = asterorm::sql;
sql::query_ast select_from(std::string table = "left_rows", int bound = 10,
                           const std::string& column = "id") {
    auto selected = sql::select({column})
                        .from(std::move(table))
                        .where(sql::col(column) > sql::val(bound))
                        .build();
    return sql::query(std::move(selected)).build();
}
sql::query_ast combine(sql::set_operator operation, sql::set_quantifier quantifier) {
    return sql::query_ast_detail::combine(select_from(), select_from("right_rows", 20, "other"),
                                          operation, quantifier)
        .build();
}
void reject(const sql::query_ast& query) {
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().kind == asterorm::db_error_kind::parse_failed);
}
} // namespace

TEST_CASE("Set query: all operators render portable operands and shared binds", "[set-query]") {
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    const auto operation = GENERATE(sql::set_operator::union_, sql::set_operator::intersect_,
                                    sql::set_operator::except_);
    const auto quantifier = GENERATE(sql::set_quantifier::distinct, sql::set_quantifier::all);
    const auto profile =
        sql::query_compile_options{sql::mysql_query_profile::set_operations_8_0_31};
    auto query = combine(operation, quantifier);
    auto compiled = sql::compiler{target, profile}.compile(query);
    if (target == sql::dialect::sqlite && operation != sql::set_operator::union_ &&
        quantifier == sql::set_quantifier::all) {
        REQUIRE_FALSE(compiled);
        return;
    }
    REQUIRE(compiled);
    std::string keyword;
    switch (operation) {
    case sql::set_operator::union_:
        keyword = "UNION";
        break;
    case sql::set_operator::intersect_:
        keyword = "INTERSECT";
        break;
    case sql::set_operator::except_:
        keyword = "EXCEPT";
        break;
    }
    if (quantifier == sql::set_quantifier::all) {
        keyword += " ALL";
    }
    std::string expected = "SELECT * FROM (SELECT \"id\" FROM \"left_rows\" WHERE \"id\" > $1) AS "
                           "\"asterorm_set_0\" " +
                           keyword +
                           " SELECT * FROM (SELECT \"other\" FROM \"right_rows\" WHERE \"other\" > "
                           "$2) AS \"asterorm_set_1\"";
    if (target == sql::dialect::mysql) {
        std::ranges::replace(expected, '"', '`');
        expected.replace(expected.find("$1"), 2, "?");
        expected.replace(expected.find("$2"), 2, "?");
    }
    REQUIRE(compiled->sql == expected);
    REQUIRE(compiled->params == asterorm::query_parameters{"10", "20"});
    REQUIRE(compiled->result_columns == std::vector<std::string>{"id"});
}

TEST_CASE("Set query: public factories build the stated binary operation", "[set-query]") {
    auto check = [](sql::query_ast query, sql::set_operator operation,
                    sql::set_quantifier quantifier) {
        const auto& compound = std::get<sql::compound_ast>(query.body);
        REQUIRE(compound.operation == operation);
        REQUIRE(compound.quantifier == quantifier);
        REQUIRE(compound.left);
        REQUIRE(compound.right);
    };
    check(sql::union_(select_from(), select_from()).build(), sql::set_operator::union_,
          sql::set_quantifier::distinct);
    check(sql::union_all(select_from(), select_from()).build(), sql::set_operator::union_,
          sql::set_quantifier::all);
    check(sql::intersect_(select_from(), select_from()).build(), sql::set_operator::intersect_,
          sql::set_quantifier::distinct);
    check(sql::intersect_all(select_from(), select_from()).build(), sql::set_operator::intersect_,
          sql::set_quantifier::all);
    check(sql::except_(select_from(), select_from()).build(), sql::set_operator::except_,
          sql::set_quantifier::distinct);
    check(sql::except_all(select_from(), select_from()).build(), sql::set_operator::except_,
          sql::set_quantifier::all);
}

TEST_CASE("Set query: branch and compound tails remain at their own boundaries", "[set-query]") {
    auto branch = sql::query(sql::select({"id"}).from("left_rows").build())
                      .order_by("id")
                      .limit(2)
                      .offset(1)
                      .build();
    auto query = sql::union_(std::move(branch), select_from("right_rows", 20))
                     .order_by("id", false)
                     .limit(3)
                     .offset(1)
                     .build();
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE(compiled);
    REQUIRE(compiled->sql ==
            "SELECT * FROM (SELECT \"id\" FROM \"left_rows\" ORDER BY \"id\" ASC LIMIT 2 OFFSET 1) "
            "AS \"asterorm_set_0\" UNION SELECT * FROM (SELECT \"id\" FROM \"right_rows\" WHERE "
            "\"id\" > $1) AS \"asterorm_set_1\" ORDER BY \"id\" DESC LIMIT 3 OFFSET 1");
    std::get<sql::compound_ast>(query.body).limit_val.reset();
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    if (target == sql::dialect::sqlite) {
        REQUIRE(compiled->sql.ends_with("LIMIT -1 OFFSET 1"));
    }
    if (target == sql::dialect::mysql) {
        REQUIRE(compiled->sql.ends_with("LIMIT 18446744073709551615 OFFSET 1"));
    }
    if (target == sql::dialect::postgres) {
        REQUIRE(compiled->sql.ends_with("ORDER BY \"id\" DESC OFFSET 1"));
    }
}

TEST_CASE("Set query: bindings traverse CTE compounds then the full main tree", "[set-query]") {
    auto definition =
        sql::union_all(select_from("left_rows", 1), select_from("right_rows", 2)).build();
    auto query =
        sql::union_(
            select_from("eligible", 3),
            sql::except_(select_from("middle_rows", 4), select_from("last_rows", 5)).build())
            .with_cte("eligible", std::move(definition), {"id"})
            .build();
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    auto compiled =
        sql::compiler{target, {sql::mysql_query_profile::set_operations_8_0_31}}.compile(query);
    REQUIRE(compiled);
    REQUIRE(compiled->params == asterorm::query_parameters{"1", "2", "3", "4", "5"});
    if (target != sql::dialect::mysql) {
        REQUIRE(compiled->sql.find("$1") < compiled->sql.find("$2"));
        REQUIRE(compiled->sql.find("$2") < compiled->sql.find("$3"));
        REQUIRE(compiled->sql.find("$3") < compiled->sql.find("$4"));
        REQUIRE(compiled->sql.find("$4") < compiled->sql.find("$5"));
    }
    REQUIRE(compiled->result_columns == std::vector<std::string>{"id"});
}

TEST_CASE("Set query: profiles reject unsupported operations throughout nested trees",
          "[set-query]") {
    const auto operation = GENERATE(sql::set_operator::intersect_, sql::set_operator::except_);
    const auto quantifier = GENERATE(sql::set_quantifier::distinct, sql::set_quantifier::all);
    auto query = sql::union_(select_from(), combine(operation, quantifier)).build();
    REQUIRE_FALSE(sql::compiler{sql::dialect::mysql}.compile(query));
    REQUIRE(sql::compiler{sql::dialect::mysql, {sql::mysql_query_profile::set_operations_8_0_31}}
                .compile(query));
    auto sqlite = sql::compiler{sql::dialect::sqlite}.compile(query);
    REQUIRE(sqlite.has_value() == (quantifier == sql::set_quantifier::distinct));
    REQUIRE(sql::compiler{sql::dialect::postgres}.compile(query));
}

TEST_CASE("Set query: malformed trees and tails reject locally with paths", "[set-query]") {
    auto query = sql::union_(select_from(), select_from("right_rows")).build();
    auto& compound = std::get<sql::compound_ast>(query.body);
    SECTION("null left") {
        compound.left.reset();
    }
    SECTION("null right") {
        compound.right.reset();
    }
    SECTION("arity") {
        std::get<sql::select_ast>(compound.right->body).columns.emplace_back("extra");
    }
    SECTION("implicit arm") {
        std::get<sql::select_ast>(compound.right->body).columns.clear();
    }
    SECTION("invalid operator") {
        compound.operation = static_cast<sql::set_operator>(99);
    } // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
    SECTION("invalid quantifier") {
        compound.quantifier = static_cast<sql::set_quantifier>(99);
    } // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
    SECTION("qualified ordering") {
        compound.order_bys = {{.column = "left_rows.id"}};
    }
    SECTION("right label ordering") {
        std::get<sql::select_ast>(compound.right->body).columns = {"other"};
        compound.order_bys = {{.column = "other"}};
    }
    SECTION("ordering ordinal") {
        compound.order_bys = {{.column = "1"}};
    }
    SECTION("case-only ordering") {
        compound.order_bys = {{.column = "ID"}};
    }
    SECTION("nested WITH") {
        compound.right->ctes.push_back({
            .name = "nested",
            .columns = {},
            .body = std::make_unique<sql::query_ast>(select_from()),
        });
    }
    SECTION("recursive arm") {
        compound.right->recursive = true;
    }
    SECTION("bad right graph") {
        std::get<sql::select_ast>(compound.right->body).where_clause = sql::predicate_ast{
            sql::binary_predicate{.op = sql::op_kind::and_, .left = nullptr, .right = nullptr},
        };
    }
    reject(query);
    sql::compiler compiler;
    REQUIRE_FALSE(compiler.compile(query));
    auto fresh =
        compiler.compile(sql::union_(select_from(), select_from("right_rows", 20)).build());
    REQUIRE(fresh);
    REQUIRE(fresh->params == asterorm::query_parameters{"10", "20"});
}

TEST_CASE("Set query: nested arm failures identify their path", "[set-query]") {
    auto query =
        sql::union_(select_from(), sql::except_(select_from(), select_from()).build()).build();
    auto& nested = std::get<sql::compound_ast>(std::get<sql::compound_ast>(query.body).right->body);
    std::get<sql::select_ast>(nested.right->body).columns = {"*"};
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().detail == "root.right.right");
}

TEST_CASE("Set query: depth and statement-wide occurrence budgets include operands",
          "[set-query]") {
    SECTION("depth") {
        auto query = sql::query(sql::select({"id"}).from("left_rows").build()).build();
        for (int index = 0; index < 31; ++index) {
            query =
                sql::union_all(std::move(query),
                               sql::query(sql::select({"id"}).from("right_rows").build()).build())
                    .build();
        }
        REQUIRE(sql::compiler{}.compile(query));
        query = sql::union_all(std::move(query), select_from()).build();
        reject(query);
    }
    SECTION("nodes") {
        auto left = sql::query(sql::select({"id"}).from("left_rows").build()).build();
        auto right = sql::query(sql::select({"id"}).from("right_rows").build()).build();
        // Envelope+compound (2), and two envelope/leaf/column/relation arms (8).
        std::get<sql::select_ast>(left.body).group_bys.assign(2000, "id");
        std::get<sql::select_ast>(right.body)
            .group_bys.assign(sql::max_query_nodes - 10 - 2000, "id");
        auto query = sql::union_(std::move(left), std::move(right)).build();
        REQUIRE(sql::compiler{}.compile(query));
        std::get<sql::compound_ast>(query.body).order_bys.push_back({.column = "id"});
        reject(query);
    }
}
