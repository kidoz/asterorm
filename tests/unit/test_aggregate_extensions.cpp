#include "asterorm/repository.hpp"
#include "asterorm/sql/query.hpp"
#include "asterorm/sql/typed_extensions.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

struct extension_event {
    int id{};
    int group{};
    int value{};
    bool active{};
};
struct extension_other {
    int id{};
};
template <> struct asterorm::entity_traits<extension_event> {
    static constexpr const char* table = "events";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&extension_event::id>("id"),
                        asterorm::column<&extension_event::group>("grp"),
                        asterorm::column<&extension_event::value>("value"),
                        asterorm::column<&extension_event::active>("active"));
};
template <> struct asterorm::entity_traits<extension_other> {
    static constexpr const char* table = "other";
    static constexpr auto columns = std::make_tuple(asterorm::column<&extension_other::id>("id"));
};
namespace {
namespace sql = asterorm::sql;
namespace typed = sql::typed;
sql::query_ast selected(std::shared_ptr<sql::func_expr> function) {
    sql::select_ast leaf;
    leaf.table = "events";
    leaf.items = {{.column_name = {}, .fn = std::move(function), .alias = "answer"}};
    return sql::query(std::move(leaf)).build();
}
sql::select_ast latest() {
    return sql::select({"id", "grp", "value"})
        .from("events")
        .distinct_on({"grp"}, sql::distinct_on_ties::require_tiebreaker)
        .where(sql::col("id") > sql::val(0))
        .order_by("grp")
        .order_by("id", false)
        .build();
}
void rejected(const sql::query_ast& query) {
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().kind == asterorm::db_error_kind::parse_failed);
    REQUIRE_FALSE(sql::compiler{}.compile(std::get<sql::select_ast>(query.body)));
}
} // namespace

TEST_CASE("FILTER: arguments, predicate and OVER render/bind in lexical order",
          "[aggregate-extension]") {
    auto original = sql::sum(sql::func("coalesce", {sql::val(10), sql::val(20)}));
    auto filtered = sql::filter(original, sql::col("value") > sql::val(30));
    auto function = sql::over(
        filtered, {
                      .partition_bys = {"grp"},
                      .order_bys = {{.column = "id"}},
                      .rows = sql::rows_between(sql::unbounded_preceding(), sql::current_row()),
                  });
    auto query = selected(function);
    auto& leaf = std::get<sql::select_ast>(query.body);
    leaf.joins.push_back({
        .kind = sql::join_kind::inner,
        .table = "other",
        .alias = {},
        .on_clause = sql::col("events.id") == sql::val(40),
    });
    leaf.where_clause = sql::col("events.id") > sql::val(50);
    leaf.having_clause =
        sql::filter(sql::count_all(), sql::col("value") > sql::val(60)) > sql::val(70);
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite);
    auto compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    std::string expected =
        "SELECT sum(coalesce($1, $2)) FILTER (WHERE \"value\" > $3) OVER (PARTITION BY \"grp\" "
        "ORDER BY \"id\" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"answer\" FROM "
        "\"events\" INNER JOIN \"other\" ON \"events\".\"id\" = $4 WHERE \"events\".\"id\" > $5 "
        "HAVING count(*) FILTER (WHERE \"value\" > $6) > $7";
    if (target == sql::dialect::sqlite) {
        for (int index = 1; index <= 7; ++index) {
            auto token = "$" + std::to_string(index);
            expected.replace(expected.find(token), token.size(), "CAST(" + token + " AS INTEGER)");
        }
    }
    REQUIRE(compiled->sql == expected);
    REQUIRE(compiled->params ==
            asterorm::query_parameters{"10", "20", "30", "40", "50", "60", "70"});
    REQUIRE_FALSE(original->filter);
    REQUIRE(filtered->filter);
    REQUIRE_FALSE(filtered->over);
    REQUIRE(sql::compiler{target}.compile(leaf)->sql == compiled->sql);
}

TEST_CASE("FILTER: scalar aggregates, HAVING and typed nullability are preserved",
          "[aggregate-extension][typed]") {
    auto filtered = typed::filter(typed::sum(typed::col<&extension_event::value>),
                                  typed::col<&extension_event::active> == typed::val(true))
                        .as("total");
    auto counted =
        typed::filter(typed::count_all(), typed::col<&extension_event::value> > typed::val(10))
            .as("counted");
    STATIC_REQUIRE(
        std::same_as<typename decltype(filtered)::projected_type, std::optional<std::int64_t>>);
    STATIC_REQUIRE(std::same_as<typename decltype(counted)::projected_type, std::int64_t>);
    auto scalar = typed::select_cols(filtered, counted).build();
    auto compiled = sql::compiler{}.compile(sql::query(std::move(scalar)).build());
    REQUIRE(compiled);
    REQUIRE(compiled->params == asterorm::query_parameters{asterorm::encode_parameter(true),
                                                           asterorm::encode_parameter(10)});
    auto spec = typed::window()
                    .partition_by(typed::col<&extension_event::group>)
                    .order_by(typed::col<&extension_event::id>);
    auto projected = typed::over(filtered, spec).as("total");
    auto selection = typed::select_cols(typed::col<&extension_event::id>, projected).build();
    auto window_query = sql::query(std::move(selection)).build();
    REQUIRE(sql::compiler{}.compile(window_query));
}

TEST_CASE("FILTER: unsupported contexts and nested aggregate predicates reject",
          "[aggregate-extension]") {
    auto function = sql::filter(sql::sum(sql::col("value")), sql::col("active") == sql::val(true));
    auto query = selected(function);
    auto& leaf = std::get<sql::select_ast>(query.body);
    SECTION("WHERE") {
        leaf.where_clause = function > sql::val(0);
    }
    SECTION("JOIN") {
        leaf.joins.push_back({
            .kind = sql::join_kind::inner,
            .table = "other",
            .alias = {},
            .on_clause = function > sql::val(0),
        });
    }
    SECTION("unknown aggregate") {
        function->name = "custom_sum";
    }
    SECTION("ranking") {
        function->name = "rank";
    }
    SECTION("arity") {
        function->args.clear();
    }
    SECTION("raw fragment") {
        function->raw_arg = "value + 1";
    }
    SECTION("aggregate in filter") {
        function->filter =
            std::make_shared<const sql::predicate_ast>(sql::sum(sql::col("value")) > sql::val(0));
    }
    SECTION("nested filter") {
        function->filter = std::make_shared<const sql::predicate_ast>(
            sql::filter(sql::count_all(), sql::col("id") > sql::val(1)) > sql::val(0));
    }
    SECTION("window in filter") {
        function->filter = std::make_shared<const sql::predicate_ast>(
            sql::over(sql::row_number(),
                      {.partition_bys = {}, .order_bys = {{.column = "id"}}, .rows = {}}) >
            sql::val(0));
    }
    SECTION("NUL value") {
        function->filter = std::make_shared<const sql::predicate_ast>(
            sql::col("id") == sql::val(std::string("x\0y", 3)));
    }
    SECTION("null predicate child") {
        function->filter = std::make_shared<const sql::predicate_ast>(sql::predicate_ast{
            sql::binary_predicate{.op = sql::op_kind::and_, .left = nullptr, .right = nullptr},
        });
    }
    rejected(query);
}

TEST_CASE("FILTER: graph cycles, depth and occurrence budgets include predicates",
          "[aggregate-extension]") {
    SECTION("active cycle") {
        auto function = sql::count_all();
        function->filter = std::make_shared<const sql::predicate_ast>(function > sql::val(0));
        auto query = selected(function);
        auto compiled = sql::compiler{}.compile(query);
        function->filter.reset();
        REQUIRE_FALSE(compiled);
        REQUIRE(compiled.error().message.find("cycle") != std::string::npos);
    }
    SECTION("occurrences") {
        auto function = sql::filter(sql::count_all(), sql::col("value") > sql::val(1));
        auto query = selected(function);
        auto& leaf = std::get<sql::select_ast>(query.body);
        // root/leaf/function/relation + comparison/column/parameter = 7.
        leaf.group_bys.assign(sql::max_query_nodes - 7, "id");
        REQUIRE(sql::compiler{}.compile(query));
        leaf.group_bys.emplace_back("id");
        REQUIRE_FALSE(sql::compiler{}.compile(query));
    }
    SECTION("depth") {
        auto predicate = sql::col("id") > sql::val(1);
        for (int index = 0; index < 60; ++index) {
            predicate = std::move(predicate) && (sql::col("id") > sql::val(0));
        }
        auto function = sql::filter(sql::count_all(), std::move(predicate));
        auto query = selected(function);
        REQUIRE(sql::compiler{}.compile(query));
        auto too_deep = sql::col("id") > sql::val(1);
        for (int index = 0; index < 61; ++index) {
            too_deep = std::move(too_deep) && (sql::col("id") > sql::val(0));
        }
        function->filter = std::make_shared<const sql::predicate_ast>(std::move(too_deep));
        REQUIRE_FALSE(sql::compiler{}.compile(query));
    }
}

TEST_CASE("Extensions: DISTINCT ON requires ordered keys and explicit tie policy",
          "[aggregate-extension]") {
    auto leaf = latest();
    auto compiled = sql::compiler{}.compile(leaf);
    REQUIRE(compiled);
    REQUIRE(compiled->sql == "SELECT DISTINCT ON (\"grp\") \"id\", \"grp\", \"value\" FROM "
                             "\"events\" WHERE \"id\" > $1 ORDER BY \"grp\" ASC, \"id\" DESC");
    REQUIRE(compiled->params == asterorm::query_parameters{"0"});
    auto typed_selection = typed::select<extension_event>()
                               .distinct_on(sql::distinct_on_ties::require_tiebreaker,
                                            typed::col<&extension_event::group>)
                               .order_by(typed::col<&extension_event::group>)
                               .order_by(typed::col<&extension_event::id>, typed::desc)
                               .build();
    REQUIRE(sql::compiler{}.compile(typed_selection));
    SECTION("missing policy") {
        leaf.distinct_on->ties = sql::distinct_on_ties::unspecified;
    }
    SECTION("invalid policy") {
        leaf.distinct_on->ties = static_cast<sql::distinct_on_ties>(
            99); // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
    }
    SECTION("no keys") {
        leaf.distinct_on->columns.clear();
    }
    SECTION("duplicate keys") {
        leaf.distinct_on->columns.emplace_back("grp");
        leaf.order_bys.insert(leaf.order_bys.begin(), {.column = "grp"});
    }
    SECTION("prefix mismatch") {
        leaf.order_bys[0].column = "value";
    }
    SECTION("no order") {
        leaf.order_bys.clear();
    }
    SECTION("no extra key") {
        leaf.order_bys.resize(1);
    }
    SECTION("repeated extra key") {
        leaf.order_bys[1].column = "grp";
    }
    SECTION("wildcard key") {
        leaf.distinct_on->columns = {"*"};
    }
    SECTION("NUL key") {
        leaf.distinct_on->columns = {std::string("g\0p", 3)};
    }
    SECTION("locking") {
        leaf.locking.strength = sql::select_lock_strength::update;
    }
    rejected(sql::query(std::move(leaf)).build());
}

TEST_CASE("Extensions: explicit peer acceptance and unsupported backends never emulate",
          "[aggregate-extension]") {
    auto leaf = sql::select({"id"})
                    .from("events")
                    .distinct_on({"grp"}, sql::distinct_on_ties::allow_peers)
                    .order_by("grp")
                    .build();
    REQUIRE(sql::compiler{}.compile(leaf));
    REQUIRE_FALSE(sql::compiler{sql::dialect::sqlite}.compile(leaf));
    REQUIRE_FALSE(sql::compiler{sql::dialect::mysql}.compile(leaf));
    auto filtered = selected(sql::filter(sql::count_all(), sql::col("id") > sql::val(0)));
    REQUIRE_FALSE(sql::compiler{sql::dialect::mysql}.compile(filtered));
    REQUIRE_FALSE(sql::compiler{sql::dialect::clickhouse}.compile(filtered));
    sql::insert_select_ast insertion{
        .table = "archived",
        .columns = {"id"},
        .source = std::move(leaf),
    };
    REQUIRE_FALSE(sql::compiler{}.compile(insertion));
    auto stream = sql::select({"id", "grp", "value", "active"})
                      .from("events")
                      .distinct_on({"grp"}, sql::distinct_on_ties::allow_peers)
                      .order_by("grp")
                      .build();
    REQUIRE_FALSE(asterorm::detail::validate_stream_source<extension_event>(stream));
}

TEST_CASE("DISTINCT ON: key and ordering occurrences share the query budget",
          "[aggregate-extension]") {
    auto leaf = sql::select({"id"}).from("events").build();
    leaf.distinct_on =
        sql::distinct_on_clause{.columns = {}, .ties = sql::distinct_on_ties::allow_peers};
    for (std::size_t index = 0; index < (sql::max_query_nodes - 4) / 2; ++index) {
        auto key = "key_" + std::to_string(index);
        leaf.distinct_on->columns.push_back(key);
        leaf.order_bys.push_back({.column = key});
    }
    auto query = sql::query(std::move(leaf)).build();
    REQUIRE(sql::compiler{}.compile(query));
    auto& selected = std::get<sql::select_ast>(query.body);
    selected.distinct_on->columns.emplace_back("extra");
    selected.order_bys.push_back({.column = "extra"});
    REQUIRE_FALSE(sql::compiler{}.compile(query));
}
