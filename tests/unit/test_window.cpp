#include "asterorm/sql/query.hpp"
#include "asterorm/sql/typed_window.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <limits>

struct window_unit_event {
    int id{};
    int group{};
    std::optional<int> value;
    bool active{};
};
struct window_other_event {
    int id{};
};
template <> struct asterorm::entity_traits<window_unit_event> {
    static constexpr const char* table = "events";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&window_unit_event::id>("id"),
                        asterorm::column<&window_unit_event::group>("grp"),
                        asterorm::column<&window_unit_event::value>("value"),
                        asterorm::column<&window_unit_event::active>("active"));
};
template <> struct asterorm::entity_traits<window_other_event> {
    static constexpr const char* table = "other";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&window_other_event::id>("id"));
};
namespace {
namespace sql = asterorm::sql;
namespace typed = sql::typed;
sql::over_clause ordered() {
    return {.partition_bys = {"grp"}, .order_bys = {{.column = "id"}}, .rows = {}};
}
sql::query_ast selected(std::shared_ptr<sql::func_expr> function) {
    sql::select_ast leaf;
    leaf.table = "events";
    leaf.items = {{.column_name = {}, .fn = std::move(function), .alias = "answer"}};
    return sql::query(std::move(leaf)).build();
}
void reject(const sql::query_ast& query) {
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().kind == asterorm::db_error_kind::parse_failed);
    // Legacy/typed SELECT compilation shares the same window preflight.
    REQUIRE_FALSE(sql::compiler{}.compile(std::get<sql::select_ast>(query.body)));
}
} // namespace

TEST_CASE("Window: golden SQL and lexical bindings are shared by legacy and owned SELECTs",
          "[window]") {
    auto function = sql::lag(sql::func("coalesce", {sql::val(10), sql::val(20)}), 2,
                             sql::expr_ast{sql::val(30)});
    auto clause = ordered();
    clause.rows = sql::rows_between(sql::unbounded_preceding(), sql::current_row());
    auto query = selected(sql::over(function, clause));
    auto& leaf = std::get<sql::select_ast>(query.body);
    leaf.joins.push_back({
        .kind = sql::join_kind::inner,
        .table = "other",
        .alias = {},
        .on_clause = sql::col("events.id") == sql::val(40),
    });
    leaf.where_clause = sql::col("events.id") > sql::val(50);
    leaf.having_clause = sql::col("events.id") < sql::val(60);
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    auto compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    std::string expected = "SELECT lag(coalesce($1, $2), 2, $3) OVER (PARTITION BY \"grp\" ORDER "
                           "BY \"id\" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "
                           "\"answer\" FROM \"events\" INNER JOIN \"other\" ON \"events\".\"id\" = "
                           "$4 WHERE \"events\".\"id\" > $5 HAVING \"events\".\"id\" < $6";
    if (target != sql::dialect::postgres) {
        for (int index = 1; index <= 6; ++index) {
            auto token = "$" + std::to_string(index);
            auto offset = expected.find(token);
            expected.replace(offset, token.size(),
                             "CAST(" + token +
                                 (target == sql::dialect::mysql ? " AS SIGNED)" : " AS INTEGER)"));
        }
    }
    if (target == sql::dialect::mysql) {
        std::ranges::replace(expected, '"', '`');
        for (int index = 1; index <= 6; ++index) {
            expected.replace(expected.find("$" + std::to_string(index)), 2, "?");
        }
    }
    REQUIRE(compiled->sql == expected);
    REQUIRE(compiled->params == asterorm::query_parameters{"10", "20", "30", "40", "50", "60"});
    auto legacy = sql::compiler{target}.compile(leaf);
    REQUIRE(legacy);
    REQUIRE(legacy->sql == compiled->sql);
    REQUIRE(legacy->params == compiled->params);
    REQUIRE_FALSE(function->over); // Decoration does not mutate a reusable function.
}

TEST_CASE("Window: all helpers and aggregates render through one OVER surface", "[window]") {
    auto clause = ordered();
    clause.rows = sql::rows_between(sql::unbounded_preceding(), sql::unbounded_following());
    std::vector<std::shared_ptr<sql::func_expr>> functions{
        sql::row_number(),
        sql::rank(),
        sql::dense_rank(),
        sql::lag(sql::col("value")),
        sql::lead(sql::col("value")),
        sql::first_value(sql::col("value")),
        sql::last_value(sql::col("value")),
        sql::nth_value(sql::col("value"), 2),
        sql::count_all(),
        sql::count(sql::col("value")),
        sql::sum(sql::col("value")),
        sql::avg(sql::col("value")),
        sql::min_(sql::col("value")),
        sql::max_(sql::col("value")),
    };
    for (const auto& function : functions) {
        auto compiled = sql::compiler{}.compile(selected(sql::over(function, clause)));
        REQUIRE(compiled);
        REQUIRE(compiled->sql.find(" OVER (") != std::string::npos);
    }
}

TEST_CASE("Window: malformed clauses, positions and contexts reject before rendering", "[window]") {
    auto clause = ordered();
    auto function = sql::over(sql::row_number(), clause);
    SECTION("missing order") {
        function->over->order_bys.clear();
    }
    SECTION("missing OVER") {
        function->over.reset();
    }
    SECTION("unknown function") {
        function->name = "user_window";
    }
    SECTION("bad arity") {
        function->args = {sql::val(1)};
    }
    SECTION("distinct") {
        function->distinct = true;
    }
    SECTION("raw") {
        function->raw_arg = "*";
    }
    SECTION("wildcard key") {
        function->over->partition_bys = {"*"};
    }
    SECTION("NUL key") {
        function->over->order_bys[0].column = std::string("bad\0key", 7);
    }
    SECTION("negative offset") {
        function = sql::over(sql::lag(sql::col("value"), -1), clause);
    }
    SECTION("zero nth") {
        function = sql::over(sql::nth_value(sql::col("value"), 0), clause);
        function->over->rows = sql::rows_between(sql::current_row(), sql::unbounded_following());
    }
    SECTION("missing frame") {
        function = sql::over(sql::first_value(sql::col("value")), clause);
    }
    SECTION("unexpected position") {
        function->window_position = 1;
    }
    SECTION("negative frame") {
        function->over->rows = sql::rows_between(sql::preceding(-1), sql::current_row());
    }
    SECTION("reversed frame") {
        function->over->rows = sql::rows_between(sql::following(2), sql::following(1));
    }
    SECTION("start unbounded following") {
        function->over->rows =
            sql::rows_between(sql::unbounded_following(), sql::unbounded_following());
    }
    SECTION("end unbounded preceding") {
        function->over->rows =
            sql::rows_between(sql::unbounded_preceding(), sql::unbounded_preceding());
    }
    SECTION("zero category reversal") {
        function->over->rows = sql::rows_between(sql::following(0), sql::current_row());
    }
    SECTION("unused bound offset") {
        function->over->rows = sql::rows_between(
            sql::frame_bound{.kind = sql::frame_bound_kind::current_row, .offset = 1},
            sql::following(1));
    }
    SECTION("FILTER on ranking") {
        function->filter =
            std::make_shared<const sql::predicate_ast>(sql::is_null(sql::col("value")));
    }
    SECTION("nested window") {
        function = sql::over(sql::sum(sql::over(sql::row_number(), clause)), clause);
    }
    auto query = selected(function);
    SECTION("WHERE window") {
        std::get<sql::select_ast>(query.body).where_clause = function > sql::val(1);
    }
    reject(query);
}

TEST_CASE("Window: bounds and keys participate in graph budgets and unsupported dialects reject",
          "[window]") {
    auto function = sql::over(sql::count_all(), {});
    auto query = selected(function);
    // root + leaf + function + OVER + relation = 5.
    function->over->partition_bys.assign(sql::max_query_nodes - 5, "id");
    REQUIRE(sql::compiler{}.compile(query));
    function->over->partition_bys.emplace_back("id");
    REQUIRE_FALSE(sql::compiler{}.compile(query));
    function->over->partition_bys.assign(sql::max_query_nodes, "id");
    REQUIRE_FALSE(sql::compiler{}.compile(std::get<sql::select_ast>(query.body)));
    function->over->partition_bys.clear();
    REQUIRE_FALSE(
        sql::compiler{sql::dialect::clickhouse}.compile(std::get<sql::select_ast>(query.body)));
    auto first = sql::compiler{}.compile(query);
    REQUIRE(first);
    REQUIRE(first->params.empty());
}

TEST_CASE("Window: typed values retain specs, aliases and conservative result types",
          "[window][typed]") {
    auto spec = typed::window()
                    .partition_by(typed::col<&window_unit_event::group>)
                    .order_by(typed::col<&window_unit_event::id>);
    auto full = spec.rows_between(sql::unbounded_preceding(), sql::unbounded_following());
    auto number = typed::over(typed::row_number(), spec).as("number");
    auto previous =
        typed::over(typed::lag(typed::col<&window_unit_event::value>, 2, typed::val(-1)), spec)
            .as("previous");
    auto last =
        typed::over(typed::last_value(typed::col<&window_unit_event::value>), full).as("last");
    auto sum = typed::over(typed::sum(typed::col<&window_unit_event::value>), full).as("total");
    STATIC_REQUIRE(std::same_as<typename decltype(number)::projected_type, std::int64_t>);
    STATIC_REQUIRE(std::same_as<typename decltype(previous)::projected_type, std::optional<int>>);
    STATIC_REQUIRE(std::same_as<typename decltype(last)::projected_type, std::optional<int>>);
    STATIC_REQUIRE(
        std::same_as<typename decltype(sum)::projected_type, std::optional<std::int64_t>>);
    auto selection =
        typed::select_cols(typed::col<&window_unit_event::id>, number, previous, last, sum);
    using row = std::tuple<int, std::int64_t, std::optional<int>, std::optional<int>,
                           std::optional<std::int64_t>>;
    STATIC_REQUIRE(std::same_as<typename decltype(selection)::result_type, row>);
    auto compiled = sql::compiler{}.compile(sql::query(selection.build()).build());
    REQUIRE(compiled);
    REQUIRE(compiled->params == asterorm::query_parameters{"-1"});
    REQUIRE(compiled->sql.find("lag(\"value\", 2, $1) OVER (PARTITION BY \"grp\" ORDER BY \"id\" "
                               "ASC) AS \"previous\"") != std::string::npos);
    REQUIRE(compiled->sql.find("UNBOUNDED FOLLOWING) AS \"last\"") != std::string::npos);
}

TEST_CASE(
    "Window: numeric hints cast only window statements and compiler reuse restores legacy SQL",
    "[window]") {
    auto query = selected(
        sql::over(sql::lag(sql::col("value"), 1, sql::expr_ast{sql::val(1.5)}), ordered()));
    sql::compiler compiler{sql::dialect::mysql};
    auto compiled = compiler.compile(query);
    REQUIRE(compiled);
    REQUIRE(compiled->sql.find("CAST(? AS DOUBLE)") != std::string::npos);
    auto ordinary = sql::select({"id"}).from("events").where(sql::col("id") > sql::val(1)).build();
    REQUIRE(compiler.compile(ordinary)->sql == "SELECT `id` FROM `events` WHERE `id` > ?");
    auto& function = std::get<sql::select_ast>(query.body).items[0].fn;
    function->args[1] = sql::val(std::numeric_limits<std::uint64_t>::max());
    REQUIRE_FALSE(sql::compiler{sql::dialect::sqlite}.compile(query));
    REQUIRE(compiler.compile(query)->sql.find("CAST(? AS UNSIGNED)") != std::string::npos);
    function->args[1] = sql::val(std::optional<int>{});
    auto null_value = sql::compiler{sql::dialect::sqlite}.compile(query);
    REQUIRE(null_value);
    REQUIRE_FALSE(null_value->params[0].value);
    function->args[1] =
        sql::param_expr{asterorm::encode_parameter(1),
                        static_cast<sql::parameter_kind>(
                            99)}; // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
    REQUIRE_FALSE(compiler.compile(query));
}
