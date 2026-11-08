// Intentionally first: the advanced execution header is self-contained.
#include "asterorm/sql/query.hpp"
#include "asterorm/sql/typed_extensions.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/sql/typed_window.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <stdexcept>

#include "../../docs/evidence/advanced-query-acceptance/composition.hpp"
#include "asterorm/session.hpp"

struct cte_unit_row {
    int id{};
    std::optional<std::string> note;
};
template <> struct asterorm::entity_traits<cte_unit_row> {
    // Deliberately no table or primary key; reversed order tests named mapping.
    static constexpr auto columns = std::make_tuple(asterorm::column<&cte_unit_row::note>("note"),
                                                    asterorm::column<&cte_unit_row::id>("id"));
};
struct cte_missing_row {
    int missing{};
};
template <> struct asterorm::entity_traits<cte_missing_row> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&cte_missing_row::missing>("missing"));
};
struct cte_duplicate_row {
    int id{};
};
template <> struct asterorm::entity_traits<cte_duplicate_row> {
    static constexpr auto columns = std::make_tuple(asterorm::column<&cte_duplicate_row::id>("id"),
                                                    asterorm::column<&cte_duplicate_row::id>("id"));
};

namespace {
namespace sql = asterorm::sql;
using asterorm::result;

sql::query_ast leaf(std::string table = "source") {
    return sql::query(sql::select({"id", "note"}).from(std::move(table)).build()).build();
}
sql::query_ast bound_query() {
    auto source =
        sql::select({"id", "note"}).from("source").where(sql::col("id") > sql::val(10)).build();
    auto consumer =
        sql::select({"id", "note"}).from("eligible").where(sql::col("id") < sql::val(100)).build();
    return sql::query(std::move(consumer))
        .with_cte("eligible", sql::query(std::move(source)).build(), {"id", "note"})
        .build();
}
void rejected(const sql::query_ast& query) {
    auto compiled = sql::compiler{}.compile(query);
    REQUIRE_FALSE(compiled);
    REQUIRE(compiled.error().kind == asterorm::db_error_kind::parse_failed);
}
struct buffered_result {
    std::vector<std::string> labels{"id", "note"};
    std::vector<std::vector<std::optional<std::string>>> values{{"7", std::nullopt}};
    [[nodiscard]] int rows() const {
        return static_cast<int>(values.size());
    }
    [[nodiscard]] int columns() const {
        return static_cast<int>(labels.size());
    }
    [[nodiscard]] std::string_view column_name(int index) const {
        return labels.at(index);
    }
    [[nodiscard]] std::optional<std::string> get_string(int row_index, int column_index) const {
        return values.at(row_index).at(column_index);
    }
};
struct execution_state {
    int connections{};
    int calls{};
    buffered_result output;
    std::optional<asterorm::db_error> failure;
    std::string statement;
    asterorm::query_parameters params;
};
struct query_connection {
    static constexpr auto sql_dialect = sql::dialect::postgres;
    execution_state* state;
    bool open{true};
    [[nodiscard]] bool is_open() const {
        return open;
    }
    void close() {
        open = false;
    }
    [[nodiscard]] result<buffered_result>
    execute([[maybe_unused]] std::string_view statement) const {
        return state->output;
    }
    [[nodiscard]] result<buffered_result>
    execute_prepared(std::string_view statement, const asterorm::query_parameters& params) const {
        ++state->calls;
        state->statement = statement;
        state->params = params;
        if (state->failure) {
            return std::unexpected(*state->failure);
        }
        return state->output;
    }
};
struct query_driver {
    execution_state* state;
    [[nodiscard]] result<query_connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        ++state->connections;
        return query_connection{.state = state};
    }
};
template <sql::dialect Target> struct unsupported_connection : query_connection {
    static constexpr auto sql_dialect = Target;
};
template <sql::dialect Target> struct unsupported_session {
    using connection_type = unsupported_connection<Target>;
    int acquisitions{};
    int events{};
    connection_type connection{{nullptr}};
    template <typename Callback> auto with_connection(Callback&& callback) {
        ++acquisitions;
        return callback(connection);
    }
    result<buffered_result>
    observed_execute([[maybe_unused]] connection_type& connection_ref,
                     [[maybe_unused]] const std::string& statement,
                     [[maybe_unused]] const asterorm::query_parameters& params) {
        ++events;
        return buffered_result{};
    }
};
struct unknown_session {};
struct no_observer_session {
    using connection_type = query_connection;
};
struct wrong_observer_session {
    using connection_type = query_connection;
    void observed_execute([[maybe_unused]] query_connection& connection,
                          [[maybe_unused]] const std::string& statement,
                          [[maybe_unused]] const asterorm::query_parameters& params) {}
};
} // namespace

TEST_CASE("CTE: owned envelope preserves legacy boundaries", "[cte]") {
    STATIC_REQUIRE_FALSE(std::is_copy_constructible_v<sql::query_ast>);
    STATIC_REQUIRE(std::is_move_constructible_v<sql::query_ast>);
    STATIC_REQUIRE_FALSE(std::is_convertible_v<sql::query_ast, sql::select_ast>);
    auto query = bound_query();
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    auto compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    const auto* const expected =
        target == sql::dialect::mysql
            ? "WITH `eligible`(`id`, `note`) AS (SELECT `id`, `note` FROM `source` WHERE `id` > ?) "
              "SELECT `id`, `note` FROM `eligible` WHERE `id` < ?"
            : "WITH \"eligible\"(\"id\", \"note\") AS (SELECT \"id\", \"note\" FROM \"source\" "
              "WHERE \"id\" > $1) SELECT \"id\", \"note\" FROM \"eligible\" WHERE \"id\" < $2";
    REQUIRE(compiled->sql == expected);
    REQUIRE(compiled->params == asterorm::query_parameters{"10", "100"});
    REQUIRE(compiled->result_columns == std::vector<std::string>{"id", "note"});
}

TEST_CASE("CTE: all leaves and shared occurrences use one lexical binding sequence", "[cte]") {
    auto shared = sql::func("coalesce", {sql::val(10), sql::val(20)});
    sql::select_ast first;
    first.table = "source";
    first.items = {{.column_name = std::nullopt, .fn = shared, .alias = "first"},
                   {.column_name = std::nullopt, .fn = shared, .alias = "second"}};
    first.joins.push_back({.kind = sql::join_kind::inner,
                           .table = "other",
                           .alias = std::nullopt,
                           .on_clause = sql::col("id") == sql::val(30)});
    first.where_clause = sql::col("id") > sql::val(40);
    first.group_bys = {"id"};
    first.having_clause = sql::count(sql::col("id")) > sql::val(50);
    auto second =
        sql::select({"first"}).from("first_cte").where(sql::col("first") > sql::val(60)).build();
    auto main =
        sql::select({"first"}).from("second_cte").where(sql::col("first") < sql::val(70)).build();
    auto query = sql::query(std::move(main))
                     .with_cte("first_cte", sql::query(std::move(first)).build())
                     .with_cte("second_cte", sql::query(std::move(second)).build())
                     .build();
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    auto compiled = sql::compiler{target}.compile(query);
    REQUIRE(compiled);
    REQUIRE(compiled->params ==
            asterorm::query_parameters{"10", "20", "10", "20", "30", "40", "50", "60", "70"});
    if (target == sql::dialect::mysql) {
        REQUIRE(std::count(compiled->sql.begin(), compiled->sql.end(), '?') == 9);
    } else {
        for (int index = 1; index <= 9; ++index) {
            REQUIRE(compiled->sql.find("$" + std::to_string(index)) != std::string::npos);
        }
        REQUIRE(compiled->sql.find("$10") == std::string::npos);
    }
}

TEST_CASE("CTE: unsupported envelopes and malformed definitions reject", "[cte]") {
    auto query = leaf();
    SECTION("recursive") {
        query.recursive = true;
    }
    SECTION("compound") {
        query.body = sql::compound_ast{};
    }
    SECTION("null body") {
        query.ctes.push_back({.name = "eligible", .columns = {}, .body = nullptr});
    }
    SECTION("nested WITH") {
        auto nested = sql::query(sql::select({"id"}).from("eligible").build())
                          .with_cte("eligible", leaf())
                          .build();
        query.ctes.push_back({.name = "outer_cte",
                              .columns = {},
                              .body = std::make_unique<sql::query_ast>(std::move(nested))});
    }
    SECTION("self") {
        query = sql::query(sql::select({"id"}).from("eligible").build())
                    .with_cte("eligible", leaf("eligible"))
                    .build();
    }
    SECTION("forward") {
        query = sql::query(sql::select({"id"}).from("later").build())
                    .with_cte("early", leaf("later"))
                    .with_cte("later", leaf())
                    .build();
    }
    SECTION("case-only reference") {
        query = sql::query(sql::select({"id"}).from("Eligible").build())
                    .with_cte("eligible", leaf())
                    .build();
    }
    SECTION("duplicate CTE") {
        query = sql::query(sql::select({"id"}).from("source").build())
                    .with_cte("eligible", leaf())
                    .with_cte("Eligible", leaf())
                    .build();
    }
    SECTION("bad CTE name") {
        query.ctes.push_back(
            {.name = "bad.name", .columns = {}, .body = std::make_unique<sql::query_ast>(leaf())});
    }
    SECTION("column arity") {
        query.ctes.push_back({.name = "eligible",
                              .columns = {"one"},
                              .body = std::make_unique<sql::query_ast>(leaf())});
    }
    SECTION("duplicate CTE columns") {
        query.ctes.push_back({.name = "eligible",
                              .columns = {"one", "One"},
                              .body = std::make_unique<sql::query_ast>(leaf())});
    }
    rejected(query);
    // Failure never poisons a reused compiler's bindings or graph state.
    sql::compiler compiler;
    REQUIRE_FALSE(compiler.compile(query));
    auto fresh = compiler.compile(bound_query());
    REQUIRE(fresh);
    REQUIRE(fresh->params == asterorm::query_parameters{"10", "100"});
}

TEST_CASE("CTE: projection and predicate structure is checked", "[cte]") {
    auto query = leaf();
    auto& selected = std::get<sql::select_ast>(query.body);
    SECTION("implicit projection") {
        selected.columns.clear();
    }
    SECTION("wildcard") {
        selected.columns = {"*"};
    }
    SECTION("qualified wildcard") {
        selected.columns = {"source.*"};
    }
    SECTION("duplicate output") {
        selected.columns = {"id", "other.ID"};
    }
    SECTION("invalid output") {
        selected.columns = {"bad-name"};
    }
    SECTION("long output") {
        selected.columns = {std::string(64, 'a')};
    }
    SECTION("mixed projections") {
        selected.items = {{.column_name = "id", .fn = nullptr, .alias = std::nullopt}};
    }
    SECTION("no source") {
        selected.columns.clear();
        selected.items = {{}};
    }
    SECTION("two sources") {
        selected.columns.clear();
        selected.items = {{.column_name = "id", .fn = sql::count_all(), .alias = "count"}};
    }
    SECTION("unaliased function") {
        selected.columns.clear();
        selected.items = {
            {.column_name = std::nullopt, .fn = sql::count_all(), .alias = std::nullopt}};
    }
    SECTION("conflicting alias") {
        selected.columns.clear();
        selected.items = {
            {.column_name = std::nullopt, .fn = sql::as(sql::count_all(), "one"), .alias = "two"}};
    }
    SECTION("nested alias") {
        selected.where_clause = sql::as(sql::count_all(), "bad") > sql::val(1);
    }
    SECTION("raw fragment") {
        auto function = sql::count_all();
        function->raw_arg = "id + 1";
        selected.where_clause = function > sql::val(1);
    }
    SECTION("null function") {
        selected.where_clause =
            sql::expr_ast{std::shared_ptr<sql::func_expr>{}} == sql::expr_ast{sql::val(1)};
    }
    SECTION("null predicate") {
        selected.where_clause = sql::predicate_ast{
            sql::binary_predicate{.op = sql::op_kind::and_, .left = nullptr, .right = nullptr}};
    }
    SECTION("invalid comparison") {
        selected.where_clause = sql::predicate_ast{
            sql::comparison_predicate{
                .op = sql::op_kind::and_, .left = sql::col("id"), .right = sql::val(1)},
        };
    }
    SECTION("invalid unary") {
        selected.where_clause = sql::predicate_ast{
            sql::unary_predicate{.op = static_cast<sql::unary_op_kind>(99),
                                 .operand = sql::col("id")},
        };
    }
    SECTION("invalid join") {
        selected.joins.push_back({.kind = static_cast<sql::join_kind>(99),
                                  .table = "other",
                                  .alias = {},
                                  .on_clause = {}});
    }
    SECTION("missing ON") {
        selected.joins.push_back(
            {.kind = sql::join_kind::inner, .table = "other", .alias = {}, .on_clause = {}});
    }
    SECTION("cross ON") {
        selected.joins.push_back({.kind = sql::join_kind::cross,
                                  .table = "other",
                                  .alias = {},
                                  .on_clause = sql::col("id") == sql::val(1)});
    }
    SECTION("locking") {
        selected.locking.strength = sql::select_lock_strength::update;
    }
    SECTION("NUL identifier") {
        selected.table = std::string("bad\0table", 9);
    }
    SECTION("NUL parameter") {
        selected.where_clause = sql::col("id") == sql::val(std::string("bad\0value", 9));
    }
    rejected(query);
}

TEST_CASE("CTE: query traversal budgets have inclusive boundaries", "[cte]") {
    SECTION("nodes") {
        auto query = sql::query(sql::select({"id"}).from("source").build()).build();
        auto& selected = std::get<sql::select_ast>(query.body);
        selected.group_bys.assign(sql::max_query_nodes - 4, "id");
        REQUIRE(sql::compiler{}.compile(query));
        selected.group_bys.emplace_back("id");
        rejected(query);
    }
    SECTION("depth") {
        auto nested = sql::func("abs", {sql::val(1)});
        for (std::size_t index = 1; index < sql::max_query_depth - 2; ++index) {
            nested = sql::func("abs", {nested});
        }
        auto query = leaf();
        auto& selected = std::get<sql::select_ast>(query.body);
        selected.columns.clear();
        selected.items = {{.column_name = std::nullopt, .fn = nested, .alias = "value"}};
        REQUIRE(sql::compiler{}.compile(query));
        selected.items[0].fn = sql::func("abs", {nested});
        rejected(query);
    }
    SECTION("CTEs") {
        auto query = leaf();
        for (std::size_t index = 0; index < sql::max_query_ctes; ++index) {
            query.ctes.push_back({.name = "cte_" + std::to_string(index),
                                  .columns = {},
                                  .body = std::make_unique<sql::query_ast>(leaf())});
        }
        REQUIRE(sql::compiler{}.compile(query));
        query.ctes.push_back(
            {.name = "extra", .columns = {}, .body = std::make_unique<sql::query_ast>(leaf())});
        rejected(query);
    }
    SECTION("active cycle") {
        auto function = sql::func("abs");
        function->args.emplace_back(function);
        auto query = leaf();
        std::get<sql::select_ast>(query.body).where_clause = function > sql::val(1);
        auto compiled = sql::compiler{}.compile(query);
        function->args.clear(); // Break the intentional shared_ptr cycle.
        REQUIRE_FALSE(compiled);
        REQUIRE(compiled.error().message.find("cycle") != std::string::npos);
    }
}

TEST_CASE("CTE: profiles, encoded errors, NULL and qualified base tables", "[cte]") {
    auto query = bound_query();
    REQUIRE_FALSE(sql::compiler{sql::dialect::clickhouse}.compile(query));
    REQUIRE_FALSE(sql::compiler{static_cast<sql::dialect>(99)}.compile(query));
    REQUIRE_FALSE(
        sql::compiler{sql::dialect::postgres, {static_cast<sql::mysql_query_profile>(99)}}.compile(
            query));
    REQUIRE(sql::compiler{sql::dialect::mysql, {sql::mysql_query_profile::set_operations_8_0_31}}
                .compile(query));
    auto& selected = std::get<sql::select_ast>(query.body);
    selected.table = "public.eligible";
    REQUIRE(sql::compiler{}.compile(query));
    selected.where_clause = sql::col("id") == sql::val(std::optional<int>{});
    auto null_query = sql::compiler{}.compile(query);
    REQUIRE(null_query);
    REQUIRE_FALSE(null_query->params.back().value);
    asterorm::db_error original;
    original.kind = asterorm::db_error_kind::constraint_violation;
    original.sqlstate = "22000";
    original.message = "encoding identity";
    original.detail = "retained";
    asterorm::query_parameter parameter;
    parameter.error = original;
    selected.where_clause =
        sql::expr_ast{sql::col("id")} == sql::expr_ast{sql::param_expr{parameter}};
    auto failed = sql::compiler{}.compile(query);
    REQUIRE_FALSE(failed);
    REQUIRE(failed.error().kind == original.kind);
    REQUIRE(failed.error().sqlstate == original.sqlstate);
    REQUIRE(failed.error().detail == original.detail);
}

TEST_CASE("SELECT: function projection aliases normalize and SQLite offsets work", "[cte][sql]") {
    auto selected = sql::select({"id"}).from("source").offset(2).build();
    REQUIRE(sql::compiler{sql::dialect::sqlite}.compile(selected)->sql ==
            "SELECT \"id\" FROM \"source\" LIMIT -1 OFFSET 2");
    REQUIRE(sql::compiler{sql::dialect::postgres}.compile(selected)->sql ==
            "SELECT \"id\" FROM \"source\" OFFSET 2");
    REQUIRE(sql::compiler{sql::dialect::mysql}.compile(selected)->sql ==
            "SELECT `id` FROM `source` LIMIT 18446744073709551615 OFFSET 2");
    selected.columns.clear();
    const int alias_mode = GENERATE(0, 1, 2);
    auto function = sql::count_all();
    if (alias_mode != 0) {
        function->alias = "total";
    }
    selected.items = {
        {
            .column_name = std::nullopt,
            .fn = function,
            .alias = alias_mode != 1 ? std::optional<std::string>{"total"} : std::nullopt,
        },
    };
    REQUIRE(sql::compiler{}.compile(selected)->sql ==
            "SELECT count(*) AS \"total\" FROM \"source\" OFFSET 2");
    selected.items[0].alias = "different";
    function->alias = "total";
    REQUIRE_FALSE(sql::compiler{}.compile(selected));
}

TEST_CASE("CTE execution: compilation and mapping reject before lease or observation",
          "[cte][observer]") {
    execution_state state;
    asterorm::pool_config config;
    config.max_size = 1;
    config.min_size = 0;
    asterorm::connection_pool<query_driver> pool{query_driver{&state}, config};
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const auto&) { ++events; });
    auto query = bound_query();
    REQUIRE_FALSE(sql::query_as<cte_missing_row>(db, query));
    REQUIRE_FALSE(sql::query_as<cte_duplicate_row>(db, query));
    query.recursive = true;
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    REQUIRE(state.connections == 0);
    REQUIRE(state.calls == 0);
    REQUIRE(events == 0);
    REQUIRE(pool.stats().total == 0);
    unsupported_session<sql::dialect::clickhouse> clickhouse;
    unsupported_session<static_cast<sql::dialect>(99)> invalid;
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(clickhouse, query));
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(invalid, query));
    REQUIRE(clickhouse.acquisitions == 0);
    REQUIRE(clickhouse.events == 0);
    REQUIRE(invalid.acquisitions == 0);
    REQUIRE(invalid.events == 0);
    query.recursive = false;
    auto& selected = std::get<sql::select_ast>(query.body);
    selected.where_clause = sql::col("id") == sql::val(std::string("bad\0value", 9));
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    REQUIRE(state.connections == 0);
    REQUIRE(events == 0);
    unknown_session unknown;
    no_observer_session no_observer;
    wrong_observer_session wrong_observer;
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(unknown, query));
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(no_observer, query));
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(wrong_observer, query));
}

TEST_CASE("CTE execution: named codecs, metadata, events and native errors", "[cte][observer]") {
    execution_state state;
    asterorm::pool_config config;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{&state}, config};
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        REQUIRE(event.param_count == 2);
        REQUIRE(event.sql == state.statement);
        REQUIRE(pool.stats().in_use == 1);
        REQUIRE((event.error != nullptr) == state.failure.has_value());
    });
    auto query = bound_query();
    SECTION("named nullable result") {
        auto rows = sql::query_as<cte_unit_row>(db, query);
        REQUIRE(rows);
        REQUIRE(rows->size() == 1);
        REQUIRE(rows->front().id == 7);
        REQUIRE_FALSE(rows->front().note);
        REQUIRE(state.params == asterorm::query_parameters{"10", "100"});
    }
    SECTION("empty result") {
        state.output.values.clear();
        auto rows = sql::query_as<cte_unit_row>(db, query);
        REQUIRE(rows);
        REQUIRE(rows->empty());
    }
    SECTION("bad value") {
        state.output.values[0][0] = "bad";
        REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    }
    SECTION("nonoptional NULL") {
        state.output.values[0][0].reset();
        REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    }
    SECTION("missing label on empty result") {
        state.output.values.clear();
        state.output.labels = {"id"};
        REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    }
    SECTION("duplicate label on empty result") {
        state.output.values.clear();
        state.output.labels = {"id", "id"};
        REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    }
    SECTION("native diagnostic") {
        asterorm::db_error native;
        native.kind = asterorm::db_error_kind::query_failed;
        native.sqlstate = "42P01";
        native.message = "native identity";
        native.table = "source";
        state.failure = native;
        auto rows = sql::query_as<cte_unit_row>(db, query);
        REQUIRE_FALSE(rows);
        REQUIRE(rows.error().sqlstate == native.sqlstate);
        REQUIRE(rows.error().message == native.message);
        REQUIRE(rows.error().table == native.table);
    }
    REQUIRE(state.calls == 1);
    REQUIRE(events == 1);
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("CTE execution: observer exceptions propagate once and release the lease",
          "[cte][observer]") {
    execution_state state;
    asterorm::pool_config config;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{&state}, config};
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const auto&) {
        ++events;
        throw std::runtime_error("observer identity");
    });
    REQUIRE_THROWS_WITH(sql::query_as<cte_unit_row>(db, bound_query()), "observer identity");
    REQUIRE(events == 1);
    REQUIRE(state.calls == 1);
    REQUIRE(pool.stats().in_use == 0);
    db.set_observer({});
    REQUIRE(sql::query_as<cte_unit_row>(db, bound_query()));
}

TEST_CASE("CTE: definition and main occurrences share the node budget", "[cte]") {
    auto query =
        sql::query(sql::select({"id"}).from("eligible").build())
            .with_cte("eligible", sql::query(sql::select({"id"}).from("source").build()).build())
            .build();
    auto& definition = std::get<sql::select_ast>(query.ctes[0].body->body);
    auto& main = std::get<sql::select_ast>(query.body);
    definition.group_bys.assign(2000, "id");
    main.group_bys.assign(sql::max_query_nodes - 8 - 2000, "id");
    REQUIRE(sql::compiler{}.compile(query));
    main.group_bys.emplace_back("id");
    rejected(query);
}

TEST_CASE("Set execution: invalid shapes reject before leasing and valid trees observe once",
          "[set-query][observer]") {
    execution_state state;
    asterorm::pool_config config;
    config.max_size = 1;
    config.min_size = 0;
    asterorm::connection_pool<query_driver> pool{query_driver{&state}, config};
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        REQUIRE(event.param_count == 4);
    });
    auto query = sql::union_(bound_query(), leaf()).build(); // An operand WITH is never hoisted.
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    REQUIRE(state.connections == 0);
    REQUIRE(events == 0);
    auto right = leaf();
    std::get<sql::select_ast>(right.body).columns.pop_back();
    query = sql::union_(leaf(), std::move(right)).build();
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    REQUIRE(state.connections == 0);
    REQUIRE(events == 0);
    unsupported_session<sql::dialect::mysql> mysql;
    query = sql::intersect_(leaf(), leaf()).build();
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(mysql, query));
    REQUIRE(mysql.acquisitions == 0);
    REQUIRE(mysql.events == 0);
    auto first =
        sql::select({"id", "note"}).from("eligible").where(sql::col("id") > sql::val(3)).build();
    query =
        sql::union_all(sql::query(std::move(first)).build(), leaf())
            .with_cte("eligible", sql::union_(sql::query(sql::select({"id", "note"})
                                                             .from("source")
                                                             .where(sql::col("id") > sql::val(1))
                                                             .build())
                                                  .build(),
                                              sql::query(sql::select({"id", "note"})
                                                             .from("other")
                                                             .where(sql::col("id") > sql::val(2))
                                                             .build())
                                                  .build())
                                      .build())
            .build();
    std::get<sql::select_ast>(std::get<sql::compound_ast>(query.body).right->body).where_clause =
        sql::col("id") < sql::val(4);
    auto rows = sql::query_as<cte_unit_row>(db, query);
    REQUIRE(rows);
    REQUIRE(rows->size() == 1);
    REQUIRE(rows->front().id == 7);
    REQUIRE(state.params == asterorm::query_parameters{"1", "2", "3", "4"});
    REQUIRE(events == 1);
    REQUIRE(state.calls == 1);
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE(
    "Recursive execution: invalid definitions reject before leasing and valid roots observe once",
    "[recursive-query][observer]") {
    auto anchor =
        sql::query(
            sql::select({"id", "note"}).from("roots").where(sql::col("id") == sql::val(1)).build())
            .build();
    auto step =
        sql::query(
            sql::select({"id", "note"}).from("walk").where(sql::col("id") < sql::val(5)).build())
            .build();
    auto query =
        sql::query(
            sql::select({"id", "note"}).from("walk").where(sql::col("id") > sql::val(0)).build())
            .with_cte("walk", sql::union_all(std::move(anchor), std::move(step)).build(),
                      {"id", "note"})
            .recursive()
            .build();
    execution_state state;
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{&state}, config};
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        REQUIRE(event.param_count == 3);
    });
    query.ctes[0].columns.clear();
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    REQUIRE(state.connections == 0);
    REQUIRE(state.calls == 0);
    REQUIRE(events == 0);
    query.ctes[0].columns = {"id", "note"};
    auto rows = sql::query_as<cte_unit_row>(db, query);
    REQUIRE(rows);
    REQUIRE(rows->size() == 1);
    REQUIRE(rows->front().id == 7);
    REQUIRE(state.params == asterorm::query_parameters{"1", "5", "0"});
    REQUIRE(state.calls == 1);
    REQUIRE(events == 1);
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Window execution: invalid specs reject before lease and typed windows observe once",
          "[window][observer]") {
    execution_state state;
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{&state}, config};
    asterorm::session db{pool};
    auto function = sql::over(sql::row_number(), {});
    sql::select_ast leaf;
    leaf.table = "source";
    leaf.items = {{.column_name = {}, .fn = function, .alias = "id"},
                  {.column_name = "note", .fn = {}, .alias = {}}};
    auto query = sql::query(std::move(leaf)).build();
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(db, query));
    REQUIRE(state.connections == 0);
    REQUIRE(state.calls == 0);
    function->over->order_bys = {{.column = "id"}};
    int events{};
    db.set_observer([&](const auto&) { ++events; });
    auto rows = sql::query_as<cte_unit_row>(db, query);
    REQUIRE(rows);
    REQUIRE(events == 1);
    namespace typed = sql::typed;
    auto window = typed::window().order_by(typed::col<&cte_unit_row::id>);
    sql::select_ast ast;
    ast.table = "source";
    ast.items = {typed::over(typed::row_number(), window).to_select_item()};
    using item = decltype(typed::over(typed::row_number(), window));
    typed::typed_select_builder<cte_unit_row, std::tuple<std::int64_t>, item> rebuilt{
        std::move(ast)};
    auto typed_rows = typed::query(db, rebuilt);
    REQUIRE(typed_rows);
    REQUIRE(std::get<0>(typed_rows->front()) == 7);
    REQUIRE(events == 2);
    REQUIRE(state.calls == 2);
    unsupported_session<sql::dialect::clickhouse> unsupported;
    // Use the actual window AST so the dialect guard also covers custom selectables.
    sql::select_ast guarded;
    guarded.table = "source";
    guarded.items = {{.column_name = {}, .fn = function, .alias = "id"}};
    typed::typed_select_builder<cte_unit_row, std::tuple<std::int64_t>, item> guarded_builder{
        std::move(guarded)};
    REQUIRE_FALSE(typed::query(unsupported, guarded_builder));
    REQUIRE(unsupported.acquisitions == 0);
    REQUIRE(unsupported.events == 0);
}

TEST_CASE("Extensions execution: backend rejection precedes lease and valid FILTER observes once",
          "[aggregate-extension][observer]") {
    sql::select_ast leaf;
    leaf.table = "source";
    leaf.items = {
        {
            .column_name = {},
            .fn = sql::filter(sql::count_all(), sql::col("id") > sql::val(0)),
            .alias = "id",
        },
        {
            .column_name = {},
            .fn = sql::max_(sql::col("note")),
            .alias = "note",
        },
    };
    auto query = sql::query(std::move(leaf)).build();
    unsupported_session<sql::dialect::mysql> mysql;
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(mysql, query));
    REQUIRE(mysql.acquisitions == 0);
    REQUIRE(mysql.events == 0);
    unsupported_session<sql::dialect::sqlite> sqlite;
    auto distinct = sql::query(sql::select({"id", "note"})
                                   .from("source")
                                   .distinct_on({"id"}, sql::distinct_on_ties::allow_peers)
                                   .order_by("id")
                                   .build())
                        .build();
    REQUIRE_FALSE(sql::query_as<cte_unit_row>(sqlite, distinct));
    REQUIRE(sqlite.acquisitions == 0);
    REQUIRE(sqlite.events == 0);
    execution_state state;
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{&state}, config};
    asterorm::session db{pool};
    int events{};
    db.set_observer([&](const auto&) { ++events; });
    auto rows = sql::query_as<cte_unit_row>(db, query);
    REQUIRE(rows);
    REQUIRE(rows->front().id == 7);
    REQUIRE(events == 1);
    REQUIRE(state.calls == 1);
    REQUIRE(state.params == asterorm::query_parameters{"0"});
}

TEST_CASE("Advanced query: typed and runtime composition share SQL, labels and lexical bindings",
          "[advanced-query]") {
    const auto target = GENERATE(sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql);
    const bool filtered = target != sql::dialect::mysql;
    const bool distinct = target == sql::dialect::postgres;
    auto runtime = asterorm::acceptance::composed_query(filtered, distinct);
    auto typed = asterorm::acceptance::composed_query(filtered, distinct, true);
    sql::compiler renderer{target};
    auto compiled = renderer.compile(runtime);
    auto typed_compiled = renderer.compile(typed);
    REQUIRE(compiled);
    REQUIRE(typed_compiled);
    REQUIRE(compiled->sql == typed_compiled->sql);
    REQUIRE(compiled->params == typed_compiled->params);
    REQUIRE(compiled->result_columns ==
            std::vector<std::string>{"id", "parent", "number", "total", "previous"});
    asterorm::query_parameters expected{"1", "9", "0", "0"};
    if (filtered) {
        expected.emplace_back("10");
    }
    expected.insert(expected.end(), {"-1", "5", "98"});
    REQUIRE(compiled->params == expected);
    REQUIRE(compiled->sql.starts_with("WITH RECURSIVE "));
    REQUIRE(compiled->sql.find("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") !=
            std::string::npos);
    auto invalid = asterorm::acceptance::composed_query(true, true);
    std::get<sql::select_ast>(invalid.ctes[2].body->body).items[3].fn->args.clear();
    REQUIRE_FALSE(renderer.compile(invalid));
    // Compiler reuse must reset both numbering and statement-wide affinity casts.
    auto plain = renderer.compile(
        sql::select({"id"}).from("plain").where(sql::col("id") == sql::val(2)).build());
    REQUIRE(plain);
    REQUIRE(plain->params == asterorm::query_parameters{"2"});
    REQUIRE(plain->sql.find("CAST(") == std::string::npos);
    REQUIRE(renderer.compile(runtime)->sql == compiled->sql);
}

TEST_CASE("Advanced query: invalid later definitions and branches reject before leasing",
          "[advanced-query]") {
    auto query = asterorm::acceptance::composed_query();
    SECTION("later window has malformed FILTER") {
        auto& ranked = std::get<sql::select_ast>(query.ctes[2].body->body);
        ranked.items[3].fn->filter =
            std::make_shared<const sql::predicate_ast>(sql::sum(sql::col("value")) > sql::val(0));
    }
    SECTION("forward reference hidden in a CTE set branch") {
        auto& seeds = std::get<sql::compound_ast>(query.ctes[0].body->body);
        std::get<sql::select_ast>(seeds.right->body).table = "ranked";
    }
    SECTION("recursive step gains a window") {
        auto& walk = std::get<sql::compound_ast>(query.ctes[1].body->body);
        auto& step = std::get<sql::select_ast>(walk.right->body);
        step.where_clause = sql::over(sql::count_all(), {}) > sql::val(0);
    }
    SECTION("unselected compound operand has wrong arity") {
        auto& compound = std::get<sql::compound_ast>(query.body);
        std::get<sql::select_ast>(compound.right->body).columns.pop_back();
    }
    SECTION("global budget spans multiple CTEs") {
        auto& seeds = std::get<sql::compound_ast>(query.ctes[0].body->body);
        auto& first = std::get<sql::select_ast>(seeds.left->body);
        first.group_bys.assign(2100, "id");
        REQUIRE(sql::compiler{}.compile(query));
        auto& ranked = std::get<sql::select_ast>(query.ctes[2].body->body);
        ranked.group_bys.assign(2100, "id");
    }
    unsupported_session<sql::dialect::postgres> db;
    auto rejected = sql::query_as<asterorm::acceptance::composed_row>(db, query);
    REQUIRE_FALSE(rejected);
    REQUIRE(rejected.error().kind == asterorm::db_error_kind::parse_failed);
    REQUIRE(db.acquisitions == 0);
    REQUIRE(db.events == 0);
}
