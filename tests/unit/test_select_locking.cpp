#include <catch2/catch_test_macros.hpp>
#include <string>
#include <tuple>

#include "asterorm/sql/compiler.hpp"
#include "asterorm/sql/typed.hpp"

struct locking_row {
    int id{};
    bool ready{};
};

template <> struct asterorm::entity_traits<locking_row> {
    static constexpr const char* table = "locking_rows";
    static constexpr auto columns = std::make_tuple(asterorm::column<&locking_row::id>("id"),
                                                    asterorm::column<&locking_row::ready>("ready"));
};

TEST_CASE("SELECT locking: typed and string SQL and parameters match", "[select-locking]") {
    namespace sql = asterorm::sql;
    namespace typed = sql::typed;
    for (auto target : {sql::dialect::postgres, sql::dialect::mysql}) {
        for (bool share : {false, true}) {
            for (auto wait : {
                     sql::select_lock_wait::block,
                     sql::select_lock_wait::nowait,
                     sql::select_lock_wait::skip_locked,
                 }) {
                CAPTURE(target, share, wait);
                auto string_builder = sql::select({"id", "ready"});
                string_builder.from("locking_rows")
                    .where(sql::col("ready") == sql::val(true))
                    .order_by("id")
                    .limit(2)
                    .offset(1);
                auto typed_builder = typed::select_cols(typed::col<&locking_row::id>,
                                                        typed::col<&locking_row::ready>);
                typed_builder.where(typed::col<&locking_row::ready> == typed::val(true))
                    .order_by(typed::col<&locking_row::id>)
                    .limit(2)
                    .offset(1);
                if (share) {
                    string_builder.for_share();
                    typed_builder.for_share();
                } else {
                    string_builder.for_update();
                    typed_builder.for_update();
                }
                if (wait == sql::select_lock_wait::nowait) {
                    string_builder.nowait();
                    typed_builder.nowait();
                } else if (wait == sql::select_lock_wait::skip_locked) {
                    string_builder.skip_locked();
                    typed_builder.skip_locked();
                }
                auto string_ast = string_builder.build();
                auto typed_ast = typed_builder.build();
                sql::compiler compiler{target};
                auto string_query = compiler.compile(string_ast);
                auto typed_query = compiler.compile(typed_ast);
                REQUIRE(string_query.has_value());
                REQUIRE(typed_query.has_value());
                REQUIRE(string_query->sql == typed_query->sql);
                REQUIRE(string_query->params == typed_query->params);
                REQUIRE(string_query->params ==
                        asterorm::query_parameters{asterorm::encode_parameter(true)});
                std::string suffix = share ? " FOR SHARE" : " FOR UPDATE";
                if (wait == sql::select_lock_wait::nowait) {
                    suffix += " NOWAIT";
                } else if (wait == sql::select_lock_wait::skip_locked) {
                    suffix += " SKIP LOCKED";
                }
                REQUIRE(string_query->sql.ends_with(" LIMIT 2 OFFSET 1" + suffix));
                string_ast.locking = {};
                auto unlocked = compiler.compile(string_ast);
                REQUIRE(unlocked.has_value());
                REQUIRE(string_query->sql == unlocked->sql + suffix);
                REQUIRE(string_query->params == unlocked->params);
            }
        }
    }
}

TEST_CASE("SELECT locking: last strength and wait policy win independently", "[select-locking]") {
    namespace sql = asterorm::sql;
    namespace typed = sql::typed;
    auto string_ast = sql::select({"id"})
                          .from("locking_rows")
                          .nowait()
                          .for_update()
                          .skip_locked()
                          .for_share()
                          .build();
    auto typed_ast = typed::select_cols(typed::col<&locking_row::id>)
                         .nowait()
                         .for_update()
                         .skip_locked()
                         .for_share()
                         .build();
    auto string_query = sql::compiler{}.compile(string_ast);
    auto typed_query = sql::compiler{}.compile(typed_ast);
    REQUIRE(string_query.has_value());
    REQUIRE(typed_query.has_value());
    REQUIRE(string_query->sql == typed_query->sql);
    REQUIRE(string_query->sql.ends_with(" FOR SHARE SKIP LOCKED"));
}

TEST_CASE("SELECT locking: reject unsupported dialects and invalid shapes", "[select-locking]") {
    namespace sql = asterorm::sql;
    for (auto target : {sql::dialect::sqlite, sql::dialect::clickhouse}) {
        auto ast = sql::select({"id"}).from("locking_rows").build();
        auto baseline = sql::compiler{}.compile(ast);
        auto compatible = sql::compiler{target}.compile(ast);
        REQUIRE(baseline.has_value());
        REQUIRE(compatible.has_value());
        REQUIRE(compatible->sql == baseline->sql);
        REQUIRE(compatible->params == baseline->params);
        ast.locking.strength = sql::select_lock_strength::update;
        auto rejected = sql::compiler{target}.compile(ast);
        REQUIRE_FALSE(rejected.has_value());
        REQUIRE(rejected.error().kind == asterorm::db_error_kind::parse_failed);
    }
    SECTION("modifier without strength") {
        auto ast = sql::select({"id"}).from("locking_rows").nowait().build();
        REQUIRE_FALSE(sql::compiler{}.compile(ast).has_value());
        auto typed_ast = sql::typed::select<locking_row>().skip_locked().build();
        REQUIRE_FALSE(sql::compiler{}.compile(typed_ast).has_value());
    }
    SECTION("invalid enum values") {
        auto ast = sql::select({"id"}).from("locking_rows").for_update().build();
        // Deliberately malformed public AST values must be rejected.
        // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
        ast.locking.strength = static_cast<sql::select_lock_strength>(255);
        REQUIRE_FALSE(sql::compiler{}.compile(ast).has_value());
        ast.locking.strength = sql::select_lock_strength::share;
        // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
        ast.locking.wait = static_cast<sql::select_lock_wait>(255);
        REQUIRE_FALSE(sql::compiler{}.compile(ast).has_value());
    }
    SECTION("grouping, having, joins and functions") {
        auto ast = sql::select({"id"}).from("locking_rows").for_update().build();
        for (auto target : {sql::dialect::postgres, sql::dialect::mysql}) {
            ast.group_bys = {"id"};
            REQUIRE_FALSE(sql::compiler{target}.compile(ast).has_value());
            ast.group_bys.clear();
            ast.having_clause = sql::col("id") == sql::val(1);
            REQUIRE_FALSE(sql::compiler{target}.compile(ast).has_value());
            ast.having_clause.reset();
            ast.joins.push_back({.table = "other"});
            REQUIRE_FALSE(sql::compiler{target}.compile(ast).has_value());
            ast.joins.clear();
            ast.items.push_back({.fn = sql::count_all()});
            REQUIRE_FALSE(sql::compiler{target}.compile(ast).has_value());
            ast.items.clear();
        }
    }
}

TEST_CASE("SELECT locking: wildcard and column aliases remain supported", "[select-locking]") {
    namespace sql = asterorm::sql;
    auto ast = sql::select({"*"}).from("locking_rows").for_share().build();
    auto wildcard = sql::compiler{}.compile(ast);
    REQUIRE(wildcard.has_value());
    REQUIRE(wildcard->sql == "SELECT * FROM \"locking_rows\" FOR SHARE");
    ast.items.push_back({.column_name = "id", .alias = "job"});
    auto aliased = sql::compiler{}.compile(ast);
    REQUIRE(aliased.has_value());
    REQUIRE(aliased->sql == "SELECT \"id\" AS \"job\" FROM \"locking_rows\" FOR SHARE");
}
