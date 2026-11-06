#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>

#include "asterorm/core/entity_traits.hpp"
#include "asterorm/sql/compiler.hpp"
#include "asterorm/sql/typed.hpp"

namespace {

struct typed_user {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
    std::optional<std::string> deleted_at;
};

struct user_summary {
    std::string email;
    std::string name;
};

} // namespace

template <> struct asterorm::entity_traits<typed_user> {
    static constexpr const char* table = "typed_users";
    [[maybe_unused]] static constexpr auto primary_key = asterorm::pk<&typed_user::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&typed_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&typed_user::email>("email"), asterorm::column<&typed_user::name>("name"),
        asterorm::column<&typed_user::active>("active"),
        asterorm::column<&typed_user::deleted_at>("deleted_at"));
};

TEST_CASE("Typed DSL: column name lookup compiles to the trait name", "[typed][lookup]") {
    using namespace asterorm::sql::typed;

    STATIC_REQUIRE(typed_col<&typed_user::email>::column_name() == std::string_view{"email"});
    STATIC_REQUIRE(typed_col<&typed_user::deleted_at>::column_name() ==
                   std::string_view{"deleted_at"});
}

TEST_CASE("Typed DSL: select<Entity>() builds a full-column SELECT", "[typed][select]") {
    using namespace asterorm::sql::typed;

    auto query = select<typed_user>().build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql ==
            R"(SELECT "id", "email", "name", "active", "deleted_at" FROM "typed_users")");
    REQUIRE(compiled.params.empty());
}

TEST_CASE("Typed DSL: where with typed_col == typed_val emits = $1", "[typed][where]") {
    using namespace asterorm::sql::typed;

    auto query = select<typed_user>()
                     .where(col<&typed_user::active> == val(true))
                     .order_by(col<&typed_user::id>, asc)
                     .limit(10)
                     .build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql ==
            R"(SELECT "id", "email", "name", "active", "deleted_at" FROM "typed_users")"
            R"( WHERE "active" = $1 ORDER BY "id" ASC LIMIT 10)");
    REQUIRE(compiled.params.size() == 1);
    REQUIRE(compiled.params[0].value == std::optional<std::string>{"t"});
    REQUIRE(compiled.params[0].type == asterorm::parameter_type::boolean);
}

TEST_CASE("Typed DSL: && / || combine same-entity predicates", "[typed][where][logical]") {
    using namespace asterorm::sql::typed;

    auto query = select<typed_user>()
                     .where((col<&typed_user::active> == val(true)) &&
                            (col<&typed_user::email> != val(std::string{"banned@x"})))
                     .build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql.find(R"(WHERE ("active" = $1 AND "email" != $2))") != std::string::npos);
    REQUIRE(compiled.params.size() == 2);
}

TEST_CASE("Typed DSL: string literals remain text parameters", "[typed][where]") {
    using namespace asterorm::sql::typed;

    auto query =
        select<typed_user>().where(col<&typed_user::email> == val("alice'o@example.com")).build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql.contains(R"(WHERE "email" = $1)"));
    REQUIRE(compiled.params.size() == 1);
    REQUIRE(compiled.params[0].value == std::optional<std::string>{"alice'o@example.com"});
    REQUIRE(compiled.params[0].type == asterorm::parameter_type::text);
}

TEST_CASE("Typed DSL: is_null / is_not_null compile to IS NULL / IS NOT NULL",
          "[typed][where][null]") {
    using namespace asterorm::sql::typed;

    auto null_query = select<typed_user>().where(is_null(col<&typed_user::deleted_at>)).build();
    auto not_null_query =
        select<typed_user>().where(is_not_null(col<&typed_user::deleted_at>)).build();

    asterorm::sql::compiler compiler;
    auto null_sql = compiler.compile(null_query).value().sql;
    auto not_null_sql = compiler.compile(not_null_query).value().sql;

    REQUIRE(null_sql.find(R"(WHERE "deleted_at" IS NULL)") != std::string::npos);
    REQUIRE(not_null_sql.find(R"(WHERE "deleted_at" IS NOT NULL)") != std::string::npos);

    auto null_compiled = compiler.compile(null_query).value();
    REQUIRE(null_compiled.params.empty()); // IS NULL takes no parameter
}

TEST_CASE("Typed DSL: select_cols<DTO> validates field-type sequence", "[typed][projection]") {
    using namespace asterorm::sql::typed;

    auto query = select_cols<user_summary>(col<&typed_user::email>, col<&typed_user::name>)
                     .where(col<&typed_user::active> == val(true))
                     .build();

    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql == R"(SELECT "email", "name" FROM "typed_users" WHERE "active" = $1)");
}

TEST_CASE("Typed DSL: typed_col vs typed_col compares two columns of same entity",
          "[typed][where][col-vs-col]") {
    using namespace asterorm::sql::typed;

    auto query =
        select<typed_user>().where(col<&typed_user::email> != col<&typed_user::name>).build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql.find(R"(WHERE "email" != "name")") != std::string::npos);
    REQUIRE(compiled.params.empty());
}

TEST_CASE("Typed DSL: inferred projections preserve column types and order",
          "[typed][projection][inferred]") {
    using namespace asterorm::sql::typed;

    auto selection = select_cols(col<&typed_user::email>, col<&typed_user::active>,
                                 col<&typed_user::deleted_at>, col<&typed_user::id>);
    STATIC_REQUIRE(std::is_same_v<
                   typename decltype(selection)::result_type,
                   std::tuple<std::string, bool, std::optional<std::string>, std::optional<int>>>);
    STATIC_REQUIRE(std::is_same_v<typename decltype(selection)::entity_type, typed_user>);
    STATIC_REQUIRE(decltype(selection)::projection_size == 4);

    auto single_column = select_cols(col<&typed_user::email>);
    STATIC_REQUIRE(
        std::is_same_v<typename decltype(single_column)::result_type, std::tuple<std::string>>);
}

TEST_CASE("Typed DSL: inferred projections match explicit tuples and string SQL",
          "[typed][projection][inferred]") {
    namespace typed = asterorm::sql::typed;
    namespace sql = asterorm::sql;
    using row_type = std::tuple<std::string, bool, std::optional<std::string>>;

    auto inferred_ast =
        typed::select_cols(typed::col<&typed_user::email>, typed::col<&typed_user::active>,
                           typed::col<&typed_user::deleted_at>)
            .where((typed::col<&typed_user::active> == typed::val(true)) &&
                   (typed::col<&typed_user::email> != typed::val("excluded")))
            .order_by(typed::col<&typed_user::email>, typed::desc)
            .limit(5)
            .offset(2)
            .build();
    auto explicit_ast = typed::select_cols<row_type>(typed::col<&typed_user::email>,
                                                     typed::col<&typed_user::active>,
                                                     typed::col<&typed_user::deleted_at>)
                            .where((typed::col<&typed_user::active> == typed::val(true)) &&
                                   (typed::col<&typed_user::email> != typed::val("excluded")))
                            .order_by(typed::col<&typed_user::email>, typed::desc)
                            .limit(5)
                            .offset(2)
                            .build();
    auto string_ast = sql::select({"email", "active", "deleted_at"})
                          .from("typed_users")
                          .where((sql::col("active") == sql::val(true)) &&
                                 (sql::col("email") != sql::val("excluded")))
                          .order_by("email", false)
                          .limit(5)
                          .offset(2)
                          .build();

    for (auto target : {sql::dialect::postgres, sql::dialect::mysql}) {
        sql::compiler compiler{target};
        auto inferred_query = compiler.compile(inferred_ast).value();
        auto explicit_query = compiler.compile(explicit_ast).value();
        auto string_query = compiler.compile(string_ast).value();
        REQUIRE(inferred_query.sql == explicit_query.sql);
        REQUIRE(inferred_query.params == explicit_query.params);
        REQUIRE(inferred_query.sql == string_query.sql);
        REQUIRE(inferred_query.params == string_query.params);
    }
}

TEST_CASE("Typed DSL: explicit selectable DTOs remain unambiguous",
          "[typed][projection][inferred]") {
    using namespace asterorm::sql::typed;
    struct selectable_summary {
        std::string email;
        using projected_type = std::string;
        using entity_type = typed_user;
        static asterorm::sql::select_item to_select_item() {
            return typed_col<&typed_user::email>::to_select_item();
        }
    };

    auto explicit_selection = select_cols<selectable_summary>(selectable_summary{});
    STATIC_REQUIRE(
        std::is_same_v<typename decltype(explicit_selection)::result_type, selectable_summary>);
    auto inferred_selection = select_cols(selectable_summary{});
    STATIC_REQUIRE(std::is_same_v<typename decltype(inferred_selection)::result_type,
                                  std::tuple<std::string>>);
}

TEST_CASE("Typed DSL: custom column aliases and functions preserve their lowering",
          "[typed][projection][custom]") {
    namespace typed = asterorm::sql::typed;
    namespace sql = asterorm::sql;
    struct contact_column : typed::typed_col<&typed_user::email> {
        static sql::select_item to_select_item() {
            auto item = typed::typed_col<&typed_user::email>::to_select_item();
            item.alias = "contact";
            return item;
        }
    };
    struct email_length : typed::typed_col<&typed_user::email> {
        using projected_type = std::int64_t;
        static sql::select_item to_select_item() {
            return {.column_name = std::nullopt,
                    .fn = sql::as(sql::func("length", {sql::col("email")}), "email_length"),
                    .alias = std::nullopt};
        }
    };

    auto selection =
        typed::select_cols(contact_column{}, email_length{}, typed::col<&typed_user::active>);
    STATIC_REQUIRE(std::is_same_v<typename decltype(selection)::result_type,
                                  std::tuple<std::string, std::int64_t, bool>>);
    auto ast = selection.where(typed::col<&typed_user::active> == typed::val(true)).build();
    for (auto target : {sql::dialect::postgres, sql::dialect::mysql}) {
        sql::compiler compiler{target};
        auto compiled = compiler.compile(ast).value();
        if (target == sql::dialect::postgres) {
            REQUIRE(
                compiled.sql ==
                R"(SELECT "email" AS "contact", length("email") AS "email_length", "active" FROM "typed_users" WHERE "active" = $1)");
        } else {
            REQUIRE(compiled.sql ==
                    "SELECT `email` AS `contact`, length(`email`) AS `email_length`, "
                    "`active` FROM `typed_users` WHERE `active` = ?");
        }
        REQUIRE(compiled.params.size() == 1);
        REQUIRE(compiled.params[0].type == asterorm::parameter_type::boolean);
        REQUIRE(compiled.params[0].value == std::optional<std::string>{"t"});
    }
}

TEST_CASE("Typed DSL: mixed columns and aggregates retain projection order",
          "[typed][projection][aggregate]") {
    using namespace asterorm::sql::typed;
    auto selection =
        select_cols(col<&typed_user::email>, count(col<&typed_user::id>), col<&typed_user::active>);
    STATIC_REQUIRE(std::is_same_v<typename decltype(selection)::result_type,
                                  std::tuple<std::string, std::int64_t, bool>>);
    auto ast = selection.where(col<&typed_user::active> == val(true))
                   .group_by(col<&typed_user::email>, col<&typed_user::active>)
                   .build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(ast).value();
    REQUIRE(
        compiled.sql ==
        R"(SELECT "email", count("id"), "active" FROM "typed_users" WHERE "active" = $1 GROUP BY "email", "active")");
    REQUIRE(compiled.params.size() == 1);
    REQUIRE(compiled.params[0].type == asterorm::parameter_type::boolean);
}

namespace {
struct typed_user_stats {
    std::int64_t total;
    std::int64_t active_count;
};
} // namespace

TEST_CASE("Typed DSL: count_all and count(col) emit COUNT and have int64_t result",
          "[typed][aggregate][count]") {
    using namespace asterorm::sql::typed;

    STATIC_REQUIRE(std::is_same_v<count_all_t::projected_type, std::int64_t>);
    STATIC_REQUIRE(std::is_same_v<count_col_t<&typed_user::id>::projected_type, std::int64_t>);

    auto query =
        select_cols<typed_user_stats>(count_all(), count(col<&typed_user::active>)).build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql.find(R"(SELECT count(*), count("active") FROM "typed_users")") !=
            std::string::npos);
    REQUIRE(compiled.params.empty());
}

namespace {
struct order_row {
    int order_id;
    int qty;
    double price;
};

struct order_stats {
    std::int64_t row_count;
    std::int64_t qty_sum;
    double price_sum;
    int qty_min;
    int qty_max;
    double price_avg;
};
} // namespace

template <> struct asterorm::entity_traits<order_row> {
    static constexpr const char* table = "typed_orders";
    [[maybe_unused]] static constexpr auto primary_key = asterorm::pk<&order_row::order_id>("id");
    static constexpr auto columns = std::make_tuple(asterorm::column<&order_row::order_id>("id"),
                                                    asterorm::column<&order_row::qty>("qty"),
                                                    asterorm::column<&order_row::price>("price"));
};

TEST_CASE("Typed DSL: sum/min/max/avg derive correct projected_type", "[typed][aggregate][types]") {
    using namespace asterorm::sql::typed;

    STATIC_REQUIRE(std::is_same_v<sum_col_t<&order_row::qty>::projected_type, std::int64_t>);
    STATIC_REQUIRE(std::is_same_v<sum_col_t<&order_row::price>::projected_type, double>);
    STATIC_REQUIRE(std::is_same_v<min_col_t<&order_row::qty>::projected_type, int>);
    STATIC_REQUIRE(std::is_same_v<max_col_t<&order_row::price>::projected_type, double>);
    STATIC_REQUIRE(std::is_same_v<avg_col_t<&order_row::qty>::projected_type, double>);
}

TEST_CASE("Typed DSL: inferred aggregate tuples use existing projected types",
          "[typed][aggregate][inferred]") {
    using namespace asterorm::sql::typed;

    auto selection =
        select_cols(count_all(), count(col<&order_row::qty>), sum(col<&order_row::qty>),
                    sum(col<&order_row::price>), min(col<&order_row::qty>),
                    max(col<&order_row::price>), avg(col<&order_row::qty>));
    STATIC_REQUIRE(
        std::is_same_v<
            typename decltype(selection)::result_type,
            std::tuple<std::int64_t, std::int64_t, std::int64_t, double, int, double, double>>);
    STATIC_REQUIRE(std::is_same_v<typename decltype(selection)::entity_type, order_row>);
}

TEST_CASE("Typed DSL: mixed aggregate projection compiles to expected SQL",
          "[typed][aggregate][projection]") {
    using namespace asterorm::sql::typed;

    auto query = select_cols<order_stats>(count_all(), sum(col<&order_row::qty>),
                                          sum(col<&order_row::price>), min(col<&order_row::qty>),
                                          max(col<&order_row::qty>), avg(col<&order_row::price>))
                     .build();

    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(
        compiled.sql ==
        R"(SELECT count(*), sum("qty"), sum("price"), min("qty"), max("qty"), avg("price") FROM "typed_orders")");
}

TEST_CASE("Typed DSL: group_by and having emit GROUP BY / HAVING", "[typed][aggregate][group-by]") {
    using namespace asterorm::sql::typed;

    auto query = select_cols<order_stats>(count_all(), sum(col<&order_row::qty>),
                                          sum(col<&order_row::price>), min(col<&order_row::qty>),
                                          max(col<&order_row::qty>), avg(col<&order_row::price>))
                     .group_by(col<&order_row::order_id>)
                     .having(col<&order_row::qty> > val(0))
                     .build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query).value();

    REQUIRE(compiled.sql.find(R"(GROUP BY "id")") != std::string::npos);
    REQUIRE(compiled.sql.find(R"(HAVING "qty" > $1)") != std::string::npos);
    REQUIRE(compiled.params.size() == 1);
}
