#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>

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
    auto compiled = compiler.compile(query);

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
    auto compiled = compiler.compile(query);

    REQUIRE(compiled.sql ==
            R"(SELECT "id", "email", "name", "active", "deleted_at" FROM "typed_users")"
            R"( WHERE "active" = $1 ORDER BY "id" ASC LIMIT 10)");
    REQUIRE(compiled.params.size() == 1);
    REQUIRE(compiled.params[0] == std::optional<std::string>{"t"});
}

TEST_CASE("Typed DSL: && / || combine same-entity predicates", "[typed][where][logical]") {
    using namespace asterorm::sql::typed;

    auto query = select<typed_user>()
                     .where((col<&typed_user::active> == val(true)) &&
                            (col<&typed_user::email> != val(std::string{"banned@x"})))
                     .build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query);

    REQUIRE(compiled.sql.find(R"(WHERE ("active" = $1 AND "email" != $2))") != std::string::npos);
    REQUIRE(compiled.params.size() == 2);
}

TEST_CASE("Typed DSL: is_null / is_not_null compile to IS NULL / IS NOT NULL",
          "[typed][where][null]") {
    using namespace asterorm::sql::typed;

    auto null_query = select<typed_user>().where(is_null(col<&typed_user::deleted_at>)).build();
    auto not_null_query =
        select<typed_user>().where(is_not_null(col<&typed_user::deleted_at>)).build();

    asterorm::sql::compiler compiler;
    auto null_sql = compiler.compile(null_query).sql;
    auto not_null_sql = compiler.compile(not_null_query).sql;

    REQUIRE(null_sql.find(R"(WHERE "deleted_at" IS NULL)") != std::string::npos);
    REQUIRE(not_null_sql.find(R"(WHERE "deleted_at" IS NOT NULL)") != std::string::npos);

    auto null_compiled = compiler.compile(null_query);
    REQUIRE(null_compiled.params.empty()); // IS NULL takes no parameter
}

TEST_CASE("Typed DSL: select_cols<DTO> validates field-type sequence", "[typed][projection]") {
    using namespace asterorm::sql::typed;

    auto query = select_cols<user_summary>(col<&typed_user::email>, col<&typed_user::name>)
                     .where(col<&typed_user::active> == val(true))
                     .build();

    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query);

    REQUIRE(compiled.sql == R"(SELECT "email", "name" FROM "typed_users" WHERE "active" = $1)");
}

TEST_CASE("Typed DSL: typed_col vs typed_col compares two columns of same entity",
          "[typed][where][col-vs-col]") {
    using namespace asterorm::sql::typed;

    auto query =
        select<typed_user>().where(col<&typed_user::email> != col<&typed_user::name>).build();
    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query);

    REQUIRE(compiled.sql.find(R"(WHERE "email" != "name")") != std::string::npos);
    REQUIRE(compiled.params.empty());
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
    auto compiled = compiler.compile(query);

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

TEST_CASE("Typed DSL: mixed aggregate projection compiles to expected SQL",
          "[typed][aggregate][projection]") {
    using namespace asterorm::sql::typed;

    auto query = select_cols<order_stats>(count_all(), sum(col<&order_row::qty>),
                                          sum(col<&order_row::price>), min(col<&order_row::qty>),
                                          max(col<&order_row::qty>), avg(col<&order_row::price>))
                     .build();

    asterorm::sql::compiler compiler;
    auto compiled = compiler.compile(query);

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
    auto compiled = compiler.compile(query);

    REQUIRE(compiled.sql.find(R"(GROUP BY "id")") != std::string::npos);
    REQUIRE(compiled.sql.find(R"(HAVING "qty" > $1)") != std::string::npos);
    REQUIRE(compiled.params.size() == 1);
}
