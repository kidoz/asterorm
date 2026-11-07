#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <limits>
#include <stdexcept>

#include "asterorm/repository.hpp"

struct bulk_unit_row {
    int tenant{};
    int id{};
    int price{};
    bool active{};
    std::optional<std::string> note;
    int derived{};
    int defaulted{};
};
template <> struct asterorm::entity_traits<bulk_unit_row> {
    static constexpr const char* table = "bulk_rows";
    static constexpr auto primary_key = std::make_tuple(
        asterorm::pk<&bulk_unit_row::tenant>("tenant"), asterorm::pk<&bulk_unit_row::id>("id"));
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&bulk_unit_row::tenant>("tenant"),
        asterorm::column<&bulk_unit_row::id>("id"),
        asterorm::column<&bulk_unit_row::price>("price"),
        asterorm::column<&bulk_unit_row::active>("active"),
        asterorm::column<&bulk_unit_row::note>("note"),
        asterorm::column<&bulk_unit_row::derived>("derived", asterorm::generated::always),
        asterorm::column<&bulk_unit_row::defaulted>("defaulted", asterorm::generated::by_default));
};
struct bulk_versioned_row : bulk_unit_row {};
struct bulk_keyless_row : bulk_unit_row {};
struct insert_generated_row {
    int id{};
};
template <> struct asterorm::entity_traits<insert_generated_row> {
    static constexpr const char* table = "generated_only";
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&insert_generated_row::id>("id", asterorm::generated::by_default));
};
template <> struct asterorm::entity_traits<bulk_keyless_row> {
    static constexpr const char* table = "bulk_rows";
    static constexpr auto columns = asterorm::entity_traits<bulk_unit_row>::columns;
};
template <>
struct asterorm::entity_traits<bulk_versioned_row> : asterorm::entity_traits<bulk_unit_row> {
    static constexpr auto version_column = asterorm::version<&bulk_unit_row::price>("price");
};

namespace {
namespace sql = asterorm::sql;
struct legacy_bulk_connection {
    [[maybe_unused]] static constexpr auto sql_dialect = sql::dialect::postgres;
};
struct legacy_bulk_session {
    using connection_type = legacy_bulk_connection;
};
template <typename Count> struct bulk_result {
    Count count{};
    [[nodiscard]] Count affected_rows() const {
        return count;
    }
};
template <sql::dialect Dialect, bool Supported> struct bulk_connection {
    static constexpr auto sql_dialect = Dialect;
    static constexpr bool supports_bulk_predicate_dml = Supported;
};
template <sql::dialect Dialect = sql::dialect::postgres, bool Supported = true,
          typename Count = std::uint64_t>
struct bulk_session {
    using connection_type = bulk_connection<Dialect, Supported>;
    connection_type connection;
    int acquisitions{};
    int events{};
    Count count{2};
    std::string statement;
    asterorm::query_parameters params;
    std::optional<asterorm::db_error> failure;
    std::optional<asterorm::db_error> acquisition_error;
    bool throw_observer{};
    template <typename Callback> auto with_connection(Callback&& callback) {
        ++acquisitions;
        if (acquisition_error) {
            return decltype(callback(connection)){std::unexpected(*acquisition_error)};
        }
        return callback(connection);
    }
    asterorm::result<bulk_result<Count>>
    observed_execute([[maybe_unused]] connection_type& leased_connection, std::string_view query,
                     const asterorm::query_parameters& parameters) {
        ++events;
        statement = query;
        params = parameters;
        if (throw_observer) {
            throw std::runtime_error("observer identity");
        }
        if (failure) {
            return std::unexpected(*failure);
        }
        return bulk_result<Count>{count};
    }
};
auto matching_rows() {
    return sql::col("tenant") == sql::val(7);
}
} // namespace

TEST_CASE("Repository INSERT SELECT infers writable columns and uses one observed execution",
          "[core][bulk-dml][insert-select]") {
    auto source = [] {
        return sql::select({"tenant", "id", "price", "active", "note"})
            .from("input_rows")
            .where(sql::col("active") == sql::val(true))
            .build();
    };
    bulk_session session;
    asterorm::repository repo{session};
    REQUIRE(repo.insert_select<bulk_unit_row>(source()) == 2);
    REQUIRE(session.acquisitions == 1);
    REQUIRE(session.events == 1);
    REQUIRE(
        session.statement ==
        R"(INSERT INTO "bulk_rows" ("tenant", "id", "price", "active", "note") SELECT "tenant", "id", "price", "active", "note" FROM "input_rows" WHERE "active" = $1)");
    REQUIRE(session.params.size() == 1);
    REQUIRE(session.params[0].type == asterorm::parameter_type::boolean);
    REQUIRE(repo.insert_select<bulk_versioned_row>(source()) == 2);
    session.count = 0;
    REQUIRE(repo.insert_select<bulk_unit_row>(source()) == 0);
    session.failure = asterorm::db_error{
        .kind = asterorm::db_error_kind::constraint_violation,
        .sqlstate = "23505",
        .message = "duplicate identity",
    };
    auto failed = repo.insert_select<bulk_unit_row>(source());
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().sqlstate == "23505");
    session.failure.reset();
    session.throw_observer = true;
    REQUIRE_THROWS_WITH(repo.insert_select<bulk_unit_row>(source()), "observer identity");
    REQUIRE(session.events == 5);
    bulk_session<sql::dialect::mysql> mysql;
    asterorm::repository mysql_repo{mysql};
    REQUIRE(mysql_repo.insert_select<bulk_unit_row>(source()) == 2);
    REQUIRE(mysql.statement.starts_with(
        "INSERT INTO `bulk_rows` (`tenant`, `id`, `price`, `active`, `note`) SELECT"));
    auto self = source();
    self.table = "db.BULK_ROWS";
    auto rejected_self = mysql_repo.insert_select<bulk_unit_row>(std::move(self));
    REQUIRE_FALSE(rejected_self.has_value());
    REQUIRE(mysql.acquisitions == 1);
}

TEST_CASE(
    "Repository INSERT SELECT rejects local errors and unsupported capabilities before leasing",
    "[core][bulk-dml][insert-select]") {
    bulk_session session;
    asterorm::repository repo{session};
    REQUIRE_FALSE(
        repo.insert_select<bulk_unit_row>(sql::select({"*"}).from("input").build()).has_value());
    REQUIRE_FALSE(
        repo.insert_select<bulk_unit_row>(sql::select({"id"}).from("input").build()).has_value());
    REQUIRE_FALSE(
        repo.insert_select<insert_generated_row>(sql::select({"id"}).from("input").build())
            .has_value());
    auto malformed = sql::select({"tenant", "id", "price", "active", "note"}).from("input").build();
    malformed.where_clause = sql::predicate_ast{
        sql::binary_predicate{
            .op = sql::op_kind::and_,
            .left = nullptr,
            .right = nullptr,
        },
    };
    auto rejected_shape = repo.insert_select<bulk_unit_row>(std::move(malformed));
    REQUIRE_FALSE(rejected_shape.has_value());
    REQUIRE(session.acquisitions == 0);
    REQUIRE(session.events == 0);
    legacy_bulk_session legacy;
    asterorm::repository legacy_repo{legacy};
    REQUIRE_FALSE(
        legacy_repo.insert_select<bulk_unit_row>(sql::select({"id"}).from("input").build())
            .has_value());
    bulk_session<sql::dialect::clickhouse> clickhouse;
    asterorm::repository ch_repo{clickhouse};
    REQUIRE_FALSE(ch_repo.insert_select<bulk_unit_row>(sql::select({"id"}).from("input").build())
                      .has_value());
    REQUIRE(clickhouse.acquisitions == 0);
}

TEST_CASE("Bulk arithmetic validates every mapped operand before acquisition",
          "[core][bulk-dml][arithmetic]") {
    bulk_session session;
    asterorm::repository repo{session};
    auto updated = repo.update_where<bulk_unit_row>(
        matching_rows(),
        {
            {"price", sql::multiply(sql::add(sql::col("price"), sql::val(2)), sql::val(3))},
        });
    REQUIRE(updated == 2);
    REQUIRE(session.statement ==
            R"(UPDATE "bulk_rows" SET "price" = (("price" + $1) * $2) WHERE "tenant" = $3)");
    REQUIRE(session.params.size() == 3);
    REQUIRE(session.acquisitions == 1);
    std::vector<sql::arithmetic_ptr> invalid;
    for (const auto* name : {"unknown", "bulk_rows.price", "*", ""}) {
        invalid.push_back(sql::multiply(sql::col("price"), sql::add(sql::col(name), sql::val(1))));
    }
    invalid.push_back(sql::arithmetic_ptr{});
    invalid.push_back(sql::add(sql::col("price"), sql::arithmetic_ptr{}));
    for (const auto& expression : invalid) {
        auto rejected = repo.update_where<bulk_unit_row>(matching_rows(), {{"price", expression}});
        REQUIRE_FALSE(rejected.has_value());
        REQUIRE(rejected.error().kind == asterorm::db_error_kind::parse_failed);
    }
    REQUIRE_FALSE(repo.update_where<bulk_unit_row>(
                          matching_rows(), {{"tenant", sql::add(sql::col("tenant"), sql::val(1))}})
                      .has_value());
    REQUIRE_FALSE(
        repo.update_where<bulk_unit_row>(
                matching_rows(), {{"derived", sql::multiply(sql::col("price"), sql::val(2))}})
            .has_value());
    asterorm::query_parameter encoded;
    encoded.error = asterorm::db_error{
        .kind = asterorm::db_error_kind::parse_failed,
        .message = "nested codec identity",
    };
    auto rejected = repo.update_where<bulk_unit_row>(
        matching_rows(), {{"price", sql::add(sql::col("price"), sql::param_expr{encoded})}});
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(rejected.error().message == "nested codec identity");
    sql::arithmetic_ptr nested = sql::add(sql::col("price"), sql::val(1));
    for (std::size_t depth = 0; depth < sql::max_arithmetic_depth; ++depth) {
        nested = sql::add(nested, sql::val(1));
    }
    REQUIRE_FALSE(
        repo.update_where<bulk_unit_row>(matching_rows(), {{"price", nested}}).has_value());
    auto cycle = std::make_shared<sql::arithmetic_expr>(sql::arithmetic_expr{
        .op = sql::arithmetic_op::add,
        .left = sql::col("price"),
        .right = sql::val(2),
    });
    cycle->right = sql::arithmetic_ptr{cycle};
    auto cyclic =
        repo.update_where<bulk_unit_row>(matching_rows(), {{"price", sql::arithmetic_ptr{cycle}}});
    cycle->right = sql::val(0);
    REQUIRE_FALSE(cyclic.has_value());
    sql::arithmetic_ptr wide = sql::add(sql::val(1), sql::val(2));
    for (int expansion = 0; expansion < 9; ++expansion) {
        wide = sql::add(wide, wide);
    }
    REQUIRE_FALSE(repo.update_where<bulk_unit_row>(matching_rows(), {{"price", wide}}).has_value());
    REQUIRE(session.acquisitions == 1);
    REQUIRE(session.events == 1);
}

TEST_CASE("Affected-row counts retain wide boundaries and reject malformed inputs",
          "[core][bulk-dml]") {
    using asterorm::detail::parse_row_count;
    for (const auto* text : {"0", "2147483647", "2147483648", "18446744073709551615"}) {
        REQUIRE(parse_row_count(text).has_value());
    }
    REQUIRE(*parse_row_count("18446744073709551615") == std::numeric_limits<std::uint64_t>::max());
    for (const auto* text : {"", "-1", "+1", " 1", "1 ", "12x", "18446744073709551616"}) {
        auto count = parse_row_count(text);
        REQUIRE_FALSE(count.has_value());
        REQUIRE(count.error().kind == asterorm::db_error_kind::query_failed);
    }
    REQUIRE_FALSE(asterorm::detail::checked_row_count<std::uint64_t>(-1).has_value());
    REQUIRE(*asterorm::detail::checked_row_count<std::uint32_t>(UINT64_C(4294967295)) ==
            UINT32_MAX);
    REQUIRE_FALSE(
        asterorm::detail::checked_row_count<std::uint32_t>(UINT64_C(4294967296)).has_value());
}

TEST_CASE("Bulk repository mutations compile and observe one statement", "[core][bulk-dml]") {
    bulk_session session;
    asterorm::repository repo{session};
    const std::string hostile = "quote '; DROP TABLE bulk_rows; --";
    auto changed = repo.update_where<bulk_unit_row>(
        matching_rows(), {{"active", sql::val(false)}, {"note", sql::val(hostile)}});
    REQUIRE(changed == 2);
    REQUIRE(session.acquisitions == 1);
    REQUIRE(session.events == 1);
    REQUIRE(session.statement ==
            R"(UPDATE "bulk_rows" SET "active" = $1, "note" = $2 WHERE "tenant" = $3)");
    REQUIRE(session.params.size() == 3);
    REQUIRE(session.params[0].type == asterorm::parameter_type::boolean);
    REQUIRE(session.params[1] == hostile.c_str());
    REQUIRE(session.params[2] == "7");
    session.count = 0;
    auto removed =
        repo.delete_where<bulk_unit_row>(sql::is_null(sql::col("note")) && matching_rows());
    REQUIRE(removed == 0);
    REQUIRE(session.statement ==
            R"(DELETE FROM "bulk_rows" WHERE ("note" IS NULL AND "tenant" = $1))");
    REQUIRE(session.acquisitions == 2);
    REQUIRE(session.events == 2);
    REQUIRE(repo.update_where<bulk_unit_row>(matching_rows(), {{"defaulted", sql::val(11)}}) == 0);
    REQUIRE(repo.update_where<bulk_unit_row>(matching_rows(),
                                             {{"price", sql::raw_expr{"price * 2"}}}) == 0);
    REQUIRE(session.params.size() == 1);
    REQUIRE(repo.update_where<bulk_unit_row>(
                matching_rows(), {{"note", sql::val(std::optional<std::string>{})}}) == 0);
    REQUIRE_FALSE(session.params[0].value.has_value());
    REQUIRE(repo.update_where<bulk_keyless_row>(matching_rows(), {{"id", sql::val(12)}}) == 0);
}

TEST_CASE("Bulk mutations preserve dialect and execution diagnostics", "[core][bulk-dml]") {
    bulk_session<sql::dialect::mysql> session;
    asterorm::repository repo{session};
    session.count = UINT64_C(2147483648);
    REQUIRE(repo.update_where<bulk_unit_row>(matching_rows(), {{"price", sql::val(50)}}) ==
            UINT64_C(2147483648));
    REQUIRE(session.statement == "UPDATE `bulk_rows` SET `price` = ? WHERE `tenant` = ?");
    session.failure = asterorm::db_error{
        .kind = asterorm::db_error_kind::query_failed,
        .sqlstate = "57014",
        .message = "cancelled",
    };
    auto failed = repo.delete_where<bulk_unit_row>(matching_rows());
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().sqlstate == "57014");
    REQUIRE(failed.error().message == "cancelled");
    REQUIRE(session.events == 2);
    session.failure.reset();
    session.throw_observer = true;
    REQUIRE_THROWS_WITH(repo.delete_where<bulk_unit_row>(matching_rows()), "observer identity");
    REQUIRE(session.events == 3);
    bulk_session<sql::dialect::sqlite, true, std::int64_t> negative;
    negative.count = -1;
    asterorm::repository signed_repo{negative};
    REQUIRE_FALSE(signed_repo.delete_where<bulk_unit_row>(matching_rows()).has_value());
    bulk_session unavailable;
    unavailable.acquisition_error = asterorm::db_error{
        .kind = asterorm::db_error_kind::connection_failed,
        .sqlstate = "08006",
        .message = "lease identity",
    };
    asterorm::repository unavailable_repo{unavailable};
    auto lease_error = unavailable_repo.delete_where<bulk_unit_row>(matching_rows());
    REQUIRE_FALSE(lease_error.has_value());
    REQUIRE(lease_error.error().message == "lease identity");
    REQUIRE(unavailable.events == 0);
}

TEST_CASE("Bulk assignments reject invalid writes before acquisition", "[core][bulk-dml]") {
    bulk_session session;
    asterorm::repository repo{session};
    using assignments = std::vector<std::pair<std::string, sql::value_source>>;
    std::vector<assignments> invalid;
    invalid.emplace_back();
    for (const auto& name : std::vector<std::string>{
             "",
             "*",
             "bulk_rows.price",
             "missing",
             "tenant",
             "id",
             "derived",
             std::string{"pr\0ice", 6},
         }) {
        invalid.push_back({{name, sql::val(1)}});
    }
    invalid.push_back({{"price", sql::val(1)}, {"price", sql::val(2)}});
    invalid.push_back({{"price", sql::raw_expr{std::string{"1\0+2", 4}}}});
    for (auto& assignment : invalid) {
        auto rejected = repo.update_where<bulk_unit_row>(matching_rows(), std::move(assignment));
        REQUIRE_FALSE(rejected.has_value());
        REQUIRE(rejected.error().kind == asterorm::db_error_kind::parse_failed);
    }
    asterorm::query_parameter encoded;
    encoded.error = asterorm::db_error{
        .kind = asterorm::db_error_kind::parse_failed,
        .message = "codec identity",
    };
    auto rejected =
        repo.update_where<bulk_unit_row>(matching_rows(), {{"price", sql::param_expr{encoded}}});
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(rejected.error().message == "codec identity");
    REQUIRE(session.acquisitions == 0);
    REQUIRE(session.events == 0);
}

TEST_CASE("Bulk predicates validate shape and mapping before acquisition", "[core][bulk-dml]") {
    bulk_session session;
    asterorm::repository repo{session};
    std::vector<sql::predicate_ast> invalid;
    for (const auto* name : {"", "*", "bulk_rows.id", "missing"}) {
        invalid.push_back(sql::col(name) == sql::val(1));
    }
    invalid.push_back(sql::predicate_ast{
        sql::comparison_predicate{
            .op = sql::op_kind::and_,
            .left = sql::col("id"),
            .right = sql::val(1),
        },
    });
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) -- deliberate invalid AST input.
    const auto invalid_null_operator = static_cast<sql::unary_op_kind>(255);
    invalid.push_back(sql::predicate_ast{
        sql::unary_predicate{
            .op = invalid_null_operator,
            .operand = sql::col("id"),
        },
    });
    invalid.push_back(sql::predicate_ast{
        sql::binary_predicate{
            .op = sql::op_kind::and_,
            .left = nullptr,
            .right = nullptr,
        },
    });
    invalid.push_back(sql::predicate_ast{
        sql::binary_predicate{
            .op = sql::op_kind::eq,
            .left = std::make_unique<sql::predicate_ast>(matching_rows()),
            .right = std::make_unique<sql::predicate_ast>(matching_rows()),
        },
    });
    invalid.push_back(sql::predicate_ast{
        sql::comparison_predicate{
            .op = sql::op_kind::eq,
            .left = sql::count_all(),
            .right = sql::val(1),
        },
    });
    invalid.push_back(sql::predicate_ast{
        sql::comparison_predicate{
            .op = sql::op_kind::eq,
            .left = std::shared_ptr<sql::func_expr>{},
            .right = sql::val(1),
        },
    });
    for (auto& predicate : invalid) {
        auto rejected = repo.delete_where<bulk_unit_row>(std::move(predicate));
        REQUIRE_FALSE(rejected.has_value());
        REQUIRE(rejected.error().kind == asterorm::db_error_kind::parse_failed);
    }
    REQUIRE(session.acquisitions == 0);
    REQUIRE(session.events == 0);
}

TEST_CASE("Bulk capabilities and version policy reject without a connection", "[core][bulk-dml]") {
    bulk_session<sql::dialect::clickhouse> clickhouse;
    asterorm::repository ch_repo{clickhouse};
    REQUIRE_FALSE(ch_repo.delete_where<bulk_unit_row>(matching_rows()).has_value());
    REQUIRE(clickhouse.acquisitions == 0);
    bulk_session<sql::dialect::postgres, false> disabled;
    asterorm::repository disabled_repo{disabled};
    REQUIRE_FALSE(
        disabled_repo.update_where<bulk_unit_row>(matching_rows(), {{"price", sql::val(1)}})
            .has_value());
    REQUIRE(disabled.acquisitions == 0);
    struct unknown_session {
    } unknown;
    asterorm::repository unknown_repo{unknown};
    REQUIRE_FALSE(unknown_repo.delete_where<bulk_unit_row>(matching_rows()).has_value());
    legacy_bulk_session legacy;
    asterorm::repository legacy_repo{legacy};
    REQUIRE_FALSE(legacy_repo.delete_where<bulk_unit_row>(matching_rows()).has_value());
    bulk_session session;
    asterorm::repository repo{session};
    auto updated = repo.update_where<bulk_versioned_row>(matching_rows(), {{"price", sql::val(1)}});
    REQUIRE_FALSE(updated.has_value());
    REQUIRE(updated.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(repo.delete_where<bulk_versioned_row>(matching_rows()).has_value());
    REQUIRE(session.acquisitions == 0);
}
