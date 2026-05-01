#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <vector>

#include "asterorm/entity_traits.hpp"
#include "asterorm/repository.hpp"

struct test_user {
    std::optional<int> id;
    std::string name;
    bool active{true};
};

template <> struct asterorm::entity_traits<test_user> {
    static constexpr const char* table = "users";
    static constexpr auto primary_key = asterorm::pk<&test_user::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&test_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&test_user::name>("name"), asterorm::column<&test_user::active>("active"));
};

TEST_CASE("Core: Entity Traits Mapping", "[core][traits]") {
    REQUIRE(std::string_view(asterorm::entity_traits<test_user>::table) == "users");

    auto pk_map = asterorm::entity_traits<test_user>::primary_key;
    REQUIRE(pk_map.name == "id");

    auto cols = asterorm::entity_traits<test_user>::columns;
    REQUIRE(std::tuple_size_v<decltype(cols)> == 3);

    auto col0 = std::get<0>(cols);
    REQUIRE(col0.name == "id");
    REQUIRE(col0.gen == asterorm::generated::by_default);

    auto col1 = std::get<1>(cols);
    REQUIRE(col1.name == "name");
    REQUIRE(col1.gen == asterorm::generated::none);

    // Verify we can access the member pointer value at compile time
    REQUIRE(col1.member_ptr == &test_user::name);
}

struct fake_query_result {
    int affected{0};
    std::vector<std::vector<std::optional<std::string>>> rows_data;

    int affected_rows() const {
        return affected;
    }
    int rows() const {
        return static_cast<int>(rows_data.size());
    }
    std::optional<std::string> get_string(int row_index, int column_index) const {
        return rows_data.at(static_cast<std::size_t>(row_index))
            .at(static_cast<std::size_t>(column_index));
    }
};

struct fake_repository_connection {
    std::string sql;
    std::vector<std::optional<std::string>> params;
    fake_query_result query_result{.affected = 2};

    asterorm::result<fake_query_result>
    execute_prepared(std::string_view statement,
                     const std::vector<std::optional<std::string>>& bound_params) {
        sql = std::string{statement};
        params = bound_params;
        return query_result;
    }
};

struct fake_repository_session {
    fake_repository_connection conn;

    template <typename F> auto with_connection(F&& func) {
        return func(conn);
    }

    asterorm::result<fake_query_result>
    observed_execute(fake_repository_connection& connection, std::string_view sql,
                     const std::vector<std::optional<std::string>>& params) {
        return connection.execute_prepared(sql, params);
    }
};

TEST_CASE("Repository: batch insert uses DEFAULT for unset generated values", "[core][crud]") {
    fake_repository_session session;
    asterorm::repository repo{session};

    std::vector<test_user> rows{
        {.id = std::nullopt, .name = "generated", .active = true},
        {.id = 42, .name = "explicit", .active = false},
    };

    auto inserted = repo.insert_many(rows);
    REQUIRE(inserted.has_value());
    REQUIRE(*inserted == 2);
    REQUIRE(session.conn.sql ==
            "INSERT INTO users (id, name, active) VALUES (DEFAULT, $1, $2), ($3, $4, $5)");
    REQUIRE(session.conn.params.size() == 5);
    REQUIRE(session.conn.params[0] == "generated");
    REQUIRE(session.conn.params[1] == "t");
    REQUIRE(session.conn.params[2] == "42");
    REQUIRE(session.conn.params[3] == "explicit");
    REQUIRE(session.conn.params[4] == "f");
}

TEST_CASE("Repository: projection queries decode scalar, tuple, and mapped rows", "[core][query]") {
    fake_repository_session session;
    asterorm::repository repository{session};

    SECTION("Scalar projection") {
        session.conn.query_result.rows_data = {{{"Alice"}}, {{"Bob"}}};

        asterorm::query_options options;
        options.order_by.push_back({.column = "name", .ascending = true});
        options.limit = 2;

        auto projected_names =
            repository.select_projection<test_user, std::string>({"name"}, std::move(options));

        REQUIRE(projected_names.has_value());
        REQUIRE(*projected_names == std::vector<std::string>{"Alice", "Bob"});
        REQUIRE(session.conn.sql == R"(SELECT "name" FROM "users" ORDER BY "name" ASC LIMIT 2)");
        REQUIRE(session.conn.params.empty());
    }

    SECTION("Tuple projection with predicate") {
        session.conn.query_result.rows_data = {{{"Alice"}, {"t"}}, {{"Cara"}, {"t"}}};

        auto projected_rows =
            repository.select_projection_by<test_user, std::tuple<std::string, bool>>(
                asterorm::sql::col("active") == asterorm::sql::val(true), {"name", "active"});

        REQUIRE(projected_rows.has_value());
        REQUIRE(projected_rows->size() == 2);
        REQUIRE(std::get<0>((*projected_rows)[0]) == "Alice");
        REQUIRE(std::get<1>((*projected_rows)[0]));
        REQUIRE(std::get<0>((*projected_rows)[1]) == "Cara");
        REQUIRE(std::get<1>((*projected_rows)[1]));
        REQUIRE(session.conn.sql == R"(SELECT "name", "active" FROM "users" WHERE "active" = $1)");
        REQUIRE(session.conn.params.size() == 1);
        REQUIRE(session.conn.params[0] == "t");
    }

    SECTION("Custom projection mapper") {
        session.conn.query_result.rows_data = {{{"7"}, {"Alice"}}, {{"8"}, {"Bob"}}};

        auto labels = repository.select_map<test_user>(
            {"id", "name"}, [](const asterorm::projection_row& row_values) {
                return row_values[0].value() + ":" + row_values[1].value();
            });

        REQUIRE(labels.has_value());
        REQUIRE(*labels == std::vector<std::string>{"7:Alice", "8:Bob"});
        REQUIRE(session.conn.sql == R"(SELECT "id", "name" FROM "users")");
    }

    SECTION("Unknown projection column reports the offending name") {
        auto projected_names =
            repository.select_projection<test_user, std::string>({"missing_column"});

        REQUIRE_FALSE(projected_names.has_value());
        REQUIRE(projected_names.error().kind == asterorm::db_error_kind::parse_failed);
        REQUIRE(projected_names.error().message.find("missing_column") != std::string::npos);
    }
}
