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

    int affected_rows() const {
        return affected;
    }
    int rows() const {
        return 0;
    }
    std::optional<std::string> get_string(int, int) const {
        return std::nullopt;
    }
};

struct fake_repository_connection {
    std::string sql;
    std::vector<std::optional<std::string>> params;

    asterorm::result<fake_query_result>
    execute_prepared(std::string_view statement,
                     const std::vector<std::optional<std::string>>& bound_params) {
        sql = std::string{statement};
        params = bound_params;
        return fake_query_result{.affected = 2};
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
