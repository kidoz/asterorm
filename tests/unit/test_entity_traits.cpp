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

struct relationship_user {
    int id{};
    std::string email;
};

struct relationship_profile {
    int id{};
    int user_id{};
    std::string display_name;
};

struct relationship_post {
    int id{};
    int author_id{};
    std::string title;
};

template <> struct asterorm::entity_traits<test_user> {
    static constexpr const char* table = "users";
    static constexpr auto primary_key = asterorm::pk<&test_user::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&test_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&test_user::name>("name"), asterorm::column<&test_user::active>("active"));
};

template <> struct asterorm::entity_traits<relationship_user> {
    static constexpr const char* table = "relationship_users";
    static constexpr auto primary_key = asterorm::pk<&relationship_user::id>("id");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&relationship_user::id>("id"),
                        asterorm::column<&relationship_user::email>("email"));

    static constexpr auto relationships =
        std::make_tuple(asterorm::one_to_one<relationship_profile, &relationship_user::id,
                                             &relationship_profile::user_id>("profile"),
                        asterorm::one_to_many<relationship_post, &relationship_user::id,
                                              &relationship_post::author_id>("posts"));
};

template <> struct asterorm::entity_traits<relationship_profile> {
    static constexpr const char* table = "profiles";
    static constexpr auto primary_key = asterorm::pk<&relationship_profile::id>("id");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&relationship_profile::id>("id"),
                        asterorm::column<&relationship_profile::user_id>("user_id"),
                        asterorm::column<&relationship_profile::display_name>("display_name"));

    static constexpr auto relationships =
        std::make_tuple(asterorm::many_to_one<relationship_user, &relationship_profile::user_id,
                                              &relationship_user::id>("user"));
};

template <> struct asterorm::entity_traits<relationship_post> {
    static constexpr const char* table = "posts";
    static constexpr auto primary_key = asterorm::pk<&relationship_post::id>("id");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&relationship_post::id>("id"),
                        asterorm::column<&relationship_post::author_id>("author_id"),
                        asterorm::column<&relationship_post::title>("title"));
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

TEST_CASE("Core: Relationship trait metadata", "[core][traits][relationships]") {
    static_assert(!asterorm::has_relationships_v<test_user>);
    static_assert(asterorm::relationship_count_v<test_user> == 0);
    static_assert(asterorm::has_relationships_v<relationship_user>);
    static_assert(asterorm::relationship_count_v<relationship_user> == 2);
    static_assert(asterorm::relationship_count_v<relationship_profile> == 1);

    const auto user_relationships = asterorm::entity_traits<relationship_user>::relationships;
    const auto profile_relationship = std::get<0>(user_relationships);
    const auto posts_relationship = std::get<1>(user_relationships);

    static_assert(std::is_same_v<std::decay_t<decltype(profile_relationship)>::related_entity,
                                 relationship_profile>);
    static_assert(std::is_same_v<std::decay_t<decltype(posts_relationship)>::related_entity,
                                 relationship_post>);

    REQUIRE(profile_relationship.kind == asterorm::relationship_kind::one_to_one);
    REQUIRE(profile_relationship.name == "profile");
    REQUIRE(profile_relationship.local_member_ptr == &relationship_user::id);
    REQUIRE(profile_relationship.related_member_ptr == &relationship_profile::user_id);

    REQUIRE(posts_relationship.kind == asterorm::relationship_kind::one_to_many);
    REQUIRE(posts_relationship.name == "posts");
    REQUIRE(posts_relationship.local_member_ptr == &relationship_user::id);
    REQUIRE(posts_relationship.related_member_ptr == &relationship_post::author_id);

    const auto profile_relationships = asterorm::entity_traits<relationship_profile>::relationships;
    const auto user_relationship = std::get<0>(profile_relationships);

    static_assert(std::is_same_v<std::decay_t<decltype(user_relationship)>::related_entity,
                                 relationship_user>);
    REQUIRE(user_relationship.kind == asterorm::relationship_kind::many_to_one);
    REQUIRE(user_relationship.name == "user");
    REQUIRE(user_relationship.local_member_ptr == &relationship_profile::user_id);
    REQUIRE(user_relationship.related_member_ptr == &relationship_user::id);
}

struct fake_query_result {
    int affected{0};
    std::vector<std::vector<std::optional<std::string>>> rows_data;

    [[nodiscard]] int affected_rows() const {
        return affected;
    }
    [[nodiscard]] int rows() const {
        return static_cast<int>(rows_data.size());
    }
    [[nodiscard]]
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
        REQUIRE(projected_names.error().message.contains("missing_column"));
    }

    SECTION("Unknown order column reports the offending name") {
        asterorm::query_options options;
        options.order_by.push_back({.column = "missing_order_column", .ascending = true});

        auto projected_names =
            repository.select_projection<test_user, std::string>({"name"}, std::move(options));

        REQUIRE_FALSE(projected_names.has_value());
        REQUIRE(projected_names.error().kind == asterorm::db_error_kind::parse_failed);
        REQUIRE(projected_names.error().message.contains("missing_order_column"));
    }
}

TEST_CASE("Repository: relationship loading uses explicit follow-up selects",
          "[core][relationships]") {
    fake_repository_session session;
    asterorm::repository repository{session};

    SECTION("Load one-to-many relationship") {
        relationship_user user{.id = 7, .email = "owner@example.com"};
        session.conn.query_result.rows_data = {
            {{"10"}, {"7"}, {"first"}},
            {{"11"}, {"7"}, {"second"}},
        };

        asterorm::query_options options;
        options.order_by.push_back({.column = "id", .ascending = true});

        auto posts = repository.load_related_many<1>(user, std::move(options));

        REQUIRE(posts.has_value());
        REQUIRE(posts->size() == 2);
        REQUIRE((*posts)[0].id == 10);
        REQUIRE((*posts)[0].author_id == 7);
        REQUIRE((*posts)[0].title == "first");
        REQUIRE((*posts)[1].id == 11);
        REQUIRE((*posts)[1].author_id == 7);
        REQUIRE((*posts)[1].title == "second");
        REQUIRE(
            session.conn.sql ==
            R"(SELECT "id", "author_id", "title" FROM "posts" WHERE "author_id" = $1 ORDER BY "id" ASC)");
        REQUIRE(session.conn.params.size() == 1);
        REQUIRE(session.conn.params[0] == "7");
    }

    SECTION("Load one-to-one relationship") {
        relationship_user user{.id = 7, .email = "owner@example.com"};
        session.conn.query_result.rows_data = {{{"20"}, {"7"}, {"Owner"}}};

        auto profile = repository.load_related_one<0>(user);

        REQUIRE(profile.has_value());
        REQUIRE(profile->has_value());
        REQUIRE((*profile)->id == 20);
        REQUIRE((*profile)->user_id == 7);
        REQUIRE((*profile)->display_name == "Owner");
        REQUIRE(
            session.conn.sql ==
            R"(SELECT "id", "user_id", "display_name" FROM "profiles" WHERE "user_id" = $1 LIMIT 1)");
        REQUIRE(session.conn.params.size() == 1);
        REQUIRE(session.conn.params[0] == "7");
    }

    SECTION("Load many-to-one relationship") {
        relationship_profile profile{.id = 20, .user_id = 7, .display_name = "Owner"};
        session.conn.query_result.rows_data = {{{"7"}, {"owner@example.com"}}};

        auto user = repository.load_related_one<0>(profile);

        REQUIRE(user.has_value());
        REQUIRE(user->has_value());
        REQUIRE((*user)->id == 7);
        REQUIRE((*user)->email == "owner@example.com");
        REQUIRE(session.conn.sql ==
                R"(SELECT "id", "email" FROM "relationship_users" WHERE "id" = $1 LIMIT 1)");
        REQUIRE(session.conn.params.size() == 1);
        REQUIRE(session.conn.params[0] == "7");
    }
}
