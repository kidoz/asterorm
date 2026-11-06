#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "asterorm/repository.hpp"
#include "asterorm/sql/typed_query.hpp"

#ifdef ASTERORM_EXAMPLE_POSTGRES
#include "asterorm/postgres/driver.hpp"
#endif
#ifdef ASTERORM_EXAMPLE_SQLITE
#include "asterorm/sqlite/driver.hpp"
#endif
#ifdef ASTERORM_EXAMPLE_MYSQL
#include "asterorm/mysql/driver.hpp"
#endif

// guide:begin mapping
struct User {
    std::int64_t id{};
    std::string email;
    int age{};
    bool active{true};
    std::optional<std::string> deleted_at;

    bool operator==(const User&) const = default;
};

template <> struct asterorm::entity_traits<User> {
    static constexpr const char* table = "typed_query_migration_users";
    static constexpr auto primary_key = asterorm::pk<&User::id>("id");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&User::id>("id"), asterorm::column<&User::email>("email"),
        asterorm::column<&User::age>("age"), asterorm::column<&User::active>("active"),
        asterorm::column<&User::deleted_at>("deleted_at"));
};

namespace sql = asterorm::sql;
namespace typed = asterorm::sql::typed;
// guide:end mapping

namespace {

// guide:begin errors
template <typename T> T take(asterorm::result<T> result) {
    if (!result) {
        throw std::runtime_error(result.error().message);
    }
    return std::move(*result);
}
void take(asterorm::result<void> result) {
    if (!result) {
        throw std::runtime_error(result.error().message);
    }
}
// guide:end errors

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string{message});
    }
}

template <typename Driver> void run(Driver driver, std::string conninfo) {
    asterorm::pool_config config;
    config.conninfo = std::move(conninfo);
    // Temporary tables and SQLite :memory: belong to one connection.
    config.min_size = 1;
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    asterorm::repository repo{db};

    take(db.with_connection([](auto& connection) {
        return connection.execute(
            "CREATE TEMPORARY TABLE typed_query_migration_users ("
            "id INTEGER PRIMARY KEY, email TEXT NOT NULL, age INTEGER NOT NULL, "
            "active BOOLEAN NOT NULL, deleted_at TEXT)");
    }));
    for (auto user : std::vector<User>{
             {.id = 1,
              .email = "alice@example.com",
              .age = 30,
              .active = true,
              .deleted_at = std::nullopt},
             {.id = 2,
              .email = "bob@example.com",
              .age = 17,
              .active = true,
              .deleted_at = std::nullopt},
             {.id = 3,
              .email = "cara@example.com",
              .age = 40,
              .active = false,
              .deleted_at = std::nullopt},
             {.id = 4,
              .email = "dana@example.com",
              .age = 25,
              .active = true,
              .deleted_at = "archived"},
         }) {
        take(repo.insert(user));
    }

    // guide:begin legacy
    asterorm::query_options options;
    options.order_by = {{.column = "email", .ascending = true}};
    options.limit = 10;
    options.offset = 0;
    auto legacy_rows = take(repo.template find_by<User>(
        (sql::col("age") >= sql::val(18)) && (sql::col("active") == sql::val(true)), options));
    // guide:end legacy

    // guide:begin entities
    auto typed_rows =
        take(typed::query(db, typed::select<User>()
                                  .where((typed::col<&User::age> >= typed::val(18)) &&
                                         (typed::col<&User::active> == typed::val(true)))
                                  .order_by(typed::col<&User::email>, typed::asc)
                                  .limit(10)
                                  .offset(0)));
    // guide:end entities
    require(typed_rows == legacy_rows, "whole entities differ from the string query");
    require(typed_rows.size() == 2 && typed_rows[0].id == 1 && typed_rows[1].id == 4,
            "unexpected whole entities or ordering");

    // guide:begin tuples
    auto tuples =
        take(typed::query(db, typed::select_cols(typed::col<&User::email>, typed::col<&User::age>,
                                                 typed::col<&User::deleted_at>)
                                  .where((typed::col<&User::age> >= typed::val(18)) &&
                                         (typed::col<&User::active> == typed::val(true)))
                                  .order_by(typed::col<&User::email>)));
    using UserTuple = std::tuple<std::string, int, std::optional<std::string>>;
    static_assert(std::is_same_v<decltype(tuples), std::vector<UserTuple>>);
    // guide:end tuples
    require(tuples ==
                std::vector<UserTuple>{
                    {"alice@example.com", 30, std::nullopt},
                    {"dana@example.com", 25, "archived"},
                },
            "tuple values or NULL handling differ");

    // guide:begin dto
    struct UserSummary {
        std::string email;
        int age;
    };
    auto summaries = take(typed::query(
        db, typed::select_cols<UserSummary>(typed::col<&User::email>, typed::col<&User::age>)
                .where(typed::col<&User::email> == typed::val("alice@example.com"))));
    // guide:end dto
    require(summaries.size() == 1 && summaries[0].email == "alice@example.com" &&
                summaries[0].age == 30,
            "DTO values differ");

    // guide:begin nulls
    auto live = take(typed::query(db, typed::select<User>()
                                          .where(typed::is_null(typed::col<&User::deleted_at>) &&
                                                 (typed::col<&User::active> == typed::val(true)))
                                          .order_by(typed::col<&User::id>)));
    auto archived = take(typed::query(
        db, typed::select<User>().where(typed::is_not_null(typed::col<&User::deleted_at>))));
    // guide:end nulls
    require(live.size() == 2 && live[0].id == 1 && live[1].id == 2,
            "IS NULL returned unexpected users");
    require(archived.size() == 1 && archived[0].id == 4, "IS NOT NULL returned unexpected users");

    // guide:begin aggregates
    auto groups = take(typed::query(
        db, typed::select_cols(
                typed::col<&User::active>, typed::count_all(), typed::count(typed::col<&User::id>),
                typed::sum(typed::col<&User::age>), typed::avg(typed::col<&User::age>),
                typed::min(typed::col<&User::age>), typed::max(typed::col<&User::age>))
                .group_by(typed::col<&User::active>)
                .having(typed::col<&User::active> == typed::val(true))));
    using Group = std::tuple<bool, std::int64_t, std::int64_t, std::int64_t, double, int, int>;
    static_assert(std::is_same_v<decltype(groups), std::vector<Group>>);
    // guide:end aggregates
    require(groups == std::vector<Group>{{true, 3, 3, 72, 24.0, 17, 30}},
            "aggregate values differ");

    // guide:begin escape
    auto adult_count = take(db.template native_scalar<std::int64_t>(
        "SELECT COUNT(*) FROM typed_query_migration_users WHERE age >= $1", 18));
    // guide:end escape
    require(adult_count == 3, "native scalar fallback returned an unexpected count");
}

[[maybe_unused]] std::string configured_or(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value ? value : fallback;
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::string_view backend = argc == 2 ? argv[1] : "sqlite";
        bool ran = false;
#ifdef ASTERORM_EXAMPLE_SQLITE
        if (backend == "sqlite") {
            run(asterorm::sqlite::driver{}, ":memory:");
            ran = true;
        }
#endif
#ifdef ASTERORM_EXAMPLE_POSTGRES
        if (backend == "postgres") {
            run(asterorm::pg::driver{},
                configured_or("ASTERORM_TEST_CONNINFO",
                              "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                              "password=orm_test sslmode=disable"));
            ran = true;
        }
#endif
#ifdef ASTERORM_EXAMPLE_MYSQL
        if (backend == "mysql") {
            run(asterorm::mysql::driver{},
                configured_or("ASTERORM_MYSQL_TEST_CONNINFO",
                              "host=127.0.0.1 port=3307 dbname=orm_test user=orm_test "
                              "password=orm_test"));
            ran = true;
        }
#endif
        if (!ran) {
            throw std::runtime_error("choose an enabled backend: sqlite, postgres, mysql");
        }
        std::cout << "typed query migration: " << backend << " passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
