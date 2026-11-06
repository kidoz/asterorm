#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <future>
#include <stdexcept>
#include <thread>
#include <tuple>

#include "asterorm/migration.hpp"
#include "asterorm/mysql/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "support/select_locking.hpp"
#include "support/typed_query_observer.hpp"

struct MysqlUser {
    std::optional<std::int64_t> id;
    std::string name;
    bool active{true};
    std::optional<std::string> note;
    std::int64_t version{1};
    std::optional<std::string> created;
};

template <> struct asterorm::entity_traits<MysqlUser> {
    static constexpr const char* table = "mysql_users";
    static constexpr auto primary_key = asterorm::pk<&MysqlUser::id>("id");
    static constexpr auto version_column = asterorm::version<&MysqlUser::version>("version");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&MysqlUser::id>("id", asterorm::generated::by_default),
        asterorm::column<&MysqlUser::name>("name"), asterorm::column<&MysqlUser::active>("active"),
        asterorm::column<&MysqlUser::note>("note"),
        asterorm::column<&MysqlUser::version>("version"),
        asterorm::column<&MysqlUser::created>("created", asterorm::generated::always));
};

struct MysqlPlain {
    int id{};
    std::string label;
};
template <> struct asterorm::entity_traits<MysqlPlain> {
    static constexpr const char* table = "mysql_plain";
    static constexpr auto primary_key = asterorm::pk<&MysqlPlain::id>("id");
    static constexpr auto columns = std::make_tuple(asterorm::column<&MysqlPlain::id>("id"),
                                                    asterorm::column<&MysqlPlain::label>("label"));
};

struct MysqlComposite {
    int owner{};
    int item{};
    std::string label;
};
template <> struct asterorm::entity_traits<MysqlComposite> {
    static constexpr const char* table = "mysql_composite";
    static constexpr auto primary_key = std::make_tuple(
        asterorm::pk<&MysqlComposite::owner>("owner"), asterorm::pk<&MysqlComposite::item>("item"));
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&MysqlComposite::owner>("owner"),
                        asterorm::column<&MysqlComposite::item>("item"),
                        asterorm::column<&MysqlComposite::label>("label"));
};

struct MysqlGenerated {
    std::optional<std::int64_t> id;
};
template <> struct asterorm::entity_traits<MysqlGenerated> {
    static constexpr const char* table = "mysql_generated";
    static constexpr auto primary_key = asterorm::pk<&MysqlGenerated::id>("id");
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&MysqlGenerated::id>("id", asterorm::generated::always));
};

namespace {
std::string conninfo() {
    const auto* configured = std::getenv("ASTERORM_MYSQL_TEST_CONNINFO");
    return configured ? configured
                      : "host=127.0.0.1 port=3307 dbname=orm_test user=orm_test password=orm_test";
}

template <typename T> T take(asterorm::result<T> result) {
    INFO((result ? "success" : result.error().message));
    REQUIRE(result.has_value());
    return std::move(*result);
}
void take(asterorm::result<void> result) {
    INFO((result ? "success" : result.error().message));
    REQUIRE(result.has_value());
}
} // namespace

TEST_CASE("MySQL Integration: typed query observation", "[mysql][typed-query][observer]") {
    asterorm::test::check_typed_query_observer(asterorm::mysql::driver{}, conninfo());
}

TEST_CASE("MySQL Integration: SELECT locking", "[mysql][select-locking]") {
    asterorm::test::check_select_locking(asterorm::mysql::driver{}, conninfo());
}

TEST_CASE("MySQL native statements, bindings, results and cache", "[mysql][integration]") {
    auto connection = take(asterorm::mysql::driver{}.connect(conninfo()));
    auto values = take(connection.execute_params(
        "SELECT $2 AS second, $1 AS first, $2 AS again, '$1' AS literal", {"first", "second"}));
    REQUIRE(values.get_value(0, 0) == "second");
    REQUIRE(values.get_value(0, 1) == "first");
    REQUIRE(values.get_value(0, 2) == "second");
    REQUIRE(values.get_value(0, 3) == "$1");
    REQUIRE(values.affected_rows() == 0);
    const std::string payload = "quotes '\"; DROP TABLE mysql_users; -- \\ Unicode: Привет";
    std::string large(300000, 'x');
    large[1234] = '\0';
    auto row = take(
        connection.execute_params("SELECT ?, ?, ?, ?, ?", {
                                                              payload,
                                                              large,
                                                              std::nullopt,
                                                              asterorm::encode_parameter(true),
                                                              asterorm::encode_parameter(false),
                                                          }));
    REQUIRE(row.get_value(0, 0) == payload);
    REQUIRE(row.get_value(0, 1) == large);
    REQUIRE(row.is_null(0, 2));
    REQUIRE(row.get_value(0, 3) == "1");
    REQUIRE(row.get_value(0, 4) == "0");
    auto empty = take(connection.execute_params("SELECT ?", {""}));
    REQUIRE(empty.get_string(0, 0) == "");
    REQUIRE_FALSE(connection.execute_params("SELECT $1", {}));
    REQUIRE_FALSE(connection.execute_params("SELECT 1", {"unused"}));
    REQUIRE_FALSE(connection.execute("SELECT 1; SELECT 2"));
    REQUIRE_FALSE(connection.execute_params("SELECT ?; SELECT ?", {"a", "b"}));
    connection.set_max_prepared_statements(2);
    take(connection.execute_prepared("SELECT $1", {"one"}));
    take(connection.execute_prepared("SELECT $1 + 1", {"1"}));
    take(connection.execute_prepared("SELECT $1", {"two"}));
    take(connection.execute_prepared("SELECT $1 + 2", {"2"}));
    REQUIRE(connection.prepared_statement_count() == 2);
    REQUIRE(values.get_value(0, 0) == "second");
    auto moved = std::move(connection);
    REQUIRE_FALSE(connection.is_open()); // NOLINT(bugprone-use-after-move)
    REQUIRE(moved.prepared_statement_count() == 2);
    take(moved.execute_prepared("SELECT $1", {"after move"}));
    moved.set_max_prepared_statements(0);
    REQUIRE(moved.prepared_statement_count() == 0);
    take(moved.execute_prepared("SELECT $1", {"uncached"}));
    REQUIRE(moved.prepared_statement_count() == 0);
    take(moved.execute("DROP PROCEDURE IF EXISTS mysql_test_results"));
    take(moved.execute("CREATE PROCEDURE mysql_test_results() BEGIN SELECT 1; SELECT 2; END"));
    REQUIRE_FALSE(moved.execute("CALL mysql_test_results()"));
    take(moved.execute("SELECT 1"));
    REQUIRE_FALSE(moved.execute_prepared("CALL mysql_test_results()", {}));
    take(moved.execute_prepared("SELECT 1", {}));
    take(moved.execute("DROP PROCEDURE mysql_test_results"));
    bool worker_succeeded = false;
    std::thread worker{[connection = std::move(moved), &worker_succeeded]() mutable {
        auto selected = connection.execute_prepared("SELECT $1", {"worker"});
        worker_succeeded = selected && selected->get_value(0, 0) == "worker";
    }};
    worker.join();
    REQUIRE(worker_succeeded);
    REQUIRE_FALSE(moved.execute("SELECT 1")); // NOLINT(bugprone-use-after-move)
}

TEST_CASE("MySQL repository, typed queries, generated values and transactions",
          "[mysql][integration]") {
    asterorm::pool_config cfg;
    cfg.conninfo = conninfo();
    cfg.min_size = 1;
    cfg.max_size = 1;
    asterorm::connection_pool<asterorm::mysql::driver> pool{asterorm::mysql::driver{}, cfg};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    db.with_connection([](auto& connection) {
        take(connection.execute("DROP TABLE IF EXISTS mysql_users"));
        take(connection.execute("DROP TABLE IF EXISTS mysql_plain"));
        take(connection.execute("DROP TABLE IF EXISTS mysql_composite"));
        take(connection.execute("DROP TABLE IF EXISTS mysql_generated"));
        take(connection.execute("CREATE TABLE mysql_composite (owner INT, item INT, label TEXT, "
                                "PRIMARY KEY(owner, item)) ENGINE=InnoDB"));
        take(connection.execute(
            "CREATE TABLE mysql_generated (id BIGINT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB"));
        take(connection.execute(
            "CREATE TABLE mysql_users (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT "
            "NULL UNIQUE, active BOOLEAN NOT NULL, note TEXT NULL, version BIGINT NOT NULL DEFAULT "
            "1, created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB"));
        take(connection.execute(
            "CREATE TABLE mysql_plain (id INT PRIMARY KEY, label TEXT NOT NULL) ENGINE=InnoDB"));
        return asterorm::result<void>{};
    });

    MysqlUser alice{.name = "Alice"};
    take(repo.insert(alice));
    REQUIRE(alice.id.has_value());
    REQUIRE(alice.created.has_value());
    auto loaded = take(repo.find<MysqlUser>(*alice.id));
    REQUIRE(loaded.name == "Alice");
    REQUIRE(loaded.active);
    REQUIRE_FALSE(loaded.note);
    auto stale = loaded;
    loaded.name = "Alice changed";
    loaded.active = false;
    loaded.note = "t";
    take(repo.update(loaded));
    REQUIRE(loaded.version == 2);
    REQUIRE_FALSE(loaded.active);
    REQUIRE(loaded.note == "t");
    auto stale_write = repo.update(stale);
    REQUIRE_FALSE(stale_write);
    REQUIRE(stale_write.error().kind == asterorm::db_error_kind::stale_write);
    take(repo.patch(loaded, {"note"}));
    REQUIRE(take(repo.find<MysqlUser>(*loaded.id)).version == 3);
    loaded = take(repo.find<MysqlUser>(*loaded.id));
    loaded.name = "Upserted";
    take(repo.upsert(loaded));
    REQUIRE(loaded.name == "Upserted");
    REQUIRE(loaded.version == 4);
    MysqlUser duplicate{.name = "Upserted"};
    auto duplicate_insert = repo.insert(duplicate);
    REQUIRE_FALSE(duplicate_insert);
    REQUIRE(duplicate_insert.error().kind == asterorm::db_error_kind::constraint_violation);
    REQUIRE_FALSE(duplicate.id);
    // A conflicting alternate unique key must not silently update a different PK.
    MysqlUser wrong_key{.id = 99999, .name = "Upserted", .note = "must roll back"};
    REQUIRE_FALSE(repo.upsert(wrong_key));
    REQUIRE(take(repo.find<MysqlUser>(*loaded.id)).note == "t");
    REQUIRE_FALSE(repo.upsert(duplicate)); // Missing explicit PK is rejected before writing.

    std::vector<MysqlUser> batch{{.name = "Bob"}, {.name = "Carol"}};
    REQUIRE(take(repo.insert_many(batch)) == 2);
    REQUIRE_FALSE(batch[0].id);
    REQUIRE_FALSE(repo.upsert_many(batch));
    REQUIRE(take(repo.find_all<MysqlUser>()).size() == 3);
    REQUIRE(take(repo.find_all<MysqlUser>({.order_by = {{"id", true}}, .offset = 1})).size() == 2);
    REQUIRE(take(repo.find_by<MysqlUser>(asterorm::sql::col("active") == asterorm::sql::val(true)))
                .size() == 2);
    auto projected = take(
        (repo.select_projection<MysqlUser, std::tuple<std::string, bool>>({"name", "active"})));
    REQUIRE(projected.size() == 3);
    namespace typed = asterorm::sql::typed;
    auto active = take(typed::query(
        db, typed::select<MysqlUser>().where(typed::col<&MysqlUser::active> == typed::val(true))));
    REQUIRE(active.size() == 2);
    struct Count {
        std::int64_t value;
    };
    auto counts =
        take(typed::query(db, typed::select_cols<Count>(typed::count(typed::col<&MysqlUser::id>))));
    REQUIRE(counts.front().value == 3);

    auto tuple_rows = take(typed::query(db, typed::select_cols(typed::col<&MysqlUser::name>,
                                                               typed::col<&MysqlUser::active>,
                                                               typed::col<&MysqlUser::note>)
                                                .order_by(typed::col<&MysqlUser::name>)));
    REQUIRE(tuple_rows.size() == 3);
    REQUIRE(std::get<0>(tuple_rows[0]) == "Bob");
    REQUIRE(std::get<1>(tuple_rows[0]));
    REQUIRE_FALSE(std::get<2>(tuple_rows[0]));
    REQUIRE(std::get<0>(tuple_rows[2]) == "Upserted");
    REQUIRE_FALSE(std::get<1>(tuple_rows[2]));
    REQUIRE(std::get<2>(tuple_rows[2]) == std::optional<std::string>{"t"});

    auto tuple_stats = take(typed::query(
        db, typed::select_cols(typed::count_all(), typed::count(typed::col<&MysqlUser::note>),
                               typed::sum(typed::col<&MysqlUser::version>),
                               typed::avg(typed::col<&MysqlUser::version>))));
    REQUIRE(tuple_stats.size() == 1);
    REQUIRE(tuple_stats.front() ==
            std::tuple<std::int64_t, std::int64_t, std::int64_t, double>{3, 1, 6, 2.0});

    MysqlComposite composite{.owner = 1, .item = 2, .label = "composite"};
    take(repo.insert(composite));
    REQUIRE(take(repo.find<MysqlComposite>(std::make_tuple(1, 2))).label == "composite");
    composite.label = "changed";
    take(repo.update(composite));
    take(repo.upsert(composite));
    REQUIRE(take(repo.find<MysqlComposite>(std::make_tuple(1, 2))).label == "changed");
    take(repo.erase<MysqlComposite>(std::make_tuple(1, 2)));
    MysqlGenerated generated;
    take(repo.insert(generated));
    REQUIRE(generated.id.has_value());
    REQUIRE(take(repo.find<MysqlGenerated>(*generated.id)).id == generated.id);

    MysqlUser exception_user{.name = "exception rollback"};
    db.set_observer([](const asterorm::query_event& event) {
        if (event.sql.starts_with("SELECT")) {
            throw std::runtime_error("refresh observer failed");
        }
    });
    REQUIRE_THROWS_AS(repo.insert(exception_user), std::runtime_error);
    db.set_observer({});
    REQUIRE(take(repo.find_all<MysqlUser>()).size() == 3);

    MysqlPlain plain{.id = 10, .label = "before"};
    take(repo.insert(plain));
    take(repo.update(plain)); // Unchanged UPDATE still reports a matched row.
    plain.label = "after";
    take(repo.update(plain));
    REQUIRE(take(repo.find<MysqlPlain>(10)).label == "after");
    plain.label = "const update";
    take(repo.update(std::as_const(plain)));
    take(repo.upsert(plain));
    REQUIRE(take(repo.find<MysqlPlain>(10)).label == "const update");
    take(repo.erase<MysqlPlain>(10));

    {
        auto transaction = take(db.begin({.isolation = asterorm::isolation_level::read_committed}));
        take(db.savepoint("before insert"));
        MysqlUser transient{.name = "transient"};
        take(repo.insert(transient));
        take(db.rollback_to_savepoint("before insert"));
        take(db.release_savepoint("before insert"));
        take(transaction.commit());
    }
    REQUIRE(take(repo.find_all<MysqlUser>()).size() == 3);
    {
        auto transaction = take(db.begin());
        MysqlUser transient{.name = "rollback"};
        take(repo.insert(transient));
    }
    REQUIRE(take(repo.find_all<MysqlUser>()).size() == 3);
    {
        auto transaction = take(db.begin({.read_only = true}));
        MysqlUser forbidden{.name = "read-only"};
        REQUIRE_FALSE(repo.insert(forbidden));
        take(transaction.rollback());
    }
    REQUIRE_FALSE(db.begin({.deferrable = true}));
    auto transaction = take(db.begin());
    take(transaction.commit());
    asterorm::schema_migrator migrator{db};
    REQUIRE_FALSE(migrator.apply({}));
    take(repo.erase<MysqlUser>(*alice.id));
    REQUIRE(take(repo.find_all<MysqlUser>()).size() == 2);
}

TEST_CASE("MySQL timestamps use UTC for binding and hydration", "[mysql][integration]") {
    auto connection = take(asterorm::mysql::driver{}.connect(conninfo()));
    take(connection.execute("DROP TABLE IF EXISTS mysql_timestamps"));
    take(connection.execute(
        "CREATE TABLE mysql_timestamps (value TIMESTAMP(6) NOT NULL) ENGINE=InnoDB"));
    const auto timezone = take(connection.execute("SELECT @@SESSION.time_zone"));
    REQUIRE(timezone.get_value(0, 0) == "+00:00");

    using namespace std::chrono;
    const system_clock::time_point expected{seconds{1767225600} + microseconds{123456}};
    take(connection.execute_params("INSERT INTO mysql_timestamps VALUES ($1)",
                                   {asterorm::encode_parameter(expected)}));
    auto selected = take(connection.execute_prepared("SELECT value FROM mysql_timestamps", {}));
    REQUIRE(selected.get_value(0, 0) == "2026-01-01 00:00:00.123456");
    system_clock::time_point hydrated;
    take(asterorm::decode(selected.get_string(0, 0), hydrated));
    REQUIRE(hydrated == expected);
    take(connection.execute("DROP TABLE mysql_timestamps"));
}

TEST_CASE("MySQL deadlock victim cannot report a successful commit", "[mysql][integration]") {
    asterorm::pool_config cfg;
    cfg.conninfo = conninfo();
    cfg.min_size = 1;
    cfg.max_size = 1;
    asterorm::connection_pool<asterorm::mysql::driver> first_pool{asterorm::mysql::driver{}, cfg};
    asterorm::connection_pool<asterorm::mysql::driver> second_pool{asterorm::mysql::driver{}, cfg};
    asterorm::session first_db{first_pool};
    asterorm::session second_db{second_pool};
    first_db.with_connection([](auto& connection) {
        take(connection.execute("DROP TABLE IF EXISTS mysql_deadlock"));
        take(connection.execute(
            "CREATE TABLE mysql_deadlock (id INT PRIMARY KEY, value INT NOT NULL) "
            "ENGINE=InnoDB"));
        take(connection.execute("INSERT INTO mysql_deadlock VALUES (1, 0), (2, 0)"));
        return asterorm::result<void>{};
    });

    auto update_row = [](auto& database, int id) {
        return database.with_connection([&](auto& connection) {
            return connection.execute_params(
                "UPDATE mysql_deadlock SET value = value + 1 WHERE id = $1",
                {asterorm::encode_parameter(id)});
        });
    };
    auto first_transaction = take(first_db.begin());
    auto second_transaction = take(second_db.begin());
    take(update_row(first_db, 1));
    take(update_row(second_db, 2));

    auto first_pending = std::async(std::launch::async, [&] { return update_row(first_db, 2); });
    auto second_contended = update_row(second_db, 1);
    auto first_contended = first_pending.get();
    REQUIRE(first_contended.has_value() != second_contended.has_value());
    auto reject_invalidated_work = [](auto& database) {
        auto escaped_write = database.with_connection([](auto& connection) {
            return connection.execute("INSERT INTO mysql_deadlock VALUES (3, 1000)");
        });
        REQUIRE_FALSE(escaped_write);
        REQUIRE(escaped_write.error().message.contains("invalidated"));

        auto native_query = database.template native_scalar<int>("SELECT 42");
        REQUIRE_FALSE(native_query);
        REQUIRE(native_query.error().message.contains("invalidated"));

        asterorm::repository repo{database};
        MysqlUser forbidden{.name = "after deadlock"};
        auto repository_write = repo.insert(forbidden);
        REQUIRE_FALSE(repository_write);
        REQUIRE(repository_write.error().message.contains("invalidated"));
    };
    if (!first_contended) {
        REQUIRE(first_contended.error().kind == asterorm::db_error_kind::deadlock_detected);
        REQUIRE_FALSE(first_transaction.commit());
        reject_invalidated_work(first_db);
        take(first_transaction.rollback());
        take(second_transaction.commit());
    } else {
        REQUIRE(second_contended.error().kind == asterorm::db_error_kind::deadlock_detected);
        REQUIRE_FALSE(second_transaction.commit());
        reject_invalidated_work(second_db);
        take(second_transaction.rollback());
        take(first_transaction.commit());
    }

    const auto total =
        take(first_db.native_scalar<std::int64_t>("SELECT SUM(value) FROM mysql_deadlock"));
    REQUIRE(total == 2);
    const auto count =
        take(first_db.native_scalar<std::int64_t>("SELECT COUNT(*) FROM mysql_deadlock"));
    REQUIRE(count == 2);
    first_db.with_connection([](auto& connection) {
        take(connection.execute("DROP TABLE mysql_deadlock"));
        return asterorm::result<void>{};
    });
}
