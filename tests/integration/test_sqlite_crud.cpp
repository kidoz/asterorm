#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>

#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/sqlite/driver.hpp"

namespace {

struct sqlite_user {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
    std::optional<std::string> deleted_at;
};

} // namespace

template <> struct asterorm::entity_traits<sqlite_user> {
    static constexpr const char* table = "asterorm_users";
    static constexpr auto primary_key = asterorm::pk<&sqlite_user::id>("id");

    static constexpr auto columns =
        std::make_tuple(asterorm::column<&sqlite_user::id>("id", asterorm::generated::by_default),
                        asterorm::column<&sqlite_user::email>("email"),
                        asterorm::column<&sqlite_user::name>("name"),
                        asterorm::column<&sqlite_user::active>("active"),
                        asterorm::column<&sqlite_user::deleted_at>("deleted_at"));
};

namespace {

asterorm::pool_config make_memory_cfg() {
    asterorm::pool_config cfg;
    // Per-connection in-memory database; min_size=1 keeps a single shared
    // connection so DDL and DML observe the same database.
    cfg.conninfo = ":memory:";
    cfg.min_size = 1;
    cfg.max_size = 1;
    return cfg;
}

void create_schema(asterorm::connection_pool<asterorm::sqlite::driver>& pool) {
    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    auto& conn = **lease;
    // active is INTEGER to exercise the SQLite bool->1/0 bind translation
    // on a column with NUMERIC affinity.
    REQUIRE(conn.execute("CREATE TABLE asterorm_users ("
                         "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                         "email TEXT NOT NULL, "
                         "name TEXT NOT NULL, "
                         "active INTEGER NOT NULL, "
                         "deleted_at TEXT)")
                .has_value());
}

} // namespace

TEST_CASE("SQLite Integration: CRUD round-trip", "[sqlite][crud]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);

    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository repo{db};

    SECTION("Insert assigns RETURNING id and find round-trips") {
        sqlite_user u{.email = "alice@example.com", .name = "Alice", .active = true};
        REQUIRE(repo.insert(u).has_value());
        REQUIRE(u.id.has_value());
        REQUIRE(*u.id > 0);

        auto loaded = repo.find<sqlite_user>(*u.id);
        REQUIRE(loaded.has_value());
        REQUIRE(loaded->email == "alice@example.com");
        REQUIRE(loaded->name == "Alice");
        REQUIRE(loaded->active == true);

        loaded->name = "Alice Cooper";
        loaded->active = false;
        REQUIRE(repo.update(*loaded).has_value());

        auto reloaded = repo.find<sqlite_user>(*u.id);
        REQUIRE(reloaded.has_value());
        REQUIRE(reloaded->name == "Alice Cooper");
        REQUIRE(reloaded->active == false);

        REQUIRE(repo.erase<sqlite_user>(*u.id).has_value());
        REQUIRE_FALSE(repo.find<sqlite_user>(*u.id).has_value());
    }

    SECTION("Native query hydrates into structs and bool binds match INTEGER column") {
        sqlite_user a{.email = "a@x", .name = "A", .active = true};
        sqlite_user b{.email = "b@x", .name = "B", .active = false};
        REQUIRE(repo.insert(a).has_value());
        REQUIRE(repo.insert(b).has_value());

        auto active_rows = db.native<sqlite_user>(
            "SELECT id, email, name, active, deleted_at FROM asterorm_users WHERE active = $1",
            true);
        REQUIRE(active_rows.has_value());
        REQUIRE(active_rows->size() == 1);
        REQUIRE(active_rows->front().email == "a@x");

        auto inactive_rows = db.native<sqlite_user>(
            "SELECT id, email, name, active, deleted_at FROM asterorm_users WHERE active = $1",
            false);
        REQUIRE(inactive_rows.has_value());
        REQUIRE(inactive_rows->size() == 1);
        REQUIRE(inactive_rows->front().email == "b@x");
    }
}

TEST_CASE("SQLite Integration: transaction commit and rollback", "[sqlite][tx]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);

    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository repo{db};

    SECTION("Rolled-back insert is not visible") {
        auto tx = db.begin();
        REQUIRE(tx.has_value());

        sqlite_user u{.email = "ghost@x", .name = "Ghost"};
        REQUIRE(repo.insert(u).has_value());
        REQUIRE(u.id.has_value());

        REQUIRE(tx->rollback().has_value());

        auto found = repo.find<sqlite_user>(*u.id);
        REQUIRE_FALSE(found.has_value());
    }

    SECTION("Committed insert is visible") {
        auto tx = db.begin();
        REQUIRE(tx.has_value());

        sqlite_user u{.email = "ok@x", .name = "OK"};
        REQUIRE(repo.insert(u).has_value());
        REQUIRE(tx->commit().has_value());

        auto found = repo.find<sqlite_user>(*u.id);
        REQUIRE(found.has_value());
        REQUIRE(found->email == "ok@x");
    }

    SECTION("Non-default isolation maps to BEGIN IMMEDIATE on SQLite") {
        asterorm::transaction_options opts;
        opts.isolation = asterorm::isolation_level::serializable;
        auto tx = db.begin(opts);
        REQUIRE(tx.has_value());
        REQUIRE(tx->commit().has_value());
    }

    SECTION("read_only / deferrable are accepted on SQLite") {
        asterorm::transaction_options opts;
        opts.read_only = true;
        opts.deferrable = true;
        auto tx = db.begin(opts);
        REQUIRE(tx.has_value());
        REQUIRE(tx->rollback().has_value());
    }
}

TEST_CASE("SQLite Integration: COPY is not supported on the connection", "[sqlite][copy]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);

    auto lease = pool.acquire();
    REQUIRE(lease.has_value());

    auto in_res = (*lease)->copy_in("INSERT INTO asterorm_users VALUES (1, 'x', 'y', 1)", {"x"});
    REQUIRE_FALSE(in_res.has_value());

    auto out_res = (*lease)->copy_out("SELECT * FROM asterorm_users");
    REQUIRE_FALSE(out_res.has_value());
}

namespace {
struct sqlite_user_summary {
    std::string email;
    std::string name;
};
} // namespace

TEST_CASE("SQLite Integration: typed DSL query() round-trips entities and DTOs",
          "[sqlite][typed]") {
    namespace tsql = asterorm::sql::typed;

    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);

    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository repo{db};

    sqlite_user alice{.email = "alice@example.com", .name = "Alice", .active = true};
    sqlite_user bob{.email = "bob@example.com", .name = "Bob", .active = false};
    REQUIRE(repo.insert(alice).has_value());
    REQUIRE(repo.insert(bob).has_value());

    SECTION("Whole-entity SELECT with typed where + order_by") {
        auto users = tsql::query(db, tsql::select<sqlite_user>()
                                         .where(tsql::col<&sqlite_user::active> == tsql::val(true))
                                         .order_by(tsql::col<&sqlite_user::email>, tsql::asc));
        REQUIRE(users.has_value());
        REQUIRE(users->size() == 1);
        REQUIRE(users->front().email == "alice@example.com");
    }

    SECTION("Struct projection via select_cols<DTO>") {
        auto rows =
            tsql::query(db, tsql::select_cols<sqlite_user_summary>(tsql::col<&sqlite_user::email>,
                                                                   tsql::col<&sqlite_user::name>)
                                .where(tsql::col<&sqlite_user::active> == tsql::val(false)));
        REQUIRE(rows.has_value());
        REQUIRE(rows->size() == 1);
        REQUIRE(rows->front().email == "bob@example.com");
        REQUIRE(rows->front().name == "Bob");
    }

    SECTION("Aggregate projection: count + max into a stats DTO") {
        struct user_stats {
            std::int64_t total;
            std::int64_t active_count;
            std::string max_email;
        };

        // Insert one more row so totals are interesting.
        sqlite_user carol{.email = "carol@example.com", .name = "Carol", .active = true};
        REQUIRE(repo.insert(carol).has_value());

        auto stats =
            tsql::query(db, tsql::select_cols<user_stats>(
                                tsql::count_all(), tsql::count(tsql::col<&sqlite_user::active>),
                                tsql::max(tsql::col<&sqlite_user::email>)));
        REQUIRE(stats.has_value());
        REQUIRE(stats->size() == 1);
        REQUIRE(stats->front().total == 3);
        REQUIRE(stats->front().active_count == 3);
        REQUIRE(stats->front().max_email == "carol@example.com");
    }

    SECTION("group_by with aggregate emits expected counts per active value") {
        struct active_count_row {
            bool is_active;
            std::int64_t count;
        };

        auto rows = tsql::query(db, tsql::select_cols<active_count_row>(
                                        tsql::col<&sqlite_user::active>, tsql::count_all())
                                        .group_by(tsql::col<&sqlite_user::active>)
                                        .order_by(tsql::col<&sqlite_user::active>, tsql::asc));
        REQUIRE(rows.has_value());
        REQUIRE(rows->size() == 2);
        REQUIRE(rows->at(0).is_active == false);
        REQUIRE(rows->at(0).count == 1);
        REQUIRE(rows->at(1).is_active == true);
        REQUIRE(rows->at(1).count == 1);
    }

    SECTION("is_not_null filters out the rows we expect") {
        // No row has deleted_at populated yet, so is_null returns both, is_not_null returns 0.
        auto null_rows = tsql::query(db, tsql::select<sqlite_user>().where(
                                             tsql::is_null(tsql::col<&sqlite_user::deleted_at>)));
        REQUIRE(null_rows.has_value());
        REQUIRE(null_rows->size() == 2);

        auto not_null_rows = tsql::query(db, tsql::select<sqlite_user>().where(tsql::is_not_null(
                                                 tsql::col<&sqlite_user::deleted_at>)));
        REQUIRE(not_null_rows.has_value());
        REQUIRE(not_null_rows->empty());
    }
}

TEST_CASE("SQLite Integration: bind rejects extra parameters", "[sqlite][bind]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);

    auto lease = pool.acquire();
    REQUIRE(lease.has_value());

    // SQL has 1 placeholder; pass 2 parameters.
    auto res = (*lease)->execute_prepared(
        "SELECT $1", {std::optional<std::string>{"a"}, std::optional<std::string>{"b"}});
    REQUIRE_FALSE(res.has_value());
    REQUIRE(res.error().message.find("placeholders") != std::string::npos);
}

TEST_CASE("SQLite Integration: bounded prepared statement cache evicts LRU", "[sqlite][cache]") {
    asterorm::pool_config cfg = make_memory_cfg();
    cfg.prepared_statement_cache_size = 2;

    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, cfg};
    create_schema(pool);

    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    auto& conn = **lease;

    REQUIRE(conn.execute_prepared("SELECT 1", {}).has_value());
    REQUIRE(conn.execute_prepared("SELECT 2", {}).has_value());
    REQUIRE(conn.prepared_statement_count() == 2);

    REQUIRE(conn.execute_prepared("SELECT 3", {}).has_value());
    REQUIRE(conn.prepared_statement_count() == 2);

    conn.clear_prepared_statement_cache();
    REQUIRE(conn.prepared_statement_count() == 0);
}
