#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "asterorm/migration.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/sqlite/driver.hpp"
#include "support/bulk_predicate.hpp"
#include "support/cte_query.hpp"
#include "support/recursive_query.hpp"
#include "support/set_query.hpp"
#include "support/transactional_session.hpp"
#include "support/typed_query_observer.hpp"

TEST_CASE("SQLite Integration: typed query observation", "[sqlite][typed-query][observer]") {
    asterorm::test::check_typed_query_observer(asterorm::sqlite::driver{}, ":memory:");
}

TEST_CASE("SQLite Integration: advisory scopes reject before leasing", "[sqlite][advisory]") {
    asterorm::pool_config config;
    config.conninfo = ":memory:";
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, config};
    asterorm::session db{pool};
    bool invoked = false;
    auto result = db.with_advisory_lock(1, [&](auto&) -> asterorm::result<void> {
        invoked = true;
        return {};
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(invoked);
    REQUIRE(pool.stats().total == 0);
}

struct sqlite_lock_row {
    int id{};
};
template <> struct asterorm::entity_traits<sqlite_lock_row> {
    static constexpr const char* table = "never_opened";
    static constexpr auto columns = std::make_tuple(asterorm::column<&sqlite_lock_row::id>("id"));
};

TEST_CASE("SQLite Integration: SELECT locking rejects before leasing", "[sqlite][select-locking]") {
    STATIC_REQUIRE(asterorm::sql::dialect_for<asterorm::sqlite::connection>() ==
                   asterorm::sql::dialect::sqlite);
    asterorm::pool_config config;
    config.conninfo = ":memory:";
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, config};
    asterorm::session db{pool};
    bool observed = false;
    db.set_observer([&](const asterorm::query_event&) { observed = true; });
    auto ast = asterorm::sql::select({"id"}).from("never_opened").for_update().build();
    auto rejected =
        asterorm::sql::compiler{asterorm::sql::dialect_for_session<decltype(db)>()}.compile(ast);
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(rejected.error().kind == asterorm::db_error_kind::parse_failed);
    auto typed_rejected = asterorm::sql::typed::query(
        db, asterorm::sql::typed::select<sqlite_lock_row>().for_update());
    REQUIRE_FALSE(typed_rejected.has_value());
    REQUIRE(typed_rejected.error().kind == asterorm::db_error_kind::parse_failed);
    REQUIRE(pool.stats().total == 0);
    REQUIRE_FALSE(observed);
}

namespace {

struct sqlite_user {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
    std::optional<std::string> deleted_at;
};

struct sqlite_lenient_row {
    std::optional<int> id;
    int omitted;
};

struct sqlite_locked_user {
    std::optional<int> id;
    std::string name;
    int version{0};
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

template <> struct asterorm::entity_traits<sqlite_lenient_row> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&sqlite_lenient_row::id>("id"),
                        asterorm::column<&sqlite_lenient_row::omitted>("omitted"));
};

template <> struct asterorm::entity_traits<sqlite_locked_user> {
    static constexpr const char* table = "asterorm_locked_users";
    static constexpr auto primary_key = asterorm::pk<&sqlite_locked_user::id>("id");
    static constexpr auto version_column =
        asterorm::version<&sqlite_locked_user::version>("version");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&sqlite_locked_user::id>("id", asterorm::generated::by_default),
        asterorm::column<&sqlite_locked_user::name>("name"),
        asterorm::column<&sqlite_locked_user::version>("version"));
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
    REQUIRE(conn.execute("CREATE TABLE asterorm_locked_users ("
                         "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                         "name TEXT NOT NULL, "
                         "version INTEGER NOT NULL DEFAULT 0)")
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

    SECTION("Inferred tuple projection preserves booleans and nullable columns") {
        bob.deleted_at = "2026-11-06";
        REQUIRE(repo.update(bob).has_value());
        auto rows = tsql::query(db, tsql::select_cols(tsql::col<&sqlite_user::email>,
                                                      tsql::col<&sqlite_user::active>,
                                                      tsql::col<&sqlite_user::deleted_at>)
                                        .order_by(tsql::col<&sqlite_user::email>));
        REQUIRE(rows.has_value());
        REQUIRE(rows->size() == 2);
        REQUIRE(std::get<0>((*rows)[0]) == "alice@example.com");
        REQUIRE(std::get<1>((*rows)[0]));
        REQUIRE_FALSE(std::get<2>((*rows)[0]));
        REQUIRE(std::get<0>((*rows)[1]) == "bob@example.com");
        REQUIRE_FALSE(std::get<1>((*rows)[1]));
        REQUIRE(std::get<2>((*rows)[1]) == bob.deleted_at);

        auto stats =
            tsql::query(db, tsql::select_cols(tsql::count_all(),
                                              tsql::count(tsql::col<&sqlite_user::deleted_at>),
                                              tsql::sum(tsql::col<&sqlite_user::id>),
                                              tsql::avg(tsql::col<&sqlite_user::id>)));
        REQUIRE(stats.has_value());
        REQUIRE(stats->size() == 1);
        REQUIRE(stats->front() ==
                std::tuple<std::int64_t, std::int64_t, std::int64_t, double>{2, 1, 3, 1.5});
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

TEST_CASE("SQLite Integration: bind validates counts and preserves text", "[sqlite][bind]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);
    asterorm::session<decltype(pool)> db{pool};

    auto missing = db.with_connection(
        [](auto& conn) { return conn.execute_prepared("SELECT COALESCE($1, -99)", {}); });
    REQUIRE_FALSE(missing.has_value());
    REQUIRE(missing.error().message.find("placeholders") != std::string::npos);

    auto text = db.native_scalar<std::string>("SELECT $1", std::string{"t"});
    REQUIRE(text.has_value());
    REQUIRE(text->has_value());
    REQUIRE(**text == "t");

    asterorm::repository repo{db};
    sqlite_user user{.email = "t", .name = "f"};
    REQUIRE(repo.insert(user).has_value());
    auto loaded = repo.find<sqlite_user>(*user.id);
    REQUIRE(loaded.has_value());
    REQUIRE(loaded->email == "t");
    REQUIRE(loaded->name == "f");

    auto invalid_date = db.native_scalar<int>("SELECT $1 IS NULL", asterorm::date{2026, 2, 30});
    REQUIRE_FALSE(invalid_date.has_value());
    REQUIRE(invalid_date.error().kind == asterorm::db_error_kind::parse_failed);
}

TEST_CASE("SQLite Integration: rejects trailing SQL statements", "[sqlite][bind]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};

    asterorm::session<decltype(pool)> db{pool};
    auto result = db.with_connection([](auto& conn) {
        return conn.execute("CREATE TABLE first_table (id INTEGER); "
                            "CREATE TABLE second_table (id INTEGER)");
    });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("trailing SQL") != std::string::npos);

    auto line_comment = db.with_connection(
        [](auto& conn) { return conn.execute("SELECT 1; -- valid trailing comment"); });
    REQUIRE(line_comment.has_value());
    REQUIRE(line_comment->rows() == 1);

    auto block_comment = db.with_connection(
        [](auto& conn) { return conn.execute("SELECT 1; /* valid trailing comment */"); });
    REQUIRE(block_comment.has_value());
    REQUIRE(block_comment->rows() == 1);

    auto statement_after_comment = db.with_connection(
        [](auto& conn) { return conn.execute("SELECT 1; -- comment\nSELECT 2"); });
    REQUIRE_FALSE(statement_after_comment.has_value());
    REQUIRE(statement_after_comment.error().message.find("trailing SQL") != std::string::npos);
}

TEST_CASE("SQLite Integration: bulk writes support generated identifiers", "[sqlite][crud]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);
    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository repo{db};

    SECTION("Bulk insert") {
        const std::vector<sqlite_user> rows{
            {.email = "bulk-a@x", .name = "Bulk A"},
            {.email = "bulk-b@x", .name = "Bulk B"},
        };
        auto inserted = repo.insert_many(rows);
        REQUIRE(inserted.has_value());
        REQUIRE(*inserted == 2);
        auto count = db.native_scalar<int>("SELECT COUNT(*) FROM asterorm_users");
        REQUIRE(count.has_value());
        REQUIRE(**count == 2);
    }

    SECTION("Bulk upsert") {
        std::vector<sqlite_user> rows{
            {.email = "bulk-a@x", .name = "Bulk A"},
            {.email = "bulk-b@x", .name = "Bulk B"},
        };
        auto upserted = repo.upsert_many(rows);
        REQUIRE(upserted.has_value());
        REQUIRE(*upserted == 2);
        REQUIRE(rows[0].id.has_value());
        REQUIRE(rows[1].id.has_value());
    }
}

TEST_CASE("SQLite Integration: all versioned writes check optimistic locks", "[sqlite][crud]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);
    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository repo{db};

    sqlite_locked_user current{.name = "Initial"};
    REQUIRE(repo.insert(current).has_value());
    auto stale = repo.find<sqlite_locked_user>(*current.id).value();

    const auto& const_current = current;
    REQUIRE(repo.update(const_current).has_value());
    current = repo.find<sqlite_locked_user>(*current.id).value();
    REQUIRE(current.version == 1);

    current.name = "Mutable";
    REQUIRE(repo.update(current).has_value());
    REQUIRE(current.version == 2);

    stale.name = "Stale patch";
    auto patched = repo.patch(stale, {"name"});
    REQUIRE_FALSE(patched.has_value());
    REQUIRE(patched.error().kind == asterorm::db_error_kind::stale_write);

    const auto& const_stale = stale;
    auto updated = repo.update(const_stale);
    REQUIRE_FALSE(updated.has_value());
    REQUIRE(updated.error().kind == asterorm::db_error_kind::stale_write);

    auto reloaded = repo.find<sqlite_locked_user>(*current.id);
    REQUIRE(reloaded.has_value());
    REQUIRE(reloaded->name == "Mutable");
    REQUIRE(reloaded->version == 2);
}

TEST_CASE("SQLite Integration: lenient hydration value-initializes omitted fields", "[sqlite]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    create_schema(pool);
    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository repo{db};
    sqlite_user user{.email = "lenient@x", .name = "Lenient"};
    REQUIRE(repo.insert(user).has_value());

    auto rows = db.native_as<sqlite_lenient_row>(asterorm::native_hydration::lenient,
                                                 "SELECT id FROM asterorm_users");
    REQUIRE(rows.has_value());
    REQUIRE(rows->size() == 1);
    REQUIRE(rows->front().omitted == 0);
}

TEST_CASE("SQLite Integration: migrations use SQLite-compatible defaults", "[sqlite][migration]") {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                             make_memory_cfg()};
    asterorm::session<decltype(pool)> db{pool};
    asterorm::schema_migrator migrator{db};
    const std::vector<asterorm::migration> migrations{
        {.version = 1,
         .name = "create items",
         .up_sql = "CREATE TABLE migration_items (id INTEGER PRIMARY KEY)",
         .down_sql = "DROP TABLE migration_items"},
    };

    auto applied = migrator.apply(migrations);
    REQUIRE(applied.has_value());
    REQUIRE(applied->current_version == 1);
    REQUIRE(applied->applied_count == 1);
    auto version = migrator.current_version();
    REQUIRE(version.has_value());
    REQUIRE(*version == 1);
    auto reapplied = migrator.apply(migrations);
    REQUIRE(reapplied.has_value());
    REQUIRE(reapplied->applied_count == 0);
    auto rolled_back = migrator.rollback_to(0, migrations);
    REQUIRE(rolled_back.has_value());
    REQUIRE(rolled_back->current_version == 0);
    REQUIRE(rolled_back->applied_count == 1);
    REQUIRE_FALSE(db.has_active_transaction());
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

TEST_CASE("SQLite Integration: explicit pool timeout rejects before connecting",
          "[sqlite][pool-timeout]") {
    for (std::size_t minimum : {0, 1}) {
        asterorm::pool_config config;
        config.min_size = minimum;
        config.statement_timeout = std::chrono::milliseconds{0};
        asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{},
                                                                 config};
        auto error = pool.warmup_error();
        REQUIRE(error.has_value());
        REQUIRE(error->kind == asterorm::db_error_kind::query_failed);
        REQUIRE(error->message.contains("does not support"));
        REQUIRE_FALSE(pool.acquire().has_value());
        REQUIRE(pool.stats().total == 0);
    }
}

TEST_CASE("SQLite Integration: timeout scopes reject before leasing", "[sqlite][timeout-scope]") {
    asterorm::pool_config config;
    config.min_size = 0;
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, config};
    asterorm::session db{pool};
    bool called = false;
    auto result = db.with_statement_timeout(std::chrono::milliseconds{100},
                                            [&](auto&) -> asterorm::result<void> {
                                                called = true;
                                                return {};
                                            });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(called);
    REQUIRE(pool.stats().total == 0);
}

TEST_CASE("sqlite Integration: bulk predicate mutations", "[sqlite][bulk-dml]") {
    asterorm::test::check_bulk_predicate(asterorm::sqlite::driver{}, ":memory:");
}

TEST_CASE("SQLite transactional testing scope preserves fixtures and fails closed",
          "[sqlite][transactional-testing]") {
    asterorm::test::check_transactional_session(asterorm::sqlite::driver{}, ":memory:");
}

TEST_CASE("SQLite test scope detects native automatic rollback before another statement",
          "[sqlite][transactional-testing]") {
    STATIC_REQUIRE(noexcept(std::declval<asterorm::sqlite::connection&>().close()));
    STATIC_REQUIRE(
        noexcept(std::declval<const asterorm::sqlite::connection&>().is_transaction_idle()));
    asterorm::sqlite::connection empty{nullptr};
    REQUIRE_FALSE(empty.is_transaction_idle());
    asterorm::pool_config config;
    config.conninfo = ":memory:";
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, config};
    asterorm::session db{pool};
    using asterorm::test::scope_take;
    using scope_type = asterorm::testing::transactional_session<decltype(db)>;
    scope_take(db.with_connection([](auto& connection) {
        return connection.execute("CREATE TEMP TABLE rollback_target(id INTEGER)");
    }));
    scope_take(db.with_connection([](auto& connection) {
        return connection.execute(
            "CREATE TEMP TRIGGER reject_write BEFORE INSERT ON rollback_target BEGIN SELECT "
            "RAISE(ROLLBACK,'rollback identity'); END");
    }));
    auto scope = scope_take(scope_type::create(db));
    auto failure = db.with_connection([](auto& connection) {
        return connection.execute("INSERT INTO rollback_target VALUES(1)");
    });
    REQUIRE_FALSE(failure.has_value());
    REQUIRE(failure.error().message.contains("rollback identity"));
    bool called = false;
    auto forbidden = db.with_connection([&](auto& connection) {
        called = true;
        return connection.execute("CREATE TEMP TABLE escaped(id INTEGER)");
    });
    REQUIRE_FALSE(forbidden.has_value());
    REQUIRE_FALSE(called);
    REQUIRE_FALSE(scope.finish().has_value());
    REQUIRE(pool.stats().total == 0);
    REQUIRE_FALSE(db.has_active_transaction());
    REQUIRE_FALSE(scope_type::create(db).has_value());
}

TEST_CASE("SQLite Integration: ordinary CTE execution", "[sqlite][cte]") {
    asterorm::test::check_cte_query(asterorm::sqlite::driver{}, ":memory:");
}

TEST_CASE("SQLite Integration: set query execution", "[sqlite][set-query]") {
    asterorm::test::check_set_query(asterorm::sqlite::driver{}, ":memory:");
}

TEST_CASE("SQLite Integration: recursive query execution", "[sqlite][recursive-query]") {
    asterorm::test::check_recursive_query(asterorm::sqlite::driver{}, ":memory:");
}
