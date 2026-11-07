#include <concepts>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"
#include "asterorm/sqlite/driver.hpp"

namespace {
template <typename Value> Value take(asterorm::result<Value> outcome) {
    if (!outcome) {
        throw std::runtime_error("Session inventory operation failed");
    }
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}
void check(bool condition) {
    if (!condition) {
        throw std::runtime_error("Session inventory invariant failed");
    }
}
template <typename Driver> void inspect(Driver driver, std::string settings, bool postgres) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    check(!db.savepoint("without_root"));
    check(pool.stats().total == 0);
    auto execute = [&](const char* statement) {
        take(db.with_connection([&](auto& connection) { return connection.execute(statement); }));
    };
    auto count = [&] {
        return take(db.template native_scalar<int>("SELECT count(*) FROM inventory_rows")).value();
    };
    execute("CREATE TEMP TABLE inventory_rows(id INTEGER PRIMARY KEY,note TEXT UNIQUE)");
    execute("INSERT INTO inventory_rows VALUES(1,'reference')");
    {
        auto root = take(db.begin());
        take(db.savepoint("outer_boundary"));
        execute("INSERT INTO inventory_rows VALUES(2,'test')");
        take(db.savepoint("inner_boundary"));
        execute("INSERT INTO inventory_rows VALUES(3,'nested')");
        take(db.rollback_to_savepoint("outer_boundary"));
        check(count() == 1);
        take(db.rollback_to_savepoint("outer_boundary")); // ROLLBACK TO preserves its target
        take(db.release_savepoint("outer_boundary"));
        check(!db.rollback_to_savepoint("outer_boundary"));
        take(root.rollback());
    }
    check(count() == 1);
    {
        auto root = take(db.begin());
        take(db.savepoint("recover_error"));
        auto duplicate = db.with_connection([](auto& connection) {
            return connection.execute("INSERT INTO inventory_rows VALUES(1,'duplicate')");
        });
        check(!duplicate);
        take(db.rollback_to_savepoint("recover_error"));
        check(count() == 1);
        take(db.release_savepoint("recover_error"));
        take(root.rollback());
    }
    {
        auto root = take(db.begin());
        take(db.savepoint("ddl_boundary"));
        execute("CREATE TEMP TABLE inventory_transient(id INTEGER)");
        take(db.rollback_to_savepoint("ddl_boundary"));
        auto missing = db.template native_scalar<int>("SELECT count(*) FROM inventory_transient");
        check(!missing);
        take(root.rollback());
    }
    {
        auto old_root = take(db.begin());
        take(db.savepoint("does_not_block_commit"));
        execute("INSERT INTO inventory_rows VALUES(2,'escaped')");
        take(db.commit_transaction()); // savepoints alone do not protect the outer boundary
        check(count() == 2);
        auto fresh_root = take(db.begin());
        check(!old_root.rollback());
        check(db.has_active_transaction());
        take(fresh_root.rollback());
    }
    execute("DELETE FROM inventory_rows WHERE id=2");
    int observer_events = 0;
    db.set_observer([&](const auto&) { ++observer_events; });
    {
        auto root = take(db.begin());
        take(db.savepoint("control_events"));
        take(db.rollback_to_savepoint("control_events"));
        take(db.release_savepoint("control_events"));
        take(root.rollback());
    }
    db.set_observer({});
    check(observer_events == 0);
    execute(postgres ? "CREATE TEMP TABLE inventory_ids(id SERIAL PRIMARY KEY)"
                     : "CREATE TEMP TABLE inventory_ids(id INTEGER PRIMARY KEY AUTOINCREMENT)");
    int first_id = 0;
    {
        auto root = take(db.begin());
        first_id = take(db.template native_scalar<int>(
                            "INSERT INTO inventory_ids DEFAULT VALUES RETURNING id"))
                       .value();
        take(root.rollback());
    }
    const auto next_id = take(db.template native_scalar<int>(
                                  "INSERT INTO inventory_ids DEFAULT VALUES RETURNING id"))
                             .value();
    check(first_id == 1 && next_id == (postgres ? 2 : 1));
    const auto version = take(db.template native_scalar<std::string>(
                                  postgres ? "SHOW server_version" : "SELECT sqlite_version()"))
                             .value();
    using connection_type = decltype(db)::connection_type;
    constexpr bool idle_probe = requires(const connection_type& connection) {
        { connection.is_transaction_idle() } noexcept -> std::same_as<bool>;
    };
    std::cout << "backend=" << (postgres ? "postgres" : "sqlite") << '\n'
              << "version=" << version << '\n'
              << "idle_savepoint_rejected_before_lease=1\n"
              << "rollback_to_preserves_boundary=1\n"
              << "release_removes_boundary=1\n"
              << "ordinary_constraint_error_recovered=1\n"
              << "temporary_ddl_rolled_back=1\n"
              << "savepoint_does_not_block_managed_commit=1\n"
              << "stale_guard_cannot_rollback_fresh_transaction=1\n"
              << "transaction_control_observer_events=" << observer_events << '\n'
              << "nonthrowing_connection_close="
              << noexcept(std::declval<connection_type&>().close()) << '\n'
              << "nonthrowing_native_idle_probe=" << idle_probe << '\n'
              << "rolled_back_id=" << first_id << '\n'
              << "next_id=" << next_id << '\n';
}
} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        return 2;
    }
    try {
        if (std::string{argv[1]} == "sqlite") {
            inspect(asterorm::sqlite::driver{}, ":memory:", false);
        } else if (std::string{argv[1]} == "postgres") {
            const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
            inspect(asterorm::pg::driver{},
                    configured ? configured
                               : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                 "password=orm_test sslmode=disable",
                    true);
        } else {
            return 2;
        }
    } catch (const std::exception&) {
        std::cerr << "Session inventory probe failed; connection settings omitted\n";
        return 1;
    }
}
