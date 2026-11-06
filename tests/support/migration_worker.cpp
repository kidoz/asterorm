// Subprocess fixture for PostgreSQL migration serialization; not an installed tool.
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "asterorm/migration.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"

namespace {
template <typename Value> Value take(asterorm::result<Value> result) {
    if (!result) {
        throw std::runtime_error(result.error().sqlstate + ": " + result.error().message);
    }
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*result);
    }
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 4) {
            throw std::runtime_error("Expected schema, operation, and worker label");
        }
        const std::string schema{argv[1]};
        const std::string operation{argv[2]};
        if (schema.empty() || schema.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_") !=
                                  std::string::npos) {
            throw std::runtime_error("Invalid fixture schema");
        }
        const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
        asterorm::pool_config config;
        config.conninfo = configured ? configured
                                     : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                       "password=orm_test sslmode=disable";
        config.max_size = 1;
        config.acquire_timeout = std::chrono::milliseconds{100};
        asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
        asterorm::session db{pool};
        take(db.with_connection(
            [](auto& connection) { return connection.execute("SET statement_timeout = '5s'"); }));
        take(db.native_scalar<std::string>("SELECT set_config('application_name', $1, false)",
                                           schema + "_" + argv[3]));
        const auto execute = [&](const std::string& sql) {
            take(db.with_connection([&](auto& connection) { return connection.execute(sql); }));
        };
        if (operation == "cleanup") {
            execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            return 0;
        }
        if (operation == "gate") {
            execute("CREATE SCHEMA " + schema);
            // Publicly reserved bigint key "ASTERORM", independent of schema/name.
            take(db.with_advisory_lock(
                0x41535445524f524d, [&](auto& locked) -> asterorm::result<void> {
                    std::cout << "ready" << '\n' << std::flush;
                    std::string command;
                    if (!(std::cin >> command) || command != "wait") {
                        throw std::runtime_error("Expected wait command");
                    }
                    const auto deadline =
                        std::chrono::steady_clock::now() + std::chrono::seconds{5};
                    while (true) {
                        auto waiting = take(locked.template native_scalar<int>(
                            "SELECT count(*)::int FROM pg_catalog.pg_locks AS locks "
                            "JOIN pg_catalog.pg_stat_activity AS activity ON activity.pid = "
                            "locks.pid "
                            "WHERE locks.locktype = 'advisory' AND NOT locks.granted "
                            "AND activity.application_name IN ($1, $2)",
                            schema + "_one", schema + "_two"));
                        if (waiting == 2) {
                            break;
                        }
                        if (std::chrono::steady_clock::now() >= deadline) {
                            throw std::runtime_error(
                                "Both workers did not wait for the migration lock");
                        }
                        std::this_thread::yield();
                    }
                    auto absent = take(locked.template native_scalar<bool>(
                        "SELECT to_regclass($1) IS NULL", schema + ".history"));
                    if (!absent.value_or(false)) {
                        throw std::runtime_error("History was created before acquiring the lock");
                    }
                    std::cout << "blocked" << '\n' << std::flush;
                    if (!(std::cin >> command) || command != "release") {
                        throw std::runtime_error("Expected release command");
                    }
                    return {};
                }));
            return 0;
        }
        const std::vector<asterorm::migration> migrations{
            {
                .version = 1,
                .name = "create items",
                .up_sql = "CREATE TABLE " + schema + ".items (id INTEGER PRIMARY KEY)",
                .down_sql = "DROP TABLE " + schema + ".items",
            },
            {
                .version = 2,
                .name = "add value",
                .up_sql = "ALTER TABLE " + schema + ".items ADD COLUMN value TEXT",
                .down_sql = "ALTER TABLE " + schema + ".items DROP COLUMN value",
            },
        };
        asterorm::schema_migrator migrator{db, schema + ".history"};
        if (operation == "apply" || operation == "rollback") {
            const auto status = operation == "apply" ? take(migrator.apply(migrations))
                                                     : take(migrator.rollback_to(0, migrations));
            std::cout << status.current_version << ' ' << status.applied_count << '\n'
                      << std::flush;
        } else if (operation == "current") {
            std::cout << take(migrator.current_version()) << '\n' << std::flush;
        } else if (operation == "status") {
            const auto version = take(migrator.current_version());
            const auto count =
                take(db.native_scalar<int>("SELECT count(*)::int FROM " + schema + ".history"));
            const auto exists = take(
                db.native_scalar<bool>("SELECT to_regclass($1) IS NOT NULL", schema + ".items"));
            std::cout << version << ' ' << count.value_or(-1) << ' ' << exists.value_or(false)
                      << '\n'
                      << std::flush;
        } else {
            throw std::runtime_error("Unknown worker operation");
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
