// Audit fixture for the pre-lock/timeout implementation, not a regression suite.
// The migration rendezvous deliberately exposes the current unlocked version read.
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "asterorm/migration.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/sqlite/driver.hpp"

struct audit_row {
    int id{};
};

template <> struct asterorm::entity_traits<audit_row> {
    static constexpr const char* table = "runtime_audit_rows";
    static constexpr auto primary_key = asterorm::pk<&audit_row::id>("id");
    static constexpr auto columns = std::make_tuple(asterorm::column<&audit_row::id>("id"));
};

namespace {
namespace typed = asterorm::sql::typed;

template <typename Value> Value take(asterorm::result<Value> result) {
    if (!result) {
        throw std::runtime_error(result.error().sqlstate + ": " + result.error().message);
    }
    return std::move(*result);
}

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string{message});
    }
}

template <typename Builder> void inspect_builder(std::string_view name) {
    std::cout << name << ": for_update=" << requires(Builder& builder) { builder.for_update(); }
              << " for_share=" << requires(Builder& builder) { builder.for_share(); }
              << " nowait=" << requires(Builder& builder) { builder.nowait(); }
              << " skip_locked=" << requires(Builder& builder) { builder.skip_locked(); } << '\n';
}

template <typename Session, typename Config> void inspect_runtime() {
    auto callback = [](auto&) -> asterorm::result<void> { return {}; };
    std::cout << "runtime: advisory_helper=" << requires(Session& db) {
        db.with_advisory_lock(std::int64_t{1}, callback);
    } << " timeout_helper=" << requires(Session& db) {
        db.with_statement_timeout(std::chrono::milliseconds{10}, callback);
    } << " pool_statement_timeout=" << requires(Config& config) {
        config.statement_timeout;
    } << '\n';
}

template <typename Driver>
void inspect_routing(std::string_view backend, Driver driver, const std::string& conninfo) {
    asterorm::pool_config config;
    config.conninfo = conninfo;
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    take(db.with_connection([](auto& connection) {
        return connection.execute("CREATE TEMPORARY TABLE runtime_audit_rows (id INTEGER)");
    }));
    take(db.with_connection([](auto& connection) {
        return connection.execute("INSERT INTO runtime_audit_rows VALUES (1)");
    }));

    int events = 0;
    int errors = 0;
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        errors += event.error != nullptr;
    });
    auto native = take(db.template native_scalar<int>("SELECT id FROM runtime_audit_rows"));
    require(native == 1, "unexpected native row");
    std::cout << backend << " native_success: events=" << events << '\n';
    events = 0;
    auto repository =
        take(repo.template find_by<audit_row>(asterorm::sql::col("id") == asterorm::sql::val(1)));
    require(repository.size() == 1 && repository[0].id == 1, "unexpected repository row");
    std::cout << backend << " repository_success: events=" << events << '\n';
    events = 0;
    auto typed_rows = take(typed::query(db, typed::select<audit_row>()));
    require(typed_rows.size() == 1 && typed_rows[0].id == 1, "unexpected typed row");
    std::cout << backend << " typed_success: events=" << events << '\n';
    events = 0;
    take(db.with_connection(
        [](auto& connection) { return connection.execute("DROP TABLE runtime_audit_rows"); }));
    auto native_error = db.template native_scalar<int>("SELECT id FROM runtime_audit_rows");
    require(!native_error, "missing table unexpectedly exists");
    std::cout << backend << " native_error: events=" << events << " errors=" << errors << '\n';
    events = errors = 0;
    auto typed_error = typed::query(db, typed::select<audit_row>());
    require(!typed_error, "typed missing table unexpectedly exists");
    std::cout << backend << " typed_error: events=" << events << " errors=" << errors << '\n';
}

struct schema_cleanup {
    asterorm::pg::connection& connection;
    std::string schema;

    ~schema_cleanup() {
        try {
            (void)connection.execute("ROLLBACK");
            (void)connection.execute("DROP SCHEMA " + schema + " CASCADE");
        } catch (...) {
            std::fputs("audit schema cleanup threw an exception\n", stderr);
        }
    }
};

void inspect_postgres(const std::string& conninfo) {
    auto first = take(asterorm::pg::driver{}.connect(conninfo));
    auto second = take(asterorm::pg::driver{}.connect(conninfo));
    take(first.execute("SET statement_timeout = '5s'"));
    take(second.execute("SET statement_timeout = '5s'"));
    std::cout << "postgres_version=" << take(first.execute("SHOW server_version")).get_value(0, 0)
              << '\n';
    auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string schema = "asterorm_runtime_audit_" + std::to_string(stamp);
    take(first.execute("CREATE SCHEMA " + schema));
    schema_cleanup cleanup{.connection = first, .schema = schema};
    take(first.execute("CREATE TABLE " + schema + ".jobs (id INTEGER PRIMARY KEY)"));
    take(first.execute("INSERT INTO " + schema + ".jobs VALUES (1), (2)"));
    take(first.execute("BEGIN"));
    auto held = take(first.execute("SELECT id FROM " + schema +
                                   ".jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"));
    take(second.execute("BEGIN"));
    auto skipped = take(second.execute("SELECT id FROM " + schema +
                                       ".jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"));
    require(held.get_int64(0, 0) == 1 && skipped.get_int64(0, 0) == 2, "queue locks overlap");
    auto nowait =
        second.execute("SELECT id FROM " + schema + ".jobs WHERE id = 1 FOR UPDATE NOWAIT");
    require(!nowait && nowait.error().sqlstate == "55P03", "NOWAIT did not reject locked row");
    std::cout << "queue: held=1 skipped=2 nowait_sqlstate=" << nowait.error().sqlstate << '\n';
    take(second.execute("ROLLBACK"));
    take(first.execute("ROLLBACK"));

    asterorm::query_parameters key{asterorm::encode_parameter(static_cast<std::int64_t>(stamp))};
    take(first.execute("BEGIN"));
    take(first.execute_params("SELECT pg_advisory_lock($1::bigint)", key));
    take(first.execute("ROLLBACK"));
    auto blocked = take(second.execute_params("SELECT pg_try_advisory_lock($1::bigint)", key));
    require(blocked.get_value(0, 0) == "f", "session lock did not survive rollback");
    take(first.execute_params("SELECT pg_advisory_unlock($1::bigint)", key));
    auto acquired = take(second.execute_params("SELECT pg_try_advisory_lock($1::bigint)", key));
    require(acquired.get_value(0, 0) == "t", "session unlock did not release lock");
    take(second.execute_params("SELECT pg_advisory_unlock($1::bigint)", key));
    take(first.execute("BEGIN"));
    take(first.execute_params("SELECT pg_advisory_xact_lock($1::bigint)", key));
    auto xact_blocked =
        take(second.execute_params("SELECT pg_try_advisory_xact_lock($1::bigint)", key));
    require(xact_blocked.get_value(0, 0) == "f", "transaction advisory lock did not block");
    take(first.execute("ROLLBACK"));
    auto xact_released =
        take(second.execute_params("SELECT pg_try_advisory_xact_lock($1::bigint)", key));
    require(xact_released.get_value(0, 0) == "t", "transaction lock survived rollback");
    std::cout << "advisory: session_survives_rollback=true explicit_unlock_releases=true "
                 "xact_released_on_rollback=true\n";

    take(first.execute("SET LOCAL statement_timeout = '50ms'"));
    auto outside = take(first.execute("SHOW statement_timeout"));
    require(outside.get_value(0, 0) == "5s", "SET LOCAL changed an autocommit session");
    take(first.execute("BEGIN"));
    take(first.execute("SET LOCAL statement_timeout = '50ms'"));
    require(take(first.execute("SHOW statement_timeout")).get_value(0, 0) == "50ms",
            "transaction timeout was not set");
    auto cancelled = first.execute_prepared("SELECT pg_sleep(0.2)", {});
    require(!cancelled && cancelled.error().sqlstate == "57014", "statement was not cancelled");
    std::cout << "timeout: sqlstate=" << cancelled.error().sqlstate
              << " classified_connection_failed="
              << (cancelled.error().kind == asterorm::db_error_kind::connection_failed)
              << " connection_open=" << first.is_open() << '\n';
    take(first.execute("ROLLBACK"));
    auto restored = take(first.execute("SHOW statement_timeout"));
    require(restored.get_value(0, 0) == "5s", "timeout leaked after rollback");
    require(take(first.execute("SELECT 1")).get_int64(0, 0) == 1, "cancelled connection unusable");
    std::cout << "timeout: autocommit_local_ignored=true rollback_restores=true "
                 "connection_reusable=true\n";

    asterorm::pool_config config;
    config.conninfo = conninfo;
    config.min_size = config.max_size = 2;
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    auto first_lease = take(pool.acquire());
    auto second_lease = take(pool.acquire());
    take(first_lease->execute("SET statement_timeout = '5s'"));
    take(second_lease->execute("SET statement_timeout = '5s'"));
    first_lease.release_to_pool();
    second_lease.release_to_pool();
    asterorm::session setup{pool};
    asterorm::schema_migrator setup_migrator{setup, schema + ".history"};
    require(take(setup_migrator.current_version()) == 0, "history was not empty");
    take(first.execute("CREATE TABLE " + schema + ".effects (id INTEGER)"));
    const std::vector<asterorm::migration> migrations{
        {
            .version = 1,
            .name = "audit race",
            .up_sql = "INSERT INTO " + schema + ".effects VALUES (1)",
            .down_sql = "DELETE FROM " + schema + ".effects",
        },
    };
    std::barrier rendezvous{2};
    auto apply = [&] {
        bool arrived = false;
        try {
            asterorm::session worker{pool};
            worker.set_observer([&](const asterorm::query_event& event) {
                if (event.sql.starts_with("SELECT COALESCE(MAX(version)")) {
                    arrived = true;
                    rendezvous.arrive_and_wait();
                }
            });
            asterorm::schema_migrator migrator{worker, schema + ".history"};
            auto result = migrator.apply(migrations);
            if (!arrived) {
                rendezvous.arrive_and_drop();
            }
            return result;
        } catch (...) {
            if (!arrived) {
                rendezvous.arrive_and_drop();
            }
            throw;
        }
    };
    auto first_apply = std::async(std::launch::async, apply);
    auto second_apply = std::async(std::launch::async, apply);
    auto first_result = first_apply.get();
    auto second_result = second_apply.get();
    const int successful = first_result.has_value() + second_result.has_value();
    std::cout << "migration_race: successful=" << successful;
    for (const auto* result : {&first_result, &second_result}) {
        if (!*result) {
            std::cout << " failure_sqlstate=" << result->error().sqlstate;
        }
    }
    auto effects = take(first.execute("SELECT COUNT(*) FROM " + schema + ".effects"));
    std::cout << " committed_effects=" << effects.get_value(0, 0) << '\n';
}
} // namespace

int main() {
    try {
        std::cout << std::boolalpha;
        const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
        const std::string conninfo = configured
                                         ? configured
                                         : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                           "password=orm_test sslmode=disable";
        inspect_builder<asterorm::sql::select_builder>("string_builder");
        inspect_builder<decltype(typed::select<audit_row>())>("typed_builder");
        using pool_type = asterorm::connection_pool<asterorm::pg::driver>;
        inspect_runtime<asterorm::session<pool_type>, asterorm::pool_config>();
        inspect_routing("postgres", asterorm::pg::driver{}, conninfo);
        inspect_routing("sqlite", asterorm::sqlite::driver{}, ":memory:");
        inspect_postgres(conninfo);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
