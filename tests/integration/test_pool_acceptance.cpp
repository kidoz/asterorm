#include "../support/pool_acceptance.hpp"
#include "asterorm/core/migration.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/sql/typed.hpp"
#include <catch2/generators/catch_generators.hpp>
#include <cstdlib>

struct pool_acceptance_row {
    int id{};
};
template <> struct asterorm::entity_traits<pool_acceptance_row> {
    static constexpr const char* table = "pool_acceptance_rows";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&pool_acceptance_row::id>("id"));
};
namespace {
template <typename Value> Value take(asterorm::result<Value> outcome) {
    REQUIRE(outcome);
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}
std::string settings() {
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    return configured ? configured
                      : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test "
                        "sslmode=disable";
}
struct counting_driver {
    int* connects;
    [[nodiscard]] asterorm::result<asterorm::pg::connection>
    connect(const std::string& settings) const {
        ++*connects;
        return asterorm::pg::driver{}.connect(settings);
    }
};
} // namespace
TEST_CASE("PG combined pool acceptance", "[pg][pool-acceptance]") {
    asterorm::test::check_pool_acceptance(asterorm::pg::driver{}, settings());
}
TEST_CASE("PG combined pool retains overdue managed streams across close",
          "[pg][pool-acceptance]") {
    const auto mode = GENERATE(asterorm::stream_mode::single_row, asterorm::stream_mode::cursor);
    auto config = asterorm::test::acceptance_config<asterorm::pg::driver>(settings());
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    auto transaction = take(db.begin());
    take(db.with_connection([](auto& connection) -> asterorm::result<void> {
        auto created =
            connection.execute("CREATE TEMP TABLE pool_acceptance_rows(id INTEGER) ON COMMIT DROP");
        if (!created) {
            return std::unexpected(created.error());
        }
        auto inserted =
            connection.execute("INSERT INTO pool_acceptance_rows SELECT generate_series(1,3)");
        if (!inserted) {
            return std::unexpected(inserted.error());
        }
        return {};
    }));
    int events{};
    db.set_observer([&](const auto& event) {
        REQUIRE(event.error == nullptr);
        ++events;
    });
    auto stream = take(repo.stream<pool_acceptance_row>(
        asterorm::sql::typed::select<pool_acceptance_row>()
            .order_by(asterorm::sql::typed::col<&pool_acceptance_row::id>)
            .build(),
        {.mode = mode, .chunk_size = 1}));
    auto first = take(stream.next());
    REQUIRE(first);
    REQUIRE(first->id == 1);
    std::vector<asterorm::pool_diagnostic> records;
    REQUIRE(asterorm::test::await_pool([&] {
        records = pool.take_diagnostics();
        return !records.empty();
    }));
    REQUIRE(pool.stats().overdue_leases == 1);
    REQUIRE(pool.stats().retirements == 0);
    REQUIRE(events == 0);
    pool.close();
    REQUIRE_FALSE(
        db.native_scalar<int>("SELECT 1")); // Stream ownership still gates unrelated work.
    REQUIRE_FALSE(transaction.commit());
    for (int expected : {2, 3}) {
        auto row = take(stream.next());
        REQUIRE(row);
        REQUIRE(row->id == expected);
    }
    REQUIRE_FALSE(take(stream.next()));
    REQUIRE(events == 1);
    REQUIRE(pool.stats().leased == 1); // The caller transaction still owns its pin.
    take(transaction.commit());
    REQUIRE(pool.stats().total == 0);
    REQUIRE(pool.stats().retirements == 1);
    REQUIRE(pool.take_diagnostics().empty());
    asterorm::test::check_pool_accounting(pool.stats());
}
TEST_CASE("PgBouncer combined pool switches backends without replacing its client",
          "[.][pgbouncer][pool-acceptance]") {
    using namespace std::chrono_literals;
    const auto* configured = std::getenv("ASTERORM_PGBOUNCER_TEST_CONNINFO");
    REQUIRE(configured != nullptr);
    auto config = asterorm::test::acceptance_config<asterorm::pg::driver>(configured);
    config.profile = asterorm::pool_profile::transaction_pool;
    config.statement_timeout.reset();
    config.idle_timeout = config.max_lifetime = 5min;
    config.leak_threshold = 20ms;
    int first_connects{};
    asterorm::connection_pool<counting_driver> first_pool{counting_driver{&first_connects}, config};
    asterorm::connection_pool<asterorm::pg::driver> second_pool{asterorm::pg::driver{}, config};
    asterorm::connection_pool<asterorm::pg::driver> blocking_pool{asterorm::pg::driver{}, config};
    asterorm::session first{first_pool};
    asterorm::session second{second_pool};
    asterorm::session blocker{blocking_pool};
    auto first_transaction = take(first.begin());
    auto second_transaction = take(second.begin());
    const auto original_pid = *take(first.native_scalar<int>("SELECT pg_backend_pid()"));
    const auto alternate_pid = *take(second.native_scalar<int>("SELECT pg_backend_pid()"));
    REQUIRE(original_pid != alternate_pid);
    take(first_transaction.commit());
    auto blocked = take(blocker.begin());
    REQUIRE(take(blocker.native_scalar<int>("SELECT pg_backend_pid()")) == original_pid);
    take(second_transaction.commit());
    (void)first_pool.take_diagnostics();
    auto switched = take(first.begin());
    REQUIRE(take(first.native_scalar<int>("SELECT pg_backend_pid()")) == alternate_pid);
    REQUIRE(first_connects == 1);
    REQUIRE(asterorm::test::await_pool([&] { return first_pool.stats().overdue_leases == 1; }));
    std::vector<asterorm::pool_diagnostic> records;
    REQUIRE(asterorm::test::await_pool([&] {
        records = first_pool.take_diagnostics();
        return !records.empty();
    }));
    REQUIRE(records.size() == 1);
    take(first.with_statement_timeout(1000ms, [&](auto& scoped) -> asterorm::result<void> {
        auto cursor = scoped.with_connection([](auto& connection) {
            return connection.open_cursor("SELECT $1::integer", {"42"}, 1);
        });
        REQUIRE(cursor);
        auto row = take(cursor->next());
        REQUIRE(row);
        REQUIRE(row->get_int64(0, 0) == 42);
        REQUIRE_FALSE(take(cursor->next()));
        return {};
    }));
    REQUIRE(take(first.native_scalar<int>(
                "SELECT count(*)::integer FROM pg_prepared_statements")) == 0);
    REQUIRE_FALSE(first.with_advisory_lock(7, [](auto&) -> asterorm::result<void> { return {}; }));
    first_pool.close();
    REQUIRE(take(first.native_scalar<int>("SELECT $1::integer", 43)) == 43);
    take(switched.commit());
    take(blocked.commit());
    REQUIRE(first_connects == 1);
    REQUIRE(first_pool.stats().total == 0);
    REQUIRE(first_pool.stats().retirements == 1);
    REQUIRE(first_pool.take_diagnostics().empty());
    asterorm::test::check_pool_accounting(first_pool.stats());
}
