#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#if defined(__APPLE__) || defined(__linux__)
#include <sys/resource.h>
#endif

#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/sql/typed.hpp"

struct streamed_job {
    int id{};
    std::optional<std::string> name;
    bool active{};
};
template <> struct asterorm::entity_traits<streamed_job> {
    static constexpr const char* table = "repository_stream_jobs";
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&streamed_job::id>("id"), asterorm::column<&streamed_job::name>("name"),
        asterorm::column<&streamed_job::active>("active"));
};
struct slow_stream_job : streamed_job {};
template <>
struct asterorm::entity_traits<slow_stream_job> : asterorm::entity_traits<streamed_job> {
    static constexpr const char* table = "repository_stream_slow";
};
struct error_stream_job : streamed_job {};
template <>
struct asterorm::entity_traits<error_stream_job> : asterorm::entity_traits<streamed_job> {
    static constexpr const char* table = "repository_stream_error";
};
struct memory_stream_job : streamed_job {};
template <>
struct asterorm::entity_traits<memory_stream_job> : asterorm::entity_traits<streamed_job> {
    static constexpr const char* table = "repository_stream_memory";
};
struct wrong_stream_job {
    int id{};
    int name{};
    bool active{};
};
template <> struct asterorm::entity_traits<wrong_stream_job> {
    static constexpr const char* table = "repository_stream_jobs";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&wrong_stream_job::id>("id"),
                        asterorm::column<&wrong_stream_job::name>("name"),
                        asterorm::column<&wrong_stream_job::active>("active"));
};

namespace {
namespace typed = asterorm::sql::typed;
template <typename Value> Value take_stream(asterorm::result<Value> outcome) {
    REQUIRE(outcome.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}
struct stream_fixture {
    asterorm::pool_config config;
    asterorm::connection_pool<asterorm::pg::driver> pool;
    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository<decltype(db)> repo{db};
    explicit stream_fixture(std::optional<std::chrono::milliseconds> timeout = {})
        : config([&] {
              asterorm::pool_config options;
              const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
              options.conninfo = configured ? configured
                                            : "host=127.0.0.1 port=5432 dbname=orm_test "
                                              "user=orm_test password=orm_test sslmode=disable";
              options.min_size = 0;
              options.max_size = 1;
              options.statement_timeout = timeout;
              return options;
          }()),
          pool(asterorm::pg::driver{}, config) {
        take_stream(db.with_connection([](auto& connection) -> asterorm::result<void> {
            auto created = connection.execute("CREATE TEMP TABLE repository_stream_jobs (id "
                                              "INTEGER PRIMARY KEY,name TEXT,active BOOLEAN)");
            if (!created) {
                return std::unexpected(created.error());
            }
            auto seeded = connection.execute_params("INSERT INTO repository_stream_jobs VALUES "
                                                    "(1,$1,TRUE),(2,'second',FALSE),(3,NULL,TRUE)",
                                                    {asterorm::encode_parameter("quote' ; --")});
            if (!seeded) {
                return std::unexpected(seeded.error());
            }
            return {};
        }));
    }
    void execute(const char* statement) {
        take_stream(
            db.with_connection([&](auto& connection) { return connection.execute(statement); }));
    }
};
} // namespace

TEST_CASE("PG repository stream hydrates owned typed rows and reports one terminal event",
          "[pg][repository-stream]") {
    stream_fixture test;
    const auto pid = take_stream(test.db.native_scalar<int>("SELECT pg_backend_pid()"));
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error == nullptr);
        REQUIRE(event.param_count == 1);
        REQUIRE(test.pool.stats().in_use == 1);
    });
    auto stream = [&] {
        asterorm::repository temporary{test.db};
        return take_stream(temporary.stream<streamed_job>(
            typed::select<streamed_job>()
                .where(typed::col<&streamed_job::active> == typed::val(true))
                .order_by(typed::col<&streamed_job::id>)
                .build()));
    }();
    REQUIRE_FALSE(test.db.native_scalar<int>("SELECT 1").has_value());
    auto first = take_stream(stream.next());
    REQUIRE(first->id == 1);
    REQUIRE(first->name == "quote' ; --");
    REQUIRE(first->active);
    auto third = take_stream(stream.next());
    REQUIRE(third->id == 3);
    REQUIRE_FALSE(third->name.has_value());
    REQUIRE(events == 0);
    REQUIRE_FALSE(take_stream(stream.next()).has_value());
    REQUIRE_FALSE(take_stream(stream.next()).has_value());
    REQUIRE(events == 1);
    REQUIRE(test.pool.stats().in_use == 0);
    test.db.set_observer({});
    REQUIRE(take_stream(test.db.native_scalar<int>("SELECT pg_backend_pid()")) == pid);
}

TEST_CASE("PG repository codec errors discard and retain the primary diagnostic",
          "[pg][repository-stream]") {
    stream_fixture test;
    auto transaction = take_stream(test.db.begin());
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.error->kind == asterorm::db_error_kind::parse_failed);
    });
    auto stream =
        take_stream(test.repo.stream<wrong_stream_job>(typed::select<wrong_stream_job>().build()));
    auto failed = stream.next();
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().kind == asterorm::db_error_kind::parse_failed);
    REQUIRE(stream.next().error().message == failed.error().message);
    REQUIRE(events == 1);
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE_FALSE(transaction.commit().has_value());
    REQUIRE(test.pool.stats().total == 0);
    test.db.set_observer({});
    REQUIRE(take_stream(test.db.native_scalar<int>("SELECT 1")) == 1);
}

TEST_CASE("PG repository stream retains late server errors and observer exceptions",
          "[pg][repository-stream]") {
    const bool throwing = GENERATE(false, true);
    stream_fixture test;
    test.execute("CREATE TEMP VIEW repository_stream_error AS SELECT CASE WHEN value=3 THEN "
                 "1/(value-value) ELSE value END AS id,'row'::text AS name,TRUE AS active "
                 "FROM generate_series(1,3) AS value");
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.error->sqlstate == "22012");
        if (throwing) {
            throw std::runtime_error("stream observer identity");
        }
    });
    auto stream =
        take_stream(test.repo.stream<error_stream_job>(typed::select<error_stream_job>().build()));
    REQUIRE(take_stream(stream.next())->id == 1);
    REQUIRE(take_stream(stream.next())->id == 2);
    if (throwing) {
        REQUIRE_THROWS_WITH(stream.next(), "stream observer identity");
    } else {
        REQUIRE_FALSE(stream.next().has_value());
    }
    auto retained = stream.next();
    REQUIRE_FALSE(retained.has_value());
    REQUIRE(retained.error().sqlstate == "22012");
    REQUIRE(events == 1);
    REQUIRE(test.pool.stats().total == 0);
}

TEST_CASE("PG repository stream close and abandonment invalidate caller transactions",
          "[pg][repository-stream]") {
    const bool close = GENERATE(false, true);
    stream_fixture test;
    auto transaction = take_stream(test.db.begin());
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.error != nullptr);
        REQUIRE(event.error->sqlstate.empty());
    });
    {
        auto stream =
            take_stream(test.repo.stream<streamed_job>(typed::select<streamed_job>().build()));
        REQUIRE(take_stream(stream.next()).has_value());
        if (close) {
            take_stream(stream.close());
            REQUIRE_FALSE(take_stream(stream.next()).has_value());
        }
    }
    REQUIRE(events == (close ? 1 : 0));
    REQUIRE(test.pool.stats().in_use == 0);
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE_FALSE(transaction.rollback().has_value());
}

TEST_CASE("PG repository admission preserves failed transactions and rejects timeout scopes",
          "[pg][repository-stream]") {
    stream_fixture test;
    int events = 0;
    test.db.set_observer([&](const auto&) { ++events; });
    auto transaction = take_stream(test.db.begin());
    auto failed =
        test.db.with_connection([](auto& connection) { return connection.execute("SELECT 1/0"); });
    REQUIRE_FALSE(failed.has_value());
    REQUIRE_FALSE(
        test.repo.stream<streamed_job>(typed::select<streamed_job>().build()).has_value());
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().in_use == 1);
    REQUIRE(events == 0);
    take_stream(transaction.rollback());
    auto next = take_stream(test.db.begin());
    take_stream(test.db.with_statement_timeout(
        std::chrono::milliseconds{200}, [&](auto& db) -> asterorm::result<void> {
            asterorm::repository repo{db};
            REQUIRE_FALSE(repo.template stream<streamed_job>(typed::select<streamed_job>().build())
                              .has_value());
            return {};
        }));
    REQUIRE(events == 0);
    take_stream(next.commit());
}

TEST_CASE("PG repository streaming obeys pool timeouts and releases capacity",
          "[pg][repository-stream]") {
    stream_fixture test{std::chrono::milliseconds{100}};
    test.execute("CREATE TEMP VIEW repository_stream_slow AS SELECT id,name,active FROM "
                 "repository_stream_jobs,pg_sleep(0.3)");
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.error->sqlstate == "57014");
    });
    auto stream =
        take_stream(test.repo.stream<slow_stream_job>(typed::select<slow_stream_job>().build()));
    REQUIRE(stream.next().error().sqlstate == "57014");
    REQUIRE(events == 1);
    REQUIRE(test.pool.stats().total == 0);
    test.db.set_observer({});
    REQUIRE(take_stream(test.db.native_scalar<int>("SELECT 1")) == 1);
}

static void check_repository_stream_memory(asterorm::stream_mode mode) {
#if defined(__APPLE__) || defined(__linux__)
    stream_fixture test;
    test.execute("CREATE TEMP VIEW repository_stream_memory AS SELECT value AS id,repeat('x',1024) "
                 "AS name,TRUE AS active FROM generate_series(1,100000) AS value");
    auto transaction = take_stream(test.db.begin());
    auto peak = [] {
        rusage usage{};
        REQUIRE(getrusage(RUSAGE_SELF, &usage) == 0);
#ifdef __APPLE__
        return static_cast<std::uint64_t>(usage.ru_maxrss);
#else
        return static_cast<std::uint64_t>(usage.ru_maxrss) * 1024;
#endif
    };
    const auto before = peak();
    auto stream = take_stream(test.repo.stream<memory_stream_job>(
        typed::select<memory_stream_job>().build(), {.mode = mode, .chunk_size = 256}));
    std::uint64_t rows = 0;
    std::uint64_t bytes = 0;
    while (auto row = take_stream(stream.next())) {
        ++rows;
        bytes += row->name->size();
    }
    const auto after = peak();
    const auto growth = after - before;
    REQUIRE(rows == 100000);
    REQUIRE(bytes == 102400000);
    REQUIRE(growth <= std::uint64_t{32} * 1024 * 1024);
    std::cout << "repository_stream_rss_growth_bytes=" << growth << '\n';
    std::cout << "repository_stream_rss_baseline_bytes=" << before << '\n';
    std::cout << "repository_stream_rss_peak_bytes=" << after << '\n';
    take_stream(transaction.commit());
#else
    (void)mode;
    SKIP("RSS fixture is measured on Linux and macOS");
#endif
}

TEST_CASE("PG repository stream memory stays bounded for fixed-width rows",
          "[pg][.repository-stream-memory]") {
    check_repository_stream_memory(asterorm::stream_mode::single_row);
}

TEST_CASE("PG repository cursor memory stays bounded for fixed-width rows",
          "[pg][.repository-cursor-memory]") {
    check_repository_stream_memory(asterorm::stream_mode::cursor);
}

TEST_CASE("PG repository cursor EOF retains managed transaction and bound rows",
          "[pg][repository-cursor]") {
    const auto chunk = GENERATE(1U, 2U, 3U, 65536U);
    stream_fixture test;
    auto transaction = take_stream(test.db.begin());
    test.execute("INSERT INTO repository_stream_jobs VALUES (4,'prior write',TRUE)");
    const auto pid = take_stream(test.db.native_scalar<int>("SELECT pg_backend_pid()"));
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error == nullptr);
        REQUIRE(event.param_count == 1);
        REQUIRE(test.pool.stats().in_use == 1);
    });
    auto stream = take_stream(test.repo.stream<streamed_job>(
        typed::select<streamed_job>()
            .where(typed::col<&streamed_job::active> == typed::val(true))
            .order_by(typed::col<&streamed_job::id>)
            .build(),
        {.mode = asterorm::stream_mode::cursor, .chunk_size = chunk}));
    REQUIRE_FALSE(transaction.commit().has_value());
    REQUIRE_FALSE(transaction.rollback().has_value());
    REQUIRE_FALSE(test.db.native_scalar<int>("SELECT 1").has_value());
    auto first = take_stream(stream.next());
    REQUIRE(first->name == "quote' ; --");
    REQUIRE(take_stream(stream.next())->id == 3);
    REQUIRE(take_stream(stream.next())->id == 4);
    REQUIRE(events == 0);
    REQUIRE_FALSE(take_stream(stream.next()).has_value());
    REQUIRE(events == 1);
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(first->id == 1);
    test.db.set_observer({});
    REQUIRE(take_stream(test.db.native_scalar<int>(
                "SELECT count(*) FROM pg_cursors WHERE starts_with(name,'_asterorm_cursor_')")) ==
            0);
    REQUIRE(take_stream(test.db.native_scalar<int>("SELECT pg_backend_pid()")) == pid);
    take_stream(transaction.commit());
    REQUIRE(take_stream(
                test.db.native_scalar<int>("SELECT count(*) FROM repository_stream_jobs")) == 4);
}

TEST_CASE("PG repository cursor early close preserves prior writes and emits once",
          "[pg][repository-cursor]") {
    const bool read_first = GENERATE(false, true);
    stream_fixture test;
    auto transaction = take_stream(test.db.begin());
    test.execute("INSERT INTO repository_stream_jobs VALUES (4,'preserved',TRUE)");
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error != nullptr);
        REQUIRE(event.error->sqlstate.empty());
        REQUIRE(test.pool.stats().in_use == 1);
    });
    auto stream = take_stream(
        test.repo.stream<streamed_job>(typed::select<streamed_job>().build(),
                                       {.mode = asterorm::stream_mode::cursor, .chunk_size = 2}));
    if (read_first) {
        REQUIRE(take_stream(stream.next()).has_value());
    }
    take_stream(stream.close());
    take_stream(stream.close());
    REQUIRE_FALSE(take_stream(stream.next()).has_value());
    REQUIRE(events == 1);
    REQUIRE(test.db.has_active_transaction());
    test.db.set_observer({});
    REQUIRE(take_stream(test.db.native_scalar<int>(
                "SELECT count(*) FROM pg_cursors WHERE starts_with(name,'_asterorm_cursor_')")) ==
            0);
    take_stream(transaction.commit());
    REQUIRE(take_stream(
                test.db.native_scalar<int>("SELECT count(*) FROM repository_stream_jobs")) == 4);
}

TEST_CASE("PG repository cursor empty reads complete and failed admission keeps rollback",
          "[pg][repository-cursor]") {
    stream_fixture test;
    const auto no_transaction = test.repo.stream<streamed_job>(
        typed::select<streamed_job>().build(), {.mode = asterorm::stream_mode::cursor});
    REQUIRE_FALSE(no_transaction.has_value());
    auto transaction = take_stream(test.db.begin());
    auto empty = take_stream(test.repo.stream<streamed_job>(
        typed::select<streamed_job>().where(typed::col<&streamed_job::id> < typed::val(0)).build(),
        {.mode = asterorm::stream_mode::cursor}));
    REQUIRE_FALSE(take_stream(empty.next()).has_value());
    REQUIRE(test.db.has_active_transaction());
    REQUIRE_FALSE(test.db.native_scalar<int>("SELECT 1/0").has_value());
    int events = 0;
    test.db.set_observer([&](const auto&) { ++events; });
    const auto failed = test.repo.stream<streamed_job>(typed::select<streamed_job>().build(),
                                                       {.mode = asterorm::stream_mode::cursor});
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(events == 0);
    REQUIRE(test.db.has_active_transaction());
    test.db.set_observer({});
    take_stream(transaction.rollback());
}

TEST_CASE("PG repository cursor failures invalidate and retain the first diagnostic",
          "[pg][repository-cursor]") {
    const bool decode_error = GENERATE(false, true);
    stream_fixture test;
    auto transaction = take_stream(test.db.begin());
    test.execute("CREATE TEMP VIEW repository_stream_error AS SELECT CASE WHEN value=3 THEN "
                 "1/(value-value) ELSE value END AS id,'name'::text AS name,TRUE AS active "
                 "FROM generate_series(1,4) value");
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error != nullptr);
        REQUIRE(test.pool.stats().in_use == 1);
    });
    asterorm::db_error failure;
    if (decode_error) {
        auto stream = take_stream(test.repo.stream<wrong_stream_job>(
            typed::select<wrong_stream_job>().build(),
            {.mode = asterorm::stream_mode::cursor, .chunk_size = 2}));
        failure = stream.next().error();
        REQUIRE(failure.kind == asterorm::db_error_kind::parse_failed);
        REQUIRE(stream.next().error().message == failure.message);
    } else {
        auto stream = take_stream(test.repo.stream<error_stream_job>(
            typed::select<error_stream_job>().build(),
            {.mode = asterorm::stream_mode::cursor, .chunk_size = 2}));
        REQUIRE(take_stream(stream.next())->id == 1);
        REQUIRE(take_stream(stream.next())->id == 2);
        failure = stream.next().error();
        REQUIRE(failure.sqlstate == "22012");
        REQUIRE(stream.next().error().message == failure.message);
    }
    REQUIRE(events == 1);
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE_FALSE(transaction.commit().has_value());
}

TEST_CASE("PG repository cursor abandonment and parent destruction discard quietly",
          "[pg][repository-cursor]") {
    const bool abandon_parent = GENERATE(false, true);
    stream_fixture test;
    int events = 0;
    asterorm::row_stream<streamed_job> survivor;
    {
        auto transaction = take_stream(test.db.begin());
        test.db.set_observer([&](const auto&) { ++events; });
        auto stream = take_stream(test.repo.stream<streamed_job>(
            typed::select<streamed_job>().build(), {.mode = asterorm::stream_mode::cursor}));
        REQUIRE(take_stream(stream.next()).has_value());
        if (abandon_parent) {
            survivor = std::move(stream);
        }
    }
    REQUIRE(events == 0);
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().total == 0);
    if (abandon_parent) {
        REQUIRE_FALSE(survivor.next().has_value());
        REQUIRE(events == 1);
    }
    test.db.set_observer({});
    REQUIRE(take_stream(test.db.native_scalar<int>("SELECT 1")) == 1);
}

TEST_CASE("PG repository cursor FETCH obeys the pool statement timeout",
          "[pg][repository-cursor]") {
    stream_fixture test{std::chrono::milliseconds{100}};
    auto transaction = take_stream(test.db.begin());
    test.execute("CREATE TEMP VIEW repository_stream_slow AS SELECT id,name,active FROM "
                 "repository_stream_jobs,pg_sleep(0.3)");
    auto stream = take_stream(test.repo.stream<slow_stream_job>(
        typed::select<slow_stream_job>().build(),
        {.mode = asterorm::stream_mode::cursor, .chunk_size = 1}));
    REQUIRE(stream.next().error().sqlstate == "57014");
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE_FALSE(transaction.commit().has_value());
}
