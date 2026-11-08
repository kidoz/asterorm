#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"

namespace {
template <typename Value> Value stream_take(asterorm::result<Value> outcome) {
    REQUIRE(outcome.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}

asterorm::pool_config stream_config() {
    asterorm::pool_config config;
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    config.conninfo = configured ? configured
                                 : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                   "password=orm_test sslmode=disable";
    config.min_size = 0;
    config.max_size = 1;
    return config;
}

asterorm::pg::connection stream_connection() {
    return stream_take(asterorm::pg::driver{}.connect(stream_config().conninfo));
}

std::int64_t backend_pid(asterorm::pg::connection& connection) {
    return *stream_take(connection.execute("SELECT pg_backend_pid()")).get_int64(0, 0);
}
} // namespace

TEST_CASE("PG single-row reader owns bound rows and reuses only after clean EOF",
          "[pg][single-row]") {
    auto connection = stream_connection();
    const auto original_pid = backend_pid(connection);
    stream_take(connection.execute_prepared("SELECT 1", {}));
    const auto cached = connection.prepared_statement_count();
    auto reader = stream_take(connection.open_single_row(
        "SELECT $1::text AS note,$2::boolean AS active,$3::integer AS missing",
        {
            asterorm::encode_parameter("quote' ; --"),
            asterorm::encode_parameter(true),
            asterorm::encode_parameter(std::optional<int>{}),
        }));
    REQUIRE(connection.is_open());
    REQUIRE_FALSE(connection.is_pool_reusable());
    REQUIRE_FALSE(connection.is_transaction_idle());
    REQUIRE_FALSE(connection.open_single_row("SELECT 2").has_value());
    REQUIRE_FALSE(connection.execute("SELECT 2").has_value());
    REQUIRE_FALSE(connection.execute_params("SELECT 2", {}).has_value());
    REQUIRE_FALSE(connection.execute_prepared("SELECT 2", {}).has_value());
    REQUIRE_FALSE(connection.advisory_lock(1).has_value());
    REQUIRE_FALSE(connection.advisory_unlock(1).has_value());
    REQUIRE_FALSE(connection.statement_timeout().has_value());
    REQUIRE_FALSE(connection.set_statement_timeout(std::chrono::milliseconds{100}).has_value());
    REQUIRE_FALSE(
        connection.set_local_statement_timeout(std::chrono::milliseconds{100}).has_value());
    REQUIRE_FALSE(connection.copy_out("COPY (SELECT 1) TO STDOUT").has_value());
    REQUIRE_FALSE(connection.copy_in("COPY unused FROM STDIN", {}).has_value());
    REQUIRE_THROWS_AS(connection.clear_prepared_statement_cache(), std::logic_error);
    REQUIRE_THROWS_AS(connection.set_max_prepared_statements(0), std::logic_error);
    auto row = stream_take(reader.next());
    REQUIRE(row.has_value());
    REQUIRE(row->rows() == 1);
    REQUIRE(row->get_string(0, 0) == "quote' ; --");
    REQUIRE(row->get_string(0, 1) == "t");
    REQUIRE(row->is_null(0, 2));
    REQUIRE_FALSE(stream_take(reader.next()).has_value());
    REQUIRE_FALSE(stream_take(reader.next()).has_value());
    REQUIRE(connection.is_pool_reusable());
    REQUIRE(connection.prepared_statement_count() == cached);
    REQUIRE(backend_pid(connection) == original_pid);
    auto next_reader = stream_take(connection.open_single_row("SELECT 7"));
    reader.close(); // completed handles cannot close a newer query
    REQUIRE(stream_take(next_reader.next())->get_int64(0, 0) == 7);
    REQUIRE_FALSE(stream_take(next_reader.next()).has_value());
}

TEST_CASE("PG single-row reader handles empty results and a hundred thousand rows",
          "[pg][single-row]") {
    auto connection = stream_connection();
    auto empty = stream_take(connection.open_single_row("SELECT 1 WHERE FALSE"));
    REQUIRE_FALSE(stream_take(empty.next()).has_value());
    REQUIRE(connection.is_pool_reusable());
    auto reader = stream_take(
        connection.open_single_row("SELECT value FROM generate_series(1,$1::integer) AS value",
                                   {asterorm::encode_parameter(100000)}));
    std::int64_t total = 0;
    int count = 0;
    bool single_rows = true;
    while (auto row = stream_take(reader.next())) {
        single_rows = single_rows && row->rows() == 1;
        total += row->get_int64(0, 0).value();
        ++count;
    }
    REQUIRE(single_rows);
    REQUIRE(count == 100000);
    REQUIRE(total == 5000050000LL);
    REQUIRE(connection.is_pool_reusable());
}

TEST_CASE("PG single-row late errors retain diagnostics after delivered rows", "[pg][single-row]") {
    auto connection = stream_connection();
    auto reader = stream_take(
        connection.open_single_row("SELECT CASE WHEN value=5 THEN 10/(value-value) ELSE value END "
                                   "FROM generate_series(1,8) AS value"));
    auto retained = stream_take(reader.next());
    REQUIRE(retained.has_value());
    for (int value = 2; value <= 4; ++value) {
        REQUIRE(stream_take(reader.next())->get_int64(0, 0) == value);
    }
    auto failure = reader.next();
    REQUIRE_FALSE(failure.has_value());
    REQUIRE(failure.error().sqlstate == "22012");
    REQUIRE_FALSE(failure.error().message.empty());
    REQUIRE_FALSE(connection.is_open());
    REQUIRE(retained->get_int64(0, 0) == 1); // owned result survives connection discard
    reader.close();
    auto repeated = reader.next();
    REQUIRE_FALSE(repeated.has_value());
    REQUIRE(repeated.error().message == failure.error().message);
    REQUIRE(repeated.error().sqlstate == failure.error().sqlstate);
}

TEST_CASE("PG single-row reader rejects non-row and multiple-command results", "[pg][single-row]") {
    const auto* query = GENERATE("SET application_name='single_row_test'", "SELECT 1; SELECT 2");
    auto connection = stream_connection();
    auto reader = stream_take(connection.open_single_row(query));
    auto failure = reader.next();
    REQUIRE_FALSE(failure.has_value());
    REQUIRE_FALSE(failure.error().message.empty());
    REQUIRE_FALSE(connection.is_open());
}

TEST_CASE("PG reader close and destruction discard unfinished queries", "[pg][single-row]") {
    const bool explicit_close = GENERATE(false, true);
    auto connection = stream_connection();
    {
        auto reader = stream_take(
            connection.open_single_row("SELECT value FROM generate_series(1,100000) AS value"));
        REQUIRE(stream_take(reader.next())->get_int64(0, 0) == 1);
        if (explicit_close) {
            reader.close();
            reader.close();
            REQUIRE_FALSE(stream_take(reader.next()).has_value());
        }
    }
    REQUIRE_FALSE(connection.is_open());
}

TEST_CASE("PG readers follow moved connections and invalidate safely on owner loss",
          "[pg][single-row]") {
    auto original = stream_connection();
    auto first_reader = stream_take(original.open_single_row("SELECT 11"));
    auto moved_connection = std::move(original);
    auto moved_reader = std::move(first_reader);
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    REQUIRE_FALSE(first_reader.next().has_value());
    REQUIRE(stream_take(moved_reader.next())->get_int64(0, 0) == 11);
    auto destination = stream_connection();
    auto replaced_reader = stream_take(destination.open_single_row("SELECT 12"));
    destination = std::move(moved_connection);
    REQUIRE_FALSE(replaced_reader.next().has_value());
    REQUIRE_FALSE(stream_take(moved_reader.next()).has_value());
    REQUIRE(destination.is_pool_reusable());
    std::optional<asterorm::pg::connection> ephemeral{stream_connection()};
    auto orphaned = stream_take(ephemeral->open_single_row("SELECT 1"));
    ephemeral.reset();
    REQUIRE_FALSE(orphaned.next().has_value());
    auto replacement = stream_take(destination.open_single_row("SELECT 13"));
    auto replaced_connection = stream_connection();
    auto active = stream_take(replaced_connection.open_single_row("SELECT 14"));
    active = std::move(replacement);
    REQUIRE_FALSE(replaced_connection.is_open());
    REQUIRE(stream_take(active.next())->get_int64(0, 0) == 13);
    REQUIRE_FALSE(stream_take(active.next()).has_value());
}

TEST_CASE("PG single-row reader shares session reservations and transaction pins",
          "[pg][single-row]") {
    auto config = stream_config();
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    const auto original_pid = stream_take(db.native_scalar<int>("SELECT pg_backend_pid()"));
    auto transaction = stream_take(db.begin());
    auto reservation = stream_take(asterorm::detail::session_stream_access::acquire(db));
    auto reader = stream_take(reservation.with_connection([](auto& connection) {
        return connection.open_single_row("SELECT generate_series(1,3)");
    }));
    int events = 0;
    db.set_observer([&](const auto&) { ++events; });
    REQUIRE_FALSE(db.native_scalar<int>("SELECT 1").has_value());
    int count = 0;
    while (stream_take(reservation.with_connection([&](auto&) { return reader.next(); }))) {
        ++count;
    }
    REQUIRE(count == 3);
    REQUIRE(events == 0); // terminal observation belongs to the later repository bridge
    stream_take(reservation.complete());
    REQUIRE(db.has_active_transaction());
    REQUIRE(pool.stats().in_use == 1);
    stream_take(transaction.commit());
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE(stream_take(db.native_scalar<int>("SELECT pg_backend_pid()")) == original_pid);
}

TEST_CASE("PG abandoned transaction invalidates a live native reader", "[pg][single-row]") {
    auto config = stream_config();
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    const auto original_pid = stream_take(db.native_scalar<int>("SELECT pg_backend_pid()"));
    using guard_type = asterorm::transaction_guard<decltype(db)>;
    std::optional<guard_type> transaction{stream_take(db.begin())};
    auto reservation = stream_take(asterorm::detail::session_stream_access::acquire(db));
    auto reader = stream_take(reservation.with_connection([](auto& connection) {
        return connection.open_single_row("SELECT generate_series(1,1000)");
    }));
    REQUIRE(
        stream_take(reservation.with_connection([&](auto&) { return reader.next(); })).has_value());
    transaction.reset();
    REQUIRE_FALSE(db.has_active_transaction());
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE_FALSE(reader.next().has_value());
    REQUIRE_FALSE(reservation.complete().has_value());
    REQUIRE(stream_take(db.native_scalar<int>("SELECT pg_backend_pid()")) != original_pid);
}

TEST_CASE("PG pool discards a reader escaped from an unreserved callback", "[pg][single-row]") {
    auto config = stream_config();
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    auto escaped = stream_take(db.with_connection([](auto& connection) {
        return connection.open_single_row("SELECT generate_series(1,1000)");
    }));
    REQUIRE(pool.stats().total == 0);
    REQUIRE_FALSE(escaped.next().has_value());
    auto lease = stream_take(pool.acquire());
    auto explicitly_released =
        stream_take(lease->open_single_row("SELECT generate_series(1,1000)"));
    lease.release_to_pool();
    // A returned lease no longer grants connection access. The escaped reader
    // must still observe native owner invalidation before capacity is reused.
    REQUIRE_FALSE(explicitly_released.next().has_value());
    REQUIRE(pool.stats().total == 0);
    REQUIRE(stream_take(db.native_scalar<int>("SELECT 7")) == 7);
}

TEST_CASE("PG single-row timeouts retain SQLSTATE and release reserved capacity",
          "[pg][single-row]") {
    auto config = stream_config();
    config.statement_timeout = std::chrono::milliseconds{100};
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    auto transaction = stream_take(db.begin());
    auto reservation = stream_take(asterorm::detail::session_stream_access::acquire(db));
    auto reader = stream_take(reservation.with_connection(
        [](auto& connection) { return connection.open_single_row("SELECT pg_sleep(0.3)"); }));
    auto cancelled = reservation.with_connection([&](auto&) { return reader.next(); });
    REQUIRE_FALSE(cancelled.has_value());
    REQUIRE(cancelled.error().sqlstate == "57014");
    REQUIRE(cancelled.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE_FALSE(db.has_active_transaction());
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE_FALSE(transaction.commit().has_value());
    REQUIRE(stream_take(db.native_scalar<int>("SELECT 1")) == 1);
}

TEST_CASE("PG single-row reader reports a lost physical connection", "[pg][single-row]") {
    auto connection = stream_connection();
    const auto original_pid = backend_pid(connection);
    auto peer = stream_connection();
    auto reader = stream_take(connection.open_single_row("SELECT pg_sleep(10)"));
    stream_take(peer.execute_params("SELECT pg_terminate_backend($1::integer)",
                                    {asterorm::encode_parameter(original_pid)}));
    auto failure = reader.next();
    REQUIRE_FALSE(failure.has_value());
    REQUIRE_FALSE(connection.is_open());
    REQUIRE_FALSE(failure.error().message.empty());
}

TEST_CASE("PG single-row reader preserves native diagnostic fields", "[pg][single-row]") {
    auto connection = stream_connection();
    stream_take(connection.execute(
        "CREATE FUNCTION pg_temp.single_row_fail(value INTEGER) RETURNS INTEGER LANGUAGE plpgsql "
        "AS $$ BEGIN IF value=3 THEN RAISE EXCEPTION USING ERRCODE='P0001', "
        "MESSAGE='reader failure', DETAIL='reader detail', HINT='reader hint', "
        "TABLE='stream_rows', "
        "COLUMN='id', CONSTRAINT='stream_check'; END IF; RETURN value; END $$"));
    auto reader = stream_take(connection.open_single_row(
        "SELECT pg_temp.single_row_fail(value) FROM generate_series(1,3) AS value"));
    REQUIRE(stream_take(reader.next())->get_int64(0, 0) == 1);
    REQUIRE(stream_take(reader.next())->get_int64(0, 0) == 2);
    auto failure = reader.next();
    REQUIRE_FALSE(failure.has_value());
    REQUIRE(failure.error().sqlstate == "P0001");
    REQUIRE(failure.error().message == "reader failure");
    REQUIRE(failure.error().detail == "reader detail");
    REQUIRE(failure.error().hint == "reader hint");
    REQUIRE(failure.error().table == "stream_rows");
    REQUIRE(failure.error().column == "id");
    REQUIRE(failure.error().constraint == "stream_check");
}

TEST_CASE("PG single-row admission leaves an already failed transaction caller-owned",
          "[pg][single-row]") {
    auto connection = stream_connection();
    stream_take(connection.execute("BEGIN"));
    REQUIRE_FALSE(connection.execute("SELECT 1/0").has_value());
    auto rejected = connection.open_single_row("SELECT 1");
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(rejected.error().sqlstate.empty());
    REQUIRE(connection.is_open());
    stream_take(connection.execute("ROLLBACK"));
    auto reader = stream_take(connection.open_single_row("SELECT 1"));
    REQUIRE(stream_take(reader.next())->get_int64(0, 0) == 1);
    REQUIRE_FALSE(stream_take(reader.next()).has_value());
}
