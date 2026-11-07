#include <libpq-fe.h>
#include <sys/resource.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"

namespace {
using NativeConnection = std::unique_ptr<PGconn, decltype(&PQfinish)>;
using NativeResult = std::unique_ptr<PGresult, decltype(&PQclear)>;

struct InventoryRow {
    int id{};
};
template <typename Repository>
concept has_stream = requires(Repository& repo, asterorm::sql::select_ast query) {
    repo.template stream<InventoryRow>(std::move(query), 256);
};
using Session = asterorm::session<asterorm::connection_pool<asterorm::pg::driver>>;
static_assert(!has_stream<asterorm::repository<Session>>);

void check(bool condition, std::string_view description) {
    if (!condition) {
        throw std::runtime_error(std::string{description});
    }
}

NativeConnection connect() {
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    NativeConnection connection{
        PQconnectdb(configured ? configured
                               : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                 "password=orm_test sslmode=disable"),
        PQfinish};
    check(PQstatus(connection.get()) == CONNECTION_OK,
          "Cannot connect to the existing PostgreSQL test service (details omitted)");
    return connection;
}

NativeResult execute(PGconn* connection, const char* sql) {
    NativeResult result{PQexec(connection, sql), PQclear};
    check(result && (PQresultStatus(result.get()) == PGRES_COMMAND_OK ||
                     PQresultStatus(result.get()) == PGRES_TUPLES_OK),
          "Unexpected native command failure (diagnostics omitted)");
    return result;
}

std::string sqlstate(PGresult* result) {
    const auto* state = PQresultErrorField(result, PG_DIAG_SQLSTATE);
    return state ? state : "";
}

void send_rows(PGconn* connection, const char* sql, const char* count) {
    const char* values[]{count};
    check(PQsendQueryParams(connection, sql, 1, nullptr, values, nullptr, nullptr, 0) == 1,
          "Parameterized dispatch failed");
    check(PQsetSingleRowMode(connection) == 1, "Single-row admission failed");
}

void inspect_protocol(PGconn* connection) {
    std::cout << "server_version=" << PQserverVersion(connection) << '\n';
    std::cout << "libpq_version=" << PQlibVersion() << '\n';
    std::cout << "repository_stream_present=false\n";
    auto unsupported =
        asterorm::pg::result::from_native(PQmakeEmptyPGresult(connection, PGRES_SINGLE_TUPLE));
    check(!unsupported, "Inventory changed: result factory accepts single tuples");
    std::cout << "result_factory_single_tuple_rejected=true\n";
    send_rows(connection, "SELECT generate_series(1,$1::int)", "7");
    int rows = 0;
    int terminals = 0;
    while (auto* raw = PQgetResult(connection)) {
        NativeResult result{raw, PQclear};
        if (PQresultStatus(raw) == PGRES_SINGLE_TUPLE) {
            check(PQntuples(raw) == 1, "Expected exactly one row per native result");
            ++rows;
        } else {
            check(PQresultStatus(raw) == PGRES_TUPLES_OK && PQntuples(raw) == 0,
                  "Expected empty terminal tuple result");
            ++terminals;
        }
    }
    check(rows == 7 && terminals == 1, "Single-row completion count mismatch");
    execute(connection, "SELECT 1");
    std::cout << "single_row_rows=7\nsingle_row_empty_terminal=1\nnormal_reuse=true\n";

    check(PQsendQueryParams(connection,
                            "SELECT CASE WHEN value=5 THEN 10/(value-value) ELSE value END "
                            "FROM generate_series(1,8) AS value",
                            0, nullptr, nullptr, nullptr, nullptr, 0) == 1,
          "Late-error dispatch failed");
    check(PQsetSingleRowMode(connection) == 1, "Late-error mode admission failed");
    rows = 0;
    std::string failure;
    while (auto* raw = PQgetResult(connection)) {
        NativeResult result{raw, PQclear};
        if (PQresultStatus(raw) == PGRES_SINGLE_TUPLE) {
            ++rows;
        } else if (PQresultStatus(raw) == PGRES_FATAL_ERROR) {
            failure = sqlstate(raw);
        } else {
            check(false, "Unexpected late-error status");
        }
    }
    check(rows == 4 && failure == "22012", "Expected four rows before division failure");
    execute(connection, "SELECT 1");
    std::cout << "late_error_prefix_rows=4\nlate_error_sqlstate=22012\n";

    NativeResult outside{PQexec(connection,
                                "DECLARE inventory_cursor NO SCROLL CURSOR WITHOUT HOLD "
                                "FOR SELECT 1"),
                         PQclear};
    check(outside && sqlstate(outside.get()) == "25P01", "Expected transaction requirement");
    execute(connection, "BEGIN");
    const char* values[]{"7"};
    NativeResult declaration{
        PQexecParams(connection,
                     "DECLARE inventory_cursor NO SCROLL CURSOR WITHOUT HOLD FOR "
                     "SELECT generate_series(1,$1::int)",
                     1, nullptr, values, nullptr, nullptr, 0),
        PQclear};
    check(declaration && PQresultStatus(declaration.get()) == PGRES_COMMAND_OK,
          "Bound cursor declaration failed");
    for (const int expected : {3, 3, 1, 0}) {
        auto batch = execute(connection, "FETCH FORWARD 3 FROM inventory_cursor");
        check(PQntuples(batch.get()) == expected, "Cursor batch mismatch");
    }
    execute(connection, "CLOSE inventory_cursor");
    auto remaining =
        execute(connection, "SELECT count(*) FROM pg_cursors WHERE name='inventory_cursor'");
    check(std::string_view{PQgetvalue(remaining.get(), 0, 0)} == "0", "Cursor was not closed");
    execute(connection, "ROLLBACK");
    std::cout << "cursor_outside_transaction_sqlstate=25P01\n";
    std::cout << "cursor_parameter_binding=true\ncursor_batches=3,3,1,0\ncursor_closed=true\n";

    send_rows(connection, "SELECT value,pg_sleep(0.001) FROM generate_series(1,$1::int) AS value",
              "100000");
    NativeResult first{PQgetResult(connection), PQclear};
    check(first && PQresultStatus(first.get()) == PGRES_SINGLE_TUPLE,
          "Expected a row before cancellation");
    std::unique_ptr<PGcancel, decltype(&PQfreeCancel)> cancellation{PQgetCancel(connection),
                                                                    PQfreeCancel};
    char error_buffer[256]{};
    check(cancellation && PQcancel(cancellation.get(), error_buffer, sizeof(error_buffer)) == 1,
          "Cancellation dispatch failed (diagnostics omitted)");
    failure.clear();
    while (auto* raw = PQgetResult(connection)) {
        NativeResult result{raw, PQclear};
        if (PQresultStatus(raw) == PGRES_FATAL_ERROR) {
            failure = sqlstate(raw);
        }
    }
    check(failure == "57014", "Expected native cancellation diagnostic");
    execute(connection, "SELECT 1");
    std::cout << "cancel_sqlstate=57014\ncancel_drained_reuse=true\n";
}

std::uint64_t peak_rss_bytes() {
    rusage usage{};
    check(getrusage(RUSAGE_SELF, &usage) == 0, "RSS observation failed");
#ifdef __APPLE__
    return static_cast<std::uint64_t>(usage.ru_maxrss);
#else
    return static_cast<std::uint64_t>(usage.ru_maxrss) * 1024;
#endif
}

void scan(PGconn* connection, std::string_view mode) {
    const auto before = peak_rss_bytes();
    constexpr auto* query =
        "SELECT value,repeat('x',1024) FROM generate_series(1,$1::int) AS value";
    const char* values[]{"100000"};
    std::uint64_t rows = 0;
    std::uint64_t payload = 0;
    int max_batch = 0;
    auto consume = [&](PGresult* result) {
        check(PQresultStatus(result) == PGRES_TUPLES_OK ||
                  PQresultStatus(result) == PGRES_SINGLE_TUPLE,
              "Scan failed");
        const int batch_size = PQntuples(result);
        max_batch = std::max(max_batch, batch_size);
        for (int row = 0; row < batch_size; ++row) {
            payload += static_cast<std::uint64_t>(PQgetlength(result, row, 1));
            ++rows;
        }
    };
    if (mode == "buffered") {
        NativeResult result{
            PQexecParams(connection, query, 1, nullptr, values, nullptr, nullptr, 0), PQclear};
        check(static_cast<bool>(result), "Buffered scan returned no result");
        consume(result.get());
    } else if (mode == "single-row") {
        send_rows(connection, query, "100000");
        while (auto* raw = PQgetResult(connection)) {
            NativeResult result{raw, PQclear};
            consume(raw);
        }
    } else {
        check(mode == "cursor", "Unknown scan mode");
        execute(connection, "BEGIN");
        const std::string declaration =
            "DECLARE inventory_scan NO SCROLL CURSOR WITHOUT HOLD FOR " + std::string{query};
        NativeResult declared{
            PQexecParams(connection, declaration.c_str(), 1, nullptr, values, nullptr, nullptr, 0),
            PQclear};
        check(declared && PQresultStatus(declared.get()) == PGRES_COMMAND_OK,
              "Scan cursor declaration failed");
        for (;;) {
            auto batch = execute(connection, "FETCH FORWARD 256 FROM inventory_scan");
            consume(batch.get());
            if (PQntuples(batch.get()) == 0) {
                break;
            }
        }
        execute(connection, "CLOSE inventory_scan");
        execute(connection, "ROLLBACK");
    }
    check(rows == 100000 && payload == 102400000, "Scan payload mismatch");
    std::cout << "mode=" << mode << "\nrows=" << rows << "\npayload_bytes=" << payload
              << "\nmax_batch_rows=" << max_batch << "\npeak_rss_before_bytes=" << before
              << "\npeak_rss_after_bytes=" << peak_rss_bytes() << '\n';
}
} // namespace

int main(int argc, char** argv) {
    try {
        auto connection = connect();
        if (argc == 1) {
            inspect_protocol(connection.get());
        } else {
            scan(connection.get(), argv[1]);
        }
    } catch (const std::exception& failure) {
        std::cerr << failure.what() << '\n';
        return 1;
    }
}
