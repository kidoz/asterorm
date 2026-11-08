#include "asterorm/pool/connection_pool.hpp"
#include "asterorm/postgres/connection.hpp"
#include <iostream>

namespace {
struct observed_driver {
    PGconn** raw;
    int* calls;
    int mode;
    [[nodiscard]] asterorm::result<asterorm::pg::connection>
    connect(const std::string& settings) const {
        ++*calls;
        *raw = PQconnectdb(settings.c_str());
        if (PQstatus(*raw) != CONNECTION_OK || PQsetnonblocking(*raw, mode) != 0) {
            asterorm::db_error error;
            error.kind = asterorm::db_error_kind::connection_failed;
            error.message = PQerrorMessage(*raw);
            PQfinish(std::exchange(*raw, nullptr));
            return std::unexpected(std::move(error));
        }
        return asterorm::pg::connection{*raw};
    }
};
int run(const std::string& scenario, const std::string& port, int mode) {
    PGconn* raw{};
    int calls{};
    asterorm::pool_config config;
    config.conninfo =
        "host=127.0.0.1 port=" + port +
        " user=pool_probe dbname=pool_probe sslmode=disable gssencmode=disable connect_timeout=2";
    config.max_size = 1;
    config.pre_ping = true;
    config.pre_ping_timeout = std::chrono::milliseconds{
        scenario == "timeout" || scenario == "drain_timeout" ? 500 : 2000};
    asterorm::connection_pool<observed_driver> pool{
        observed_driver{.raw = &raw, .calls = &calls, .mode = mode}, config};
    if (pool.warmup_error()) {
        std::cerr << pool.warmup_error()->message;
        return 1;
    }
    auto acquired = pool.acquire();
    if (scenario != "success") {
        if (acquired || pool.stats().total != 0 || calls != 1) {
            return 2;
        }
        const auto& error = acquired.error();
        if (scenario == "timeout" || scenario == "drain_timeout") {
            return error.kind == asterorm::db_error_kind::timeout && error.sqlstate.empty() ? 0 : 3;
        }
        if (scenario == "error" || scenario == "fatal") {
            return error.kind == (scenario == "fatal"
                                      ? asterorm::db_error_kind::connection_failed
                                      : asterorm::db_error_kind::constraint_violation) &&
                           error.sqlstate == (scenario == "fatal" ? "57P01" : "23505") &&
                           error.message == "health sentinel" &&
                           error.detail == "detail sentinel" && error.hint == "hint sentinel" &&
                           error.table == "health_table" && error.column == "health_column" &&
                           error.constraint == "health_constraint"
                       ? 0
                       : 4;
        }
        return error.kind == (scenario == "wrong" ? asterorm::db_error_kind::query_failed
                                                  : asterorm::db_error_kind::connection_failed)
                   ? 0
                   : 5;
    }
    if (!acquired || PQisnonblocking(raw) != mode || (*acquired)->prepared_statement_count() != 0 ||
        PQtransactionStatus(raw) != PQTRANS_IDLE) {
        return 6;
    }
    if (PQsetnonblocking(raw, 1) != 0 || PQenterPipelineMode(raw) != 1) {
        return 10;
    }
    auto rejected =
        (*acquired)->pool_ping(std::chrono::steady_clock::now() + std::chrono::seconds{1});
    if (rejected || !(*acquired)->is_open() || (*acquired)->is_pool_reusable() ||
        PQexitPipelineMode(raw) != 1 || PQsetnonblocking(raw, mode) != 0) {
        return 11;
    }
    auto query = (*acquired)->execute_params("SELECT 41", {});
    if (!query || query->get_string(0, 0) != "41") {
        return 7;
    }
    acquired->release_to_pool();
    auto reused = pool.acquire();
    if (!reused || calls != 1 || PQisnonblocking(raw) != mode ||
        (*reused)->prepared_statement_count() != 0 || PQtransactionStatus(raw) != PQTRANS_IDLE) {
        return 8;
    }
    return 0;
}
} // namespace
int main(int count, char** arguments) {
    try {
        if (count != 4) {
            return 90;
        }
        return run(arguments[1], arguments[2], std::string_view{arguments[3]} == "1" ? 1 : 0);
    } catch (const std::exception& error) {
        std::cerr << error.what();
        return 99;
    }
}
