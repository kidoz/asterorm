#include "asterorm/postgres/connection.hpp"

#include <utility>

#include "asterorm/core/error.hpp"

namespace asterorm::pg {

connection::connection(PGconn* conn) : conn_(conn) {}

connection::connection(connection&& other) noexcept
    : conn_(std::exchange(other.conn_, nullptr)),
      prepared_statements_(std::move(other.prepared_statements_)),
      next_stmt_id_(std::exchange(other.next_stmt_id_, 0)) {}

connection& connection::operator=(connection&& other) noexcept {
    if (this != &other) {
        close();
        conn_ = std::exchange(other.conn_, nullptr);
        prepared_statements_ = std::move(other.prepared_statements_);
        next_stmt_id_ = std::exchange(other.next_stmt_id_, 0);
    }
    return *this;
}

connection::~connection() { close(); }

bool connection::is_open() const { return conn_ != nullptr && PQstatus(conn_) == CONNECTION_OK; }

void connection::close() {
    if (conn_) {
        PQfinish(conn_);
        conn_ = nullptr;
    }
    prepared_statements_.clear();
    next_stmt_id_ = 0;
}

static db_error make_error_from_pgresult(PGconn* conn, PGresult* res, db_error_kind kind) {
    db_error err;
    err.kind = kind;
    const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    if (sqlstate) { err.sqlstate = sqlstate; }

    const char* msg = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    if (msg) {
        err.message = msg;
    } else if (conn) {
        err.message = PQerrorMessage(conn);
    }

    const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
    if (detail) { err.detail = detail; }

    const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
    if (hint) { err.hint = hint; }

    const char* table = PQresultErrorField(res, PG_DIAG_TABLE_NAME);
    if (table) { err.table = table; }

    const char* col = PQresultErrorField(res, PG_DIAG_COLUMN_NAME);
    if (col) { err.column = col; }

    const char* constraint = PQresultErrorField(res, PG_DIAG_CONSTRAINT_NAME);
    if (constraint) { err.constraint = constraint; }

    return err;
}

asterorm::result<pg::result> connection::execute(std::string_view sql) {
    std::string sql_str{sql};
    PGresult* res = PQexec(conn_, sql_str.c_str());

    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        PQclear(res);
        return std::unexpected(err);
    }

    return pg::result{res};
}

asterorm::result<pg::result> connection::execute_params(std::string_view sql,
                                                        const std::vector<std::optional<std::string>>& params) {
    std::string sql_str{sql};

    std::vector<const char*> param_values;
    param_values.reserve(params.size());
    for (const auto& p : params) {
        param_values.push_back(p ? p->c_str() : nullptr);
    }

    PGresult* res = PQexecParams(conn_, sql_str.c_str(), static_cast<int>(params.size()),
                                 nullptr,              // paramTypes (let db infer)
                                 param_values.data(),  // paramValues
                                 nullptr,              // paramLengths (text doesn't need it)
                                 nullptr,              // paramFormats (text)
                                 0);                   // resultFormat (text)

    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        PQclear(res);
        return std::unexpected(err);
    }

    return pg::result{res};
}

asterorm::result<pg::result> connection::execute_prepared(std::string_view sql,
                                                          const std::vector<std::optional<std::string>>& params) {
    std::string sql_str{sql};
    std::string stmt_name;

    auto it = prepared_statements_.find(sql_str);
    if (it == prepared_statements_.end()) {
        stmt_name = "s_" + std::to_string(next_stmt_id_++);

        PGresult* prep_res =
            PQprepare(conn_, stmt_name.c_str(), sql_str.c_str(), static_cast<int>(params.size()), nullptr);
        ExecStatusType prep_status = PQresultStatus(prep_res);
        if (prep_status != PGRES_COMMAND_OK) {
            auto err = make_error_from_pgresult(conn_, prep_res, db_error_kind::query_failed);
            PQclear(prep_res);
            return std::unexpected(err);
        }
        PQclear(prep_res);

        prepared_statements_[sql_str] = stmt_name;
    } else {
        stmt_name = it->second;
    }

    std::vector<const char*> param_values;
    param_values.reserve(params.size());
    for (const auto& p : params) {
        param_values.push_back(p ? p->c_str() : nullptr);
    }

    PGresult* res = PQexecPrepared(conn_, stmt_name.c_str(), static_cast<int>(params.size()), param_values.data(),
                                   nullptr,  // paramLengths
                                   nullptr,  // paramFormats
                                   0);       // resultFormat

    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        PQclear(res);
        return std::unexpected(err);
    }

    return pg::result{res};
}

}  // namespace asterorm::pg
