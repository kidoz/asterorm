#include "asterorm/postgres/connection.hpp"

#include <utility>

#include "asterorm/core/error.hpp"

namespace asterorm::pg {

connection::connection(PGconn* conn) : conn_(conn) {}

connection::connection(connection&& other) noexcept
    : conn_(std::exchange(other.conn_, nullptr)),
      prepared_statements_(std::move(other.prepared_statements_)),
      prepared_statement_order_(std::move(other.prepared_statement_order_)),
      next_stmt_id_(std::exchange(other.next_stmt_id_, 0)),
      max_prepared_statements_(std::exchange(other.max_prepared_statements_, 128)) {}

connection& connection::operator=(connection&& other) noexcept {
    if (this != &other) {
        close();
        conn_ = std::exchange(other.conn_, nullptr);
        prepared_statements_ = std::move(other.prepared_statements_);
        prepared_statement_order_ = std::move(other.prepared_statement_order_);
        next_stmt_id_ = std::exchange(other.next_stmt_id_, 0);
        max_prepared_statements_ = std::exchange(other.max_prepared_statements_, 128);
    }
    return *this;
}

connection::~connection() {
    close();
}

bool connection::is_open() const {
    return conn_ != nullptr && PQstatus(conn_) == CONNECTION_OK;
}

void connection::close() {
    if (conn_) {
        PQfinish(conn_);
        conn_ = nullptr;
    }
    prepared_statements_.clear();
    prepared_statement_order_.clear();
    next_stmt_id_ = 0;
}

static db_error make_error_from_pgresult(PGconn* conn, PGresult* res, db_error_kind fallback) {
    db_error err;
    if (!res) {
        err.kind = fallback;
        if (conn) {
            err.message = PQerrorMessage(conn);
        }
        if (err.message.empty()) {
            err.message = "PostgreSQL operation failed without a result";
        }
        return err;
    }

    const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    if (sqlstate) {
        err.sqlstate = sqlstate;
    }
    err.kind = err.sqlstate.empty() ? fallback : classify_sqlstate(err.sqlstate);

    const char* msg = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    if (msg) {
        err.message = msg;
    } else if (conn) {
        err.message = PQerrorMessage(conn);
    }

    const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
    if (detail) {
        err.detail = detail;
    }

    const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
    if (hint) {
        err.hint = hint;
    }

    const char* table = PQresultErrorField(res, PG_DIAG_TABLE_NAME);
    if (table) {
        err.table = table;
    }

    const char* col = PQresultErrorField(res, PG_DIAG_COLUMN_NAME);
    if (col) {
        err.column = col;
    }

    const char* constraint = PQresultErrorField(res, PG_DIAG_CONSTRAINT_NAME);
    if (constraint) {
        err.constraint = constraint;
    }

    return err;
}

static db_error make_error_from_connection(PGconn* conn, db_error_kind fallback,
                                           std::string_view fallback_message) {
    db_error err;
    err.kind = fallback;
    if (conn) {
        err.message = PQerrorMessage(conn);
    }
    if (err.message.empty()) {
        err.message = std::string{fallback_message};
    }
    return err;
}

static bool command_or_rows_ok(PGresult* res) {
    if (!res)
        return false;
    const ExecStatusType status = PQresultStatus(res);
    return status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK;
}

static void clear_result(PGresult* res) {
    if (res) {
        PQclear(res);
    }
}

asterorm::result<pg::result> connection::execute(std::string_view sql) {
    std::string sql_str{sql};
    PGresult* res = PQexec(conn_, sql_str.c_str());

    if (!command_or_rows_ok(res)) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        clear_result(res);
        return std::unexpected(err);
    }

    return pg::result{res};
}

asterorm::result<pg::result>
connection::execute_params(std::string_view sql,
                           const std::vector<std::optional<std::string>>& params) {
    std::string sql_str{sql};

    std::vector<const char*> param_values;
    param_values.reserve(params.size());
    for (const auto& p : params) {
        param_values.push_back(p ? p->c_str() : nullptr);
    }

    PGresult* res = PQexecParams(conn_, sql_str.c_str(), static_cast<int>(params.size()),
                                 nullptr,             // paramTypes (let db infer)
                                 param_values.data(), // paramValues
                                 nullptr,             // paramLengths (text doesn't need it)
                                 nullptr,             // paramFormats (text)
                                 0);                  // resultFormat (text)

    if (!command_or_rows_ok(res)) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        clear_result(res);
        return std::unexpected(err);
    }

    return pg::result{res};
}

asterorm::result<pg::result>
connection::execute_prepared(std::string_view sql,
                             const std::vector<std::optional<std::string>>& params) {
    if (max_prepared_statements_ == 0) {
        return execute_params(sql, params);
    }

    std::string sql_str{sql};
    std::string stmt_name;

    auto it = prepared_statements_.find(sql_str);
    if (it == prepared_statements_.end()) {
        auto prepared = prepare_statement(sql_str, params.size());
        if (!prepared)
            return std::unexpected(prepared.error());
        stmt_name = std::move(*prepared);
    } else {
        stmt_name = it->second;
    }

    std::vector<const char*> param_values;
    param_values.reserve(params.size());
    for (const auto& p : params) {
        param_values.push_back(p ? p->c_str() : nullptr);
    }

    PGresult* res = PQexecPrepared(conn_, stmt_name.c_str(), static_cast<int>(params.size()),
                                   param_values.data(),
                                   nullptr, // paramLengths
                                   nullptr, // paramFormats
                                   0);      // resultFormat

    if (!command_or_rows_ok(res)) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        if (err.sqlstate == "26000") {
            clear_result(res);
            forget_prepared_statement(sql_str);
            auto prepared = prepare_statement(sql_str, params.size());
            if (!prepared)
                return std::unexpected(prepared.error());
            res = PQexecPrepared(conn_, prepared->c_str(), static_cast<int>(params.size()),
                                 param_values.data(), nullptr, nullptr, 0);
            if (command_or_rows_ok(res)) {
                return pg::result{res};
            }
            err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        }
        clear_result(res);
        return std::unexpected(err);
    }

    return pg::result{res};
}

void connection::set_max_prepared_statements(size_t max_statements) {
    max_prepared_statements_ = max_statements;
    while (prepared_statements_.size() > max_prepared_statements_) {
        evict_one_prepared_statement();
    }
}

size_t connection::prepared_statement_count() const {
    return prepared_statements_.size();
}

void connection::clear_prepared_statement_cache() {
    while (!prepared_statement_order_.empty()) {
        evict_one_prepared_statement();
    }
    prepared_statements_.clear();
}

asterorm::result<std::string> connection::prepare_statement(std::string_view sql,
                                                            size_t param_count) {
    while (prepared_statements_.size() >= max_prepared_statements_) {
        evict_one_prepared_statement();
    }

    std::string sql_str{sql};
    std::string stmt_name = "s_" + std::to_string(next_stmt_id_++);

    PGresult* prep_res = PQprepare(conn_, stmt_name.c_str(), sql_str.c_str(),
                                   static_cast<int>(param_count), nullptr);
    ExecStatusType prep_status = prep_res ? PQresultStatus(prep_res) : PGRES_FATAL_ERROR;
    if (prep_status != PGRES_COMMAND_OK) {
        auto err = make_error_from_pgresult(conn_, prep_res, db_error_kind::query_failed);
        clear_result(prep_res);
        return std::unexpected(err);
    }
    clear_result(prep_res);

    prepared_statements_[sql_str] = stmt_name;
    prepared_statement_order_.push_back(sql_str);
    return stmt_name;
}

void connection::evict_one_prepared_statement() {
    if (prepared_statement_order_.empty())
        return;

    const auto sql = std::move(prepared_statement_order_.front());
    prepared_statement_order_.pop_front();

    auto it = prepared_statements_.find(sql);
    if (it == prepared_statements_.end())
        return;

    if (conn_) {
        std::string deallocate = "DEALLOCATE ";
        deallocate += it->second;
        PGresult* res = PQexec(conn_, deallocate.c_str());
        clear_result(res);
    }
    prepared_statements_.erase(it);
}

void connection::forget_prepared_statement(std::string_view sql) {
    std::string key{sql};
    prepared_statements_.erase(key);
    for (auto it = prepared_statement_order_.begin(); it != prepared_statement_order_.end(); ++it) {
        if (*it == key) {
            prepared_statement_order_.erase(it);
            break;
        }
    }
}

asterorm::result<void> connection::copy_in(std::string_view sql,
                                           const std::vector<std::string>& lines) {
    std::string sql_str{sql};
    PGresult* res = PQexec(conn_, sql_str.c_str());
    if (PQresultStatus(res) != PGRES_COPY_IN) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        clear_result(res);
        close(); // Poisoned state, close connection
        return std::unexpected(err);
    }
    clear_result(res);

    for (const auto& line : lines) {
        std::string line_with_nl = line + "\n";
        if (PQputCopyData(conn_, line_with_nl.c_str(), static_cast<int>(line_with_nl.size())) !=
            1) {
            auto err = make_error_from_connection(conn_, db_error_kind::query_failed,
                                                  "PostgreSQL COPY input failed");
            close(); // Poisoned state, close connection
            return std::unexpected(err);
        }
    }

    if (PQputCopyEnd(conn_, nullptr) != 1) {
        auto err = make_error_from_connection(conn_, db_error_kind::query_failed,
                                              "PostgreSQL COPY end failed");
        close(); // Poisoned state, close connection
        return std::unexpected(err);
    }

    res = PQgetResult(conn_);
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        clear_result(res);
        close(); // Poisoned state, close connection
        return std::unexpected(err);
    }
    clear_result(res);

    return {};
}

asterorm::result<std::vector<std::string>> connection::copy_out(std::string_view sql) {
    std::string sql_str{sql};
    PGresult* res = PQexec(conn_, sql_str.c_str());
    if (PQresultStatus(res) != PGRES_COPY_OUT) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        clear_result(res);
        close(); // Poisoned state, close connection
        return std::unexpected(err);
    }
    clear_result(res);

    std::vector<std::string> lines;
    char* buffer = nullptr;
    int bytes = 0;
    while ((bytes = PQgetCopyData(conn_, &buffer, 0)) > 0) {
        if (buffer) {
            lines.emplace_back(buffer, bytes);
            PQfreemem(buffer);
        }
    }

    if (bytes == -2) {
        auto err = make_error_from_connection(conn_, db_error_kind::query_failed,
                                              "PostgreSQL COPY output failed");
        close(); // Poisoned state, close connection
        return std::unexpected(err);
    }

    res = PQgetResult(conn_);
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        clear_result(res);
        close(); // Poisoned state, close connection
        return std::unexpected(err);
    }
    clear_result(res);

    return lines;
}

asterorm::result<void> connection::copy_in_rows(std::string_view sql,
                                                const std::vector<copy_row>& rows) {
    return copy_in(sql, encode_copy_rows(rows));
}

asterorm::result<std::vector<copy_row>> connection::copy_out_rows(std::string_view sql) {
    auto lines = copy_out(sql);
    if (!lines)
        return std::unexpected(lines.error());
    return decode_copy_rows(*lines);
}

} // namespace asterorm::pg
