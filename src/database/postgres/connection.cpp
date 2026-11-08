#include "asterorm/postgres/connection.hpp"
#include "diagnostics.hpp"

#include <atomic>
#include <charconv>
#include <limits>
#include <stdexcept>
#include <utility>

#include "asterorm/core/error.hpp"
#include "asterorm/core/statement_timeout.hpp"

namespace asterorm::pg {

namespace stream_detail {
struct state {
    connection* owner{};
    bool complete{};
    bool closed{};
    std::optional<db_error> error;
    std::string fetch_sql;
    std::string close_sql;
    std::size_t chunk_size{};
};
} // namespace stream_detail

static db_error reader_error(const char* message) {
    db_error error;
    error.kind = db_error_kind::query_failed;
    error.message = message;
    return error;
}

connection::connection(PGconn* conn) : conn_(conn) {}

connection::connection(connection&& other) noexcept
    : conn_(std::exchange(other.conn_, nullptr)), active_reader_(std::move(other.active_reader_)),
      prepared_statements_(std::move(other.prepared_statements_)),
      prepared_statement_order_(std::move(other.prepared_statement_order_)),
      next_stmt_id_(std::exchange(other.next_stmt_id_, 0)),
      max_prepared_statements_(std::exchange(other.max_prepared_statements_, 128)) {
    if (active_reader_) {
        active_reader_->owner = this;
    }
}

connection& connection::operator=(connection&& other) noexcept {
    if (this != &other) {
        close();
        conn_ = std::exchange(other.conn_, nullptr);
        active_reader_ = std::move(other.active_reader_);
        if (active_reader_) {
            active_reader_->owner = this;
        }
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

void connection::close() noexcept {
    if (active_reader_) {
        active_reader_->owner = nullptr;
        active_reader_.reset();
    }
    if (conn_) {
        PQfinish(conn_);
        conn_ = nullptr;
    }
    prepared_statements_.clear();
    prepared_statement_order_.clear();
    next_stmt_id_ = 0;
}

bool connection::is_pool_reusable() const noexcept {
    return is_transaction_idle() && PQpipelineStatus(conn_) == PQ_PIPELINE_OFF;
}

bool connection::single_row_ready() const noexcept {
    return !active_reader_ && is_open() &&
           (PQtransactionStatus(conn_) == PQTRANS_IDLE ||
            PQtransactionStatus(conn_) == PQTRANS_INTRANS);
}

bool connection::cursor_ready() const noexcept {
    return !active_reader_ && is_open() && PQtransactionStatus(conn_) == PQTRANS_INTRANS;
}

bool connection::is_transaction_idle() const noexcept {
    return !active_reader_ && conn_ && PQstatus(conn_) == CONNECTION_OK &&
           PQtransactionStatus(conn_) == PQTRANS_IDLE;
}

asterorm::result<std::chrono::milliseconds> connection::statement_timeout() {
    auto setting = execute_params(
        "SELECT setting, unit FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'", {});
    if (!setting) {
        return std::unexpected(setting.error());
    }
    if (setting->rows() == 1 && setting->columns() == 2 &&
        setting->get_string(0, 1) == std::optional<std::string>{"ms"}) {
        auto text = setting->get_string(0, 0);
        if (text) {
            std::chrono::milliseconds::rep count{};
            auto parsed = std::from_chars(text->data(), text->data() + text->size(), count);
            if (parsed.ec == std::errc{} && parsed.ptr == text->data() + text->size() &&
                detail::validate_statement_timeout(std::chrono::milliseconds{count})) {
                return std::chrono::milliseconds{count};
            }
        }
    }
    db_error error;
    error.kind = db_error_kind::parse_failed;
    error.message = "Invalid statement timeout setting returned by server";
    return std::unexpected(std::move(error));
}

asterorm::result<void> connection::set_statement_timeout(std::chrono::milliseconds duration) {
    auto valid = detail::validate_statement_timeout(duration);
    if (!valid) {
        return valid;
    }
    if (!is_transaction_idle()) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Session statement timeout requires an idle connection";
        return std::unexpected(std::move(error));
    }
    auto configured = execute_params("SELECT pg_catalog.set_config('statement_timeout', $1, false)",
                                     {encode_parameter(std::to_string(duration.count()) + "ms")});
    if (!configured) {
        return std::unexpected(configured.error());
    }
    return {};
}

asterorm::result<void> connection::set_local_statement_timeout(std::chrono::milliseconds duration) {
    auto valid = detail::validate_statement_timeout(duration);
    if (!valid) {
        return valid;
    }
    if (!conn_ || PQstatus(conn_) != CONNECTION_OK ||
        PQtransactionStatus(conn_) != PQTRANS_INTRANS) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Local statement timeout requires a healthy active transaction";
        return std::unexpected(std::move(error));
    }
    auto configured = execute_params("SELECT pg_catalog.set_config('statement_timeout', $1, true)",
                                     {encode_parameter(std::to_string(duration.count()) + "ms")});
    if (!configured) {
        return std::unexpected(configured.error());
    }
    return {};
}

asterorm::result<void> connection::advisory_lock(std::int64_t key) {
    auto locked = execute_params("SELECT pg_catalog.pg_advisory_lock($1::bigint)",
                                 {asterorm::encode_parameter(key)});
    if (!locked) {
        return std::unexpected(locked.error());
    }
    return {};
}

asterorm::result<void> connection::advisory_unlock(std::int64_t key) {
    auto unlocked = execute_params("SELECT pg_catalog.pg_advisory_unlock($1::bigint)",
                                   {asterorm::encode_parameter(key)});
    if (!unlocked) {
        return std::unexpected(unlocked.error());
    }
    if (unlocked->rows() != 1 || unlocked->columns() != 1 ||
        unlocked->get_string(0, 0) != std::optional<std::string>{"t"}) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Advisory lock was not released by this connection";
        return std::unexpected(std::move(error));
    }
    return {};
}

using diagnostics::make_error_from_pgresult;

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

static asterorm::result<void> validate_text_params(const query_parameters& params) {
    for (const auto& parameter : params) {
        if (parameter.error)
            return std::unexpected(*parameter.error);
        if (parameter.value && parameter.value->find('\0') != std::string::npos) {
            db_error error;
            error.kind = db_error_kind::query_failed;
            error.message = "PostgreSQL text parameter contains an embedded NUL byte";
            return std::unexpected(error);
        }
    }
    return {};
}

asterorm::result<pg::result> connection::execute(std::string_view sql) {
    if (active_reader_) {
        return std::unexpected(reader_error("Connection is busy with a native reader"));
    }
    std::string sql_str{sql};
    PGresult* res = PQexec(conn_, sql_str.c_str());

    if (!command_or_rows_ok(res)) {
        auto err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        clear_result(res);
        return std::unexpected(err);
    }

    return pg::result::from_native(res);
}

asterorm::result<pg::result> connection::execute_params(std::string_view sql,
                                                        const query_parameters& params) {
    if (active_reader_) {
        return std::unexpected(reader_error("Connection is busy with a native reader"));
    }
    auto validation = validate_text_params(params);
    if (!validation)
        return std::unexpected(validation.error());

    std::string sql_str{sql};

    std::vector<const char*> param_values;
    param_values.reserve(params.size());
    for (const auto& parameter : params) {
        param_values.push_back(parameter.value ? parameter.value->c_str() : nullptr);
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

    return pg::result::from_native(res);
}

asterorm::result<pg::result> connection::execute_prepared(std::string_view sql,
                                                          const query_parameters& params) {
    if (active_reader_) {
        return std::unexpected(reader_error("Connection is busy with a native reader"));
    }
    if (max_prepared_statements_ == 0) {
        return execute_params(sql, params);
    }

    auto validation = validate_text_params(params);
    if (!validation)
        return std::unexpected(validation.error());

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
    for (const auto& parameter : params) {
        param_values.push_back(parameter.value ? parameter.value->c_str() : nullptr);
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
                return pg::result::from_native(res);
            }
            err = make_error_from_pgresult(conn_, res, db_error_kind::query_failed);
        }
        clear_result(res);
        return std::unexpected(err);
    }

    return pg::result::from_native(res);
}

asterorm::result<single_row_reader> connection::open_single_row(std::string_view sql,
                                                                const query_parameters& params) {
    if (active_reader_) {
        return std::unexpected(reader_error("Connection is busy with a native reader"));
    }
    if (sql.empty() || sql.contains('\0')) {
        auto error = reader_error("Single-row SQL must be nonempty and contain no NUL bytes");
        error.kind = db_error_kind::parse_failed;
        return std::unexpected(std::move(error));
    }
    auto valid = validate_text_params(params);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    if (params.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return std::unexpected(reader_error("Too many single-row parameters"));
    }
    if (!single_row_ready()) {
        return std::unexpected(reader_error("Single-row reading requires a healthy connection"));
    }
    std::string statement{sql};
    std::vector<const char*> values;
    values.reserve(params.size());
    for (const auto& parameter : params) {
        values.push_back(parameter.value ? parameter.value->c_str() : nullptr);
    }
    auto state = std::make_shared<stream_detail::state>();
    state->owner = this;
    active_reader_ = state;
    try {
        if (PQsendQueryParams(conn_, statement.c_str(), static_cast<int>(params.size()), nullptr,
                              values.data(), nullptr, nullptr, 0) != 1) {
            auto error = make_error_from_connection(conn_, db_error_kind::query_failed,
                                                    "Single-row dispatch failed");
            close();
            return std::unexpected(std::move(error));
        }
        if (PQsetSingleRowMode(conn_) != 1) {
            close();
            return std::unexpected(reader_error("Single-row mode activation failed"));
        }
        return single_row_reader{std::move(state)};
    } catch (...) {
        close();
        throw;
    }
}

asterorm::result<cursor_reader> connection::open_cursor(std::string_view sql,
                                                        const query_parameters& params,
                                                        std::size_t chunk_size) {
    if (sql.empty() || sql.contains('\0') || chunk_size == 0 || chunk_size > 65536) {
        auto error = reader_error("Invalid cursor SQL or chunk size");
        error.kind = db_error_kind::parse_failed;
        return std::unexpected(std::move(error));
    }
    auto valid = validate_text_params(params);
    if (!valid) {
        return std::unexpected(std::move(valid.error()));
    }
    if (params.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return std::unexpected(reader_error("Too many cursor parameters"));
    }
    if (!cursor_ready()) {
        return std::unexpected(reader_error("Cursor reading requires a healthy transaction"));
    }
    static std::atomic<std::uint64_t> next_cursor{0};
    const auto name = "\"_asterorm_cursor_" +
                      std::to_string(next_cursor.fetch_add(1, std::memory_order_relaxed)) + "\"";
    std::string statement = "DECLARE " + name + " NO SCROLL CURSOR WITHOUT HOLD FOR ";
    statement.append(sql);
    auto state = std::make_shared<stream_detail::state>();
    state->fetch_sql = "FETCH FORWARD " + std::to_string(chunk_size) + " FROM " + name;
    state->close_sql = "CLOSE " + name;
    state->chunk_size = chunk_size;
    std::vector<const char*> values;
    values.reserve(params.size());
    for (const auto& parameter : params) {
        values.push_back(parameter.value ? parameter.value->c_str() : nullptr);
    }
    state->owner = this;
    active_reader_ = state;
    try {
        std::unique_ptr<PGresult, decltype(&PQclear)> response{
            PQexecParams(conn_, statement.c_str(), static_cast<int>(params.size()), nullptr,
                         values.data(), nullptr, nullptr, 0),
            PQclear};
        if (!response || PQresultStatus(response.get()) != PGRES_COMMAND_OK ||
            std::string_view{PQcmdStatus(response.get())} != "DECLARE CURSOR" ||
            PQstatus(conn_) != CONNECTION_OK || PQtransactionStatus(conn_) != PQTRANS_INTRANS) {
            auto error =
                make_error_from_pgresult(conn_, response.get(), db_error_kind::query_failed);
            close();
            return std::unexpected(std::move(error));
        }
        return cursor_reader{std::move(state)};
    } catch (...) {
        close();
        throw;
    }
}

cursor_reader::cursor_reader(std::shared_ptr<stream_detail::state> state)
    : state_(std::move(state)) {}
cursor_reader::cursor_reader(cursor_reader&&) noexcept = default;
cursor_reader& cursor_reader::operator=(cursor_reader&& other) noexcept {
    if (this != &other) {
        abandon();
        state_ = std::move(other.state_);
    }
    return *this;
}
cursor_reader::~cursor_reader() {
    abandon();
}
void cursor_reader::abandon() noexcept {
    if (state_ && state_->owner) {
        state_->owner->close();
    }
}

asterorm::result<pg::result> cursor_reader::command(const std::string& statement, bool tuples) {
    auto* owner = state_->owner;
    try {
        // SQL is an adapter-generated, already terminated string; no caller fragments.
        std::unique_ptr<PGresult, decltype(&PQclear)> response{
            PQexec(owner->conn_, statement.c_str()), PQclear};
        const auto status = tuples ? PGRES_TUPLES_OK : PGRES_COMMAND_OK;
        if (!response || PQresultStatus(response.get()) != status ||
            (!tuples && std::string_view{PQcmdStatus(response.get())} != "CLOSE CURSOR") ||
            (tuples && (PQnfields(response.get()) == 0 ||
                        std::cmp_greater(PQntuples(response.get()), state_->chunk_size))) ||
            PQstatus(owner->conn_) != CONNECTION_OK ||
            PQtransactionStatus(owner->conn_) != PQTRANS_INTRANS) {
            state_->error =
                make_error_from_pgresult(owner->conn_, response.get(), db_error_kind::query_failed);
            owner->close();
            return std::unexpected(*state_->error);
        }
        auto checked = pg::result::from_native(response.release());
        if (!checked) {
            state_->error = std::move(checked.error());
            owner->close();
            return std::unexpected(*state_->error);
        }
        return checked;
    } catch (...) {
        abandon();
        throw;
    }
}

asterorm::result<void> cursor_reader::close() {
    if (!state_) {
        return {}; // default and moved-from handles own nothing
    }
    if (state_->error) {
        return std::unexpected(*state_->error);
    }
    if (state_->complete) {
        return {};
    }
    if (!state_->owner) {
        state_->error = reader_error("Cursor connection was discarded");
        return std::unexpected(*state_->error);
    }
    auto closed = command(state_->close_sql, false);
    if (!closed) {
        return std::unexpected(std::move(closed.error()));
    }
    auto* owner = state_->owner;
    state_->complete = true;
    state_->owner = nullptr;
    owner->active_reader_.reset();
    return {};
}

asterorm::result<std::optional<pg::result>> cursor_reader::next() {
    if (!state_) {
        return std::unexpected(reader_error("Cursor reader is not active"));
    }
    if (state_->error) {
        return std::unexpected(*state_->error);
    }
    if (state_->complete) {
        return std::nullopt;
    }
    if (!state_->owner) {
        state_->error = reader_error("Cursor connection was discarded");
        return std::unexpected(*state_->error);
    }
    auto batch = command(state_->fetch_sql, true);
    if (!batch) {
        return std::unexpected(std::move(batch.error()));
    }
    if (batch->rows() == 0) {
        auto closed = close();
        if (!closed) {
            return std::unexpected(std::move(closed.error()));
        }
        return std::nullopt;
    }
    return std::optional<pg::result>{std::move(*batch)};
}

single_row_reader::single_row_reader(std::shared_ptr<stream_detail::state> state)
    : state_(std::move(state)) {}

single_row_reader::single_row_reader(single_row_reader&& other) noexcept = default;

single_row_reader& single_row_reader::operator=(single_row_reader&& other) noexcept {
    if (this != &other) {
        close();
        state_ = std::move(other.state_);
    }
    return *this;
}

single_row_reader::~single_row_reader() {
    close();
}

void single_row_reader::close() noexcept {
    if (state_ && state_->owner) {
        state_->closed = true;
        state_->owner->close();
    }
}

asterorm::result<std::optional<pg::result>> single_row_reader::fail(db_error error) {
    state_->error = std::move(error);
    if (state_->owner) {
        state_->owner->close();
    }
    return std::unexpected(*state_->error);
}

asterorm::result<std::optional<pg::result>> single_row_reader::next() {
    if (!state_) {
        return std::unexpected(reader_error("Single-row reader is not active"));
    }
    if (state_->error) {
        return std::unexpected(*state_->error);
    }
    if (state_->complete || state_->closed) {
        return std::nullopt;
    }
    if (!state_->owner) {
        return fail(reader_error("Single-row connection was discarded"));
    }
    auto* owner = state_->owner;
    try {
        std::unique_ptr<PGresult, decltype(&PQclear)> response{PQgetResult(owner->conn_), PQclear};
        if (!response) {
            return fail(make_error_from_connection(owner->conn_, db_error_kind::query_failed,
                                                   "Single-row query has no terminal result"));
        }
        const auto status = PQresultStatus(response.get());
        if (status == PGRES_SINGLE_TUPLE) {
            auto row = pg::result::from_single_tuple(response.release());
            if (!row) {
                return fail(std::move(row.error()));
            }
            return std::optional<pg::result>{std::move(*row)};
        }
        if (status != PGRES_TUPLES_OK) {
            auto error =
                make_error_from_pgresult(owner->conn_, response.get(), db_error_kind::query_failed);
            if (error.message.empty()) {
                error.message = "Unexpected PostgreSQL single-row result status";
            }
            return fail(std::move(error));
        }
        if (PQntuples(response.get()) != 0) {
            return fail(reader_error("Single-row terminal result unexpectedly contains rows"));
        }
        response.reset(PQgetResult(owner->conn_));
        if (response) {
            auto error =
                make_error_from_pgresult(owner->conn_, response.get(), db_error_kind::query_failed);
            if (error.message.empty()) {
                error.message = "Unexpected result after single-row completion";
            }
            return fail(std::move(error));
        }
        if (PQstatus(owner->conn_) != CONNECTION_OK) {
            return fail(make_error_from_connection(owner->conn_, db_error_kind::connection_failed,
                                                   "Connection lost at single-row completion"));
        }
        const auto transaction = PQtransactionStatus(owner->conn_);
        if (transaction != PQTRANS_IDLE && transaction != PQTRANS_INTRANS) {
            return fail(reader_error("Connection is not ready after single-row completion"));
        }
        state_->complete = true;
        state_->owner = nullptr;
        owner->active_reader_.reset();
        return std::nullopt;
    } catch (...) {
        if (state_->owner) {
            state_->owner->close();
        }
        throw;
    }
}

void connection::set_max_prepared_statements(size_t max_statements) {
    if (active_reader_) {
        throw std::logic_error("Connection is busy with a native reader");
    }
    max_prepared_statements_ = max_statements;
    while (prepared_statements_.size() > max_prepared_statements_) {
        evict_one_prepared_statement();
    }
}

size_t connection::prepared_statement_count() const {
    return prepared_statements_.size();
}

void connection::clear_prepared_statement_cache() {
    if (active_reader_) {
        throw std::logic_error("Connection is busy with a native reader");
    }
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
    if (active_reader_) {
        return std::unexpected(reader_error("Connection is busy with a native reader"));
    }
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
    if (active_reader_) {
        return std::unexpected(reader_error("Connection is busy with a native reader"));
    }
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
