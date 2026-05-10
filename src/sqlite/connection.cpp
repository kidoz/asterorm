#include "asterorm/sqlite/connection.hpp"

#include <algorithm>
#include <utility>

#include "asterorm/core/error.hpp"
#include "asterorm/core/session.hpp"

namespace asterorm::sqlite {

namespace {

db_error_kind classify_sqlite_error(int rc) {
    switch (rc & 0xFF) {
    case SQLITE_CONSTRAINT:
        return db_error_kind::constraint_violation;
    case SQLITE_CANTOPEN:
    case SQLITE_NOTADB:
    case SQLITE_AUTH:
        return db_error_kind::connection_failed;
    default:
        return db_error_kind::query_failed;
    }
}

db_error make_error(sqlite3* db, int rc, std::string_view fallback = {}) {
    db_error err;
    err.kind = classify_sqlite_error(rc);
    err.sqlstate = std::to_string(rc);
    if (db) {
        const char* msg = sqlite3_errmsg(db);
        err.message = msg ? msg : sqlite3_errstr(rc);
    } else if (!fallback.empty()) {
        err.message = std::string(fallback);
    } else {
        err.message = sqlite3_errstr(rc);
    }
    return err;
}

db_error make_error_msg(db_error_kind kind, std::string message) {
    db_error err;
    err.kind = kind;
    err.message = std::move(message);
    return err;
}

// Shared codecs encode bool as "t"/"f" (PostgreSQL text format). SQLite's
// numeric affinity won't coerce those, so a row stored with active = 1 won't
// match WHERE active = $1 when $1 is bound as the text "t". Translate the two
// reserved bool tokens to integer 1/0 at bind time so SQLite sees an INTEGER.
//
// Caveat: a caller passing the literal std::string "t" or "f" as a parameter
// is indistinguishable here from an encoded bool. In practice CRUD never
// produces single-character "t"/"f" strings for non-bool values; if you need
// to pass that exact text against a TEXT column, use a longer encoding.
int bind_text_or_null(sqlite3_stmt* stmt, int idx, const std::optional<std::string>& value) {
    if (!value) {
        return sqlite3_bind_null(stmt, idx);
    }
    if (value->size() == 1) {
        if ((*value)[0] == 't') {
            return sqlite3_bind_int(stmt, idx, 1);
        }
        if ((*value)[0] == 'f') {
            return sqlite3_bind_int(stmt, idx, 0);
        }
    }
    return sqlite3_bind_text(stmt, idx, value->data(), static_cast<int>(value->size()),
                             SQLITE_TRANSIENT);
}

// Bind params using positional or named placeholders. Repository SQL emits
// "$1, $2, ..."; SQLite treats those as named parameters so we look each one
// up by name and fall back to positional binding for unnamed "?" placeholders.
asterorm::result<void> bind_params(sqlite3* db, sqlite3_stmt* stmt,
                                   const std::vector<std::optional<std::string>>& params) {
    const int total_params = sqlite3_bind_parameter_count(stmt);
    if (params.size() > static_cast<std::size_t>(total_params)) {
        return std::unexpected(make_error_msg(db_error_kind::query_failed,
                                              "SQLite bind: " + std::to_string(params.size()) +
                                                  " parameters supplied for statement with " +
                                                  std::to_string(total_params) + " placeholders"));
    }

    for (size_t i = 0; i < params.size(); ++i) {
        const std::string name = "$" + std::to_string(i + 1);
        int idx = sqlite3_bind_parameter_index(stmt, name.c_str());
        if (idx == 0) {
            // Fall back to positional binding (e.g., for "?" placeholders).
            idx = static_cast<int>(i + 1);
        }
        const int rc = bind_text_or_null(stmt, idx, params[i]);
        if (rc != SQLITE_OK) {
            return std::unexpected(make_error(db, rc));
        }
    }
    return {};
}

asterorm::result<sqlite::result> step_and_collect(sqlite3* db, sqlite3_stmt* stmt) {
    const int col_count = sqlite3_column_count(stmt);

    std::vector<std::string> column_names;
    column_names.reserve(static_cast<size_t>(col_count));
    for (int c = 0; c < col_count; ++c) {
        const char* name = sqlite3_column_name(stmt, c);
        column_names.emplace_back(name ? name : "");
    }

    std::vector<std::vector<std::optional<std::string>>> rows;
    while (true) {
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_DONE) {
            break;
        }
        if (rc != SQLITE_ROW) {
            return std::unexpected(make_error(db, rc));
        }

        std::vector<std::optional<std::string>> row;
        row.reserve(static_cast<size_t>(col_count));
        for (int c = 0; c < col_count; ++c) {
            if (sqlite3_column_type(stmt, c) == SQLITE_NULL) {
                row.emplace_back(std::nullopt);
            } else {
                const unsigned char* text = sqlite3_column_text(stmt, c);
                const int bytes = sqlite3_column_bytes(stmt, c);
                row.emplace_back(
                    std::string(reinterpret_cast<const char*>(text), static_cast<size_t>(bytes)));
            }
        }
        rows.push_back(std::move(row));
    }

    // For DML, sqlite3_changes reports the rows changed by the most recent
    // statement on the connection. RETURNING-bearing INSERT/UPDATE/DELETE
    // also yield rows; the count remains accurate because sqlite3_step has
    // already finished applying the change before we read changes().
    const int affected = sqlite3_changes(db);
    return sqlite::result{std::move(column_names), std::move(rows), affected};
}

} // namespace

connection::connection(sqlite3* db) : db_(db) {}

connection::connection(connection&& other) noexcept
    : db_(std::exchange(other.db_, nullptr)),
      prepared_statements_(std::move(other.prepared_statements_)),
      prepared_statement_order_(std::move(other.prepared_statement_order_)),
      max_prepared_statements_(std::exchange(other.max_prepared_statements_, 128)) {}

connection& connection::operator=(connection&& other) noexcept {
    if (this != &other) {
        close();
        db_ = std::exchange(other.db_, nullptr);
        prepared_statements_ = std::move(other.prepared_statements_);
        prepared_statement_order_ = std::move(other.prepared_statement_order_);
        max_prepared_statements_ = std::exchange(other.max_prepared_statements_, 128);
    }
    return *this;
}

connection::~connection() {
    close();
}

bool connection::is_open() const {
    return db_ != nullptr;
}

void connection::clear_cache_locked() {
    for (auto& entry : prepared_statements_) {
        sqlite3_finalize(entry.second);
    }
    prepared_statements_.clear();
    prepared_statement_order_.clear();
}

void connection::close() {
    clear_cache_locked();
    if (db_) {
        sqlite3_close_v2(db_);
        db_ = nullptr;
    }
}

void connection::evict_one_prepared_statement() {
    if (prepared_statement_order_.empty()) {
        return;
    }
    const auto sql = std::move(prepared_statement_order_.front());
    prepared_statement_order_.pop_front();
    auto it = prepared_statements_.find(sql);
    if (it == prepared_statements_.end()) {
        return;
    }
    sqlite3_finalize(it->second);
    prepared_statements_.erase(it);
}

void connection::set_max_prepared_statements(std::size_t max_statements) {
    max_prepared_statements_ = max_statements;
    while (prepared_statements_.size() > max_prepared_statements_) {
        evict_one_prepared_statement();
    }
}

std::size_t connection::prepared_statement_count() const {
    return prepared_statements_.size();
}

void connection::clear_prepared_statement_cache() {
    clear_cache_locked();
}

asterorm::result<sqlite::result> connection::execute(std::string_view sql) {
    return execute_params(sql, {});
}

asterorm::result<sqlite::result>
connection::execute_params(std::string_view sql,
                           const std::vector<std::optional<std::string>>& params) {
    sqlite3_stmt* stmt = nullptr;
    const int rc =
        sqlite3_prepare_v2(db_, sql.data(), static_cast<int>(sql.size()), &stmt, nullptr);
    if (rc != SQLITE_OK) {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return std::unexpected(make_error(db_, rc));
    }

    auto bind_res = bind_params(db_, stmt, params);
    if (!bind_res) {
        sqlite3_finalize(stmt);
        return std::unexpected(bind_res.error());
    }

    auto out = step_and_collect(db_, stmt);
    sqlite3_finalize(stmt);
    return out;
}

asterorm::result<sqlite::result>
connection::execute_prepared(std::string_view sql,
                             const std::vector<std::optional<std::string>>& params) {
    if (max_prepared_statements_ == 0) {
        return execute_params(sql, params);
    }

    std::string sql_str{sql};
    sqlite3_stmt* stmt = nullptr;

    auto it = prepared_statements_.find(sql_str);
    if (it == prepared_statements_.end()) {
        while (prepared_statements_.size() >= max_prepared_statements_) {
            evict_one_prepared_statement();
        }
        const int rc = sqlite3_prepare_v2(db_, sql_str.c_str(), static_cast<int>(sql_str.size()),
                                          &stmt, nullptr);
        if (rc != SQLITE_OK) {
            if (stmt) {
                sqlite3_finalize(stmt);
            }
            return std::unexpected(make_error(db_, rc));
        }
        prepared_statements_.emplace(sql_str, stmt);
        prepared_statement_order_.push_back(sql_str);
    } else {
        stmt = it->second;
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }

    auto bind_res = bind_params(db_, stmt, params);
    if (!bind_res) {
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        return std::unexpected(bind_res.error());
    }

    auto out = step_and_collect(db_, stmt);
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    return out;
}

asterorm::result<void> connection::copy_in(std::string_view /*sql*/,
                                           const std::vector<std::string>& /*lines*/) {
    return std::unexpected(make_error_msg(db_error_kind::query_failed,
                                          "COPY IN is not supported by the SQLite backend"));
}

asterorm::result<std::vector<std::string>> connection::copy_out(std::string_view /*sql*/) {
    return std::unexpected(make_error_msg(db_error_kind::query_failed,
                                          "COPY OUT is not supported by the SQLite backend"));
}

std::string connection::build_begin_sql(const transaction_options& opts) {
    // SQLite supports BEGIN, BEGIN DEFERRED (default), BEGIN IMMEDIATE,
    // BEGIN EXCLUSIVE - none of which match ANSI isolation levels. Map the
    // strongest requested isolation to BEGIN IMMEDIATE so writers serialize
    // up front and avoid SQLITE_BUSY upgrades mid-transaction.
    switch (opts.isolation) {
    case isolation_level::repeatable_read:
    case isolation_level::serializable:
        return "BEGIN IMMEDIATE";
    case isolation_level::read_uncommitted:
    case isolation_level::read_committed:
    case isolation_level::default_level:
    default:
        return "BEGIN";
    }
    // read_only and deferrable are intentionally ignored: SQLite has no
    // per-transaction equivalents (read_only is set at connection level via
    // PRAGMA query_only; deferrable is PostgreSQL-specific).
}

} // namespace asterorm::sqlite
