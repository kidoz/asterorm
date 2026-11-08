#include "asterorm/mysql/connection.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "asterorm/core/session.hpp"
#include "asterorm/mysql/detail/params.hpp"
#include "client.hpp"

namespace asterorm::mysql {
namespace {

db_error make_error(unsigned int number, const char* state, const char* message) {
    auto failure = detail::error(message);
    failure.sqlstate = state;
    failure.kind = classify_sqlstate(failure.sqlstate);
    failure.detail = "MySQL error " + std::to_string(number);
    if (number == 1213) {
        failure.kind = db_error_kind::deadlock_detected;
    } else if (number == 1205) {
        failure.kind = db_error_kind::timeout;
    } else if (number == 2006 || number == 2013 || number == 2055) {
        failure.kind = db_error_kind::connection_failed;
    }
    return failure;
}

db_error statement_error(MYSQL_STMT* statement) {
    return make_error(mysql_stmt_errno(statement), mysql_stmt_sqlstate(statement),
                      mysql_stmt_error(statement));
}

asterorm::result<mysql::result> finish_results(MYSQL* handle,
                                               asterorm::result<mysql::result> output) {
    bool multiple = false;
    while (mysql_more_results(handle)) {
        multiple = true;
        const auto advanced = mysql_next_result(handle);
        if (advanced > 0) {
            return std::unexpected(
                make_error(mysql_errno(handle), mysql_sqlstate(handle), mysql_error(handle)));
        }
        if (advanced < 0) {
            break;
        }
        std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> extra{mysql_store_result(handle),
                                                                       mysql_free_result};
        if (!extra && mysql_field_count(handle) != 0) {
            return std::unexpected(
                make_error(mysql_errno(handle), mysql_sqlstate(handle), mysql_error(handle)));
        }
    }
    if (multiple) {
        return std::unexpected(detail::error("Multiple MySQL result sets are not supported"));
    }
    return output;
}

asterorm::result<std::uint64_t> row_count(my_ulonglong count) {
    if (count == std::numeric_limits<my_ulonglong>::max()) {
        return std::unexpected(detail::error("MySQL affected-row count is unavailable"));
    }
    return static_cast<std::uint64_t>(count);
}

asterorm::result<mysql::result> execute_statement(MYSQL_STMT* statement,
                                                  const query_parameters& params) {
    if (mysql_stmt_param_count(statement) != params.size()) {
        return std::unexpected(detail::error("MySQL prepared parameter count mismatch"));
    }
    std::vector<MYSQL_BIND> bindings(params.size());
    std::vector<std::string> values(params.size());
    for (std::size_t index = 0; index < params.size(); ++index) {
        auto& binding = bindings[index];
        if (!params[index].value) {
            binding.buffer_type = MYSQL_TYPE_NULL;
            continue;
        }
        values[index] = *params[index].value;
        if (params[index].type == parameter_type::boolean) {
            if (values[index] != "t" && values[index] != "f") {
                return std::unexpected(detail::error("Invalid encoded MySQL boolean"));
            }
            values[index] = values[index] == "t" ? "1" : "0";
        }
        binding.buffer_type = MYSQL_TYPE_STRING;
        binding.buffer = values[index].data();
        binding.buffer_length = static_cast<unsigned long>(values[index].size());
    }
    if ((!bindings.empty() && mysql_stmt_bind_param(statement, bindings.data()) != 0) ||
        mysql_stmt_execute(statement) != 0) {
        return std::unexpected(statement_error(statement));
    }

    const auto insert_id = mysql_stmt_insert_id(statement);
    std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> metadata{
        mysql_stmt_result_metadata(statement), mysql_free_result};
    if (!metadata) {
        if (mysql_stmt_field_count(statement) != 0) {
            return std::unexpected(statement_error(statement));
        }
        auto affected = row_count(mysql_stmt_affected_rows(statement));
        if (!affected) {
            return std::unexpected(affected.error());
        }
        return mysql::result{{}, {}, *affected, insert_id};
    }
    if (mysql_stmt_store_result(statement) != 0) {
        return std::unexpected(statement_error(statement));
    }
    const auto columns = mysql_num_fields(metadata.get());
    const auto* fields = mysql_fetch_fields(metadata.get());
    std::vector<std::string> names;
    names.reserve(columns);
    std::vector<MYSQL_BIND> output_bindings(columns);
    // MySQL uses bool*, while MariaDB Connector/C uses my_bool*.
    using null_flag = std::remove_pointer_t<decltype(MYSQL_BIND{}.is_null)>;
    struct column_null {
        null_flag value{};
    };
    std::vector<column_null> nulls(columns);
    std::vector<unsigned long> lengths(columns);
    for (unsigned int column = 0; column < columns; ++column) {
        names.emplace_back(fields[column].name, fields[column].name_length);
        output_bindings[column].buffer_type = MYSQL_TYPE_STRING;
        output_bindings[column].length = &lengths[column];
        output_bindings[column].is_null = &nulls[column].value;
    }
    if (mysql_stmt_bind_result(statement, output_bindings.data()) != 0) {
        return std::unexpected(statement_error(statement));
    }
    std::vector<std::vector<std::optional<std::string>>> rows;
    while (true) {
        const auto fetched = mysql_stmt_fetch(statement);
        if (fetched == MYSQL_NO_DATA) {
            break;
        }
        if (fetched != 0 && fetched != MYSQL_DATA_TRUNCATED) {
            return std::unexpected(statement_error(statement));
        }
        std::vector<std::optional<std::string>> row;
        row.reserve(columns);
        for (unsigned int column = 0; column < columns; ++column) {
            if (nulls[column].value) {
                row.emplace_back(std::nullopt);
                continue;
            }
            std::string value(lengths[column], '\0');
            MYSQL_BIND field_binding{};
            field_binding.buffer_type = MYSQL_TYPE_STRING;
            field_binding.buffer = value.data();
            field_binding.buffer_length = lengths[column];
            if (mysql_stmt_fetch_column(statement, &field_binding, column, 0) != 0) {
                return std::unexpected(statement_error(statement));
            }
            row.emplace_back(std::move(value));
        }
        rows.push_back(std::move(row));
    }
    return mysql::result{std::move(names), std::move(rows), 0, insert_id};
}

} // namespace

connection::connection(MYSQL* handle) : handle_(handle) {}
connection::~connection() {
    close();
}
connection::connection(connection&& other) noexcept
    : handle_(std::exchange(other.handle_, nullptr)), statements_(std::move(other.statements_)),
      statement_order_(std::move(other.statement_order_)),
      max_prepared_statements_(other.max_prepared_statements_),
      session_invariants_current_(other.session_invariants_current_) {
    other.statements_.clear();
    other.statement_order_.clear();
    other.session_invariants_current_ = true;
}
connection& connection::operator=(connection&& other) noexcept {
    if (this != &other) {
        close();
        handle_ = std::exchange(other.handle_, nullptr);
        statements_ = std::move(other.statements_);
        statement_order_ = std::move(other.statement_order_);
        max_prepared_statements_ = other.max_prepared_statements_;
        session_invariants_current_ = other.session_invariants_current_;
        other.statements_.clear();
        other.statement_order_.clear();
        other.session_invariants_current_ = true;
    }
    return *this;
}
bool connection::is_open() const {
    return handle_ != nullptr;
}
bool connection::in_transaction() const noexcept {
    return handle_ && (handle_->server_status & SERVER_STATUS_IN_TRANS) != 0;
}
bool connection::is_pool_reusable() const noexcept {
    return handle_ && !in_transaction() && handle_->status == MYSQL_STATUS_READY &&
           !mysql_more_results(handle_);
}
void connection::close() noexcept {
    clear_prepared_statement_cache();
    if (handle_) {
        mysql_close(std::exchange(handle_, nullptr));
    }
}
void connection::clear_prepared_statement_cache() {
    if (handle_) {
        (void)detail::initialize_client_thread();
    }
    for (const auto& [query, statement] : statements_) {
        (void)query;
        mysql_stmt_close(statement);
    }
    statements_.clear();
    statement_order_.clear();
}
void connection::evict_one() {
    const auto oldest = std::move(statement_order_.front());
    statement_order_.pop_front();
    mysql_stmt_close(statements_.at(oldest));
    statements_.erase(oldest);
}
void connection::set_max_prepared_statements(std::size_t maximum) {
    max_prepared_statements_ = maximum;
    while (statements_.size() > maximum) {
        evict_one();
    }
}
std::size_t connection::prepared_statement_count() const {
    return statements_.size();
}

asterorm::result<mysql::result> connection::execute(std::string_view sql) {
    if (!detail::initialize_client_thread()) {
        return std::unexpected(
            detail::error("MySQL thread initialization failed", db_error_kind::connection_failed));
    }
    if (!is_open()) {
        return std::unexpected(
            detail::error("MySQL connection is closed", db_error_kind::connection_failed));
    }
    if (sql != session_invariant_sql)
        session_invariants_current_ = false;
    if (mysql_real_query(handle_, sql.data(), static_cast<unsigned long>(sql.size())) != 0) {
        auto failure =
            make_error(mysql_errno(handle_), mysql_sqlstate(handle_), mysql_error(handle_));
        if (failure.kind == db_error_kind::connection_failed) {
            close();
        }
        return std::unexpected(std::move(failure));
    }
    const auto insert_id = mysql_insert_id(handle_);
    std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> response{mysql_store_result(handle_),
                                                                      mysql_free_result};
    if (!response) {
        if (mysql_field_count(handle_) != 0) {
            return std::unexpected(
                make_error(mysql_errno(handle_), mysql_sqlstate(handle_), mysql_error(handle_)));
        }
        auto affected = row_count(mysql_affected_rows(handle_));
        if (!affected) {
            return std::unexpected(affected.error());
        }
        return finish_results(handle_, mysql::result{{}, {}, *affected, insert_id});
    }
    const auto columns = mysql_num_fields(response.get());
    const auto* fields = mysql_fetch_fields(response.get());
    std::vector<std::string> names;
    names.reserve(columns);
    for (unsigned int column = 0; column < columns; ++column) {
        names.emplace_back(fields[column].name, fields[column].name_length);
    }
    std::vector<std::vector<std::optional<std::string>>> rows;
    while (auto* row_values = mysql_fetch_row(response.get())) {
        const auto* lengths = mysql_fetch_lengths(response.get());
        std::vector<std::optional<std::string>> row;
        for (unsigned int column = 0; column < columns; ++column) {
            if (row_values[column]) {
                row.emplace_back(std::string(row_values[column], lengths[column]));
            } else {
                row.emplace_back(std::nullopt);
            }
        }
        rows.push_back(std::move(row));
    }
    if (mysql_errno(handle_) != 0) {
        return std::unexpected(
            make_error(mysql_errno(handle_), mysql_sqlstate(handle_), mysql_error(handle_)));
    }
    return finish_results(handle_, mysql::result{std::move(names), std::move(rows), 0, insert_id});
}

asterorm::result<mysql::result> connection::execute_params(std::string_view sql,
                                                           const query_parameters& params) {
    return run(sql, params, false);
}
asterorm::result<mysql::result> connection::execute_prepared(std::string_view sql,
                                                             const query_parameters& params) {
    return run(sql, params, max_prepared_statements_ != 0);
}
asterorm::result<mysql::result> connection::run(std::string_view sql,
                                                const query_parameters& params, bool cached) {
    if (!detail::initialize_client_thread()) {
        return std::unexpected(
            detail::error("MySQL thread initialization failed", db_error_kind::connection_failed));
    }
    if (!is_open()) {
        return std::unexpected(
            detail::error("MySQL connection is closed", db_error_kind::connection_failed));
    }
    if (!session_invariants_current_) {
        auto restored = execute(session_invariant_sql);
        if (!restored)
            return std::unexpected(restored.error());
        session_invariants_current_ = true;
    }
    auto prepared = detail::prepare_query(sql, params);
    if (!prepared) {
        return std::unexpected(prepared.error());
    }
    std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> owned{nullptr, mysql_stmt_close};
    MYSQL_STMT* statement = nullptr;
    auto found = statements_.find(prepared->sql);
    if (cached && found != statements_.end()) {
        statement = found->second;
        std::erase(statement_order_, prepared->sql);
        statement_order_.push_back(prepared->sql);
    } else {
        owned.reset(mysql_stmt_init(handle_));
        if (!owned) {
            return std::unexpected(detail::error("mysql_stmt_init failed"));
        }
        statement = owned.get();
        if (mysql_stmt_prepare(statement, prepared->sql.data(),
                               static_cast<unsigned long>(prepared->sql.size())) != 0) {
            auto failure = statement_error(statement);
            owned.reset();
            if (failure.kind == db_error_kind::connection_failed) {
                close();
            }
            return std::unexpected(std::move(failure));
        }
        if (cached) {
            while (statements_.size() >= max_prepared_statements_) {
                evict_one();
            }
            statements_.emplace(prepared->sql, statement);
            statement_order_.push_back(prepared->sql);
            owned.release();
        }
    }
    auto output = execute_statement(statement, prepared->params);
    mysql_stmt_free_result(statement);
    bool multiple = false;
    int next_result = 0;
    while ((next_result = mysql_stmt_next_result(statement)) == 0) {
        multiple = true;
        mysql_stmt_free_result(statement);
    }
    if (next_result > 0 && output) {
        output = std::unexpected(statement_error(statement));
    }
    if (multiple && output) {
        output = std::unexpected(detail::error("Multiple MySQL result sets are not supported"));
    }
    const auto reset_failed = mysql_stmt_reset(statement) != 0;
    if (reset_failed && output) {
        output = std::unexpected(statement_error(statement));
    }
    if (!output && output.error().kind == db_error_kind::connection_failed) {
        owned.reset();
        close();
    }
    return output;
}

asterorm::result<void> connection::begin_transaction(const transaction_options& options) {
    if (options.deferrable) {
        return std::unexpected(detail::error("MySQL does not support DEFERRABLE transactions"));
    }
    std::string isolation;
    switch (options.isolation) {
    case isolation_level::read_uncommitted:
        isolation = "READ UNCOMMITTED";
        break;
    case isolation_level::read_committed:
        isolation = "READ COMMITTED";
        break;
    case isolation_level::repeatable_read:
        isolation = "REPEATABLE READ";
        break;
    case isolation_level::serializable:
        isolation = "SERIALIZABLE";
        break;
    case isolation_level::default_level:
        break;
    }
    // SET TRANSACTION affects only the next transaction, not pooled session defaults.
    if (!isolation.empty()) {
        auto configured = execute("SET TRANSACTION ISOLATION LEVEL " + isolation);
        if (!configured) {
            return std::unexpected(configured.error());
        }
    }
    auto started =
        execute(options.read_only ? "START TRANSACTION READ ONLY" : "START TRANSACTION READ WRITE");
    if (!started) {
        auto failure = started.error();
        close(); // Discard pending one-transaction characteristics after a failed start.
        return std::unexpected(std::move(failure));
    }
    return {};
}
asterorm::result<void> connection::copy_in(std::string_view /*sql*/,
                                           const std::vector<std::string>& /*lines*/) {
    return std::unexpected(detail::error("COPY IN is not supported by MySQL"));
}
asterorm::result<std::vector<std::string>> connection::copy_out(std::string_view /*sql*/) {
    return std::unexpected(detail::error("COPY OUT is not supported by MySQL"));
}

} // namespace asterorm::mysql
