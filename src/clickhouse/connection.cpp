#include "asterorm/clickhouse/connection.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/client.h>
#pragma GCC diagnostic pop

#include "asterorm/core/error.hpp"

namespace asterorm::ch {

// ClickHouse has no libpq-style native parameter protocol exposed through
// clickhouse-cpp's Client::Select, so we interpolate. Rejection rules:
//   - control characters (< 0x20) other than \t are refused outright
//   - NUL bytes are refused
//   - single quotes and backslashes are doubled/escaped
// Any parameter that cannot be safely encoded causes execute_params() to fail
// with parse_failed rather than silently producing malformed SQL.
static asterorm::result<std::string>
interpolate_params(std::string_view sql,
                   const std::vector<std::optional<std::string>>& parameters) {
    auto escape_literal = [](std::string_view input_literal,
                             std::string& escaped_sql) -> asterorm::result<void> {
        escaped_sql.reserve(escaped_sql.size() + input_literal.size() + 2);
        escaped_sql += '\'';
        for (char current_character : input_literal) {
            const auto unsigned_character = static_cast<unsigned char>(current_character);
            if (unsigned_character == '\0') {
                db_error error;
                error.kind = db_error_kind::parse_failed;
                error.message = "ClickHouse parameter contains NUL byte";
                return std::unexpected(error);
            }
            if (unsigned_character < 0x20 && unsigned_character != '\t' &&
                unsigned_character != '\n' && unsigned_character != '\r') {
                db_error error;
                error.kind = db_error_kind::parse_failed;
                error.message = "ClickHouse parameter contains control byte";
                return std::unexpected(error);
            }
            if (current_character == '\'') {
                escaped_sql += "''";
            } else if (current_character == '\\') {
                escaped_sql += "\\\\";
            } else {
                escaped_sql += current_character;
            }
        }
        escaped_sql += '\'';
        return {};
    };

    std::string interpolated_sql;
    interpolated_sql.reserve(sql.size() + parameters.size() * 10);
    for (size_t sql_position = 0; sql_position < sql.size(); ++sql_position) {
        if (sql[sql_position] == '$' && sql_position + 1 < sql.size() &&
            std::isdigit(sql[sql_position + 1])) {
            size_t placeholder_end = sql_position + 1;
            int parameter_index = 0;
            while (placeholder_end < sql.size() && std::isdigit(sql[placeholder_end])) {
                parameter_index = parameter_index * 10 + (sql[placeholder_end] - '0');
                placeholder_end++;
            }
            if (parameter_index > 0 && parameter_index <= static_cast<int>(parameters.size())) {
                if (parameters[parameter_index - 1].has_value()) {
                    auto escape_result =
                        escape_literal(*parameters[parameter_index - 1], interpolated_sql);
                    if (!escape_result)
                        return std::unexpected(escape_result.error());
                } else {
                    interpolated_sql += "NULL";
                }
                sql_position = placeholder_end - 1;
                continue;
            }
        }
        interpolated_sql += sql[sql_position];
    }
    return interpolated_sql;
}

connection::connection() = default;

connection::connection(std::unique_ptr<clickhouse::Client> client) : client_(std::move(client)) {}

connection::~connection() {
    close();
}

connection::connection(connection&& other) noexcept = default;
connection& connection::operator=(connection&& other) noexcept = default;

bool connection::is_open() const {
    return client_ != nullptr; // clickhouse-cpp doesn't have a simple is_open, we assume if client_
                               // exists, it's open.
}

void connection::close() {
    client_.reset();
}

asterorm::result<ch::result> connection::execute(std::string_view sql) {
    if (!client_) {
        db_error error;
        error.kind = db_error_kind::connection_failed;
        error.message = "Connection is closed";
        return std::unexpected(error);
    }

    ch::result query_result;
    try {
        client_->Select(std::string(sql), [&query_result](const clickhouse::Block& block) {
            query_result.add_block(block);
        });
    } catch (const std::exception& exception) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = exception.what();
        return std::unexpected(error);
    }
    return query_result;
}

asterorm::result<ch::result>
connection::execute_params(std::string_view sql,
                           const std::vector<std::optional<std::string>>& parameters) {
    auto interpolated_sql = interpolate_params(sql, parameters);
    if (!interpolated_sql)
        return std::unexpected(interpolated_sql.error());
    return execute(*interpolated_sql);
}

asterorm::result<ch::result>
connection::execute_prepared(std::string_view sql,
                             const std::vector<std::optional<std::string>>& parameters) {
    return execute_params(sql, parameters);
}

} // namespace asterorm::ch
