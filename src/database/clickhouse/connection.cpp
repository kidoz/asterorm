#include "asterorm/clickhouse/connection.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/client.h>
#pragma GCC diagnostic pop

#include <cctype>
#include <cstdint>
#include <limits>

#include "asterorm/clickhouse/detail/params.hpp"
#include "asterorm/core/error.hpp"

namespace asterorm::ch {

namespace detail {

enum class sql_scan_state : std::uint8_t {
    normal,
    single_quoted_literal,
    double_quoted_identifier,
    backtick_identifier,
    line_comment,
    block_comment,
};

// ClickHouse-cpp's named-parameter substitution ({name: Type}) requires the
// caller to declare the server-side type per parameter. The ORM's text
// parameters do not carry enough server-side type information, so binding
// everything as String would break Int/Date/UUID/Array columns. We interpolate
// values as SQL literals and rely on the server to parse each literal in context.
//
// Rejection rules for the interpolation path:
//   - NUL bytes are refused outright.
//   - control characters (< 0x20) other than \t/\n/\r are refused.
//   - single quotes and backslashes are doubled/escaped.
//   - `$n` outside literals, quoted identifiers (including backticks), and
//     comments must name a supplied parameter. Overflow and `$0` fail closed.
// Placeholder replacement only happens in normal SQL text.
// Any parameter that cannot be safely encoded fails with parse_failed rather
// than silently producing malformed SQL.
asterorm::result<std::string> interpolate_params(std::string_view sql,
                                                 const query_parameters& parameters) {
    for (const auto& parameter : parameters) {
        if (parameter.error)
            return std::unexpected(*parameter.error);
    }

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
    interpolated_sql.reserve(sql.size() + (parameters.size() * 10));
    sql_scan_state scan_state = sql_scan_state::normal;
    for (std::size_t sql_position = 0; sql_position < sql.size(); ++sql_position) {
        char current_character = sql[sql_position];
        char next_character = sql_position + 1 < sql.size() ? sql[sql_position + 1] : '\0';

        switch (scan_state) {
        case sql_scan_state::single_quoted_literal:
            interpolated_sql += current_character;
            if ((current_character == '\\' && sql_position + 1 < sql.size()) ||
                (current_character == '\'' && next_character == '\'')) {
                interpolated_sql += next_character;
                ++sql_position;
            } else if (current_character == '\'') {
                scan_state = sql_scan_state::normal;
            }
            continue;
        case sql_scan_state::double_quoted_identifier:
            interpolated_sql += current_character;
            if (current_character == '"' && next_character == '"') {
                interpolated_sql += next_character;
                ++sql_position;
            } else if (current_character == '"') {
                scan_state = sql_scan_state::normal;
            }
            continue;
        case sql_scan_state::backtick_identifier:
            interpolated_sql += current_character;
            if (current_character == '`' && next_character == '`') {
                interpolated_sql += next_character;
                ++sql_position;
            } else if (current_character == '`') {
                scan_state = sql_scan_state::normal;
            }
            continue;
        case sql_scan_state::line_comment:
            interpolated_sql += current_character;
            if (current_character == '\n' || current_character == '\r') {
                scan_state = sql_scan_state::normal;
            }
            continue;
        case sql_scan_state::block_comment:
            interpolated_sql += current_character;
            if (current_character == '*' && next_character == '/') {
                interpolated_sql += next_character;
                ++sql_position;
                scan_state = sql_scan_state::normal;
            }
            continue;
        case sql_scan_state::normal:
            break;
        }

        if (current_character == '\'') {
            interpolated_sql += current_character;
            scan_state = sql_scan_state::single_quoted_literal;
            continue;
        }
        if (current_character == '"') {
            interpolated_sql += current_character;
            scan_state = sql_scan_state::double_quoted_identifier;
            continue;
        }
        if (current_character == '`') {
            interpolated_sql += current_character;
            scan_state = sql_scan_state::backtick_identifier;
            continue;
        }
        if (current_character == '-' && next_character == '-') {
            interpolated_sql += current_character;
            interpolated_sql += next_character;
            ++sql_position;
            scan_state = sql_scan_state::line_comment;
            continue;
        }
        if (current_character == '/' && next_character == '*') {
            interpolated_sql += current_character;
            interpolated_sql += next_character;
            ++sql_position;
            scan_state = sql_scan_state::block_comment;
            continue;
        }

        const auto next_byte = static_cast<unsigned char>(
            sql_position + 1 < sql.size() ? sql[sql_position + 1] : '\0');
        if (current_character == '$' && std::isdigit(next_byte)) {
            std::size_t placeholder_end = sql_position + 1;
            std::size_t parameter_index = 0;
            bool index_overflow = false;
            while (placeholder_end < sql.size() &&
                   std::isdigit(static_cast<unsigned char>(sql[placeholder_end]))) {
                const auto digit = static_cast<std::size_t>(sql[placeholder_end] - '0');
                if (parameter_index > (std::numeric_limits<std::size_t>::max() - digit) / 10)
                    index_overflow = true;
                else
                    parameter_index = (parameter_index * 10) + digit;
                placeholder_end++;
            }
            if (index_overflow || parameter_index == 0 || parameter_index > parameters.size()) {
                db_error error;
                error.kind = db_error_kind::parse_failed;
                error.message = "ClickHouse parameter index is out of range";
                return std::unexpected(error);
            }
            if (parameters[parameter_index - 1].has_value()) {
                auto escape_result =
                    escape_literal(*parameters[parameter_index - 1], interpolated_sql);
                if (!escape_result) {
                    return std::unexpected(escape_result.error());
                }
            } else {
                interpolated_sql += "NULL";
            }
            sql_position = placeholder_end - 1;
            continue;
        }
        interpolated_sql += current_character;
    }
    return interpolated_sql;
}

} // namespace detail

connection::connection() = default;

connection::connection(std::unique_ptr<clickhouse::Client> client) : client_(std::move(client)) {}

connection::~connection() {
    close();
}

connection::connection(connection&& other) noexcept = default;
connection& connection::operator=(connection&& other) noexcept = default;

bool connection::is_open() const {
    return client_ != nullptr;
}

void connection::close() {
    client_.reset();
}

namespace {
asterorm::result<ch::result> run_query(clickhouse::Client& client, const std::string& sql) {
    ch::result query_result;
    try {
        clickhouse::Query query{sql};
        query.OnData(
            [&query_result](const clickhouse::Block& block) { query_result.add_block(block); });
        client.Execute(query);
    } catch (const std::exception& exception) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = exception.what();
        return std::unexpected(error);
    }
    return query_result;
}
} // namespace

asterorm::result<ch::result> connection::execute(std::string_view sql) {
    if (!client_) {
        db_error error;
        error.kind = db_error_kind::connection_failed;
        error.message = "Connection is closed";
        return std::unexpected(error);
    }
    return run_query(*client_, std::string{sql});
}

asterorm::result<ch::result> connection::execute_params(std::string_view sql,
                                                        const query_parameters& parameters) {
    if (!client_) {
        db_error error;
        error.kind = db_error_kind::connection_failed;
        error.message = "Connection is closed";
        return std::unexpected(error);
    }
    auto interpolated_sql = detail::interpolate_params(sql, parameters);
    if (!interpolated_sql) {
        return std::unexpected(interpolated_sql.error());
    }
    return run_query(*client_, *interpolated_sql);
}

asterorm::result<ch::result> connection::execute_prepared(std::string_view sql,
                                                          const query_parameters& parameters) {
    return execute_params(sql, parameters);
}

} // namespace asterorm::ch
