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
interpolate_params(std::string_view sql, const std::vector<std::optional<std::string>>& params) {
    auto escape_literal = [](std::string_view in, std::string& out) -> asterorm::result<void> {
        out.reserve(out.size() + in.size() + 2);
        out += '\'';
        for (char c : in) {
            const auto uc = static_cast<unsigned char>(c);
            if (uc == '\0') {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "ClickHouse parameter contains NUL byte";
                return std::unexpected(err);
            }
            if (uc < 0x20 && uc != '\t' && uc != '\n' && uc != '\r') {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "ClickHouse parameter contains control byte";
                return std::unexpected(err);
            }
            if (c == '\'') {
                out += "''";
            } else if (c == '\\') {
                out += "\\\\";
            } else {
                out += c;
            }
        }
        out += '\'';
        return {};
    };

    std::string result;
    result.reserve(sql.size() + params.size() * 10);
    for (size_t i = 0; i < sql.size(); ++i) {
        if (sql[i] == '$' && i + 1 < sql.size() && std::isdigit(sql[i + 1])) {
            size_t j = i + 1;
            int idx = 0;
            while (j < sql.size() && std::isdigit(sql[j])) {
                idx = idx * 10 + (sql[j] - '0');
                j++;
            }
            if (idx > 0 && idx <= static_cast<int>(params.size())) {
                if (params[idx - 1].has_value()) {
                    auto ok = escape_literal(*params[idx - 1], result);
                    if (!ok)
                        return std::unexpected(ok.error());
                } else {
                    result += "NULL";
                }
                i = j - 1;
                continue;
            }
        }
        result += sql[i];
    }
    return result;
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
        db_error err;
        err.kind = db_error_kind::connection_failed;
        err.message = "Connection is closed";
        return std::unexpected(err);
    }

    ch::result res;
    try {
        client_->Select(std::string(sql),
                        [&res](const clickhouse::Block& block) { res.add_block(block); });
    } catch (const std::exception& e) {
        db_error err;
        err.kind = db_error_kind::query_failed;
        err.message = e.what();
        return std::unexpected(err);
    }
    return res;
}

asterorm::result<ch::result>
connection::execute_params(std::string_view sql,
                           const std::vector<std::optional<std::string>>& params) {
    auto interpolated = interpolate_params(sql, params);
    if (!interpolated)
        return std::unexpected(interpolated.error());
    return execute(*interpolated);
}

asterorm::result<ch::result>
connection::execute_prepared(std::string_view sql,
                             const std::vector<std::optional<std::string>>& params) {
    return execute_params(sql, params);
}

} // namespace asterorm::ch
