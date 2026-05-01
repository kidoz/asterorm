#include "asterorm/clickhouse/connection.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/client.h>
#pragma GCC diagnostic pop

#include "asterorm/core/error.hpp"

namespace asterorm::ch {

static std::string interpolate_params(std::string_view sql,
                                      const std::vector<std::optional<std::string>>& params) {
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
                    result += "'";
                    for (char c : *params[idx - 1]) {
                        if (c == '\'') {
                            result += "''";
                        } else if (c == '\\') {
                            result += "\\\\";
                        } else {
                            result += c;
                        }
                    }
                    result += "'";
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
        return std::unexpected(db_error{db_error_kind::connection_failed, "Connection is closed",
                                        "", "", "", "", "", ""});
    }

    ch::result res;
    try {
        client_->Select(std::string(sql),
                        [&res](const clickhouse::Block& block) { res.add_block(block); });
    } catch (const std::exception& e) {
        return std::unexpected(
            db_error{db_error_kind::query_failed, e.what(), "", "", "", "", "", ""});
    }
    return res;
}

asterorm::result<ch::result>
connection::execute_params(std::string_view sql,
                           const std::vector<std::optional<std::string>>& params) {
    std::string interpolated = interpolate_params(sql, params);
    return execute(interpolated);
}

asterorm::result<ch::result>
connection::execute_prepared(std::string_view sql,
                             const std::vector<std::optional<std::string>>& params) {
    return execute_params(sql, params);
}

} // namespace asterorm::ch