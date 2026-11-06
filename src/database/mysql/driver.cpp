#include "asterorm/mysql/driver.hpp"

#include <cctype>
#include <charconv>
#include <limits>
#include <memory>
#include <unordered_map>

#include "asterorm/mysql/detail/params.hpp"
#include "client.hpp"

namespace asterorm::mysql {
namespace {

asterorm::result<std::unordered_map<std::string, std::string>>
parse_options(std::string_view input) {
    std::unordered_map<std::string, std::string> options;
    std::size_t position = 0;
    while (position < input.size()) {
        while (position < input.size() &&
               std::isspace(static_cast<unsigned char>(input[position]))) {
            ++position;
        }
        if (position == input.size()) {
            break;
        }
        const auto start = position;
        while (position < input.size() && input[position] != '=' &&
               !std::isspace(static_cast<unsigned char>(input[position]))) {
            ++position;
        }
        if (position == input.size() || input[position] != '=') {
            return std::unexpected(detail::error("Expected MySQL key=value option"));
        }
        ++position;
        std::string key{input.substr(start, position - start - 1)};
        if (key != "host" && key != "port" && key != "user" && key != "password" &&
            key != "dbname" && key != "unix_socket" && key != "connect_timeout") {
            return std::unexpected(detail::error("Unknown MySQL connection option: " + key));
        }
        std::string value;
        const char quote =
            position < input.size() && (input[position] == '\'' || input[position] == '"')
                ? input[position++]
                : '\0';
        bool closed = quote == '\0';
        while (position < input.size()) {
            const char character = input[position++];
            if (quote && character == quote) {
                closed = true;
                break;
            }
            if (!quote && std::isspace(static_cast<unsigned char>(character))) {
                break;
            }
            if (character == '\\') {
                if (position == input.size()) {
                    return std::unexpected(detail::error("Incomplete MySQL option escape"));
                }
                value += input[position++];
            } else {
                value += character;
            }
        }
        if (!closed || (quote && position < input.size() &&
                        !std::isspace(static_cast<unsigned char>(input[position])))) {
            return std::unexpected(detail::error("Invalid quoted MySQL option"));
        }
        if (value.contains('\0') || !options.emplace(key, std::move(value)).second) {
            return std::unexpected(detail::error("Duplicate or invalid MySQL option"));
        }
    }
    return options;
}

} // namespace

asterorm::result<connection> driver::connect(const std::string& conninfo) const {
    if (!detail::initialize_client_thread()) {
        return std::unexpected(
            detail::error("MySQL client initialization failed", db_error_kind::connection_failed));
    }
    auto options = parse_options(conninfo);
    if (!options) {
        return std::unexpected(options.error());
    }
    unsigned int port = 3306;
    unsigned int timeout = 5;
    for (auto [key, destination] : {std::pair{"port", &port}, {"connect_timeout", &timeout}}) {
        if (const auto found = options->find(key); found != options->end()) {
            const auto& value = found->second;
            const auto parsed =
                std::from_chars(value.data(), value.data() + value.size(), *destination);
            if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() ||
                *destination == 0 || (std::string_view{key} == "port" && port > 65535)) {
                return std::unexpected(detail::error("Invalid MySQL numeric connection option"));
            }
        }
    }
    std::unique_ptr<MYSQL, decltype(&mysql_close)> handle{mysql_init(nullptr), mysql_close};
    if (!handle) {
        return std::unexpected(
            detail::error("mysql_init failed", db_error_kind::connection_failed));
    }
    if (mysql_options(handle.get(), MYSQL_OPT_CONNECT_TIMEOUT, &timeout) != 0) {
        return std::unexpected(
            detail::error(mysql_error(handle.get()), db_error_kind::connection_failed));
    }
    const auto get_option = [&](const char* key) -> const char* {
        const auto found = options->find(key);
        return found == options->end() ? nullptr : found->second.c_str();
    };
    // FOUND_ROWS gives UPDATE matched-row semantics even for unchanged values.
    if (!mysql_real_connect(handle.get(), get_option("host"), get_option("user"),
                            get_option("password"), get_option("dbname"), port,
                            get_option("unix_socket"), CLIENT_FOUND_ROWS)) {
        auto failure = detail::error(mysql_error(handle.get()), db_error_kind::connection_failed);
        failure.sqlstate = mysql_sqlstate(handle.get());
        return std::unexpected(std::move(failure));
    }
    if (mysql_set_character_set(handle.get(), "utf8mb4") != 0) {
        return std::unexpected(
            detail::error(mysql_error(handle.get()), db_error_kind::connection_failed));
    }
    connection connected{handle.release()};
    auto initialized =
        connected.execute("SET SESSION sql_mode = CONCAT_WS(',', @@SESSION.sql_mode, "
                          "'ANSI_QUOTES', 'NO_BACKSLASH_ESCAPES'), time_zone = '+00:00'");
    if (!initialized) {
        return std::unexpected(initialized.error());
    }
    return connected;
}

} // namespace asterorm::mysql
