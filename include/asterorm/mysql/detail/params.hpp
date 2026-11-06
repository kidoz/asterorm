#pragma once

#include <cctype>
#include <charconv>
#include <string>
#include <string_view>
#include <vector>

#include "asterorm/core/parameter.hpp"
#include "asterorm/core/result.hpp"

namespace asterorm::mysql::detail {

inline db_error error(std::string message, db_error_kind kind = db_error_kind::query_failed) {
    db_error failure;
    failure.kind = kind;
    failure.message = std::move(message);
    return failure;
}

struct prepared_query {
    std::string sql;
    query_parameters params;
};

// The driver fixes ANSI_QUOTES and NO_BACKSLASH_ESCAPES for the session.
// Only placeholders are rewritten; literals, identifiers and comments are copied.
inline asterorm::result<prepared_query> prepare_query(std::string_view sql,
                                                      const query_parameters& params) {
    for (const auto& parameter : params) {
        if (parameter.error) {
            return std::unexpected(*parameter.error);
        }
    }
    prepared_query output;
    std::vector<bool> used(params.size(), false);
    bool numbered = false;
    bool positional = false;
    std::size_t next_position = 0;
    for (std::size_t position = 0; position < sql.size();) {
        const char current = sql[position];
        if (current == '\'' || current == '"' || current == '`') {
            const char quote = current;
            output.sql += sql[position++];
            bool closed = false;
            while (position < sql.size()) {
                const char character = sql[position++];
                output.sql += character;
                if (character == quote) {
                    if (position < sql.size() && sql[position] == quote) {
                        output.sql += sql[position++];
                    } else {
                        closed = true;
                        break;
                    }
                }
            }
            if (!closed) {
                return std::unexpected(error("Unterminated MySQL SQL quote"));
            }
            continue;
        }
        const bool dash_comment =
            sql.substr(position, 2) == "--" &&
            (position + 2 == sql.size() ||
             std::isspace(static_cast<unsigned char>(sql[position + 2])) != 0);
        if (current == '#' || dash_comment) {
            const auto end = sql.find_first_of("\r\n", position);
            const auto length =
                end == std::string_view::npos ? sql.size() - position : end - position;
            output.sql.append(sql.substr(position, length));
            position += length;
            continue;
        }
        if (sql.substr(position, 2) == "/*") {
            // Version comments contain executable SQL, so treating them as inert
            // would bind a different query from the one the server executes.
            if (sql.substr(position, 3) == "/*!" || sql.substr(position, 4) == "/*M!") {
                return std::unexpected(error("Executable SQL comments are not supported"));
            }
            const auto end = sql.find("*/", position + 2);
            if (end == std::string_view::npos) {
                return std::unexpected(error("Unterminated MySQL SQL comment"));
            }
            output.sql.append(sql.substr(position, end + 2 - position));
            position = end + 2;
            continue;
        }
        const auto identifier_character = [](char character) {
            return std::isalnum(static_cast<unsigned char>(character)) != 0 || character == '_' ||
                   character == '$';
        };
        std::size_t number_end = position + 1;
        while (number_end < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[number_end])) != 0) {
            ++number_end;
        }
        const bool numbered_placeholder =
            current == '$' && number_end > position + 1 &&
            (position == 0 || !identifier_character(sql[position - 1])) &&
            (number_end == sql.size() || !identifier_character(sql[number_end]));
        if (current == '?' || numbered_placeholder) {
            std::size_t index = 0;
            if (current == '?') {
                positional = true;
                index = next_position++;
                ++position;
            } else {
                numbered = true;
                const auto start = ++position;
                while (position < sql.size() &&
                       std::isdigit(static_cast<unsigned char>(sql[position])) != 0) {
                    ++position;
                }
                const auto parsed =
                    std::from_chars(sql.data() + start, sql.data() + position, index);
                if (parsed.ec != std::errc{} || index == 0) {
                    return std::unexpected(error("Invalid MySQL parameter index"));
                }
                --index;
            }
            if ((numbered && positional) || index >= params.size()) {
                return std::unexpected(
                    error("MySQL parameter count or placeholder style mismatch"));
            }
            used[index] = true;
            output.sql += '?';
            output.params.push_back(params[index]);
        } else {
            output.sql += current;
            ++position;
        }
    }
    for (bool parameter_used : used) {
        if (!parameter_used) {
            return std::unexpected(error("Unused MySQL parameter"));
        }
    }
    return output;
}

} // namespace asterorm::mysql::detail
