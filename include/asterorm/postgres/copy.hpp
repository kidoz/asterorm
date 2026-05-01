#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "asterorm/core/error.hpp"
#include "asterorm/core/result.hpp"

namespace asterorm::pg {

using copy_field = std::optional<std::string>;
using copy_row = std::vector<copy_field>;

inline std::string encode_copy_field(const copy_field& field) {
    if (!field)
        return "\\N";

    std::string out;
    out.reserve(field->size());
    for (char c : *field) {
        switch (c) {
        case '\\':
            out += "\\\\";
            break;
        case '\t':
            out += "\\t";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        default:
            out += c;
            break;
        }
    }
    return out;
}

inline std::string encode_copy_row(const copy_row& row) {
    std::string out;
    for (std::size_t i = 0; i < row.size(); ++i) {
        if (i != 0)
            out += '\t';
        out += encode_copy_field(row[i]);
    }
    return out;
}

inline asterorm::result<copy_row> decode_copy_row(std::string_view line) {
    if (!line.empty() && line.back() == '\n')
        line.remove_suffix(1);
    if (!line.empty() && line.back() == '\r')
        line.remove_suffix(1);

    copy_row row;
    std::string field;
    std::string raw_field;
    bool escaped = false;

    auto push_field = [&]() {
        if (raw_field == "\\N") {
            row.push_back(std::nullopt);
        } else {
            row.push_back(std::move(field));
        }
        field.clear();
        raw_field.clear();
        escaped = false;
    };

    for (char c : line) {
        if (escaped) {
            raw_field += c;
            switch (c) {
            case 'b':
                field += '\b';
                break;
            case 'f':
                field += '\f';
                break;
            case 'n':
                field += '\n';
                break;
            case 'r':
                field += '\r';
                break;
            case 't':
                field += '\t';
                break;
            case 'v':
                field += '\v';
                break;
            case '\\':
                field += '\\';
                break;
            case 'N':
                field += "\\N";
                break;
            default:
                field += c;
                break;
            }
            escaped = false;
            continue;
        }

        if (c == '\\') {
            escaped = true;
            raw_field += c;
            continue;
        }

        if (c == '\t') {
            push_field();
            continue;
        }

        raw_field += c;
        field += c;
    }

    if (escaped) {
        db_error err;
        err.kind = db_error_kind::parse_failed;
        err.message = "COPY row ends with incomplete escape sequence";
        return std::unexpected(err);
    }

    push_field();
    return row;
}

inline std::vector<std::string> encode_copy_rows(const std::vector<copy_row>& rows) {
    std::vector<std::string> lines;
    lines.reserve(rows.size());
    for (const auto& row : rows) {
        lines.push_back(encode_copy_row(row));
    }
    return lines;
}

inline asterorm::result<std::vector<copy_row>>
decode_copy_rows(const std::vector<std::string>& lines) {
    std::vector<copy_row> rows;
    rows.reserve(lines.size());
    for (const auto& line : lines) {
        auto decoded = decode_copy_row(line);
        if (!decoded)
            return std::unexpected(decoded.error());
        rows.push_back(std::move(*decoded));
    }
    return rows;
}

} // namespace asterorm::pg
