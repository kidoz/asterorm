#include "asterorm/mysql/result.hpp"

#include <charconv>
#include <utility>

namespace asterorm::mysql {

result::result(std::vector<std::string> column_names,
               std::vector<std::vector<std::optional<std::string>>> rows, int affected,
               std::uint64_t insert_id)
    : column_names_(std::move(column_names)), rows_(std::move(rows)), affected_rows_(affected),
      insert_id_(insert_id) {}

std::uint64_t result::last_insert_id() const {
    return insert_id_;
}

int result::rows() const {
    return static_cast<int>(rows_.size());
}

int result::columns() const {
    return static_cast<int>(column_names_.size());
}

int result::affected_rows() const {
    return affected_rows_;
}

std::string_view result::column_name(int col) const {
    if (col < 0 || static_cast<size_t>(col) >= column_names_.size()) {
        return {};
    }
    return column_names_[static_cast<size_t>(col)];
}

bool result::is_null(int row, int col) const {
    if (row < 0 || static_cast<size_t>(row) >= rows_.size()) {
        return true;
    }
    if (col < 0 || static_cast<size_t>(col) >= rows_[static_cast<size_t>(row)].size()) {
        return true;
    }
    return !rows_[static_cast<size_t>(row)][static_cast<size_t>(col)].has_value();
}

std::string_view result::get_value(int row, int col) const {
    if (is_null(row, col)) {
        return {};
    }
    return *rows_[static_cast<size_t>(row)][static_cast<size_t>(col)];
}

std::optional<std::string> result::get_string(int row, int col) const {
    if (is_null(row, col)) {
        return std::nullopt;
    }
    return rows_[static_cast<size_t>(row)][static_cast<size_t>(col)];
}

std::optional<int64_t> result::get_int64(int row, int col) const {
    if (is_null(row, col)) {
        return std::nullopt;
    }
    auto text_value = get_value(row, col);
    int64_t decoded_value = 0;
    auto [parse_end, error_code] =
        std::from_chars(text_value.data(), text_value.data() + text_value.size(), decoded_value);
    if (error_code == std::errc{} && parse_end == text_value.data() + text_value.size()) {
        return decoded_value;
    }
    return std::nullopt;
}

} // namespace asterorm::mysql
