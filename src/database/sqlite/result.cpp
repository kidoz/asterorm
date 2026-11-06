#include "asterorm/sqlite/result.hpp"

#include <charconv>
#include <utility>

namespace asterorm::sqlite {

result::result(std::vector<std::string> column_names,
               std::vector<std::vector<std::optional<std::string>>> rows, int affected)
    : column_names_(std::move(column_names)), rows_(std::move(rows)), affected_rows_(affected) {}

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
    if (row < 0 || static_cast<size_t>(row) >= rows_.size())
        return true;
    if (col < 0 || static_cast<size_t>(col) >= rows_[static_cast<size_t>(row)].size())
        return true;
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
    auto val = get_value(row, col);
    int64_t out = 0;
    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), out);
    if (ec == std::errc{}) {
        return out;
    }
    return std::nullopt;
}

} // namespace asterorm::sqlite
