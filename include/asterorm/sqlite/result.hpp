#pragma once
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace asterorm::sqlite {

// Materialized result set. SQLite cursors (sqlite3_stmt*) are single-use and
// owned by the connection's statement cache, so rows are copied out as text
// during execution and the statement is reset before the result is returned.
class result {
  public:
    result() = default;

    result(std::vector<std::string> column_names,
           std::vector<std::vector<std::optional<std::string>>> rows, int affected);

    result(const result&) = delete;
    result& operator=(const result&) = delete;
    result(result&&) noexcept = default;
    result& operator=(result&&) noexcept = default;

    int rows() const;
    int columns() const;
    int affected_rows() const;
    std::string_view column_name(int col) const;
    bool is_null(int row, int col) const;
    std::string_view get_value(int row, int col) const;

    std::optional<std::string> get_string(int row, int col) const;
    std::optional<int64_t> get_int64(int row, int col) const;

  private:
    std::vector<std::string> column_names_;
    std::vector<std::vector<std::optional<std::string>>> rows_;
    int affected_rows_{0};
};

} // namespace asterorm::sqlite
