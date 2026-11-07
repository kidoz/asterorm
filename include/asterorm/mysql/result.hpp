#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace asterorm::mysql {

// Materialized text results remain valid after the statement is reused.
class result {
  public:
    result() = default;

    result(std::vector<std::string> column_names,
           std::vector<std::vector<std::optional<std::string>>> rows, std::uint64_t affected,
           std::uint64_t insert_id = 0);

    result(const result&) = delete;
    result& operator=(const result&) = delete;
    result(result&&) noexcept = default;
    result& operator=(result&&) noexcept = default;

    [[nodiscard]] int rows() const;
    [[nodiscard]] int columns() const;
    [[nodiscard]] std::uint64_t affected_rows() const;
    [[nodiscard]] std::uint64_t last_insert_id() const;
    [[nodiscard]] std::string_view column_name(int col) const;
    [[nodiscard]] bool is_null(int row, int col) const;
    [[nodiscard]] std::string_view get_value(int row, int col) const;

    [[nodiscard]] std::optional<std::string> get_string(int row, int col) const;
    [[nodiscard]] std::optional<int64_t> get_int64(int row, int col) const;

  private:
    std::vector<std::string> column_names_;
    std::vector<std::vector<std::optional<std::string>>> rows_;
    std::uint64_t affected_rows_{0};
    std::uint64_t insert_id_{0};
};

} // namespace asterorm::mysql
