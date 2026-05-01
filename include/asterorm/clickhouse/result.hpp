#pragma once
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace clickhouse {
class Block;
}

namespace asterorm::ch {

class result {
  public:
    result(const result&) = delete;
    result& operator=(const result&) = delete;

    result(result&& other) noexcept;
    result& operator=(result&& other) noexcept;

    ~result();
    result();

    int rows() const;
    int columns() const;
    int affected_rows() const;
    std::string_view column_name(int col) const;
    bool is_null(int row, int col) const;
    std::string_view get_value(int row, int col) const;

    // Decoding helpers
    std::optional<std::string> get_string(int row, int col) const;
    std::optional<int64_t> get_int64(int row, int col) const;

    void add_block(const clickhouse::Block& block);

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace asterorm::ch