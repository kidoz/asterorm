#pragma once
#include <libpq-fe.h>

#include <optional>
#include <string>
#include <string_view>

namespace asterorm::pg {

class result {
  public:
    result(const result&) = delete;
    result& operator=(const result&) = delete;

    result(result&& other) noexcept;
    result& operator=(result&& other) noexcept;

    ~result();

    explicit result(PGresult* res);

    int rows() const;
    int columns() const;
    int affected_rows() const;
    std::string_view column_name(int col) const;
    bool is_null(int row, int col) const;
    std::string_view get_value(int row, int col) const;

    // Decoding helpers
    std::optional<std::string> get_string(int row, int col) const;
    std::optional<int64_t> get_int64(int row, int col) const;

  private:
    PGresult* res_{nullptr};
};

} // namespace asterorm::pg
