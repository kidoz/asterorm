#pragma once
#include <libpq-fe.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "asterorm/core/result.hpp"

namespace asterorm::pg {

class result {
  public:
    result(const result&) = delete;
    result& operator=(const result&) = delete;

    result(result&& other) noexcept;
    result& operator=(result&& other) noexcept;

    ~result();

    explicit result(PGresult* res);

    // Takes ownership on success and failure; validates the native command count.
    static asterorm::result<result> from_native(PGresult* res);

    int rows() const;
    int columns() const;
    std::uint64_t affected_rows() const;
    std::string_view command_tag() const;
    std::string_view column_name(int col) const;
    bool is_null(int row, int col) const;
    std::string_view get_value(int row, int col) const;

    // Decoding helpers
    std::optional<std::string> get_string(int row, int col) const;
    std::optional<int64_t> get_int64(int row, int col) const;

  private:
    PGresult* res_{nullptr};
    std::uint64_t affected_rows_{0};
};

} // namespace asterorm::pg
