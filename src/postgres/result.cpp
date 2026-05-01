#include "asterorm/postgres/result.hpp"

#include <charconv>
#include <utility>

namespace asterorm::pg {

result::result(PGresult* res) : res_(res) {}

result::result(result&& other) noexcept : res_(std::exchange(other.res_, nullptr)) {}

result& result::operator=(result&& other) noexcept {
    if (this != &other) {
        if (res_) {
            PQclear(res_);
        }
        res_ = std::exchange(other.res_, nullptr);
    }
    return *this;
}

result::~result() {
    if (res_) {
        PQclear(res_);
    }
}

int result::rows() const {
    return res_ ? PQntuples(res_) : 0;
}

int result::affected_rows() const {
    if (!res_)
        return 0;
    const char* tuples = PQcmdTuples(res_);
    if (!tuples || *tuples == '\0')
        return 0;
    int out = 0;
    auto [ptr, ec] = std::from_chars(tuples, tuples + std::char_traits<char>::length(tuples), out);
    return ec == std::errc{} ? out : 0;
}

int result::columns() const {
    return res_ ? PQnfields(res_) : 0;
}

std::string_view result::column_name(int col) const {
    return res_ ? PQfname(res_, col) : "";
}

bool result::is_null(int row, int col) const {
    return res_ ? (PQgetisnull(res_, row, col) == 1) : true;
}

std::string_view result::get_value(int row, int col) const {
    if (is_null(row, col)) {
        return {};
    }
    return {PQgetvalue(res_, row, col), static_cast<size_t>(PQgetlength(res_, row, col))};
}

std::optional<std::string> result::get_string(int row, int col) const {
    if (is_null(row, col)) {
        return std::nullopt;
    }
    return std::string{get_value(row, col)};
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

} // namespace asterorm::pg
