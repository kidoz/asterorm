#include "asterorm/postgres/result.hpp"

#include <charconv>
#include <memory>
#include <utility>

#include "asterorm/core/row_count.hpp"

namespace asterorm::pg {

namespace {
asterorm::result<std::uint64_t> native_row_count(PGresult* native) {
    const char* count = native ? PQcmdTuples(native) : nullptr;
    if (!count || *count == '\0') {
        return 0;
    }
    return asterorm::detail::parse_row_count(count);
}
} // namespace

result::result(PGresult* res) try : res_(res), affected_rows_(native_row_count(res).value()) {
} catch (...) {
    if (res) {
        PQclear(res);
    }
    throw;
}

asterorm::result<result> result::from_native(PGresult* res) {
    std::unique_ptr<PGresult, decltype(&PQclear)> owner{res, PQclear};
    if (!res ||
        (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK)) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "PostgreSQL native result is not successful";
        if (res) {
            if (const auto* state = PQresultErrorField(res, PG_DIAG_SQLSTATE)) {
                error.sqlstate = state;
                error.kind = classify_sqlstate(state);
            }
            if (const auto* message = PQresultErrorMessage(res); message && *message) {
                error.message = message;
            }
        }
        return std::unexpected(std::move(error));
    }
    auto count = native_row_count(res);
    if (!count) {
        return std::unexpected(count.error());
    }
    result output{nullptr};
    output.res_ = owner.release();
    output.affected_rows_ = *count;
    return output;
}

asterorm::result<result> result::from_single_tuple(PGresult* res) {
    std::unique_ptr<PGresult, decltype(&PQclear)> owner{res, PQclear};
    if (!res || PQresultStatus(res) != PGRES_SINGLE_TUPLE || PQntuples(res) != 1) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Expected exactly one PostgreSQL single-tuple row";
        return std::unexpected(std::move(error));
    }
    result output{nullptr};
    output.res_ = owner.release();
    return output;
}

result::result(result&& other) noexcept
    : res_(std::exchange(other.res_, nullptr)),
      affected_rows_(std::exchange(other.affected_rows_, 0)) {}

result& result::operator=(result&& other) noexcept {
    if (this != &other) {
        if (res_) {
            PQclear(res_);
        }
        res_ = std::exchange(other.res_, nullptr);
        affected_rows_ = std::exchange(other.affected_rows_, 0);
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

std::uint64_t result::affected_rows() const {
    return affected_rows_;
}

std::string_view result::command_tag() const {
    return res_ ? PQcmdStatus(res_) : "";
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
