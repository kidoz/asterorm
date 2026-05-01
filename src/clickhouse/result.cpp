#include "asterorm/clickhouse/result.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/block.h>
#include <clickhouse/client.h>
#pragma GCC diagnostic pop

#include <charconv>
#include <utility>

namespace asterorm::ch {

struct result::Impl {
    std::vector<clickhouse::Block> blocks;
    int total_rows = 0;
};

result::result() : impl_(std::make_unique<Impl>()) {}

result::result(result&& other) noexcept : impl_(std::move(other.impl_)) {}

result& result::operator=(result&& other) noexcept {
    if (this != &other) {
        impl_ = std::move(other.impl_);
    }
    return *this;
}

result::~result() = default;

void result::add_block(const clickhouse::Block& block) {
    if (impl_) {
        impl_->blocks.push_back(block);
        impl_->total_rows += block.GetRowCount();
    }
}

int result::rows() const {
    return impl_ ? impl_->total_rows : 0;
}

int result::affected_rows() const {
    return rows(); // clickhouse-cpp might not give exact mutations count, returning rows or 0
}

int result::columns() const {
    if (!impl_ || impl_->blocks.empty())
        return 0;
    return impl_->blocks.front().GetColumnCount();
}

std::string_view result::column_name(int col) const {
    if (!impl_ || impl_->blocks.empty())
        return "";
    return impl_->blocks.front().GetColumnName(col);
}

bool result::is_null(int row, int col) const {
    if (!impl_ || impl_->blocks.empty())
        return true;

    int row_in_block = row;
    for (const auto& block : impl_->blocks) {
        int r = block.GetRowCount();
        if (row_in_block < r) {
            auto column = block[col];
            if (column->Type()->GetCode() == clickhouse::Type::Nullable) {
                auto nullable_col = column->As<clickhouse::ColumnNullable>();
                return nullable_col && nullable_col->IsNull(row_in_block);
            }
            return false;
        }
        row_in_block -= r;
    }
    return true;
}

std::string_view result::get_value(int row, int col) const {
    if (is_null(row, col)) {
        return "";
    }

    int row_in_block = row;
    for (const auto& block : impl_->blocks) {
        int r = block.GetRowCount();
        if (row_in_block < r) {
            auto column = block[col];
            if (column->Type()->GetCode() == clickhouse::Type::Nullable) {
                column = column->As<clickhouse::ColumnNullable>()->Nested();
            }

            // To provide a string_view, we need a stable string storage or use string-based columns
            // directly. ClickHouse returns strong types. We'll decode to string on the fly and
            // store it somewhere, but string_view requires it to outlive the call. For now, as a
            // simple implementation, if it's a string column we can return view:
            if (column->Type()->GetCode() == clickhouse::Type::String) {
                return column->As<clickhouse::ColumnString>()->At(row_in_block);
            } else if (column->Type()->GetCode() == clickhouse::Type::FixedString) {
                return column->As<clickhouse::ColumnFixedString>()->At(row_in_block);
            }
            return ""; // For other types, clickhouse-cpp requires explicit casting which is complex
                       // here.
        }
        row_in_block -= r;
    }
    return "";
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

} // namespace asterorm::ch