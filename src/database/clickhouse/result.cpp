#include "asterorm/clickhouse/result.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/block.h>
#include <clickhouse/client.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#pragma GCC diagnostic pop

#include <algorithm>
#include <charconv>
#include <limits>
#include <string>
#include <utility>

namespace asterorm::ch {

namespace {
int to_public_count(std::size_t count) {
    constexpr auto max_public_count = static_cast<std::size_t>(std::numeric_limits<int>::max());
    return static_cast<int>(std::min(count, max_public_count));
}
} // namespace

struct result::Impl {
    std::vector<clickhouse::Block> blocks;
    std::size_t total_rows = 0;
    mutable std::string value_cache;
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
    return impl_ ? to_public_count(impl_->total_rows) : 0;
}

int result::affected_rows() const {
    return rows(); // clickhouse-cpp might not give exact mutations count, returning rows or 0
}

int result::columns() const {
    if (!impl_ || impl_->blocks.empty()) {
        return 0;
    }
    return to_public_count(impl_->blocks.front().GetColumnCount());
}

std::string_view result::column_name(int col) const {
    if (!impl_ || impl_->blocks.empty() || col < 0) {
        return "";
    }
    const auto column_index = static_cast<std::size_t>(col);
    if (column_index >= impl_->blocks.front().GetColumnCount()) {
        return "";
    }
    return impl_->blocks.front().GetColumnName(column_index);
}

bool result::is_null(int row, int col) const {
    if (!impl_ || impl_->blocks.empty() || row < 0 || col < 0) {
        return true;
    }

    auto row_in_block = static_cast<std::size_t>(row);
    const auto column_index = static_cast<std::size_t>(col);
    for (const auto& block : impl_->blocks) {
        const auto block_row_count = block.GetRowCount();
        if (column_index >= block.GetColumnCount()) {
            return true;
        }
        if (row_in_block < block_row_count) {
            auto column = block[column_index];
            if (column->Type()->GetCode() == clickhouse::Type::Nullable) {
                auto nullable_col = column->As<clickhouse::ColumnNullable>();
                return nullable_col && nullable_col->IsNull(row_in_block);
            }
            return false;
        }
        row_in_block -= block_row_count;
    }
    return true;
}

std::string_view result::get_value(int row, int col) const {
    if (is_null(row, col)) {
        return "";
    }

    auto row_in_block = static_cast<std::size_t>(row);
    const auto column_index = static_cast<std::size_t>(col);
    for (const auto& block : impl_->blocks) {
        const auto block_row_count = block.GetRowCount();
        if (row_in_block < block_row_count) {
            auto column = block[column_index];
            if (column->Type()->GetCode() == clickhouse::Type::Nullable) {
                column = column->As<clickhouse::ColumnNullable>()->Nested();
            }

            if (column->Type()->GetCode() == clickhouse::Type::String) {
                return column->As<clickhouse::ColumnString>()->At(row_in_block);
            }
            if (column->Type()->GetCode() == clickhouse::Type::FixedString) {
                return column->As<clickhouse::ColumnFixedString>()->At(row_in_block);
            }

            switch (column->Type()->GetCode()) {
            case clickhouse::Type::Int8:
                impl_->value_cache = std::to_string(
                    static_cast<int>(column->As<clickhouse::ColumnInt8>()->At(row_in_block)));
                return impl_->value_cache;
            case clickhouse::Type::Int16:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnInt16>()->At(row_in_block));
                return impl_->value_cache;
            case clickhouse::Type::Int32:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnInt32>()->At(row_in_block));
                return impl_->value_cache;
            case clickhouse::Type::Int64:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnInt64>()->At(row_in_block));
                return impl_->value_cache;
            case clickhouse::Type::UInt8:
                impl_->value_cache = std::to_string(static_cast<unsigned int>(
                    column->As<clickhouse::ColumnUInt8>()->At(row_in_block)));
                return impl_->value_cache;
            case clickhouse::Type::UInt16:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnUInt16>()->At(row_in_block));
                return impl_->value_cache;
            case clickhouse::Type::UInt32:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnUInt32>()->At(row_in_block));
                return impl_->value_cache;
            case clickhouse::Type::UInt64:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnUInt64>()->At(row_in_block));
                return impl_->value_cache;
            case clickhouse::Type::Float32:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnFloat32>()->At(row_in_block));
                return impl_->value_cache;
            case clickhouse::Type::Float64:
                impl_->value_cache =
                    std::to_string(column->As<clickhouse::ColumnFloat64>()->At(row_in_block));
                return impl_->value_cache;
            default:
                return "";
            }
        }
        row_in_block -= block_row_count;
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
