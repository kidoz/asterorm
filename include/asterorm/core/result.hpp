#pragma once
#include <expected>

#include "asterorm/core/error.hpp"

namespace asterorm {

template <typename T> using result = std::expected<T, db_error>;

} // namespace asterorm
