#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "asterorm/core/result.hpp"

namespace asterorm::ch::detail {

asterorm::result<std::string>
interpolate_params(std::string_view sql, const std::vector<std::optional<std::string>>& parameters);

} // namespace asterorm::ch::detail
