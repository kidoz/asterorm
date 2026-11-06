#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "asterorm/core/parameter.hpp"
#include "asterorm/core/result.hpp"

namespace asterorm::ch::detail {

asterorm::result<std::string> interpolate_params(std::string_view sql,
                                                 const query_parameters& parameters);

} // namespace asterorm::ch::detail
