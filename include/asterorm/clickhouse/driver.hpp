#pragma once
#include <string>

#include "asterorm/clickhouse/connection.hpp"
#include "asterorm/core/result.hpp"

namespace asterorm::ch {

class driver {
  public:
    asterorm::result<connection> connect(const std::string& conninfo) const;
};

} // namespace asterorm::ch