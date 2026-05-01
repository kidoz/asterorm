#pragma once
#include <string>

#include "asterorm/core/result.hpp"
#include "asterorm/postgres/connection.hpp"

namespace asterorm::pg {

class driver {
  public:
    asterorm::result<connection> connect(const std::string& conninfo) const;
};

} // namespace asterorm::pg
