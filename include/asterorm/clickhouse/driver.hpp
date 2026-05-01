#pragma once
#include <string>

#include "asterorm/clickhouse/connection.hpp"
#include "asterorm/core/result.hpp"

// EXPERIMENTAL: the ClickHouse adapter emulates parameterized queries via
// server-side string interpolation because clickhouse-cpp's Client::Select
// does not expose a native bind protocol. Non-encodable bytes (NUL, control
// characters other than TAB/LF/CR) are rejected. Do NOT use this driver with
// untrusted input without your own validation. Gated behind meson option
// -Dclickhouse=enabled (disabled by default).

namespace asterorm::ch {

class driver {
  public:
    asterorm::result<connection> connect(const std::string& conninfo) const;
};

} // namespace asterorm::ch