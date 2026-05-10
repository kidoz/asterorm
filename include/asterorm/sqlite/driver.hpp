#pragma once
#include <string>

#include "asterorm/core/result.hpp"
#include "asterorm/sqlite/connection.hpp"

namespace asterorm::sqlite {

// Opens a SQLite database. The connection string is interpreted as either a
// raw path (e.g. "test.db", ":memory:") or a SQLite URI when it starts with
// "file:". URIs allow flags such as "?mode=rwc" and "?cache=shared".
class driver {
  public:
    asterorm::result<connection> connect(const std::string& conninfo) const;
};

} // namespace asterorm::sqlite
