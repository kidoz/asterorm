#pragma once

#include <string>

#include "asterorm/mysql/connection.hpp"

namespace asterorm::mysql {

// Whitespace-separated key=value options. Quote values containing spaces with
// single or double quotes; backslash escapes the next character. Supported:
// host, port, user, password, dbname, unix_socket, connect_timeout.
class driver {
  public:
    [[nodiscard]] asterorm::result<connection> connect(const std::string& conninfo) const;
};

} // namespace asterorm::mysql
