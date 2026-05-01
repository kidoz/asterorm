#pragma once
#include <libpq-fe.h>

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "asterorm/core/result.hpp"
#include "asterorm/postgres/result.hpp"

namespace asterorm::pg {

class connection {
  public:
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    connection(connection&& other) noexcept;
    connection& operator=(connection&& other) noexcept;

    ~connection();

    bool is_open() const;
    void close();

    // Used by driver
    explicit connection(PGconn* conn);

    asterorm::result<pg::result> execute(std::string_view sql);
    asterorm::result<pg::result>
    execute_params(std::string_view sql, const std::vector<std::optional<std::string>>& params);
    asterorm::result<pg::result>
    execute_prepared(std::string_view sql, const std::vector<std::optional<std::string>>& params);

    asterorm::result<void> copy_in(std::string_view sql, const std::vector<std::string>& lines);
    asterorm::result<std::vector<std::string>> copy_out(std::string_view sql);

  private:
    PGconn* conn_{nullptr};
    std::unordered_map<std::string, std::string> prepared_statements_;
    size_t next_stmt_id_{0};
};

} // namespace asterorm::pg
