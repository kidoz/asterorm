#pragma once
#include <libpq-fe.h>

#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "asterorm/core/result.hpp"
#include "asterorm/postgres/copy.hpp"
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

    void set_max_prepared_statements(size_t max_statements);
    [[nodiscard]] size_t prepared_statement_count() const;
    void clear_prepared_statement_cache();

    asterorm::result<void> copy_in(std::string_view sql, const std::vector<std::string>& lines);
    asterorm::result<std::vector<std::string>> copy_out(std::string_view sql);
    asterorm::result<void> copy_in_rows(std::string_view sql, const std::vector<copy_row>& rows);
    asterorm::result<std::vector<copy_row>> copy_out_rows(std::string_view sql);

  private:
    asterorm::result<std::string> prepare_statement(std::string_view sql, size_t param_count);
    void evict_one_prepared_statement();
    void forget_prepared_statement(std::string_view sql);

    PGconn* conn_{nullptr};
    std::unordered_map<std::string, std::string> prepared_statements_;
    std::deque<std::string> prepared_statement_order_;
    size_t next_stmt_id_{0};
    size_t max_prepared_statements_{128};
};

} // namespace asterorm::pg
