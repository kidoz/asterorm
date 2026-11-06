#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <mysql.h>

#include "asterorm/core/parameter.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/mysql/result.hpp"
#include "asterorm/sql/compiler.hpp"

namespace asterorm {
struct transaction_options;
}

namespace asterorm::mysql {

class connection {
  public:
    connection() = default;
    explicit connection(MYSQL* handle);
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;
    connection(connection&& other) noexcept;
    connection& operator=(connection&& other) noexcept;
    ~connection();

    [[nodiscard]] bool is_open() const;
    [[nodiscard]] bool in_transaction() const;
    void close();
    void set_max_prepared_statements(std::size_t maximum);
    [[nodiscard]] std::size_t prepared_statement_count() const;
    void clear_prepared_statement_cache();

    asterorm::result<mysql::result> execute(std::string_view sql);
    asterorm::result<mysql::result> execute_params(std::string_view sql,
                                                   const query_parameters& params);
    asterorm::result<mysql::result> execute_prepared(std::string_view sql,
                                                     const query_parameters& params);
    asterorm::result<void> begin_transaction(const transaction_options& options);
    asterorm::result<void> copy_in(std::string_view sql, const std::vector<std::string>& lines);
    asterorm::result<std::vector<std::string>> copy_out(std::string_view sql);

    static constexpr sql::dialect sql_dialect = sql::dialect::mysql;
    static constexpr bool supports_returning = false;
    static constexpr bool supports_atomic_migrations = false;

  private:
    asterorm::result<mysql::result> run(std::string_view sql, const query_parameters& params,
                                        bool cached);
    void evict_one();

    MYSQL* handle_{nullptr};
    std::unordered_map<std::string, MYSQL_STMT*> statements_;
    std::deque<std::string> statement_order_;
    std::size_t max_prepared_statements_{128};
};

} // namespace asterorm::mysql
