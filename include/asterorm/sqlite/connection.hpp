#pragma once
#include <sqlite3.h>

#include <cstddef>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "asterorm/core/result.hpp"
#include "asterorm/sqlite/result.hpp"

namespace asterorm {
struct transaction_options;
} // namespace asterorm

namespace asterorm::sqlite {

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
    explicit connection(sqlite3* db);

    asterorm::result<sqlite::result> execute(std::string_view sql);
    asterorm::result<sqlite::result>
    execute_params(std::string_view sql, const std::vector<std::optional<std::string>>& params);
    asterorm::result<sqlite::result>
    execute_prepared(std::string_view sql, const std::vector<std::optional<std::string>>& params);

    // Bounded LRU prepared-statement cache. The connection_pool calls this
    // with pool_config::prepared_statement_cache_size on construction.
    void set_max_prepared_statements(std::size_t max_statements);
    [[nodiscard]] std::size_t prepared_statement_count() const;
    void clear_prepared_statement_cache();

    // SQLite has no streaming bulk-load equivalent to PostgreSQL COPY. These
    // are provided so the same Connection concept satisfies session<Pool>, but
    // they always return a "not supported" error.
    asterorm::result<void> copy_in(std::string_view sql, const std::vector<std::string>& lines);
    asterorm::result<std::vector<std::string>> copy_out(std::string_view sql);

    // Mapped from asterorm::transaction_options into SQLite-compatible BEGIN
    // syntax. SQLite has no per-transaction isolation level or read-only mode,
    // so REPEATABLE READ / SERIALIZABLE upgrade to BEGIN IMMEDIATE; lower
    // levels and read_only/deferrable are ignored.
    static std::string build_begin_sql(const transaction_options& opts);

  private:
    void evict_one_prepared_statement();
    void clear_cache_locked();

    sqlite3* db_{nullptr};
    std::unordered_map<std::string, sqlite3_stmt*> prepared_statements_;
    std::deque<std::string> prepared_statement_order_;
    std::size_t max_prepared_statements_{128};
};

} // namespace asterorm::sqlite
