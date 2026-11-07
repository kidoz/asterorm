#pragma once
#include <libpq-fe.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "asterorm/core/parameter.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/postgres/copy.hpp"
#include "asterorm/postgres/cursor.hpp"
#include "asterorm/postgres/result.hpp"
#include "asterorm/postgres/single_row.hpp"
#include "asterorm/sql/compiler.hpp"

namespace asterorm::pg {

class connection {
  public:
    static constexpr bool supports_bulk_predicate_dml = true;
    static constexpr bool supports_transactional_test_scope = true;
    static constexpr bool supports_single_row_stream = true;
    static constexpr bool supports_cursor_stream = true;
    static constexpr sql::dialect sql_dialect = sql::dialect::postgres;

    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    connection(connection&& other) noexcept;
    connection& operator=(connection&& other) noexcept;

    ~connection();

    bool is_open() const;
    void close() noexcept;
    [[nodiscard]] bool is_pool_reusable() const noexcept;
    [[nodiscard]] bool single_row_ready() const noexcept;
    [[nodiscard]] bool cursor_ready() const noexcept;

    [[nodiscard]] bool is_transaction_idle() const noexcept;
    asterorm::result<std::chrono::milliseconds> statement_timeout();
    asterorm::result<void> set_statement_timeout(std::chrono::milliseconds duration);
    asterorm::result<void> set_local_statement_timeout(std::chrono::milliseconds duration);

    asterorm::result<void> advisory_lock(std::int64_t key);
    asterorm::result<void> advisory_unlock(std::int64_t key);

    // Used by driver
    explicit connection(PGconn* conn);

    asterorm::result<pg::result> execute(std::string_view sql);
    asterorm::result<pg::result> execute_params(std::string_view sql,
                                                const query_parameters& params);
    asterorm::result<pg::result> execute_prepared(std::string_view sql,
                                                  const query_parameters& params);
    asterorm::result<single_row_reader> open_single_row(std::string_view sql,
                                                        const query_parameters& params = {});
    asterorm::result<cursor_reader>
    open_cursor(std::string_view sql, const query_parameters& params, std::size_t chunk_size);

    void set_max_prepared_statements(size_t max_statements);
    [[nodiscard]] size_t prepared_statement_count() const;
    void clear_prepared_statement_cache();

    asterorm::result<void> copy_in(std::string_view sql, const std::vector<std::string>& lines);
    asterorm::result<std::vector<std::string>> copy_out(std::string_view sql);
    asterorm::result<void> copy_in_rows(std::string_view sql, const std::vector<copy_row>& rows);
    asterorm::result<std::vector<copy_row>> copy_out_rows(std::string_view sql);

  private:
    friend class single_row_reader;
    friend class cursor_reader;
    asterorm::result<std::string> prepare_statement(std::string_view sql, size_t param_count);
    void evict_one_prepared_statement();
    void forget_prepared_statement(std::string_view sql);

    PGconn* conn_{nullptr};
    std::shared_ptr<stream_detail::state> active_reader_;
    std::unordered_map<std::string, std::string> prepared_statements_;
    std::deque<std::string> prepared_statement_order_;
    size_t next_stmt_id_{0};
    size_t max_prepared_statements_{128};
};

} // namespace asterorm::pg
