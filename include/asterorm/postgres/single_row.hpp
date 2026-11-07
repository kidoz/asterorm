#pragma once
#include <memory>
#include <optional>

#include "asterorm/postgres/result.hpp"

namespace asterorm::pg {
class connection;
namespace stream_detail {
struct state;
}

// Low-level, synchronous native reader. Does not hydrate entities or emit events.
class single_row_reader {
  public:
    single_row_reader() = default;
    single_row_reader(const single_row_reader&) = delete;
    single_row_reader& operator=(const single_row_reader&) = delete;
    single_row_reader(single_row_reader&& other) noexcept;
    single_row_reader& operator=(single_row_reader&& other) noexcept;
    ~single_row_reader();

    asterorm::result<std::optional<pg::result>> next();
    // An unfinished query discards its connection, without draining or SQL.
    void close() noexcept;

  private:
    friend class connection;
    explicit single_row_reader(std::shared_ptr<stream_detail::state> state);
    asterorm::result<std::optional<pg::result>> fail(db_error error);
    std::shared_ptr<stream_detail::state> state_;
};
} // namespace asterorm::pg
