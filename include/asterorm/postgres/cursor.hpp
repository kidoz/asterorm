#pragma once
#include <memory>
#include <optional>
#include <string>

#include "asterorm/postgres/result.hpp"

namespace asterorm::pg {
class connection;
namespace stream_detail {
struct state;
}

// Native, transaction-bound reader. Each next() owns at most chunk_size rows.
class cursor_reader {
  public:
    cursor_reader() = default;
    cursor_reader(const cursor_reader&) = delete;
    cursor_reader& operator=(const cursor_reader&) = delete;
    cursor_reader(cursor_reader&&) noexcept;
    cursor_reader& operator=(cursor_reader&&) noexcept;
    ~cursor_reader();

    asterorm::result<std::optional<pg::result>> next();
    // Explicit CLOSE retains a healthy caller transaction. Destruction discards.
    asterorm::result<void> close();
    void abandon() noexcept;

  private:
    friend class connection;
    explicit cursor_reader(std::shared_ptr<stream_detail::state> state);
    asterorm::result<pg::result> command(const std::string& statement, bool tuples);
    std::shared_ptr<stream_detail::state> state_;
};
} // namespace asterorm::pg
