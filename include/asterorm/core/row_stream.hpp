#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "asterorm/core/hydration.hpp"
#include "asterorm/core/observer.hpp"
#include "asterorm/core/stream_reservation.hpp"

namespace asterorm {
enum class stream_mode : std::uint8_t { single_row, cursor };
struct stream_options {
    stream_mode mode{stream_mode::single_row};
    std::size_t chunk_size{256};
};

namespace detail {
template <typename Entity> struct row_stream_state {
    virtual ~row_stream_state() = default;
    virtual result<std::optional<Entity>> next() = 0;
    virtual result<void> close() = 0;
};
struct row_stream_access;
} // namespace detail

template <typename Entity> class row_stream {
  public:
    row_stream() = default;
    row_stream(const row_stream&) = delete;
    row_stream& operator=(const row_stream&) = delete;
    row_stream(row_stream&&) noexcept = default;
    row_stream& operator=(row_stream&&) noexcept = default;

    result<std::optional<Entity>> next() {
        auto state = state_; // keep calls alive if an observer replaces/destroys its handle
        if (!state) {
            return std::unexpected(detail::stream_ownership_error("Row stream is not active"));
        }
        return state->next();
    }
    result<void> close() {
        auto state = state_;
        return state ? state->close() : result<void>{};
    }

  private:
    friend struct detail::row_stream_access;
    explicit row_stream(std::shared_ptr<detail::row_stream_state<Entity>> state)
        : state_(std::move(state)) {}
    std::shared_ptr<detail::row_stream_state<Entity>> state_;
};

namespace detail {
template <typename Connection, bool Cursor> struct stream_transport;
template <typename Connection> struct stream_transport<Connection, false> {
    using reader_type = typename decltype(std::declval<Connection&>().open_single_row(
        std::declval<std::string_view>(), std::declval<const query_parameters&>()))::value_type;
    static bool ready(Connection& connection) noexcept {
        return connection.single_row_ready();
    }
    static auto open(Connection& connection, std::string_view statement,
                     const query_parameters& parameters, [[maybe_unused]] std::size_t chunk_size) {
        return connection.open_single_row(statement, parameters);
    }
    static result<void> close(reader_type& reader) noexcept {
        static_assert(noexcept(reader.close()));
        reader.close();
        return {};
    }
};
template <typename Connection> struct stream_transport<Connection, true> {
    using reader_type = typename decltype(std::declval<Connection&>().open_cursor(
        std::declval<std::string_view>(), std::declval<const query_parameters&>(),
        std::size_t{}))::value_type;
    static bool ready(Connection& connection) noexcept {
        return connection.cursor_ready();
    }
    static auto open(Connection& connection, std::string_view statement,
                     const query_parameters& parameters, std::size_t chunk_size) {
        return connection.open_cursor(statement, parameters, chunk_size);
    }
    static result<void> close(reader_type& reader) {
        return reader.close();
    }
};

template <typename Entity, typename Session, bool Cursor>
class entity_stream_state final : public row_stream_state<Entity> {
    using connection_type = typename Session::connection_type;
    using transport = stream_transport<connection_type, Cursor>;
    using reader_type = typename transport::reader_type;
    using batch_type =
        typename decltype(std::declval<reader_type&>().next())::value_type::value_type;
    static_assert(std::is_nothrow_move_constructible_v<reader_type>);

  public:
    entity_stream_state(query_observer observer, std::string statement, query_parameters parameters,
                        std::size_t chunk_size)
        : observer_(std::move(observer)),
          event_{std::move(statement), parameters.size(), {}, nullptr},
          parameters_(std::move(parameters)),
          exception_error_(stream_ownership_error("Row stream terminated by an exception")),
          cancellation_(stream_ownership_error("Stream closed before query completion")),
          chunk_size_(Cursor ? chunk_size : 1) {}

    void adopt(stream_reservation<Session>&& reservation) noexcept {
        reservation_.emplace(std::move(reservation));
    }

    result<void> start() {
        bool admitted = false;
        auto opened = reservation_->with_connection([&](auto& connection) -> result<void> {
            if (!transport::ready(connection)) {
                set_failure(stream_ownership_error("Streaming requires a healthy connection"));
                return {}; // no dispatch; release the reservation without taking over rollback
            }
            admitted = true;
            attempted_ = true;
            try {
                auto native = timed([&] {
                    return transport::open(connection, event_.sql, parameters_, chunk_size_);
                });
                if (!native) {
                    set_failure(std::move(native.error()));
                    notify(&*failure_);
                    return std::unexpected(*failure_);
                }
                reader_.emplace(std::move(*native));
                return {};
            } catch (...) {
                report_exception();
                throw;
            }
        });
        if (!opened) {
            set_failure(std::move(opened.error()));
            if (attempted_) {
                notify(&*failure_);
            }
            return std::unexpected(*failure_);
        }
        if (!admitted) {
            (void)reservation_->complete();
            return std::unexpected(*failure_);
        }
        return {};
    }

    result<std::optional<Entity>> next() override {
        if (in_progress_) {
            return std::unexpected(stream_ownership_error("Row stream is already executing"));
        }
        if (failure_) {
            return std::unexpected(*failure_);
        }
        if (done_) {
            return std::nullopt;
        }
        progress_guard progress{in_progress_};
        try {
            auto outcome =
                reservation_->with_connection([&](auto&) -> result<std::optional<Entity>> {
                    try {
                        if (!batch_ || row_index_ == batch_->rows()) {
                            batch_.reset(); // release before fetching the next batch
                            row_index_ = 0;
                            auto native = timed([&] { return reader_->next(); });
                            if (!native) {
                                set_failure(std::move(native.error()));
                            } else if (!*native) {
                                done_ = true;
                                notify(nullptr);
                                return std::nullopt;
                            } else {
                                batch_.emplace(std::move(**native));
                                auto valid = validate_batch(*batch_);
                                if (!valid) {
                                    set_failure(std::move(valid.error()));
                                }
                            }
                        }
                        if (!failure_) {
                            Entity entity{};
                            auto decoded = hydrate_entity(*batch_, row_index_++, entity);
                            if (decoded) {
                                return std::optional<Entity>{std::move(entity)};
                            }
                            set_failure(std::move(decoded.error()));
                        }
                        batch_.reset();
                        notify(&*failure_);
                        return std::unexpected(*failure_);
                    } catch (...) {
                        report_exception();
                        throw;
                    }
                });
            if (!outcome) {
                set_failure(std::move(outcome.error()));
                notify(&*failure_); // invalidation may have removed the pin before the callback
                reservation_->discard();
                return std::unexpected(*failure_);
            }
            if (!*outcome) {
                auto completed = reservation_->complete();
                if (!completed) {
                    set_failure(std::move(completed.error()));
                    reservation_->discard();
                    return std::unexpected(*failure_);
                }
            }
            return outcome;
        } catch (...) {
            reservation_->discard();
            throw;
        }
    }

    result<void> close() override {
        if (in_progress_) {
            return std::unexpected(stream_ownership_error("Row stream is already executing"));
        }
        if (done_) {
            return {};
        }
        progress_guard progress{in_progress_};
        try {
            auto closed = reservation_->with_connection([&](auto&) -> result<void> {
                done_ = true;
                try {
                    batch_.reset();
                    auto cleanup = timed([&] { return transport::close(*reader_); });
                    if (!cleanup) {
                        set_failure(std::move(cleanup.error()));
                        notify(&*failure_);
                        return std::unexpected(*failure_);
                    }
                    notify(&cancellation_);
                    return {};
                } catch (...) {
                    report_exception();
                    throw;
                }
            });
            if (!closed) {
                set_failure(std::move(closed.error()));
                notify(&*failure_);
                reservation_->discard();
                return std::unexpected(*failure_);
            }
            if constexpr (Cursor) {
                auto completed = reservation_->complete();
                if (!completed) {
                    set_failure(std::move(completed.error()));
                    reservation_->discard();
                    return std::unexpected(*failure_);
                }
            } else {
                reservation_->discard();
            }
            return {};
        } catch (...) {
            reservation_->discard();
            throw;
        }
    }

  private:
    struct progress_guard {
        bool& active;
        explicit progress_guard(bool& flag) : active(flag) {
            active = true;
        }
        ~progress_guard() {
            active = false;
        }
    };

    template <typename Callback> decltype(auto) timed(Callback&& callback) {
        struct timer {
            std::chrono::nanoseconds& elapsed;
            std::chrono::steady_clock::time_point started{std::chrono::steady_clock::now()};
            ~timer() {
                elapsed += std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started);
            }
        } measurement{elapsed_};
        return std::invoke(std::forward<Callback>(callback));
    }

    template <typename Native> result<void> validate_batch(const Native& native) {
        constexpr auto width = std::tuple_size_v<decltype(entity_traits<Entity>::columns)>;
        bool matches = native.rows() > 0 &&
                       static_cast<std::size_t>(native.rows()) <= chunk_size_ &&
                       native.columns() == static_cast<int>(width);
        int index = 0;
        std::apply(
            [&](const auto&... column) {
                ((matches = matches && native.column_name(index++) == column.name), ...);
            },
            entity_traits<Entity>::columns);
        if (!matches) {
            auto error =
                stream_ownership_error("Native stream row does not match the entity projection");
            error.kind = db_error_kind::parse_failed;
            return std::unexpected(std::move(error));
        }
        return {};
    }

    void set_failure(db_error error) {
        if (!failure_) {
            failure_.emplace(std::move(error));
        }
        done_ = true;
        batch_.reset();
    }
    void notify(const db_error* error) {
        if (!delivered_) {
            delivered_ = true;
            event_.elapsed = elapsed_;
            event_.error = error;
            if (observer_) {
                observer_(event_);
            }
        }
    }
    void report_exception() noexcept {
        static_assert(std::is_nothrow_move_constructible_v<db_error>);
        if (!failure_) {
            failure_.emplace(std::move(exception_error_));
        }
        done_ = true;
        batch_.reset();
        try {
            notify(&*failure_);
        } catch (...) {
            // Preserve the original reader/decoder/observer exception.
        }
    }

    // Destruction order keeps the reservation alive until the native reader closes.
    std::optional<stream_reservation<Session>> reservation_;
    std::optional<reader_type> reader_;
    std::optional<batch_type> batch_;
    query_observer observer_;
    query_event event_;
    query_parameters parameters_;
    std::chrono::nanoseconds elapsed_{};
    std::optional<db_error> failure_;
    db_error exception_error_;
    db_error cancellation_;
    std::size_t chunk_size_;
    int row_index_{};
    bool attempted_{};
    bool delivered_{};
    bool done_{};
    bool in_progress_{};
};

struct row_stream_access {
    template <typename Entity, bool Cursor = false, typename Session>
    static result<row_stream<Entity>> open(Session& session, std::string statement,
                                           query_parameters parameters,
                                           std::size_t chunk_size = 1) {
        auto reserved = session_stream_access::acquire(session, Cursor);
        if (!reserved) {
            return std::unexpected(std::move(reserved.error()));
        }
        try {
            auto observer = reserved->observer_snapshot();
            if (!observer) {
                return std::unexpected(std::move(observer.error()));
            }
            auto state = std::make_shared<entity_stream_state<Entity, Session, Cursor>>(
                std::move(*observer), std::move(statement), std::move(parameters), chunk_size);
            state->adopt(std::move(*reserved));
            auto started = state->start();
            if (!started) {
                return std::unexpected(std::move(started.error()));
            }
            return row_stream<Entity>{std::move(state)};
        } catch (...) {
            try {
                (void)reserved->complete(); // allocation/snapshot failure before ownership transfer
            } catch (...) {
                reserved->discard();
            }
            throw;
        }
    }
};
} // namespace detail
} // namespace asterorm
