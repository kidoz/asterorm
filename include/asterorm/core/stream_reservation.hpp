#pragma once
#include <concepts>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>

#include "asterorm/core/observer.hpp"
#include "asterorm/core/result.hpp"

namespace asterorm::detail {

inline db_error stream_ownership_error(const char* message) {
    db_error error;
    error.kind = db_error_kind::query_failed;
    error.message = message;
    return error;
}

template <typename Connection>
concept has_stream_cleanup = requires(Connection& connection) {
    { connection.close() } noexcept -> std::same_as<void>;
};

// Internal ownership primitive, not a native reader. Session/pool must outlive it.
// Only complete() may return a known-clean connection to normal session use.
template <typename Session> class stream_reservation {
  public:
    stream_reservation(const stream_reservation&) = delete;
    stream_reservation& operator=(const stream_reservation&) = delete;

    stream_reservation(stream_reservation&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)), generation_(other.generation_) {}

    stream_reservation& operator=(stream_reservation&& other) noexcept {
        if (this != &other) {
            discard();
            owner_ = std::exchange(other.owner_, nullptr);
            generation_ = other.generation_;
        }
        return *this;
    }

    ~stream_reservation() {
        discard();
    }

    template <typename Callback>
    auto with_connection(Callback&& callback)
        -> std::invoke_result_t<Callback, typename Session::connection_type&> {
        using return_type = std::invoke_result_t<Callback, typename Session::connection_type&>;
        static_assert(std::same_as<typename return_type::error_type, db_error>);
        if (!owner_) {
            return std::unexpected(stream_ownership_error("Stream reservation is not active"));
        }
        return owner_->with_stream_connection(generation_, std::forward<Callback>(callback));
    }

    result<void> complete() {
        if (!owner_) {
            return {};
        }
        auto completed = owner_->complete_stream(generation_);
        if (completed) {
            owner_ = nullptr;
        }
        return completed;
    }

    result<query_observer> observer_snapshot() {
        if (!owner_) {
            return std::unexpected(stream_ownership_error("Stream reservation is not active"));
        }
        return owner_->stream_observer_snapshot(generation_);
    }

    void discard() noexcept {
        if (auto* owner = std::exchange(owner_, nullptr)) {
            owner->discard_stream(generation_);
        }
    }

  private:
    friend Session;
    stream_reservation(Session& owner, std::uint64_t generation)
        : owner_(&owner), generation_(generation) {}

    Session* owner_;
    std::uint64_t generation_;
};

// Future repository readers enter through this bridge after backend/AST checks.
// Reservation itself performs no native streaming and advertises no capability.
struct session_stream_access {
    template <typename Session>
    static auto acquire(Session& owner, bool require_transaction = false) {
        return owner.reserve_stream(require_transaction);
    }
};

} // namespace asterorm::detail
