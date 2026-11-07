#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "asterorm/core/error.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/core/statement_timeout.hpp"

namespace asterorm {

struct pool_config {
    size_t min_size{1};
    size_t max_size{10};
    std::chrono::milliseconds acquire_timeout{5000};
    size_t prepared_statement_cache_size{128};
    std::string conninfo;
    std::optional<std::chrono::milliseconds> statement_timeout{};
};

struct pool_stats {
    size_t total{0};
    size_t idle{0};
    size_t in_use{0};
    size_t leaked_at_close{0};
    bool closed{false};
};

namespace detail {
template <typename Conn, typename = void> struct has_prepared_cache_limit : std::false_type {};

template <typename Conn>
struct has_prepared_cache_limit<
    Conn, std::void_t<decltype(std::declval<Conn&>().set_max_prepared_statements(std::size_t{}))>>
    : std::true_type {};

template <typename Conn> void configure_connection(Conn& conn, const pool_config& config) {
    if constexpr (has_prepared_cache_limit<Conn>::value) {
        conn.set_max_prepared_statements(config.prepared_statement_cache_size);
    }
}
template <typename Conn> asterorm::result<void> validate_timeout_config(const pool_config& config) {
    if (!config.statement_timeout) {
        return {};
    }
    auto valid = validate_statement_timeout(*config.statement_timeout);
    if (!valid) {
        return valid;
    }
    if constexpr (!has_statement_timeout<Conn>) {
        return std::unexpected(unsupported_statement_timeout());
    }
    return {};
}

template <typename Conn>
asterorm::result<void> configure_statement_timeout(Conn& conn, const pool_config& config) {
    if (!config.statement_timeout) {
        return {};
    }
    if constexpr (has_statement_timeout<Conn>) {
        if (!conn.is_transaction_idle()) {
            conn.close();
            db_error error;
            error.kind = db_error_kind::query_failed;
            error.message = "Cannot configure statement timeout on an active or failed transaction";
            return std::unexpected(std::move(error));
        }
        try {
            auto configured = conn.set_statement_timeout(*config.statement_timeout);
            if (!configured) {
                conn.close();
            }
            return configured;
        } catch (...) {
            conn.close();
            throw;
        }
    } else {
        return std::unexpected(unsupported_statement_timeout());
    }
}

template <typename Conn>
void close_configured_connection(Conn& conn, const pool_config& config) noexcept {
    if constexpr (has_statement_timeout<Conn>) {
        if (config.statement_timeout) {
            conn.close();
        }
    }
}
} // namespace detail

template <typename Driver> class connection_pool;

template <typename Pool> class connection_lease {
  public:
    using connection_type = typename Pool::connection_type;

    connection_lease(connection_type&& conn, Pool* pool, std::shared_ptr<std::atomic<bool>> alive)
        : conn_(std::move(conn)), pool_(pool), alive_(std::move(alive)) {}

    ~connection_lease() {
        release_to_pool();
    }

    connection_lease(connection_lease&& other) noexcept
        : conn_(std::move(other.conn_)), pool_(other.pool_), alive_(std::move(other.alive_)) {
        other.pool_ = nullptr;
    }

    connection_lease& operator=(connection_lease&& other) noexcept {
        if (this != &other) {
            release_to_pool();
            conn_ = std::move(other.conn_);
            pool_ = other.pool_;
            alive_ = std::move(other.alive_);
            other.pool_ = nullptr;
        }
        return *this;
    }

    connection_type* operator->() {
        return &conn_;
    }
    connection_type& operator*() {
        return conn_;
    }

    void release_to_pool() {
        if (!pool_) {
            return;
        }
        if (alive_ && alive_->load(std::memory_order_acquire)) {
            pool_->release(std::move(conn_));
        }
        // If the pool has been torn down, drop the connection here; its
        // destructor will close the underlying client.
        pool_ = nullptr;
    }

  private:
    connection_type conn_;
    Pool* pool_;
    std::shared_ptr<std::atomic<bool>> alive_;
};

template <typename Driver> class connection_pool {
  public:
    using driver_connect_result =
        decltype(std::declval<Driver>().connect(std::declval<const std::string&>()));
    using connection_type = typename driver_connect_result::value_type;
    using lease_type = connection_lease<connection_pool<Driver>>;

    connection_pool(Driver driver, pool_config config)
        : driver_(std::move(driver)), config_(std::move(config)) {
        try {
            auto valid = detail::validate_timeout_config<connection_type>(config_);
            if (!valid) {
                fail_warmup(valid.error());
                return;
            }
            for (size_t i = 0; i < config_.min_size; ++i) {
                auto conn_res = driver_.connect(config_.conninfo);
                if (!conn_res) {
                    fail_warmup(conn_res.error());
                    return;
                }
                detail::configure_connection(*conn_res, config_);
                auto configured = detail::configure_statement_timeout(*conn_res, config_);
                if (!configured) {
                    fail_warmup(configured.error());
                    return;
                }
                idle_connections_.push_back(std::move(*conn_res));
                ++current_size_;
            }
        } catch (const std::exception& exception) {
            db_error error;
            error.kind = db_error_kind::connection_failed;
            error.message = exception.what();
            fail_warmup(std::move(error));
        } catch (...) {
            db_error error;
            error.kind = db_error_kind::connection_failed;
            error.message = "Connection pool warmup failed";
            fail_warmup(std::move(error));
        }
    }

    ~connection_pool() {
        close();
    }

    void close() {
        std::unique_lock lock(mutex_);
        closed_ = true;
        cv_.notify_all();
        // Wait up to close_timeout for outstanding leases to return. After the
        // deadline we mark the pool dead anyway; any leaked lease will drop its
        // connection from release_to_pool() rather than touch the pool.
        auto deadline = std::chrono::steady_clock::now() + close_timeout_;
        bool returned = cv_.wait_until(
            lock, deadline, [this] { return current_size_ == idle_connections_.size(); });
        if (!returned) {
            leaked_at_close_ = current_size_ - idle_connections_.size();
        }
        alive_flag_->store(false, std::memory_order_release);
        idle_connections_.clear();
        current_size_ = 0;
    }

    [[nodiscard]] pool_stats stats() const {
        std::lock_guard lock(mutex_);
        return pool_stats{.total = current_size_,
                          .idle = idle_connections_.size(),
                          .in_use = current_size_ - idle_connections_.size(),
                          .leaked_at_close = leaked_at_close_,
                          .closed = closed_};
    }

    // Set on invalid configuration or when warmup connections could not be prepared. `acquire`
    // keeps returning this error until the pool is replaced.
    [[nodiscard]] std::optional<db_error> warmup_error() const {
        std::lock_guard lock(mutex_);
        return warmup_error_;
    }

    asterorm::result<lease_type> acquire() {
        std::unique_lock lock(mutex_);
        if (warmup_error_)
            return std::unexpected(*warmup_error_);

        const auto timeout_time = std::chrono::steady_clock::now() + config_.acquire_timeout;

        for (;;) {
            if (closed_)
                return std::unexpected(closed_error());

            if (!idle_connections_.empty()) {
                auto conn = std::move(idle_connections_.back());
                idle_connections_.pop_back();
                if (!conn.is_open()) {
                    --current_size_;
                    cv_.notify_all();
                    continue;
                }
                if (config_.statement_timeout) {
                    // A popped idle connection still owns its existing capacity slot.
                    slot_reservation reservation{*this, lock};
                    lock.unlock();
                    auto configured = detail::configure_statement_timeout(conn, config_);
                    if (!configured) {
                        return std::unexpected(configured.error());
                    }
                    lock.lock();
                    if (closed_) {
                        lock.unlock();
                        detail::close_configured_connection(conn, config_);
                        return std::unexpected(closed_error());
                    }
                    reservation.disarm();
                }
                return lease_type{std::move(conn), this, alive_flag_};
            }

            if (current_size_ < config_.max_size) {
                ++current_size_;
                // Releases the reserved slot if connect throws or returns an error.
                slot_reservation reservation{*this, lock};
                lock.unlock();

                auto conn_res = driver_.connect(config_.conninfo);
                if (!conn_res)
                    return std::unexpected(conn_res.error());

                detail::configure_connection(*conn_res, config_);
                auto configured = detail::configure_statement_timeout(*conn_res, config_);
                if (!configured) {
                    return std::unexpected(configured.error());
                }
                lock.lock();
                if (closed_) {
                    lock.unlock();
                    detail::close_configured_connection(*conn_res, config_);
                    return std::unexpected(closed_error());
                }
                reservation.disarm();
                return lease_type{std::move(*conn_res), this, alive_flag_};
            }

            if (cv_.wait_until(lock, timeout_time) == std::cv_status::timeout) {
                if (idle_connections_.empty() && !closed_) {
                    db_error err;
                    err.kind = db_error_kind::connection_failed;
                    err.message = "Connection pool acquire timeout";
                    return std::unexpected(err);
                }
            }
        }
    }

    void release(connection_type&& conn) {
        std::lock_guard lock(mutex_);
        if (closed_) {
            // Pool was closed; drop the connection. Decrement so close()'s wait
            // predicate can make progress as leases return. Guard against the
            // post-timeout path where current_size_ was already reset to 0.
            if (current_size_ > 0) {
                --current_size_;
            }
            cv_.notify_all();
            return;
        }
        if (conn.is_open()) {
            idle_connections_.push_back(std::move(conn));
        } else {
            --current_size_;
        }
        cv_.notify_all();
    }

  private:
    Driver driver_;
    pool_config config_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<connection_type> idle_connections_;
    size_t current_size_{0};
    size_t leaked_at_close_{0};
    bool closed_{false};
    std::optional<db_error> warmup_error_;
    std::shared_ptr<std::atomic<bool>> alive_flag_{std::make_shared<std::atomic<bool>>(true)};
    std::chrono::milliseconds close_timeout_{std::chrono::seconds(10)};

    void fail_warmup(db_error error) {
        idle_connections_.clear();
        current_size_ = 0;
        warmup_error_ = std::move(error);
    }

    [[nodiscard]] static db_error closed_error() {
        db_error err;
        err.kind = db_error_kind::connection_failed;
        err.message = "Connection pool is closed";
        return err;
    }

    // Decrements current_size_ unless disarm() is called after a successful connect.
    class slot_reservation {
      public:
        slot_reservation(connection_pool& pool, std::unique_lock<std::mutex>& lock)
            : pool_(&pool), lock_(&lock) {}
        slot_reservation(const slot_reservation&) = delete;
        slot_reservation& operator=(const slot_reservation&) = delete;
        ~slot_reservation() {
            if (!active_)
                return;
            if (!lock_->owns_lock())
                lock_->lock();
            // close() may already have expired its wait and reset the count.
            if (pool_->current_size_ > 0) {
                --pool_->current_size_;
            }
            pool_->cv_.notify_all();
        }
        void disarm() {
            active_ = false;
        }

      private:
        connection_pool* pool_;
        std::unique_lock<std::mutex>* lock_;
        bool active_{true};
    };
};

} // namespace asterorm
