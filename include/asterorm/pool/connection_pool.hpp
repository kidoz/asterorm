#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <vector>

#include "asterorm/core/error.hpp"
#include "asterorm/core/result.hpp"

namespace asterorm {

struct pool_config {
    size_t min_size{1};
    size_t max_size{10};
    std::chrono::milliseconds acquire_timeout{5000};
    size_t prepared_statement_cache_size{128};
    std::string conninfo;
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
        for (size_t i = 0; i < config_.min_size; ++i) {
            auto conn_res = driver_.connect(config_.conninfo);
            if (conn_res) {
                detail::configure_connection(*conn_res, config_);
                idle_connections_.push_back(std::move(*conn_res));
                ++current_size_;
            }
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

    asterorm::result<lease_type> acquire() {
        std::unique_lock lock(mutex_);

        auto now = std::chrono::steady_clock::now();
        auto timeout_time = now + config_.acquire_timeout;

        while (idle_connections_.empty()) {
            if (closed_) {
                db_error err;
                err.kind = db_error_kind::connection_failed;
                err.message = "Connection pool is closed";
                return std::unexpected(err);
            }

            if (current_size_ < config_.max_size) {
                ++current_size_;
                lock.unlock();

                auto conn_res = driver_.connect(config_.conninfo);
                if (!conn_res) {
                    lock.lock();
                    --current_size_;
                    cv_.notify_all(); // Notify destructor if it's waiting
                    return std::unexpected(conn_res.error());
                }

                detail::configure_connection(*conn_res, config_);
                lock.lock();
                if (closed_) {
                    --current_size_;
                    cv_.notify_all();
                    db_error err;
                    err.kind = db_error_kind::connection_failed;
                    err.message = "Connection pool is closed";
                    return std::unexpected(err);
                }
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

        if (closed_) {
            db_error err;
            err.kind = db_error_kind::connection_failed;
            err.message = "Connection pool is closed";
            return std::unexpected(err);
        }

        auto conn = std::move(idle_connections_.back());
        idle_connections_.pop_back();

        if (!conn.is_open()) {
            --current_size_;
            cv_.notify_all();
            lock.unlock();
            return acquire();
        }

        return lease_type{std::move(conn), this, alive_flag_};
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
    std::shared_ptr<std::atomic<bool>> alive_flag_{std::make_shared<std::atomic<bool>>(true)};
    std::chrono::milliseconds close_timeout_{std::chrono::seconds(10)};
};

} // namespace asterorm
