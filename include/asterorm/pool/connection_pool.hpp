#pragma once

#include <chrono>
#include <condition_variable>
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
    std::string conninfo;
};

template <typename Driver>
class connection_pool;

template <typename Pool>
class connection_lease {
   public:
    using connection_type = typename Pool::connection_type;

    connection_lease(connection_type&& conn, Pool* pool) : conn_(std::move(conn)), pool_(pool) {}

    ~connection_lease() { release_to_pool(); }

    connection_lease(connection_lease&& other) noexcept : conn_(std::move(other.conn_)), pool_(other.pool_) {
        other.pool_ = nullptr;
    }

    connection_lease& operator=(connection_lease&& other) noexcept {
        if (this != &other) {
            release_to_pool();
            conn_ = std::move(other.conn_);
            pool_ = other.pool_;
            other.pool_ = nullptr;
        }
        return *this;
    }

    connection_type* operator->() { return &conn_; }
    connection_type& operator*() { return conn_; }

    void release_to_pool() {
        if (pool_ && conn_.is_open()) {
            pool_->release(std::move(conn_));
            pool_ = nullptr;
        }
    }

   private:
    connection_type conn_;
    Pool* pool_;
};

template <typename Driver>
class connection_pool {
   public:
    using driver_connect_result = decltype(std::declval<Driver>().connect(std::declval<const std::string&>()));
    using connection_type = typename driver_connect_result::value_type;
    using lease_type = connection_lease<connection_pool<Driver>>;

    connection_pool(Driver driver, pool_config config) : driver_(std::move(driver)), config_(std::move(config)) {
        for (size_t i = 0; i < config_.min_size; ++i) {
            auto conn_res = driver_.connect(config_.conninfo);
            if (conn_res) {
                idle_connections_.push_back(std::move(*conn_res));
                ++current_size_;
            }
        }
    }

    ~connection_pool() {
        std::lock_guard lock(mutex_);
        idle_connections_.clear();
    }

    asterorm::result<lease_type> acquire() {
        std::unique_lock lock(mutex_);

        auto now = std::chrono::steady_clock::now();
        auto timeout_time = now + config_.acquire_timeout;

        while (idle_connections_.empty()) {
            if (current_size_ < config_.max_size) {
                ++current_size_;
                lock.unlock();

                auto conn_res = driver_.connect(config_.conninfo);
                if (!conn_res) {
                    lock.lock();
                    --current_size_;
                    return std::unexpected(conn_res.error());
                }

                return lease_type(std::move(*conn_res), this);
            }

            if (cv_.wait_until(lock, timeout_time) == std::cv_status::timeout) {
                if (idle_connections_.empty()) {
                    db_error err;
                    err.kind = db_error_kind::connection_failed;
                    err.message = "Connection pool acquire timeout";
                    return std::unexpected(err);
                }
            }
        }

        auto conn = std::move(idle_connections_.back());
        idle_connections_.pop_back();

        if (!conn.is_open()) {
            --current_size_;
            lock.unlock();
            return acquire();
        }

        return lease_type(std::move(conn), this);
    }

    void release(connection_type&& conn) {
        std::lock_guard lock(mutex_);
        if (conn.is_open()) {
            idle_connections_.push_back(std::move(conn));
        } else {
            --current_size_;
        }
        cv_.notify_one();
    }

   private:
    Driver driver_;
    pool_config config_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<connection_type> idle_connections_;
    size_t current_size_{0};
};

}  // namespace asterorm