#pragma once

#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstdint>
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
    std::optional<std::chrono::milliseconds> statement_timeout;
};

struct pool_stats {
    size_t total{0};
    size_t idle{0};
    size_t in_use{0};
    size_t leaked_at_close{0};
    bool closed{false};
    size_t leased{0};
    size_t connecting{0};
    size_t checking{0};
    size_t retiring{0};
    size_t waiters{0};
};

namespace detail {
template <typename Connection>
concept has_pool_reuse_check = requires(Connection& connection) {
    { connection.is_pool_reusable() } noexcept -> std::same_as<bool>;
    { connection.close() } noexcept -> std::same_as<void>;
};

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

// Intrusive idle entries make the return path allocation-free. A leased entry
// and its state survive independently of the pool facade.
template <typename Connection> struct pool_entry {
    explicit pool_entry(Connection&& value) noexcept : connection(std::move(value)) {}
    Connection connection;
    std::unique_ptr<pool_entry> next;
};

template <typename Driver, typename Connection> struct pool_state {
    using entry = pool_entry<Connection>;
    using entry_ptr = std::unique_ptr<entry>;
    enum class phase : std::uint8_t { connecting, checking, leased, retiring };

    pool_state(Driver value, pool_config settings)
        : driver(std::move(value)), config(std::move(settings)) {}
    ~pool_state() {
        // Normally close() has already detached every idle entry. Also make
        // constructor-unwind cleanup iterative, rather than recursively linked.
        while (idle_head) {
            auto node = std::move(idle_head);
            idle_head = std::move(node->next);
            dispose(std::move(node));
        }
    }
    static void dispose(entry_ptr node) noexcept {
        if (node) {
            if constexpr (requires(Connection& connection) {
                              { connection.close() } noexcept -> std::same_as<void>;
                          }) {
                node->connection.close();
            }
            node.reset();
        }
    }
    std::size_t& count(phase current) noexcept {
        switch (current) {
        case phase::connecting:
            return connecting;
        case phase::checking:
            return checking;
        case phase::leased:
            return leased;
        case phase::retiring:
            return retiring;
        }
        std::terminate();
    }
    void retire(entry_ptr node, phase previous) noexcept {
        {
            std::scoped_lock lock(mutex);
            --count(previous);
            ++retiring;
        }
        dispose(std::move(node));
        finish_retirement();
    }
    void finish_retirement() noexcept {
        std::scoped_lock lock(mutex);
        --retiring;
        --total;
        condition.notify_all();
    }
    void return_entry(entry_ptr node) noexcept {
        bool reusable{};
        try {
            reusable = node->connection.is_open();
            if constexpr (has_pool_reuse_check<Connection>) {
                reusable = reusable && node->connection.is_pool_reusable();
            }
        } catch (...) {
            // Custom readiness checks cannot make lease destruction throw.
            reusable = false;
        }
        {
            std::scoped_lock lock(mutex);
            if (!closed && reusable) {
                --leased;
                ++idle;
                node->next = std::move(idle_head);
                idle_head = std::move(node);
                condition.notify_all();
                return;
            }
        }
        retire(std::move(node), phase::leased);
    }
    void close() noexcept {
        entry_ptr detached;
        {
            std::scoped_lock lock(mutex);
            if (closed) {
                return;
            }
            closed = true;
            leaked_at_close = total - idle;
            retiring += idle;
            idle = 0;
            detached = std::move(idle_head);
            condition.notify_all();
        }
        while (detached) {
            auto node = std::move(detached);
            detached = std::move(node->next);
            dispose(std::move(node));
            finish_retirement();
        }
    }
    // Used only during construction, before the facade can be shared.
    void fail_warmup(db_error error) {
        while (idle_head) {
            auto node = std::move(idle_head);
            idle_head = std::move(node->next);
            dispose(std::move(node));
        }
        total = idle = 0;
        warmup_error = std::move(error);
    }
    Driver driver;
    const pool_config config;
    mutable std::mutex mutex;
    std::condition_variable condition;
    entry_ptr idle_head;
    std::size_t total{}, idle{}, leased{}, connecting{}, checking{}, retiring{}, waiters{};
    std::size_t leaked_at_close{};
    bool closed{};
    std::optional<db_error> warmup_error;
};
inline result<void> validate_pool_config(const pool_config& config) {
    const char* invalid{};
    if (config.max_size == 0) {
        invalid = "max_size must be positive";
    } else if (config.min_size > config.max_size) {
        invalid = "min_size must not exceed max_size";
    } else if (config.acquire_timeout.count() < 0) {
        invalid = "acquire_timeout must be nonnegative";
    }
    if (invalid) {
        db_error error;
        error.kind = db_error_kind::connection_failed;
        error.message = invalid;
        return std::unexpected(std::move(error));
    }
    return {};
}
inline std::chrono::steady_clock::time_point pool_wait_deadline(std::chrono::milliseconds timeout) {
    using clock = std::chrono::steady_clock;
    const auto now = clock::now();
    // Invalid negative configuration is retained by warmup_error(); acquisition
    // must still reach that diagnostic without overflowing deadline arithmetic.
    if (timeout <= std::chrono::milliseconds::zero()) {
        return now;
    }
    // Clamp before conversion to the clock's finer tick unit can overflow.
    if (timeout >= std::chrono::duration_cast<std::chrono::milliseconds>(clock::duration::max())) {
        return clock::time_point::max();
    }
    const auto ticks = std::chrono::duration_cast<clock::duration>(timeout);
    if (now > clock::time_point::max() - ticks) {
        return clock::time_point::max();
    }
    return now + ticks;
}
inline db_error pool_closed_error() {
    db_error error;
    error.kind = db_error_kind::pool_closed;
    error.message = "Connection pool is closed";
    return error;
}
} // namespace detail

template <typename Driver> class connection_pool;

template <typename Pool> class connection_lease {
  public:
    using connection_type = Pool::connection_type;
    ~connection_lease() noexcept {
        release_to_pool();
    }
    connection_lease(const connection_lease&) = delete;
    connection_lease& operator=(const connection_lease&) = delete;
    connection_lease(connection_lease&&) noexcept = default;
    connection_lease& operator=(connection_lease&& other) noexcept {
        if (this != &other) {
            release_to_pool();
            state_ = std::move(other.state_);
            entry_ = std::move(other.entry_);
        }
        return *this;
    }
    connection_type* operator->() {
        return &entry_->connection;
    }
    connection_type& operator*() {
        return entry_->connection;
    }
    void release_to_pool() noexcept {
        if (entry_) {
            auto state = std::move(state_);
            state->return_entry(std::move(entry_));
        }
    }

  private:
    friend Pool;
    using state_type = Pool::state_type;
    connection_lease(std::shared_ptr<state_type> state, state_type::entry_ptr entry) noexcept
        : state_(std::move(state)), entry_(std::move(entry)) {}
    std::shared_ptr<state_type> state_;
    state_type::entry_ptr entry_;
};

template <typename Driver> class connection_pool {
  public:
    using driver_connect_result =
        decltype(std::declval<Driver>().connect(std::declval<const std::string&>()));
    using connection_type = driver_connect_result::value_type;

  private:
    template <typename> friend class connection_lease;
    using state_type = detail::pool_state<Driver, connection_type>;
    using phase = state_type::phase;

  public:
    using lease_type = connection_lease<connection_pool<Driver>>;
    static_assert(std::is_nothrow_move_constructible_v<connection_type>,
                  "asterorm: pooled connections must be nothrow move constructible");
    static_assert(std::is_nothrow_destructible_v<connection_type>,
                  "asterorm: pooled connections must be nothrow destructible");

    connection_pool(Driver driver, pool_config config)
        : state_(std::make_shared<state_type>(std::move(driver), std::move(config))) {
        auto& state = *state_;
        try {
            auto valid = detail::validate_pool_config(state.config);
            if (valid) {
                valid = detail::validate_timeout_config<connection_type>(state.config);
            }
            if (!valid) {
                state.fail_warmup(valid.error());
                return;
            }
            for (std::size_t index = 0; index < state.config.min_size; ++index) {
                auto connected = state.driver.connect(state.config.conninfo);
                if (!connected) {
                    state.fail_warmup(connected.error());
                    return;
                }
                auto node = std::make_unique<typename state_type::entry>(std::move(*connected));
                detail::configure_connection(node->connection, state.config);
                auto configured =
                    detail::configure_statement_timeout(node->connection, state.config);
                if (!configured) {
                    state.fail_warmup(configured.error());
                    return;
                }
                node->next = std::move(state.idle_head);
                state.idle_head = std::move(node);
                ++state.idle;
                ++state.total;
            }
        } catch (const std::exception& exception) {
            db_error error;
            error.kind = db_error_kind::connection_failed;
            error.message = exception.what();
            state.fail_warmup(std::move(error));
        } catch (...) {
            db_error error;
            error.kind = db_error_kind::connection_failed;
            error.message = "Connection pool warmup failed";
            state.fail_warmup(std::move(error));
        }
    }
    connection_pool(const connection_pool&) = delete;
    connection_pool& operator=(const connection_pool&) = delete;
    connection_pool(connection_pool&&) = delete;
    connection_pool& operator=(connection_pool&&) = delete;
    ~connection_pool() noexcept {
        close();
    }
    void close() noexcept {
        state_->close();
    }
    [[nodiscard]] pool_stats stats() const {
        const auto state = state_;
        std::lock_guard lock(state->mutex);
        return {
            .total = state->total,
            .idle = state->idle,
            .in_use = state->total - state->idle,
            .leaked_at_close = state->leaked_at_close,
            .closed = state->closed,
            .leased = state->leased,
            .connecting = state->connecting,
            .checking = state->checking,
            .retiring = state->retiring,
            .waiters = state->waiters,
        };
    }
    [[nodiscard]] std::optional<db_error> warmup_error() const {
        const auto state = state_;
        std::lock_guard lock(state->mutex);
        return state->warmup_error;
    }
    result<lease_type> acquire() {
        const auto state = state_;
        const auto deadline = detail::pool_wait_deadline(state->config.acquire_timeout);
        for (;;) {
            typename state_type::entry_ptr candidate;
            phase selected{};
            {
                std::unique_lock lock(state->mutex);
                for (;;) {
                    if (state->closed) {
                        return std::unexpected(detail::pool_closed_error());
                    }
                    if (state->warmup_error) {
                        return std::unexpected(*state->warmup_error);
                    }
                    if (state->idle_head) {
                        candidate = std::move(state->idle_head);
                        state->idle_head = std::move(candidate->next);
                        --state->idle;
                        ++state->checking;
                        selected = phase::checking;
                        break;
                    }
                    if (state->total < state->config.max_size) {
                        ++state->total;
                        ++state->connecting;
                        selected = phase::connecting;
                        break;
                    }
                    if (std::chrono::steady_clock::now() >= deadline) {
                        db_error error;
                        error.kind = db_error_kind::timeout;
                        error.message = "Connection pool acquire timeout";
                        return std::unexpected(std::move(error));
                    }
                    ++state->waiters;
                    try {
                        state->condition.wait_until(lock, deadline);
                    } catch (...) {
                        --state->waiters;
                        throw;
                    }
                    --state->waiters;
                }
            }
            reservation reserved{state, selected, std::move(candidate)};
            if (selected == phase::connecting) {
                auto connected = state->driver.connect(state->config.conninfo);
                if (!connected) {
                    return std::unexpected(connected.error());
                }
                reserved.entry =
                    std::make_unique<typename state_type::entry>(std::move(*connected));
                detail::configure_connection(reserved.entry->connection, state->config);
            } else if (!reserved.entry->connection.is_open()) {
                continue;
            }
            auto configured =
                detail::configure_statement_timeout(reserved.entry->connection, state->config);
            if (!configured) {
                return std::unexpected(configured.error());
            }
            {
                std::lock_guard lock(state->mutex);
                if (!state->closed) {
                    --state->count(selected);
                    ++state->leased;
                    reserved.active = false;
                    return lease_type{state, std::move(reserved.entry)};
                }
            }
            return std::unexpected(detail::pool_closed_error());
        }
    }

  private:
    struct reservation {
        std::shared_ptr<state_type> state;
        phase current;
        state_type::entry_ptr entry;
        bool active{true};
        ~reservation() noexcept {
            if (active) {
                state->retire(std::move(entry), current);
            }
        }
    };
    std::shared_ptr<state_type> state_;
};
} // namespace asterorm
