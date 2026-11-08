#pragma once

#include <array>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "asterorm/core/error.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/core/statement_timeout.hpp"

namespace asterorm {

enum class statement_cache_policy : std::uint8_t { per_connection, off };
enum class pool_profile : std::uint8_t { direct, transaction_pool };

struct pool_config {
    size_t min_size{1};
    size_t max_size{10};
    std::chrono::milliseconds acquire_timeout{5000};
    size_t prepared_statement_cache_size{128};
    std::string conninfo;
    std::optional<std::chrono::milliseconds> statement_timeout;
    bool pre_ping{false};
    std::chrono::milliseconds pre_ping_timeout{1000};
    std::chrono::milliseconds idle_timeout{0};
    std::chrono::milliseconds max_lifetime{0};
    std::chrono::milliseconds leak_threshold{0};
    std::chrono::milliseconds maintenance_interval{1000};
    statement_cache_policy statement_caching{statement_cache_policy::per_connection};
    pool_profile profile{pool_profile::direct};
};

enum class pool_diagnostic_severity : std::uint8_t { warning };
struct pool_diagnostic {
    std::uint64_t slot{};
    std::uint64_t generation{};
    std::chrono::milliseconds held_for{};
    pool_diagnostic_severity severity{pool_diagnostic_severity::warning};
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
    size_t overdue_leases{0};
    std::uint64_t acquires_succeeded{}, acquires_failed{}, acquire_timeouts{};
    std::uint64_t connection_failures{}, health_failures{}, retirements{}, diagnostics_dropped{};
    // Noncumulative buckets: <=1,5,10,50,100,500,1000ms, then overflow.
    std::array<std::uint64_t, 8> acquisition_duration{};
};

namespace detail {
using pool_clock = std::chrono::steady_clock;
using pool_clock_reader = pool_clock::time_point (*)() noexcept;
struct pool_test_access;
inline void pool_increment(std::uint64_t& counter) noexcept {
    if (counter != std::numeric_limits<std::uint64_t>::max()) {
        ++counter;
    }
}
inline bool pool_elapsed(pool_clock::time_point started, pool_clock::time_point now,
                         std::chrono::milliseconds limit) noexcept {
    if (limit.count() <= 0 || limit > std::chrono::duration_cast<std::chrono::milliseconds>(
                                          pool_clock::duration::max())) {
        return false;
    }
    const auto ticks = std::chrono::duration_cast<pool_clock::duration>(limit);
    return started <= pool_clock::time_point::max() - ticks && now >= started + ticks;
}
template <typename Connection>
concept has_pool_reuse_check = requires(Connection& connection) {
    { connection.is_pool_reusable() } noexcept -> std::same_as<bool>;
    { connection.close() } noexcept -> std::same_as<void>;
};

template <typename Connection>
concept has_pool_ping =
    has_pool_reuse_check<Connection> &&
    requires(Connection& connection, std::chrono::steady_clock::time_point deadline) {
        { connection.pool_ping(deadline) } -> std::same_as<result<void>>;
    };
template <typename Connection> bool pool_reusable(Connection& connection) {
    if (!connection.is_open()) {
        return false;
    }
    if constexpr (has_pool_reuse_check<Connection>) {
        return connection.is_pool_reusable();
    }
    return true;
}
template <typename Connection> result<void> validate_pool_ping(const pool_config& config) {
    if constexpr (!has_pool_ping<Connection>) {
        if (config.pre_ping) {
            db_error error;
            error.kind = db_error_kind::query_failed;
            error.message = "This backend does not support bounded pool pre-ping";
            return std::unexpected(std::move(error));
        }
    }
    return {};
}

template <typename Conn, typename = void> struct has_prepared_cache_limit : std::false_type {};

template <typename Conn>
struct has_prepared_cache_limit<
    Conn, std::void_t<decltype(std::declval<Conn&>().set_max_prepared_statements(std::size_t{}))>>
    : std::is_same<decltype(std::declval<Conn&>().set_max_prepared_statements(std::size_t{})),
                   void> {};

template <typename Conn> void configure_connection(Conn& conn, const pool_config& config) {
    if constexpr (has_prepared_cache_limit<Conn>::value) {
        conn.set_max_prepared_statements(config.statement_caching == statement_cache_policy::off
                                             ? 0
                                             : config.prepared_statement_cache_size);
    }
    if constexpr (requires {
                      { conn.set_transaction_pool_profile(true) } noexcept -> std::same_as<void>;
                  }) {
        conn.set_transaction_pool_profile(config.profile == pool_profile::transaction_pool);
    }
}
template <typename Conn> result<void> validate_pool_policy(const pool_config& config) {
    const char* invalid{};
    bool unsupported{};
    if (config.statement_caching != statement_cache_policy::off &&
        config.statement_caching != statement_cache_policy::per_connection) {
        invalid = "Unknown statement_caching policy";
    } else if (config.profile != pool_profile::direct &&
               config.profile != pool_profile::transaction_pool) {
        invalid = "Unknown pool profile";
    } else if (config.statement_caching == statement_cache_policy::off &&
               !has_prepared_cache_limit<Conn>::value) {
        invalid = "This backend does not support explicit statement cache policy";
        unsupported = true;
    } else if (config.profile == pool_profile::transaction_pool) {
        if constexpr (!requires(Conn& conn) {
                          requires Conn::supports_transaction_pool_profile;
                          {
                              conn.set_transaction_pool_profile(true)
                          } noexcept -> std::same_as<void>;
                      }) {
            invalid = "This backend does not support the transaction_pool profile";
            unsupported = true;
        } else if (config.statement_caching != statement_cache_policy::off &&
                   config.prepared_statement_cache_size != 0) {
            invalid = "transaction_pool requires effective statement caching off";
        } else if (config.statement_timeout) {
            invalid = "transaction_pool requires an absent session statement_timeout baseline";
        }
    }
    if (invalid) {
        db_error error;
        error.kind = unsupported ? db_error_kind::query_failed : db_error_kind::connection_failed;
        error.message = invalid;
        return std::unexpected(std::move(error));
    }
    return {};
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
    explicit pool_entry(Connection&& value,
                        pool_clock::time_point created = pool_clock::now()) noexcept
        : connection(std::move(value)), born_at(created), returned_at(created) {}
    Connection connection;
    std::unique_ptr<pool_entry> next;
    pool_entry* leased_previous{};
    pool_entry* leased_next{};
    pool_clock::time_point born_at, returned_at, checked_out_at;
    std::uint64_t slot{}, generation{};
    bool warned{}, retire_on_return{};
};

template <typename Driver, typename Connection> struct pool_state {
    using entry = pool_entry<Connection>;
    using entry_ptr = std::unique_ptr<entry>;
    enum class phase : std::uint8_t { connecting, checking, leased, retiring };

    pool_state(Driver value, pool_config settings, pool_clock_reader clock = pool_clock::now)
        : driver(std::move(value)), config(std::move(settings)), now(clock) {}
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
    bool lifetime_expired(const entry& node, pool_clock::time_point sampled) const noexcept {
        return pool_elapsed(node.born_at, sampled, config.max_lifetime);
    }
    bool idle_expired(const entry& node, pool_clock::time_point sampled) const noexcept {
        return lifetime_expired(node, sampled) ||
               pool_elapsed(node.returned_at, sampled, config.idle_timeout);
    }
    void checkout(entry& node) noexcept {
        node.checked_out_at = now();
        if (!node.slot) {
            pool_increment(next_slot);
            node.slot = next_slot;
        }
        pool_increment(next_generation);
        node.generation = next_generation;
        node.warned = false;
        node.leased_next = leased_head;
        if (leased_head) {
            leased_head->leased_previous = &node;
        }
        leased_head = &node;
    }
    void warn_overdue(entry& node, pool_clock::time_point sampled) noexcept {
        if (node.warned || !pool_elapsed(node.checked_out_at, sampled, config.leak_threshold)) {
            return;
        }
        node.warned = true;
        if (diagnostic_count == diagnostic_queue.size()) {
            pool_increment(diagnostics_dropped);
            return;
        }
        const auto tail = (diagnostic_head + diagnostic_count) % diagnostic_queue.size();
        diagnostic_queue[tail] = {
            .slot = node.slot,
            .generation = node.generation,
            .held_for = std::chrono::duration_cast<std::chrono::milliseconds>(sampled -
                                                                              node.checked_out_at),
        };
        ++diagnostic_count;
    }
    void unlease(entry& node) noexcept {
        if (node.leased_previous) {
            node.leased_previous->leased_next = node.leased_next;
        } else {
            leased_head = node.leased_next;
        }
        if (node.leased_next) {
            node.leased_next->leased_previous = node.leased_previous;
        }
        node.leased_previous = node.leased_next = nullptr;
    }
    void maintain() noexcept {
        entry_ptr detached;
        {
            std::scoped_lock lock(mutex);
            if (closed) {
                return;
            }
            const auto sampled = now();
            for (auto* node = leased_head; node; node = node->leased_next) {
                node->retire_on_return |= lifetime_expired(*node, sampled);
                warn_overdue(*node, sampled);
            }
            auto* link = &idle_head;
            while (*link) {
                if (!idle_expired(**link, sampled)) {
                    link = &(*link)->next;
                    continue;
                }
                auto node = std::move(*link);
                *link = std::move(node->next);
                node->next = std::move(detached);
                detached = std::move(node);
                --idle;
                ++retiring;
                pool_increment(retirements);
            }
        }
        while (detached) {
            auto node = std::move(detached);
            detached = std::move(node->next);
            dispose(std::move(node));
            finish_retirement();
        }
    }
    void retire(entry_ptr node, phase previous) noexcept {
        {
            std::scoped_lock lock(mutex);
            --count(previous);
            ++retiring;
            if (node) {
                pool_increment(retirements);
            }
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
            reusable = pool_reusable(node->connection);
        } catch (...) {
            // Custom readiness checks cannot make lease destruction throw.
            reusable = false;
        }
        {
            std::scoped_lock lock(mutex);
            const auto sampled = now();
            if (!closed) {
                warn_overdue(*node, sampled);
            }
            unlease(*node);
            reusable = reusable && !node->retire_on_return && !lifetime_expired(*node, sampled);
            if (!closed && reusable) {
                --leased;
                ++idle;
                node->returned_at = sampled;
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
            for (std::size_t index = 0; index < idle; ++index) {
                pool_increment(retirements);
            }
            idle = 0;
            detached = std::move(idle_head);
            condition.notify_all();
            maintenance_condition.notify_all();
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
    const pool_clock_reader now;
    mutable std::mutex mutex;
    std::condition_variable condition;
    std::condition_variable_any maintenance_condition;
    entry_ptr idle_head;
    entry* leased_head{};
    std::uint64_t next_slot{}, next_generation{};
    std::vector<pool_diagnostic> diagnostic_queue;
    std::size_t diagnostic_head{}, diagnostic_count{};
    std::uint64_t acquires_succeeded{}, acquires_failed{}, acquire_timeouts{};
    std::uint64_t connection_failures{}, health_failures{}, retirements{}, diagnostics_dropped{};
    std::array<std::uint64_t, 8> acquisition_duration{};
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
    } else if (config.pre_ping_timeout.count() < 0 ||
               (config.pre_ping && config.pre_ping_timeout.count() == 0)) {
        invalid = "pre_ping_timeout must be positive when enabled and nonnegative otherwise";
    } else if (config.idle_timeout.count() < 0) {
        invalid = "idle_timeout must be nonnegative";
    } else if (config.max_lifetime.count() < 0) {
        invalid = "max_lifetime must be nonnegative";
    } else if (config.leak_threshold.count() < 0) {
        invalid = "leak_threshold must be nonnegative";
    } else if (config.maintenance_interval.count() <= 0) {
        invalid = "maintenance_interval must be positive";
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
    friend struct detail::pool_test_access;
    using state_type = detail::pool_state<Driver, connection_type>;
    using phase = state_type::phase;

  public:
    using lease_type = connection_lease<connection_pool<Driver>>;
    static_assert(std::is_nothrow_move_constructible_v<connection_type>,
                  "asterorm: pooled connections must be nothrow move constructible");
    static_assert(std::is_nothrow_destructible_v<connection_type>,
                  "asterorm: pooled connections must be nothrow destructible");

    connection_pool(Driver driver, pool_config config)
        : connection_pool(std::move(driver), std::move(config), detail::pool_clock::now, true) {}

  private:
    // Internal clock/worker seam for deterministic boundary tests.
    connection_pool(Driver driver, pool_config config, detail::pool_clock_reader clock,
                    bool start_worker)
        : state_(std::make_shared<state_type>(std::move(driver), std::move(config), clock)) {
        auto& state = *state_;
        try {
            auto valid = detail::validate_pool_config(state.config);
            if (valid) {
                valid = detail::validate_pool_policy<connection_type>(state.config);
            }
            if (valid) {
                valid = detail::validate_pool_ping<connection_type>(state.config);
            }
            if (valid) {
                valid = detail::validate_timeout_config<connection_type>(state.config);
            }
            if (!valid) {
                state.fail_warmup(valid.error());
                return;
            }
            if (state.config.leak_threshold.count() > 0) {
                state.diagnostic_queue.resize(state.config.max_size);
            }
            for (std::size_t index = 0; index < state.config.min_size; ++index) {
                auto connected = state.driver.connect(state.config.conninfo);
                const auto created = state.now();
                if (!connected) {
                    state.fail_warmup(connected.error());
                    return;
                }
                auto node =
                    std::make_unique<typename state_type::entry>(std::move(*connected), created);
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
            if (start_worker &&
                (state.config.idle_timeout.count() > 0 || state.config.max_lifetime.count() > 0 ||
                 state.config.leak_threshold.count() > 0)) {
                worker_ =
                    std::jthread([weak = std::weak_ptr<state_type>{state_}](std::stop_token stop) {
                        while (!stop.stop_requested()) {
                            auto retained = weak.lock();
                            if (!retained) {
                                return;
                            }
                            std::unique_lock lock(retained->mutex);
                            retained->maintenance_condition.wait_until(
                                lock, stop,
                                detail::pool_wait_deadline(retained->config.maintenance_interval),
                                [&] { return retained->closed; });
                            if (retained->closed || stop.stop_requested()) {
                                return;
                            }
                            lock.unlock();
                            retained->maintain();
                        }
                    });
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

  public:
    connection_pool(const connection_pool&) = delete;
    connection_pool& operator=(const connection_pool&) = delete;
    connection_pool(connection_pool&&) = delete;
    connection_pool& operator=(connection_pool&&) = delete;
    ~connection_pool() noexcept {
        close();
    }
    void close() noexcept {
        worker_.request_stop();
        state_->close();
    }
    [[nodiscard]] bool allows_session_advisory_locks() const noexcept {
        return state_->config.profile != pool_profile::transaction_pool;
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
            .overdue_leases = overdue_count(*state),
            .acquires_succeeded = state->acquires_succeeded,
            .acquires_failed = state->acquires_failed,
            .acquire_timeouts = state->acquire_timeouts,
            .connection_failures = state->connection_failures,
            .health_failures = state->health_failures,
            .retirements = state->retirements,
            .diagnostics_dropped = state->diagnostics_dropped,
            .acquisition_duration = state->acquisition_duration,
        };
    }
    [[nodiscard]] std::vector<pool_diagnostic> take_diagnostics() {
        const auto state = state_;
        std::lock_guard lock(state->mutex);
        std::vector<pool_diagnostic> records;
        // Complete allocation before consuming any record; failure leaves the queue intact.
        records.reserve(state->diagnostic_count);
        for (std::size_t index = 0; index < state->diagnostic_count; ++index) {
            records.push_back(state->diagnostic_queue[(state->diagnostic_head + index) %
                                                      state->diagnostic_queue.size()]);
        }
        state->diagnostic_head = state->diagnostic_count = 0;
        return records;
    }
    [[nodiscard]] std::optional<db_error> warmup_error() const {
        const auto state = state_;
        std::lock_guard lock(state->mutex);
        return state->warmup_error;
    }
    result<lease_type> acquire() {
        const auto state = state_;
        acquisition_record record{*state};
        auto acquired = acquire_impl(state, record.outcome);
        record.succeeded = acquired.has_value();
        return acquired;
    }

  private:
    enum class acquisition_outcome : std::uint8_t { other, capacity_timeout, connection, health };
    struct acquisition_record {
        state_type& state;
        detail::pool_clock::time_point started{state.now()};
        acquisition_outcome outcome{acquisition_outcome::other};
        bool succeeded{};
        ~acquisition_record() noexcept {
            const auto elapsed = state.now() - started;
            constexpr std::array bounds{1, 5, 10, 50, 100, 500, 1000};
            std::size_t bucket{};
            while (bucket < bounds.size() && elapsed > std::chrono::milliseconds{bounds[bucket]}) {
                ++bucket;
            }
            std::lock_guard lock(state.mutex);
            detail::pool_increment(state.acquisition_duration[bucket]);
            detail::pool_increment(succeeded ? state.acquires_succeeded : state.acquires_failed);
            if (!succeeded) {
                switch (outcome) {
                case acquisition_outcome::capacity_timeout:
                    detail::pool_increment(state.acquire_timeouts);
                    break;
                case acquisition_outcome::connection:
                    detail::pool_increment(state.connection_failures);
                    break;
                case acquisition_outcome::health:
                    detail::pool_increment(state.health_failures);
                    break;
                case acquisition_outcome::other:
                    break;
                }
            }
        }
    };
    static std::size_t overdue_count(const state_type& state) noexcept {
        const auto sampled = state.now();
        std::size_t overdue{};
        for (auto* node = state.leased_head; node; node = node->leased_next) {
            overdue +=
                detail::pool_elapsed(node->checked_out_at, sampled, state.config.leak_threshold);
        }
        return overdue;
    }
    result<lease_type> acquire_impl(const std::shared_ptr<state_type>& state,
                                    acquisition_outcome& outcome) {
        const auto deadline = detail::pool_wait_deadline(state->config.acquire_timeout);
        for (;;) {
            outcome = acquisition_outcome::other;
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
                        outcome = acquisition_outcome::capacity_timeout;
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
            outcome = acquisition_outcome::connection;
            if (selected == phase::connecting) {
                auto connected = state->driver.connect(state->config.conninfo);
                const auto created = state->now();
                if (!connected) {
                    return std::unexpected(connected.error());
                }
                reserved.entry =
                    std::make_unique<typename state_type::entry>(std::move(*connected), created);
                detail::configure_connection(reserved.entry->connection, state->config);
                if (!detail::pool_reusable(reserved.entry->connection)) {
                    db_error error;
                    error.kind = db_error_kind::connection_failed;
                    error.message = "New pooled connection is not reusable";
                    return std::unexpected(std::move(error));
                }
            } else if (state->idle_expired(*reserved.entry, state->now()) ||
                       !detail::pool_reusable(reserved.entry->connection)) {
                continue;
            }
            if constexpr (detail::has_pool_ping<connection_type>) {
                if (state->config.pre_ping) {
                    outcome = acquisition_outcome::health;
                    auto healthy = reserved.entry->connection.pool_ping(
                        detail::pool_wait_deadline(state->config.pre_ping_timeout));
                    if (!healthy) {
                        return std::unexpected(healthy.error());
                    }
                }
            }
            outcome = acquisition_outcome::connection;
            auto configured =
                detail::configure_statement_timeout(reserved.entry->connection, state->config);
            if (!configured) {
                return std::unexpected(configured.error());
            }
            {
                std::lock_guard lock(state->mutex);
                outcome = acquisition_outcome::other;
                if (state->closed) {
                    return std::unexpected(detail::pool_closed_error());
                }
                if (state->lifetime_expired(*reserved.entry, state->now())) {
                    outcome = acquisition_outcome::connection;
                    db_error error;
                    error.kind = db_error_kind::connection_failed;
                    error.message = "Pooled connection expired during checkout setup";
                    return std::unexpected(std::move(error));
                }
                --state->count(selected);
                ++state->leased;
                state->checkout(*reserved.entry);
                reserved.active = false;
                return lease_type{state, std::move(reserved.entry)};
            }
        }
    }

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
    std::jthread worker_;
};
} // namespace asterorm
