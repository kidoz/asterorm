#pragma once
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/core/observer.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/pool/connection_pool.hpp"

namespace asterorm {

enum class isolation_level : std::uint8_t {
    default_level,
    read_uncommitted,
    read_committed,
    repeatable_read,
    serializable,
};

struct transaction_options {
    isolation_level isolation{isolation_level::default_level};
    bool read_only{false};
    bool deferrable{false};
};

enum class native_hydration : std::uint8_t {
    strict,
    lenient,
};

namespace detail {

template <typename> struct is_session_result : std::false_type {};
template <typename Value> struct is_session_result<asterorm::result<Value>> : std::true_type {};

template <typename Connection>
concept has_advisory_scope = requires(Connection& connection, std::int64_t key) {
    { connection.advisory_lock(key) } -> std::same_as<asterorm::result<void>>;
    { connection.advisory_unlock(key) } -> std::same_as<asterorm::result<void>>;
    { connection.close() } noexcept -> std::same_as<void>;
};

template <typename Conn>
concept has_build_begin_sql = requires(const transaction_options& opts) {
    { Conn::build_begin_sql(opts) } -> std::convertible_to<std::string>;
};

inline std::string build_begin_sql(const transaction_options& opts) {
    std::string sql = "BEGIN";
    switch (opts.isolation) {
    case isolation_level::read_uncommitted:
        sql += " ISOLATION LEVEL READ UNCOMMITTED";
        break;
    case isolation_level::read_committed:
        sql += " ISOLATION LEVEL READ COMMITTED";
        break;
    case isolation_level::repeatable_read:
        sql += " ISOLATION LEVEL REPEATABLE READ";
        break;
    case isolation_level::serializable:
        sql += " ISOLATION LEVEL SERIALIZABLE";
        break;
    case isolation_level::default_level:
        break;
    }
    sql += opts.read_only ? " READ ONLY" : "";
    sql += opts.deferrable ? " DEFERRABLE" : "";
    return sql;
}

// Dispatch to a connection-specific begin-SQL builder when the backend
// declares one (SQLite has no ANSI isolation/READ ONLY/DEFERRABLE syntax).
// Falls back to the PostgreSQL-style default otherwise.
template <typename Conn> std::string build_begin_sql_for(const transaction_options& opts) {
    if constexpr (has_build_begin_sql<Conn>) {
        return Conn::build_begin_sql(opts);
    } else {
        return build_begin_sql(opts);
    }
}

// Quote an unqualified savepoint identifier. Rejects empty input or embedded
// quotes so this cannot smuggle arbitrary SQL into a SAVEPOINT statement.
inline std::optional<std::string> quote_savepoint(std::string_view name) {
    if (name.empty())
        return std::nullopt;
    std::string out;
    out.reserve(name.size() + 2);
    out += '"';
    for (char c : name) {
        if (c == '"' || c == '\0')
            return std::nullopt;
        out += c;
    }
    out += '"';
    return out;
}

template <typename Result> bool commit_confirmed(const Result& result_set) {
    if constexpr (requires {
                      { result_set.command_tag() } -> std::convertible_to<std::string_view>;
                  }) {
        return result_set.command_tag() == "COMMIT";
    }
    return true;
}
} // namespace detail

template <typename Session> class transaction_guard {
  public:
    explicit transaction_guard(Session* s, std::uint64_t transaction_id)
        : session_(s), transaction_id_(transaction_id) {}

    transaction_guard(const transaction_guard&) = delete;
    transaction_guard& operator=(const transaction_guard&) = delete;

    transaction_guard(transaction_guard&& other) noexcept
        : session_(other.session_), transaction_id_(other.transaction_id_) {
        other.session_ = nullptr;
        other.transaction_id_ = 0;
    }

    transaction_guard& operator=(transaction_guard&& other) noexcept {
        if (this != &other) {
            if (session_) {
                auto _ = rollback();
            }
            session_ = other.session_;
            transaction_id_ = other.transaction_id_;
            other.session_ = nullptr;
            other.transaction_id_ = 0;
        }
        return *this;
    }

    ~transaction_guard() {
        if (session_) {
            auto _ = rollback(); // ignores error in dtor
        }
    }

    asterorm::result<void> commit() {
        if (!session_)
            return {};
        auto res = session_->commit_transaction(transaction_id_);
        if (res) {
            session_ = nullptr;
            transaction_id_ = 0;
        }
        return res;
    }

    asterorm::result<void> rollback() {
        if (!session_)
            return {};
        auto res = session_->rollback_transaction(transaction_id_);
        if (res) {
            session_ = nullptr;
            transaction_id_ = 0;
        }
        return res;
    }

  private:
    Session* session_;
    std::uint64_t transaction_id_;
};

template <typename Pool> class session {
  public:
    using lease_type = typename Pool::lease_type;
    using connection_type = typename Pool::connection_type;

    explicit session(Pool& pool) : pool_(&pool) {}

    // Install a user observer called for each query executed via observed_execute().
    // Pass a null function to clear. Safe to call before or between queries.
    // The observer runs while this session holds its connection lease. It must
    // not call pool::close or wait for another lease on the same pool.
    void set_observer(query_observer obs) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        observer_ = std::move(obs);
    }

    // Execute a prepared statement against the session's connection, emitting a
    // query_event if an observer is installed. Used by CRUD and native<T>; can
    // also be called directly.
    template <typename Conn>
    auto observed_execute(Conn& conn, std::string_view sql, const query_parameters& params)
        -> decltype(conn.execute_prepared(sql, params)) {
        query_observer observer;
        {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            if (advisory_poisoned_) {
                return std::unexpected(advisory_scope_error());
            }
            if (transaction_invalidated_) {
                return std::unexpected(invalidated_transaction_error());
            }
            observer = observer_;
        }

        if (!observer) {
            auto res = conn.execute_prepared(sql, params);
            if (!res) {
                record_transaction_error(res.error());
            }
            return res;
        }
        const auto start = std::chrono::steady_clock::now();
        auto res = conn.execute_prepared(sql, params);
        if (!res) {
            record_transaction_error(res.error());
        }
        const auto elapsed = std::chrono::steady_clock::now() - start;
        query_event ev{std::string{sql}, params.size(),
                       std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed), nullptr};
        if (!res)
            ev.error = &res.error();
        observer(ev);
        return res;
    }

    [[nodiscard]] bool has_active_transaction() {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return in_transaction_;
    }

    asterorm::result<transaction_guard<session>> begin(transaction_options opts = {}) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (advisory_poisoned_) {
            return std::unexpected(advisory_scope_error());
        }
        if (in_transaction_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Transaction already active in this session";
            return std::unexpected(err);
        }

        if (!active_lease_) {
            auto lease_res = pool_->acquire();
            if (!lease_res) {
                return std::unexpected(lease_res.error());
            }
            active_lease_ = std::move(*lease_res);
        }

        auto exec_res = [&]() -> asterorm::result<void> {
            if constexpr (requires(connection_type& connection) {
                              connection.begin_transaction(opts);
                          }) {
                return (*active_lease_)->begin_transaction(opts);
            } else {
                const auto sql = detail::build_begin_sql_for<connection_type>(opts);
                auto executed = (*active_lease_)->execute(sql);
                if (!executed)
                    return std::unexpected(executed.error());
                return {};
            }
        }();
        if (!exec_res) {
            if (advisory_depth_ == 0) {
                active_lease_.reset();
            }
            return std::unexpected(exec_res.error());
        }

        in_transaction_ = true;
        transaction_invalidated_ = false;
        active_transaction_id_ = ++next_transaction_id_;
        if (next_transaction_id_ == 0)
            active_transaction_id_ = ++next_transaction_id_;
        return transaction_guard<session>{this, active_transaction_id_};
    }

    // Session-level exclusive advisory scope. Adapters opt in with lock/unlock
    // operations and nonthrowing close. The callback returns result<T> by value.
    template <typename Callback>
        requires detail::is_session_result<std::invoke_result_t<Callback, session&>>::value
    auto with_advisory_lock(std::int64_t key, Callback&& callback)
        -> std::invoke_result_t<Callback, session&> {
        using return_type = std::invoke_result_t<Callback, session&>;
        if constexpr (!detail::has_advisory_scope<connection_type>) {
            db_error error;
            error.kind = db_error_kind::query_failed;
            error.message = "This backend does not support scoped advisory locking";
            return return_type{std::unexpected(std::move(error))};
        } else {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            if (advisory_poisoned_) {
                return return_type{std::unexpected(advisory_scope_error())};
            }
            if (transaction_invalidated_) {
                return return_type{std::unexpected(invalidated_transaction_error())};
            }
            if (!active_lease_) {
                auto lease = pool_->acquire();
                if (!lease) {
                    return return_type{std::unexpected(lease.error())};
                }
                active_lease_ = std::move(*lease);
            }
            ++advisory_depth_;
            advisory_scope guard{*this, key};
            try {
                auto acquired = (*active_lease_)->advisory_lock(key);
                if (!acquired) {
                    // The server might have granted the lock before a response
                    // failed; never return this connection to the idle pool.
                    discard_advisory_connection();
                    return return_type{std::unexpected(acquired.error())};
                }
            } catch (...) {
                discard_advisory_connection();
                throw;
            }
            guard.acquired = true;
            auto result = std::invoke(std::forward<Callback>(callback), *this);
            auto cleanup = guard.finish();
            if (!cleanup) {
                if (!result) {
                    auto& diagnostic = result.error().detail;
                    if (!diagnostic) {
                        diagnostic = std::string{};
                    }
                    if (!diagnostic->empty()) {
                        *diagnostic += '\n';
                    }
                    *diagnostic += "Advisory lock cleanup failed; connection discarded: ";
                    *diagnostic += cleanup.error().message;
                    return result;
                }
                return return_type{std::unexpected(cleanup.error())};
            }
            return result;
        }
    }

    // Savepoint APIs. Must be called inside an active transaction. The
    // `name` is validated (non-empty, no embedded quotes) and quoted before
    // emission; it is NOT safe to accept unsanitized user input beyond that.
    asterorm::result<void> savepoint(std::string_view name) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return emit_savepoint_locked("SAVEPOINT ", name);
    }

    asterorm::result<void> release_savepoint(std::string_view name) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return emit_savepoint_locked("RELEASE SAVEPOINT ", name);
    }

    asterorm::result<void> rollback_to_savepoint(std::string_view name) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return emit_savepoint_locked("ROLLBACK TO SAVEPOINT ", name);
    }

    asterorm::result<void> commit_transaction() {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return commit_transaction(active_transaction_id_);
    }

    asterorm::result<void> rollback_transaction() {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return rollback_transaction(active_transaction_id_);
    }

    template <typename F>
    auto transact(F&& func, transaction_options opts = {}) -> decltype(func(*this)) {
        using ret_type = decltype(func(*this));

        auto tx_res = begin(opts);
        if (!tx_res)
            return ret_type{std::unexpected(tx_res.error())};

        auto tx = std::move(*tx_res);
        auto body_res = std::invoke(std::forward<F>(func), *this);
        if (!body_res) {
            auto _ = tx.rollback();
            return body_res;
        }

        auto commit_res = tx.commit();
        if (!commit_res)
            return ret_type{std::unexpected(commit_res.error())};

        return body_res;
    }

    template <typename F>
    auto retry_transaction(F&& func, std::size_t max_attempts = 3, transaction_options opts = {})
        -> decltype(func(*this)) {
        using ret_type = decltype(func(*this));
        if (max_attempts == 0)
            max_attempts = 1;

        ret_type last_error{std::unexpected(make_retry_error("transaction was not attempted"))};
        for (std::size_t attempt = 0; attempt < max_attempts; ++attempt) {
            auto res = transact(func, opts);
            if (res)
                return res;
            last_error = std::move(res);
            if (!is_retryable_transaction_error(last_error.error()))
                return last_error;
            if (attempt + 1 < max_attempts) {
                std::this_thread::sleep_for(std::chrono::milliseconds{10 * (attempt + 1)});
            }
        }
        return last_error;
    }

    // Run a query block on the transaction/advisory scope's pinned connection,
    // or on an ephemeral lease when neither is active.
    // Invalidated transactions reject callbacks until rollback. Void callbacks
    // throw logic_error because their return type cannot carry a db_error.
    template <typename F>
    auto with_connection(F&& func) -> decltype(func(std::declval<connection_type&>())) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (advisory_poisoned_) {
            if constexpr (std::is_void_v<decltype(func(std::declval<connection_type&>()))>) {
                throw std::logic_error(advisory_scope_error().message);
            } else {
                return std::unexpected(advisory_scope_error());
            }
        }
        if (active_lease_) {
            if (transaction_invalidated_) {
                if constexpr (std::is_void_v<decltype(func(**active_lease_))>) {
                    throw std::logic_error(invalidated_transaction_error().message);
                } else {
                    return std::unexpected(invalidated_transaction_error());
                }
            }
            if constexpr (std::is_void_v<decltype(func(**active_lease_))>) {
                func(**active_lease_);
                return;
            } else {
                auto res = func(**active_lease_);
                if constexpr (requires {
                                  res.error();
                                  static_cast<bool>(res);
                              }) {
                    if (!res) {
                        record_transaction_error(res.error());
                    }
                }
                return res;
            }
        } else {
            auto lease_res = pool_->acquire();
            if (!lease_res) {
                using Ret = decltype(func(std::declval<connection_type&>()));
                return Ret(std::unexpected(lease_res.error()));
            }
            return func(**lease_res);
        }
    }

    template <typename T, typename... Args>
    asterorm::result<std::vector<T>> native(std::string_view sql, Args&&... args) {
        return native_as<T>(native_hydration::strict, sql, std::forward<Args>(args)...);
    }

    template <typename T, typename... Args>
    asterorm::result<std::vector<T>> native_as(native_hydration hydration, std::string_view sql,
                                               Args&&... args) {
        query_parameters params;
        (params.push_back(asterorm::encode_parameter(std::forward<Args>(args))), ...);

        auto res = with_connection([&](auto& conn) { return observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        std::vector<T> result_list;
        using traits = entity_traits<T>;

        for (int r = 0; r < res->rows(); ++r) {
            T entity{};
            asterorm::result<void> decode_err;
            auto populate = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (..., [&]() {
                    if (!decode_err)
                        return;
                    auto col = std::get<Is>(traits::columns);
                    // Find column index by name in the result set
                    int col_idx = -1;
                    for (int c = 0; c < res->columns(); ++c) {
                        if (res->column_name(c) == col.name) {
                            col_idx = c;
                            break;
                        }
                    }
                    if (col_idx < 0 && hydration == native_hydration::strict) {
                        db_error err;
                        err.kind = db_error_kind::parse_failed;
                        err.message = "native<T>() result is missing mapped column: ";
                        err.message += col.name;
                        decode_err = std::unexpected(err);
                        return;
                    }
                    if (col_idx >= 0) {
                        auto val_str = res->get_string(r, col_idx);
                        auto err = asterorm::decode(val_str, entity.*(col.member_ptr));
                        if (!err)
                            decode_err = std::move(err);
                    }
                }());
            };
            populate(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
            if (!decode_err)
                return std::unexpected(decode_err.error());
            result_list.push_back(std::move(entity));
        }

        return result_list;
    }

    template <typename T, typename... Args>
    asterorm::result<std::optional<T>> native_scalar(std::string_view sql, Args&&... args) {
        auto rows = native_map(
            sql,
            [](const auto& result, int row) -> asterorm::result<T> {
                if (result.columns() == 0) {
                    db_error err;
                    err.kind = db_error_kind::parse_failed;
                    err.message = "native_scalar() requires at least one result column";
                    return std::unexpected(err);
                }
                T value{};
                auto decoded = asterorm::decode(result.get_string(row, 0), value);
                if (!decoded)
                    return std::unexpected(decoded.error());
                return value;
            },
            std::forward<Args>(args)...);

        if (!rows)
            return std::unexpected(rows.error());
        if (rows->empty())
            return std::optional<T>{};
        if (!(*rows)[0])
            return std::unexpected((*rows)[0].error());
        return std::optional<T>{std::move(*(*rows)[0])};
    }

    template <typename Mapper, typename... Args>
    auto native_map(std::string_view sql, Mapper&& mapper, Args&&... args) {
        query_parameters params;
        (params.push_back(asterorm::encode_parameter(std::forward<Args>(args))), ...);

        auto res = with_connection([&](auto& conn) { return observed_execute(conn, sql, params); });
        using mapped_type = std::invoke_result_t<Mapper&, decltype(*res), int>;
        using out_type = std::vector<mapped_type>;

        if (!res)
            return asterorm::result<out_type>{std::unexpected(res.error())};

        out_type rows;
        rows.reserve(static_cast<std::size_t>(res->rows()));
        for (int r = 0; r < res->rows(); ++r) {
            rows.push_back(std::invoke(mapper, *res, r));
        }
        return asterorm::result<out_type>{std::move(rows)};
    }

  private:
    template <typename> friend class transaction_guard;

    class advisory_scope {
      public:
        advisory_scope(session& owner, std::int64_t key) : owner_(&owner), key_(key) {}
        advisory_scope(const advisory_scope&) = delete;
        advisory_scope& operator=(const advisory_scope&) = delete;
        bool acquired{false};

        asterorm::result<void> finish() {
            auto* owner = std::exchange(owner_, nullptr);
            return owner->finish_advisory_scope(key_, acquired);
        }
        ~advisory_scope() {
            if (owner_) {
                auto* owner = owner_;
                try {
                    (void)finish();
                } catch (...) {
                    // Keep the original callback exception during unwinding.
                    owner->discard_advisory_connection();
                }
            }
        }

      private:
        session* owner_;
        std::int64_t key_;
    };

    void release_advisory_pin() {
        --advisory_depth_;
        if (advisory_depth_ == 0) {
            advisory_poisoned_ = false;
            if (!in_transaction_) {
                active_lease_.reset();
            }
        }
    }

    void discard_advisory_connection() noexcept {
        if (active_lease_) {
            (*active_lease_)->close();
        }
        in_transaction_ = false;
        transaction_invalidated_ = false;
        active_transaction_id_ = 0;
        advisory_poisoned_ = advisory_depth_ != 0;
    }

    asterorm::result<void> finish_advisory_scope(std::int64_t key, bool acquired) {
        struct pin_release {
            session& owner;
            ~pin_release() {
                owner.release_advisory_pin();
            }
        } release{*this};
        if (!acquired) {
            return {};
        }
        if (advisory_poisoned_) {
            return std::unexpected(advisory_scope_error());
        }
        try {
            auto unlocked = (*active_lease_)->advisory_unlock(key);
            if (!unlocked) {
                discard_advisory_connection();
            }
            return unlocked;
        } catch (...) {
            discard_advisory_connection();
            throw;
        }
    }

    static db_error advisory_scope_error() {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Advisory scope connection was discarded; unwind the scope before reuse";
        return error;
    }

    asterorm::result<void> commit_transaction(std::uint64_t expected_transaction_id) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "No active transaction to commit";
            return std::unexpected(err);
        }
        if (expected_transaction_id != active_transaction_id_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Transaction guard does not match the active transaction";
            return std::unexpected(err);
        }
        if (transaction_invalidated_) {
            return std::unexpected(invalidated_transaction_error());
        }

        auto res = (*active_lease_)->execute("COMMIT");
        if (!res)
            return std::unexpected(res.error());

        in_transaction_ = false;
        transaction_invalidated_ = false;
        active_transaction_id_ = 0;
        if (advisory_depth_ == 0) {
            active_lease_.reset();
        }
        if (!detail::commit_confirmed(*res)) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Transaction was rolled back instead of committed";
            return std::unexpected(err);
        }
        return {};
    }

    asterorm::result<void> rollback_transaction(std::uint64_t expected_transaction_id) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "No active transaction to rollback";
            return std::unexpected(err);
        }
        if (expected_transaction_id != active_transaction_id_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Transaction guard does not match the active transaction";
            return std::unexpected(err);
        }

        auto res = (*active_lease_)->execute("ROLLBACK");
        if (!res) {
            (*active_lease_)->close(); // Force close on failed rollback so pool discards it
            in_transaction_ = false;
            transaction_invalidated_ = false;
            active_transaction_id_ = 0;
            advisory_poisoned_ = advisory_depth_ != 0;
            if (advisory_depth_ == 0) {
                active_lease_.reset();
            }
            return std::unexpected(res.error());
        }

        in_transaction_ = false;
        transaction_invalidated_ = false;
        active_transaction_id_ = 0;
        if (advisory_depth_ == 0) {
            active_lease_.reset();
        }
        return {};
    }

    asterorm::result<void> emit_savepoint_locked(std::string_view verb, std::string_view name) {
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Savepoint requires an active transaction";
            return std::unexpected(err);
        }
        if (transaction_invalidated_) {
            return std::unexpected(invalidated_transaction_error());
        }
        auto quoted = detail::quote_savepoint(name);
        if (!quoted) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "Invalid savepoint name";
            return std::unexpected(err);
        }
        std::string sql{verb};
        sql += *quoted;
        auto r = (*active_lease_)->execute(sql);
        if (!r)
            return std::unexpected(r.error());
        return {};
    }

    Pool* pool_;
    std::recursive_mutex mutex_;
    std::optional<lease_type> active_lease_;
    bool in_transaction_{false};
    bool transaction_invalidated_{false};
    std::size_t advisory_depth_{0};
    bool advisory_poisoned_{false};
    std::uint64_t active_transaction_id_{0};
    std::uint64_t next_transaction_id_{0};
    query_observer observer_;

    static db_error invalidated_transaction_error() {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Transaction was invalidated by a deadlock or serialization failure; "
                        "rollback is required";
        return error;
    }

    void record_transaction_error(const db_error& err) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (in_transaction_ && is_retryable_transaction_error(err)) {
            transaction_invalidated_ = true;
        }
    }

    static bool is_retryable_transaction_error(const db_error& err) {
        return err.kind == db_error_kind::serialization_failure ||
               err.kind == db_error_kind::deadlock_detected || err.sqlstate == "40001" ||
               err.sqlstate == "40P01";
    }

    static db_error make_retry_error(std::string message) {
        db_error err;
        err.kind = db_error_kind::query_failed;
        err.message = std::move(message);
        return err;
    }
};

} // namespace asterorm
