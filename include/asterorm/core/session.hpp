#pragma once
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
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
} // namespace detail

template <typename Session> class transaction_guard {
  public:
    explicit transaction_guard(Session* s) : session_(s) {}

    transaction_guard(const transaction_guard&) = delete;
    transaction_guard& operator=(const transaction_guard&) = delete;

    transaction_guard(transaction_guard&& other) noexcept : session_(other.session_) {
        other.session_ = nullptr;
    }

    transaction_guard& operator=(transaction_guard&& other) noexcept {
        if (this != &other) {
            if (session_) {
                auto _ = rollback();
            }
            session_ = other.session_;
            other.session_ = nullptr;
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
        auto res = session_->commit_transaction();
        if (res) {
            session_ = nullptr;
        }
        return res;
    }

    asterorm::result<void> rollback() {
        if (!session_)
            return {};
        auto res = session_->rollback_transaction();
        if (res) {
            session_ = nullptr;
        }
        return res;
    }

  private:
    Session* session_;
};

template <typename Pool> class session {
  public:
    using lease_type = typename Pool::lease_type;
    using connection_type = typename Pool::connection_type;

    explicit session(Pool& pool) : pool_(&pool) {}

    // Install a user observer called for each query executed via observed_execute().
    // Pass a null function to clear. Safe to call before or between queries.
    void set_observer(query_observer obs) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        observer_ = std::move(obs);
    }

    // Execute a prepared statement against the session's connection, emitting a
    // query_event if an observer is installed. Used by CRUD and native<T>; can
    // also be called directly.
    template <typename Conn>
    auto observed_execute(Conn& conn, std::string_view sql,
                          const std::vector<std::optional<std::string>>& params)
        -> decltype(conn.execute_prepared(sql, params)) {
        query_observer observer;
        {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            observer = observer_;
        }

        if (!observer) {
            return conn.execute_prepared(sql, params);
        }
        const auto start = std::chrono::steady_clock::now();
        auto res = conn.execute_prepared(sql, params);
        const auto elapsed = std::chrono::steady_clock::now() - start;
        query_event ev{sql, params.size(),
                       std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed), nullptr};
        if (!res)
            ev.error = &res.error();
        observer(ev);
        return res;
    }

    asterorm::result<transaction_guard<session>> begin(transaction_options opts = {}) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (in_transaction_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Transaction already active in this session";
            return std::unexpected(err);
        }

        auto lease_res = pool_->acquire();
        if (!lease_res)
            return std::unexpected(lease_res.error());

        active_lease_ = std::move(lease_res.value());

        const auto sql = detail::build_begin_sql(opts);
        auto exec_res = (*active_lease_)->execute(sql);
        if (!exec_res) {
            active_lease_.reset();
            return std::unexpected(exec_res.error());
        }

        in_transaction_ = true;
        return transaction_guard<session>{this};
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
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "No active transaction to commit";
            return std::unexpected(err);
        }

        auto res = (*active_lease_)->execute("COMMIT");
        if (!res) {
            return std::unexpected(res.error());
        }

        in_transaction_ = false;
        active_lease_.reset();

        return {};
    }

    asterorm::result<void> rollback_transaction() {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "No active transaction to rollback";
            return std::unexpected(err);
        }

        auto res = (*active_lease_)->execute("ROLLBACK");
        if (!res) {
            (*active_lease_)->close(); // Force close on failed rollback so pool discards it
            in_transaction_ = false;
            active_lease_.reset();
            return std::unexpected(res.error());
        }

        in_transaction_ = false;
        active_lease_.reset();

        return {};
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

    // Helper to safely run any query block utilizing the transaction's pinned connection
    // or an ephemeral connection lease if no transaction is active.
    template <typename F>
    auto with_connection(F&& func) -> decltype(func(std::declval<connection_type&>())) {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (in_transaction_ && active_lease_) {
            return func(**active_lease_);
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
        std::vector<std::optional<std::string>> params;
        (params.push_back(asterorm::encode(std::forward<Args>(args))), ...);

        auto res = with_connection([&](auto& conn) { return observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        std::vector<T> result_list;
        using traits = entity_traits<T>;

        for (int r = 0; r < res->rows(); ++r) {
            T entity;
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
        std::vector<std::optional<std::string>> params;
        (params.push_back(asterorm::encode(std::forward<Args>(args))), ...);

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
    asterorm::result<void> emit_savepoint_locked(std::string_view verb, std::string_view name) {
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Savepoint requires an active transaction";
            return std::unexpected(err);
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
    query_observer observer_;

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
