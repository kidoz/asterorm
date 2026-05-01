#pragma once
#include <optional>
#include <string_view>
#include <vector>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/core/result.hpp"
#include "asterorm/pool/connection_pool.hpp"

namespace asterorm {

template <typename Session>
class transaction_guard {
   public:
    explicit transaction_guard(Session* s) : session_(s) {}

    transaction_guard(const transaction_guard&) = delete;
    transaction_guard& operator=(const transaction_guard&) = delete;

    transaction_guard(transaction_guard&& other) noexcept : session_(other.session_) { other.session_ = nullptr; }

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
            auto _ = rollback();  // ignores error in dtor
        }
    }

    asterorm::result<void> commit() {
        if (!session_) return {};
        auto res = session_->commit_transaction();
        session_ = nullptr;
        return res;
    }

    asterorm::result<void> rollback() {
        if (!session_) return {};
        auto res = session_->rollback_transaction();
        session_ = nullptr;
        return res;
    }

   private:
    Session* session_;
};

template <typename Pool>
class session {
   public:
    using lease_type = typename Pool::lease_type;
    using connection_type = typename Pool::connection_type;

    explicit session(Pool& pool) : pool_(&pool) {}

    asterorm::result<transaction_guard<session>> begin() {
        if (in_transaction_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Transaction already active in this session";
            return std::unexpected(err);
        }

        auto lease_res = pool_->acquire();
        if (!lease_res) return std::unexpected(lease_res.error());

        active_lease_ = std::move(lease_res.value());

        auto exec_res = (*active_lease_)->execute("BEGIN");
        if (!exec_res) {
            active_lease_.reset();
            return std::unexpected(exec_res.error());
        }

        in_transaction_ = true;
        return transaction_guard<session>{this};
    }

    asterorm::result<void> commit_transaction() {
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "No active transaction to commit";
            return std::unexpected(err);
        }

        auto res = (*active_lease_)->execute("COMMIT");
        in_transaction_ = false;
        active_lease_.reset();

        if (!res) return std::unexpected(res.error());
        return {};
    }

    asterorm::result<void> rollback_transaction() {
        if (!in_transaction_ || !active_lease_) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "No active transaction to rollback";
            return std::unexpected(err);
        }

        auto res = (*active_lease_)->execute("ROLLBACK");
        in_transaction_ = false;
        active_lease_.reset();

        if (!res) return std::unexpected(res.error());
        return {};
    }

    // Helper to safely run any query block utilizing the transaction's pinned connection
    // or an ephemeral connection lease if no transaction is active.
    template <typename F>
    auto with_connection(F&& func) -> decltype(func(std::declval<connection_type&>())) {
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
        std::vector<std::optional<std::string>> params;
        (params.push_back(asterorm::encode(std::forward<Args>(args))), ...);

        auto res = with_connection([&](auto& conn) { return conn.execute_prepared(sql, params); });

        if (!res) return std::unexpected(res.error());

        std::vector<T> result_list;
        using traits = entity_traits<T>;

        for (int r = 0; r < res->rows(); ++r) {
            T entity;
            auto populate = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (..., [&]() {
                    auto col = std::get<Is>(traits::columns);
                    // Find column index by name in the result set
                    int col_idx = -1;
                    for (int c = 0; c < res->columns(); ++c) {
                        if (res->column_name(c) == col.name) {
                            col_idx = c;
                            break;
                        }
                    }
                    if (col_idx >= 0) {
                        auto val_str = res->get_string(r, col_idx);
                        asterorm::decode(val_str, entity.*(col.member_ptr));
                    }
                }());
            };
            populate(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
            result_list.push_back(std::move(entity));
        }

        return result_list;
    }

   private:
    Pool* pool_;
    std::optional<lease_type> active_lease_;
    bool in_transaction_{false};
};

}  // namespace asterorm
