#pragma once
#include <string>
#include <vector>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/core/session.hpp"

namespace asterorm {

namespace detail {
template <typename T, typename Tuple, std::size_t... Is>
void collect_insert_params(const T& entity, const Tuple& columns,
                           std::vector<std::optional<std::string>>& params,
                           std::vector<std::string>& col_names,
                           std::vector<std::string>& placeholders,
                           std::index_sequence<Is...> /*seq*/) {
    size_t param_idx = 1;
    (..., [&]() {
        auto col = std::get<Is>(columns);
        if (col.gen != generated::always) {
            auto val = entity.*(col.member_ptr);

            bool skip = false;
            if constexpr (is_optional_v<decltype(val)>) {
                if (!val.has_value() && col.gen == generated::by_default) {
                    skip = true;
                }
            }

            if (!skip) {
                col_names.push_back(std::string(col.name));
                placeholders.push_back("$" + std::to_string(param_idx++));
                params.push_back(asterorm::encode(val));
            }
        }
    }());
}
} // namespace detail

template <typename Session> class repository {
  public:
    explicit repository(Session& sess) : session_(&sess) {}

    template <typename T> asterorm::result<void> insert(T& entity) {
        using traits = entity_traits<T>;
        std::vector<std::optional<std::string>> params;
        std::vector<std::string> col_names;
        std::vector<std::string> placeholders;

        detail::collect_insert_params(
            entity, traits::columns, params, col_names, placeholders,
            std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        std::string sql = "INSERT INTO ";
        sql += traits::table;
        sql += " (";
        for (size_t i = 0; i < col_names.size(); ++i) {
            sql += col_names[i];
            if (i < col_names.size() - 1)
                sql += ", ";
        }
        sql += ") VALUES (";
        for (size_t i = 0; i < placeholders.size(); ++i) {
            sql += placeholders[i];
            if (i < placeholders.size() - 1)
                sql += ", ";
        }
        sql += ") RETURNING ";
        sql += traits::primary_key.name;

        auto res = session_->with_connection(
            [&](auto& conn) { return conn.execute_prepared(sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() > 0) {
            auto pk_val_str = res->get_string(0, 0);
            auto decode_res =
                asterorm::decode(pk_val_str, entity.*(traits::primary_key.member_ptr));
            if (!decode_res)
                return std::unexpected(decode_res.error());
        }

        return {};
    }

    template <typename T> asterorm::result<void> upsert(T& entity) {
        using traits = entity_traits<T>;
        std::vector<std::optional<std::string>> params;
        std::vector<std::string> col_names;
        std::vector<std::string> placeholders;

        detail::collect_insert_params(
            entity, traits::columns, params, col_names, placeholders,
            std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        std::string sql = "INSERT INTO ";
        sql += traits::table;
        sql += " (";
        for (size_t i = 0; i < col_names.size(); ++i) {
            sql += col_names[i];
            if (i < col_names.size() - 1)
                sql += ", ";
        }
        sql += ") VALUES (";
        for (size_t i = 0; i < placeholders.size(); ++i) {
            sql += placeholders[i];
            if (i < placeholders.size() - 1)
                sql += ", ";
        }
        sql += ") ON CONFLICT (";
        sql += traits::primary_key.name;
        sql += ") DO UPDATE SET ";

        bool first_update_col = true;
        for (size_t i = 0; i < col_names.size(); ++i) {
            if (col_names[i] != traits::primary_key.name) {
                if (!first_update_col)
                    sql += ", ";
                sql += col_names[i];
                sql += " = EXCLUDED.";
                sql += col_names[i];
                first_update_col = false;
            }
        }

        sql += " RETURNING ";
        sql += traits::primary_key.name;

        auto res = session_->with_connection(
            [&](auto& conn) { return conn.execute_prepared(sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() > 0) {
            auto pk_val_str = res->get_string(0, 0);
            auto decode_res =
                asterorm::decode(pk_val_str, entity.*(traits::primary_key.member_ptr));
            if (!decode_res)
                return std::unexpected(decode_res.error());
        }

        return {};
    }

    template <typename T, typename PkType> asterorm::result<T> find(const PkType& pk_val) {
        using traits = entity_traits<T>;
        std::string sql = "SELECT ";

        std::vector<std::string> col_names;
        auto collect_select = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                col_names.push_back(std::string(col.name));
            }());
        };
        collect_select(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        for (size_t i = 0; i < col_names.size(); ++i) {
            sql += col_names[i];
            if (i < col_names.size() - 1)
                sql += ", ";
        }

        sql += " FROM ";
        sql += traits::table;
        sql += " WHERE ";
        sql += traits::primary_key.name;
        sql += " = $1";

        std::vector<std::optional<std::string>> params;
        params.push_back(asterorm::encode(pk_val));

        auto res = session_->with_connection(
            [&](auto& conn) { return conn.execute_prepared(sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() == 0) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "Entity not found";
            return std::unexpected(err);
        }

        T entity;
        asterorm::result<void> decode_err;
        auto populate = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                if (!decode_err)
                    return;
                auto col = std::get<Is>(traits::columns);
                auto val_str = res->get_string(0, static_cast<int>(Is));
                auto err = asterorm::decode(val_str, entity.*(col.member_ptr));
                if (!err)
                    decode_err = std::move(err);
            }());
        };
        populate(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
        if (!decode_err)
            return std::unexpected(decode_err.error());

        return entity;
    }

    template <typename T> asterorm::result<void> update(const T& entity) {
        using traits = entity_traits<T>;
        std::string sql = "UPDATE ";
        sql += traits::table;
        sql += " SET ";

        std::vector<std::optional<std::string>> params;

        auto collect_update = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            size_t param_idx = 1;
            bool first = true;
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (col.name != traits::primary_key.name) {
                    if (!first)
                        sql += ", ";
                    sql += col.name;
                    sql += " = $" + std::to_string(param_idx++);
                    params.push_back(asterorm::encode(entity.*(col.member_ptr)));
                    first = false;
                }
            }());
            sql += " WHERE ";
            sql += traits::primary_key.name;
            sql += " = $" + std::to_string(param_idx);
            params.push_back(asterorm::encode(entity.*(traits::primary_key.member_ptr)));
        };
        collect_update(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        auto res = session_->with_connection(
            [&](auto& conn) { return conn.execute_prepared(sql, params); });

        if (!res)
            return std::unexpected(res.error());
        return {};
    }

    template <typename T, typename PkType> asterorm::result<void> erase(const PkType& pk_val) {
        using traits = entity_traits<T>;
        std::string sql = "DELETE FROM ";
        sql += traits::table;
        sql += " WHERE ";
        sql += traits::primary_key.name;
        sql += " = $1";

        std::vector<std::optional<std::string>> params;
        params.push_back(asterorm::encode(pk_val));

        auto res = session_->with_connection(
            [&](auto& conn) { return conn.execute_prepared(sql, params); });

        if (!res)
            return std::unexpected(res.error());
        return {};
    }

  private:
    Session* session_;
};

} // namespace asterorm