#pragma once
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/core/session.hpp"
#include "asterorm/sql/ast.hpp"
#include "asterorm/sql/compiler.hpp"

namespace asterorm {

struct query_order {
    std::string column;
    bool ascending{true};
};

struct query_options {
    std::vector<query_order> order_by;
    std::optional<std::size_t> limit;
    std::optional<std::size_t> offset;
};

namespace detail {
template <typename T, typename = void> struct has_version_column : std::false_type {};

template <typename T>
struct has_version_column<T, std::void_t<decltype(entity_traits<T>::version_column)>>
    : std::true_type {};

template <typename T, typename = void> struct is_tuple_like : std::false_type {};

template <typename T>
struct is_tuple_like<T, std::void_t<decltype(std::tuple_size<std::decay_t<T>>::value)>>
    : std::true_type {};

template <typename T> inline constexpr bool is_tuple_like_v = is_tuple_like<T>::value;

template <typename Mapping, typename F> void for_each_pk(const Mapping& mapping, F&& f) {
    if constexpr (is_tuple_like_v<Mapping>) {
        std::apply([&](const auto&... pk) { (std::forward<F>(f)(pk), ...); }, mapping);
    } else {
        std::forward<F>(f)(mapping);
    }
}

template <typename T> bool is_primary_key_column(std::string_view name) {
    bool matched = false;
    for_each_pk(entity_traits<T>::primary_key,
                [&](const auto& pk) { matched = matched || std::string_view(pk.name) == name; });
    return matched;
}

template <typename T> std::vector<std::string> primary_key_names() {
    std::vector<std::string> names;
    for_each_pk(entity_traits<T>::primary_key,
                [&](const auto& pk) { names.push_back(std::string(pk.name)); });
    return names;
}

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

template <typename T, std::size_t... Is>
std::vector<std::string> column_names(std::index_sequence<Is...>) {
    using traits = entity_traits<T>;
    std::vector<std::string> names;
    names.reserve(sizeof...(Is));
    (names.push_back(std::string(std::get<Is>(traits::columns).name)), ...);
    return names;
}

template <typename T> std::vector<std::string> column_names() {
    using traits = entity_traits<T>;
    return column_names<T>(
        std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
}

inline void append_csv(std::string& sql, const std::vector<std::string>& names) {
    for (std::size_t i = 0; i < names.size(); ++i) {
        sql += names[i];
        if (i + 1 < names.size())
            sql += ", ";
    }
}

template <typename T> void append_primary_key_csv(std::string& sql) {
    append_csv(sql, primary_key_names<T>());
}

template <typename T>
void append_entity_pk_predicate(std::string& sql, std::vector<std::optional<std::string>>& params,
                                const T& entity, std::size_t& param_idx) {
    bool first = true;
    for_each_pk(entity_traits<T>::primary_key, [&](const auto& pk) {
        if (!first)
            sql += " AND ";
        first = false;
        sql += pk.name;
        sql += " = $" + std::to_string(param_idx++);
        params.push_back(asterorm::encode(entity.*(pk.member_ptr)));
    });
}

template <typename T, typename Key, std::size_t... Is>
void append_tuple_key_predicate(std::string& sql, std::vector<std::optional<std::string>>& params,
                                const Key& key, std::size_t& param_idx,
                                std::index_sequence<Is...>) {
    constexpr const auto& pk = entity_traits<T>::primary_key;
    bool first = true;
    (..., [&]() {
        if (!first)
            sql += " AND ";
        first = false;
        const auto& pk_part = std::get<Is>(pk);
        sql += pk_part.name;
        sql += " = $" + std::to_string(param_idx++);
        params.push_back(asterorm::encode(std::get<Is>(key)));
    }());
}

template <typename T, typename Key>
void append_key_predicate(std::string& sql, std::vector<std::optional<std::string>>& params,
                          const Key& key, std::size_t& param_idx) {
    constexpr const auto& pk = entity_traits<T>::primary_key;
    if constexpr (is_tuple_like_v<decltype(pk)>) {
        static_assert(is_tuple_like_v<Key>,
                      "Composite primary keys require a tuple-like key value");
        static_assert(std::tuple_size_v<std::decay_t<Key>> ==
                          std::tuple_size_v<std::decay_t<decltype(pk)>>,
                      "Composite key tuple size must match entity_traits<T>::primary_key");
        append_tuple_key_predicate<T>(
            sql, params, key, param_idx,
            std::make_index_sequence<std::tuple_size_v<std::decay_t<decltype(pk)>>>{});
    } else {
        sql += pk.name;
        sql += " = $" + std::to_string(param_idx++);
        params.push_back(asterorm::encode(key));
    }
}

template <typename T> void append_returning_all(std::string& sql) {
    sql += " RETURNING ";
    append_csv(sql, column_names<T>());
}

template <typename T, typename Result, std::size_t... Is>
asterorm::result<void> hydrate_entity(const Result& res, int row, T& entity,
                                      std::index_sequence<Is...> /*seq*/) {
    using traits = entity_traits<T>;
    asterorm::result<void> decode_err;
    (..., [&]() {
        if (!decode_err)
            return;
        auto col = std::get<Is>(traits::columns);
        auto val_str = res.get_string(row, static_cast<int>(Is));
        auto err = asterorm::decode(val_str, entity.*(col.member_ptr));
        if (!err)
            decode_err = std::move(err);
    }());
    return decode_err;
}

template <typename T, typename Result>
asterorm::result<void> hydrate_entity(const Result& res, int row, T& entity) {
    using traits = entity_traits<T>;
    return hydrate_entity(res, row, entity,
                          std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
}

inline db_error no_rows_affected_error() {
    db_error err;
    err.kind = db_error_kind::query_failed;
    err.message = "No rows affected";
    return err;
}

inline db_error stale_write_error() {
    db_error err;
    err.kind = db_error_kind::stale_write;
    err.message = "Optimistic lock conflict";
    return err;
}
} // namespace detail

template <typename Session> class repository {
  public:
    explicit repository(Session& sess) : session_(&sess) {}

    // Batch insert. All rows go into a single INSERT statement; skipped
    // generated columns are handled per-row (rows must agree on which
    // generated-by-default columns are set, since the column list is shared).
    // PKs on inserted entities are NOT refreshed from RETURNING — use the
    // single-row insert() when you need that.
    template <typename T> asterorm::result<std::size_t> insert_many(const std::vector<T>& rows) {
        using traits = entity_traits<T>;
        if (rows.empty())
            return std::size_t{0};

        std::vector<std::string> col_names;
        std::vector<bool> include_mask;
        auto collect_cols = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (col.gen != generated::always) {
                    col_names.push_back(std::string(col.name));
                    include_mask.push_back(true);
                } else {
                    include_mask.push_back(false);
                }
            }());
        };
        collect_cols(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        std::vector<std::optional<std::string>> params;
        params.reserve(rows.size() * col_names.size());

        std::string sql = "INSERT INTO ";
        sql += traits::table;
        sql += " (";
        for (std::size_t i = 0; i < col_names.size(); ++i) {
            sql += col_names[i];
            if (i + 1 < col_names.size())
                sql += ", ";
        }
        sql += ") VALUES ";

        std::size_t param_idx = 1;
        for (std::size_t r = 0; r < rows.size(); ++r) {
            sql += '(';
            bool first_in_row = true;
            auto bind = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (..., [&]() {
                    auto col = std::get<Is>(traits::columns);
                    if (col.gen == generated::always)
                        return;
                    if (!first_in_row)
                        sql += ", ";
                    first_in_row = false;
                    const auto& val = rows[r].*(col.member_ptr);
                    if constexpr (is_optional_v<std::decay_t<decltype(val)>>) {
                        if (!val.has_value() && col.gen == generated::by_default) {
                            sql += "DEFAULT";
                            return;
                        }
                    }
                    sql += "$" + std::to_string(param_idx++);
                    params.push_back(asterorm::encode(val));
                }());
            };
            bind(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
            sql += ')';
            if (r + 1 < rows.size())
                sql += ", ";
        }

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });
        if (!res)
            return std::unexpected(res.error());
        return static_cast<std::size_t>(res->affected_rows());
    }

    // Partial update. Only the columns in `changed` (names from entity_traits)
    // are written. Returns parse_failed if any name doesn't match a column.
    template <typename T>
    asterorm::result<void> patch(const T& entity, std::vector<std::string> changed) {
        using traits = entity_traits<T>;
        if (changed.empty()) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "patch() requires at least one column name";
            return std::unexpected(err);
        }

        std::vector<std::optional<std::string>> params;
        std::string sql = "UPDATE ";
        sql += traits::table;
        sql += " SET ";
        std::size_t idx = 1;
        bool first = true;
        std::vector<bool> matched(changed.size(), false);

        auto apply = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                for (std::size_t i = 0; i < changed.size(); ++i) {
                    if (std::string_view(col.name) == changed[i]) {
                        if (!first)
                            sql += ", ";
                        first = false;
                        sql += col.name;
                        sql += " = $" + std::to_string(idx++);
                        params.push_back(asterorm::encode(entity.*(col.member_ptr)));
                        matched[i] = true;
                    }
                }
            }());
        };
        apply(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        for (std::size_t i = 0; i < matched.size(); ++i) {
            if (!matched[i]) {
                db_error err;
                err.kind = db_error_kind::parse_failed;
                err.message = "Unknown column in patch(): " + changed[i];
                return std::unexpected(err);
            }
        }

        sql += " WHERE ";
        detail::append_entity_pk_predicate(sql, params, entity, idx);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });
        if (!res)
            return std::unexpected(res.error());
        if (res->affected_rows() == 0) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "No rows affected";
            return std::unexpected(err);
        }
        return {};
    }

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
        sql += ")";
        detail::append_returning_all<T>(sql);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() > 0) {
            auto decode_res = detail::hydrate_entity(*res, 0, entity);
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
        detail::append_primary_key_csv<T>(sql);
        sql += ") DO UPDATE SET ";

        bool first_update_col = true;
        for (size_t i = 0; i < col_names.size(); ++i) {
            if (!detail::is_primary_key_column<T>(col_names[i])) {
                if (!first_update_col)
                    sql += ", ";
                sql += col_names[i];
                if constexpr (detail::has_version_column<T>::value) {
                    if (col_names[i] == traits::version_column.name) {
                        sql += " = ";
                        sql += traits::table;
                        sql += ".";
                        sql += col_names[i];
                        sql += " + 1";
                    } else {
                        sql += " = EXCLUDED.";
                        sql += col_names[i];
                    }
                } else {
                    sql += " = EXCLUDED.";
                    sql += col_names[i];
                }
                first_update_col = false;
            }
        }

        if (first_update_col) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "upsert() requires at least one updatable column";
            return std::unexpected(err);
        }

        detail::append_returning_all<T>(sql);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() > 0) {
            auto decode_res = detail::hydrate_entity(*res, 0, entity);
            if (!decode_res)
                return std::unexpected(decode_res.error());
        }

        return {};
    }

    template <typename T> asterorm::result<std::size_t> upsert_many(std::vector<T>& rows) {
        using traits = entity_traits<T>;
        if (rows.empty())
            return std::size_t{0};

        std::vector<std::string> col_names;
        auto collect_cols = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (col.gen != generated::always)
                    col_names.push_back(std::string(col.name));
            }());
        };
        collect_cols(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        bool has_update_col = false;
        for (const auto& name : col_names) {
            if (!detail::is_primary_key_column<T>(name)) {
                has_update_col = true;
                break;
            }
        }
        if (!has_update_col) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "upsert_many() requires at least one updatable column";
            return std::unexpected(err);
        }

        std::vector<std::optional<std::string>> params;
        params.reserve(rows.size() * col_names.size());

        std::string sql = "INSERT INTO ";
        sql += traits::table;
        sql += " (";
        detail::append_csv(sql, col_names);
        sql += ") VALUES ";

        std::size_t param_idx = 1;
        for (std::size_t r = 0; r < rows.size(); ++r) {
            sql += '(';
            bool first_in_row = true;
            auto bind = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (..., [&]() {
                    auto col = std::get<Is>(traits::columns);
                    if (col.gen == generated::always)
                        return;
                    if (!first_in_row)
                        sql += ", ";
                    first_in_row = false;
                    const auto& val = rows[r].*(col.member_ptr);
                    if constexpr (is_optional_v<std::decay_t<decltype(val)>>) {
                        if (!val.has_value() && col.gen == generated::by_default) {
                            sql += "DEFAULT";
                            return;
                        }
                    }
                    sql += "$" + std::to_string(param_idx++);
                    params.push_back(asterorm::encode(val));
                }());
            };
            bind(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
            sql += ')';
            if (r + 1 < rows.size())
                sql += ", ";
        }

        sql += " ON CONFLICT (";
        detail::append_primary_key_csv<T>(sql);
        sql += ") DO UPDATE SET ";

        bool first_update_col = true;
        for (const auto& name : col_names) {
            if (detail::is_primary_key_column<T>(name))
                continue;
            if (!first_update_col)
                sql += ", ";
            sql += name;
            if constexpr (detail::has_version_column<T>::value) {
                if (name == traits::version_column.name) {
                    sql += " = ";
                    sql += traits::table;
                    sql += ".";
                    sql += name;
                    sql += " + 1";
                } else {
                    sql += " = EXCLUDED.";
                    sql += name;
                }
            } else {
                sql += " = EXCLUDED.";
                sql += name;
            }
            first_update_col = false;
        }

        detail::append_returning_all<T>(sql);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });
        if (!res)
            return std::unexpected(res.error());

        if (res->rows() != static_cast<int>(rows.size())) {
            db_error err;
            err.kind = db_error_kind::query_failed;
            err.message = "upsert_many() returned an unexpected row count";
            return std::unexpected(err);
        }

        for (int r = 0; r < res->rows(); ++r) {
            auto decoded = detail::hydrate_entity(*res, r, rows[static_cast<std::size_t>(r)]);
            if (!decoded)
                return std::unexpected(decoded.error());
        }

        return rows.size();
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

        std::vector<std::optional<std::string>> params;
        std::size_t param_idx = 1;
        detail::append_key_predicate<T>(sql, params, pk_val, param_idx);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });

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

    // Find entities matching an AST predicate. Column list and table come
    // from entity_traits<T>. Rows are hydrated positionally using that same
    // column order. Caller builds the predicate with col()/val() from
    // asterorm/sql/ast.hpp.
    template <typename T>
    asterorm::result<std::vector<T>> find_by(sql::predicate_ast pred,
                                             std::optional<std::size_t> limit_val = std::nullopt) {
        query_options options;
        options.limit = limit_val;
        return find_by<T>(std::move(pred), std::move(options));
    }

    template <typename T>
    asterorm::result<std::vector<T>> find_by(sql::predicate_ast pred, query_options options) {
        using traits = entity_traits<T>;

        std::vector<std::string> col_names;
        auto collect = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (col_names.push_back(std::string(std::get<Is>(traits::columns).name)), ...);
        };
        collect(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        auto builder = sql::select(col_names).from(traits::table).where(std::move(pred));
        for (const auto& order : options.order_by) {
            builder.order_by(order.column, order.ascending);
        }
        if (options.limit)
            builder.limit(*options.limit);
        if (options.offset)
            builder.offset(*options.offset);
        auto ast = builder.build();

        sql::compiler c;
        auto compiled = c.compile(ast);

        auto res = session_->with_connection([&](auto& conn) {
            return session_->observed_execute(conn, compiled.sql, compiled.params);
        });
        if (!res)
            return std::unexpected(res.error());

        std::vector<T> out;
        out.reserve(static_cast<std::size_t>(res->rows()));
        for (int r = 0; r < res->rows(); ++r) {
            T entity;
            asterorm::result<void> decode_err;
            auto populate = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (..., [&]() {
                    if (!decode_err)
                        return;
                    auto col = std::get<Is>(traits::columns);
                    auto val_str = res->get_string(r, static_cast<int>(Is));
                    auto err = asterorm::decode(val_str, entity.*(col.member_ptr));
                    if (!err)
                        decode_err = std::move(err);
                }());
            };
            populate(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
            if (!decode_err)
                return std::unexpected(decode_err.error());
            out.push_back(std::move(entity));
        }
        return out;
    }

    template <typename T> asterorm::result<std::vector<T>> find_all(query_options options = {}) {
        using traits = entity_traits<T>;

        std::vector<std::string> col_names;
        auto collect = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (col_names.push_back(std::string(std::get<Is>(traits::columns).name)), ...);
        };
        collect(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        auto builder = sql::select(col_names).from(traits::table);
        for (const auto& order : options.order_by) {
            builder.order_by(order.column, order.ascending);
        }
        if (options.limit)
            builder.limit(*options.limit);
        if (options.offset)
            builder.offset(*options.offset);

        sql::compiler c;
        auto compiled = c.compile(builder.build());
        return session_->template native<T>(compiled.sql);
    }

    template <typename T> asterorm::result<std::optional<T>> find_one_by(sql::predicate_ast pred) {
        auto res = find_by<T>(std::move(pred), 1);
        if (!res)
            return std::unexpected(res.error());
        if (res->empty())
            return std::optional<T>{};
        return std::optional<T>{std::move((*res)[0])};
    }

    template <typename T> asterorm::result<void> update(T& entity) {
        if constexpr (detail::has_version_column<T>::value) {
            return update_versioned(entity);
        } else {
            return update_returning(entity);
        }
    }

    template <typename T> asterorm::result<void> update(const T& entity) {
        return update_const(entity);
    }

    template <typename T, typename PkType> asterorm::result<void> erase(const PkType& pk_val) {
        using traits = entity_traits<T>;
        std::string sql = "DELETE FROM ";
        sql += traits::table;
        sql += " WHERE ";

        std::vector<std::optional<std::string>> params;
        std::size_t param_idx = 1;
        detail::append_key_predicate<T>(sql, params, pk_val, param_idx);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->affected_rows() == 0)
            return std::unexpected(detail::no_rows_affected_error());
        return {};
    }

  private:
    template <typename T> asterorm::result<void> update_const(const T& entity) {
        using traits = entity_traits<T>;
        std::string sql = "UPDATE ";
        sql += traits::table;
        sql += " SET ";

        std::vector<std::optional<std::string>> params;
        bool has_assignment = false;

        auto collect_update = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            size_t param_idx = 1;
            bool first = true;
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (!detail::is_primary_key_column<T>(col.name) && col.gen != generated::always) {
                    if (!first)
                        sql += ", ";
                    sql += col.name;
                    sql += " = $" + std::to_string(param_idx++);
                    params.push_back(asterorm::encode(entity.*(col.member_ptr)));
                    first = false;
                    has_assignment = true;
                }
            }());
            sql += " WHERE ";
            detail::append_entity_pk_predicate(sql, params, entity, param_idx);
        };
        collect_update(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        if (!has_assignment) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "update() requires at least one writable column";
            return std::unexpected(err);
        }

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->affected_rows() == 0)
            return std::unexpected(detail::no_rows_affected_error());
        return {};
    }

    template <typename T> asterorm::result<void> update_returning(T& entity) {
        using traits = entity_traits<T>;
        std::string sql = "UPDATE ";
        sql += traits::table;
        sql += " SET ";

        std::vector<std::optional<std::string>> params;
        bool has_assignment = false;
        auto collect_update = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            size_t param_idx = 1;
            bool first = true;
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (!detail::is_primary_key_column<T>(col.name) && col.gen != generated::always) {
                    if (!first)
                        sql += ", ";
                    sql += col.name;
                    sql += " = $" + std::to_string(param_idx++);
                    params.push_back(asterorm::encode(entity.*(col.member_ptr)));
                    first = false;
                    has_assignment = true;
                }
            }());
            sql += " WHERE ";
            detail::append_entity_pk_predicate(sql, params, entity, param_idx);
        };
        collect_update(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        if (!has_assignment) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "update() requires at least one writable column";
            return std::unexpected(err);
        }

        detail::append_returning_all<T>(sql);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() == 0)
            return std::unexpected(detail::no_rows_affected_error());

        auto decoded = detail::hydrate_entity(*res, 0, entity);
        if (!decoded)
            return std::unexpected(decoded.error());
        return {};
    }

    template <typename T> asterorm::result<void> update_versioned(T& entity) {
        using traits = entity_traits<T>;
        constexpr auto version_col = traits::version_column;

        std::string sql = "UPDATE ";
        sql += traits::table;
        sql += " SET ";

        std::vector<std::optional<std::string>> params;
        bool first = true;
        std::size_t param_idx = 1;
        auto collect_update = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (detail::is_primary_key_column<T>(col.name) || col.name == version_col.name ||
                    col.gen == generated::always) {
                    return;
                }
                if (!first)
                    sql += ", ";
                sql += col.name;
                sql += " = $" + std::to_string(param_idx++);
                params.push_back(asterorm::encode(entity.*(col.member_ptr)));
                first = false;
            }());
        };
        collect_update(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        if (!first)
            sql += ", ";
        sql += version_col.name;
        sql += " = ";
        sql += version_col.name;
        sql += " + 1";

        sql += " WHERE ";
        detail::append_entity_pk_predicate(sql, params, entity, param_idx);
        sql += " AND ";
        sql += version_col.name;
        sql += " = $" + std::to_string(param_idx);
        params.push_back(asterorm::encode(entity.*(version_col.member_ptr)));

        detail::append_returning_all<T>(sql);

        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() == 0)
            return std::unexpected(detail::stale_write_error());

        auto decoded = detail::hydrate_entity(*res, 0, entity);
        if (!decoded)
            return std::unexpected(decoded.error());
        return {};
    }

    Session* session_;
};

} // namespace asterorm
