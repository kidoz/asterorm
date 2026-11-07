#pragma once
#include <algorithm>
#include <atomic>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/core/hydration.hpp"
#include "asterorm/core/row_count.hpp"
#include "asterorm/core/row_stream.hpp"
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

using projection_row = std::vector<std::optional<std::string>>;

namespace detail {
template <typename T> struct refresh_key_value {
    using type = T;
};
template <typename T> struct refresh_key_value<std::optional<T>> {
    using type = T;
};
template <typename Connection> constexpr bool supports_returning_for() {
    if constexpr (requires { Connection::supports_returning; })
        return Connection::supports_returning;
    return true;
}
// Rolls back the refresh scope on expected errors and on C++ exceptions.
template <typename Connection> class write_refresh_guard {
  public:
    write_refresh_guard(Connection& connection, bool nested, std::string name) noexcept
        : connection_(&connection), nested_(nested), name_(std::move(name)) {}
    write_refresh_guard(const write_refresh_guard&) = delete;
    write_refresh_guard& operator=(const write_refresh_guard&) = delete;
    ~write_refresh_guard() {
        rollback();
    }

    asterorm::result<void> commit() {
        auto committed = connection_->execute(nested_ ? "RELEASE SAVEPOINT " + name_ : "COMMIT");
        if (!committed) {
            auto failure = committed.error();
            connection_->close();
            active_ = false;
            return std::unexpected(std::move(failure));
        }
        active_ = false;
        return {};
    }

  private:
    void rollback() noexcept {
        if (!active_)
            return;
        active_ = false;
        try {
            auto rolled_back =
                connection_->execute(nested_ ? "ROLLBACK TO SAVEPOINT " + name_ : "ROLLBACK");
            if (!rolled_back) {
                connection_->close();
            } else if (nested_) {
                auto released = connection_->execute("RELEASE SAVEPOINT " + name_);
                if (!released)
                    connection_->close();
            }
        } catch (...) {
            connection_->close();
        }
    }

    Connection* connection_;
    bool nested_;
    std::string name_;
    bool active_{true};
};

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
void collect_insert_params(const T& entity, const Tuple& columns, query_parameters& params,
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
                params.push_back(asterorm::encode_parameter(val));
            }
        }
    }());
}

struct bulk_insert_layout {
    std::vector<std::string> col_names;
    std::vector<bool> include_mask;
};

template <typename T, std::size_t... Is>
asterorm::result<bulk_insert_layout>
collect_bulk_insert_layout(const std::vector<T>& rows, std::index_sequence<Is...> /*seq*/) {
    using traits = entity_traits<T>;
    bulk_insert_layout layout;
    bool consistent_rows = true;
    (..., [&]() {
        auto col = std::get<Is>(traits::columns);
        bool include_column = col.gen != generated::always;
        if constexpr (is_optional_v<std::decay_t<decltype(rows.front().*(col.member_ptr))>>) {
            if (col.gen == generated::by_default) {
                include_column = (rows.front().*(col.member_ptr)).has_value();
                for (const auto& row : rows) {
                    if ((row.*(col.member_ptr)).has_value() != include_column)
                        consistent_rows = false;
                }
            }
        }
        layout.include_mask.push_back(include_column);
        if (include_column)
            layout.col_names.push_back(std::string(col.name));
    }());
    if (!consistent_rows) {
        db_error error;
        error.kind = db_error_kind::parse_failed;
        error.message = "Bulk rows must agree on which generated-by-default values are set";
        return std::unexpected(error);
    }
    return layout;
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

template <typename T> bool mapped_column_exists(std::string_view column_name) {
    bool column_was_found = false;
    using traits = entity_traits<T>;
    auto inspect_columns =
        [&]<std::size_t... ColumnIndexes>(std::index_sequence<ColumnIndexes...>) {
            (..., [&]() {
                const auto column_metadata = std::get<ColumnIndexes>(traits::columns);
                if (std::string_view(column_metadata.name) == column_name) {
                    column_was_found = true;
                }
            }());
        };
    inspect_columns(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
    return column_was_found;
}

inline db_error bulk_mutation_error(std::string message,
                                    db_error_kind kind = db_error_kind::parse_failed) {
    db_error error;
    error.kind = kind;
    error.message = std::move(message);
    return error;
}

template <typename Session> constexpr bool supports_bulk_predicate_dml() {
    if constexpr (requires { typename Session::connection_type; }) {
        using connection = typename Session::connection_type;
        if constexpr (requires {
                          connection::supports_bulk_predicate_dml;
                          connection::sql_dialect;
                      }) {
            return connection::supports_bulk_predicate_dml &&
                   (connection::sql_dialect == sql::dialect::postgres ||
                    connection::sql_dialect == sql::dialect::sqlite ||
                    connection::sql_dialect == sql::dialect::mysql);
        }
    }
    return false;
}

template <typename T> result<void> validate_bulk_column(std::string_view name) {
    if (name.empty() || name.find_first_of(".*") != std::string_view::npos ||
        name.find('\0') != std::string_view::npos || !mapped_column_exists<T>(name)) {
        return std::unexpected(
            bulk_mutation_error("Invalid bulk mutation column: " + std::string{name}));
    }
    return {};
}

template <typename T> result<void> validate_bulk_operand(const sql::expr_ast& expression) {
    if (const auto* column = std::get_if<sql::column_expr>(&expression)) {
        return validate_bulk_column<T>(column->name);
    }
    if (const auto* parameter = std::get_if<sql::param_expr>(&expression)) {
        if (parameter->encoded_value.error) {
            return std::unexpected(*parameter->encoded_value.error);
        }
        return {};
    }
    return std::unexpected(
        bulk_mutation_error("Bulk mutation predicates do not support functions"));
}

template <typename T> result<void> validate_bulk_predicate(const sql::predicate_ast& predicate) {
    if (const auto* comparison = std::get_if<sql::comparison_predicate>(&predicate.val)) {
        switch (comparison->op) {
        case sql::op_kind::eq:
        case sql::op_kind::neq:
        case sql::op_kind::lt:
        case sql::op_kind::gt:
        case sql::op_kind::le:
        case sql::op_kind::ge:
            break;
        default:
            return std::unexpected(bulk_mutation_error("Invalid bulk comparison operator"));
        }
        auto left = validate_bulk_operand<T>(comparison->left);
        if (!left) {
            return left;
        }
        return validate_bulk_operand<T>(comparison->right);
    }
    if (const auto* unary = std::get_if<sql::unary_predicate>(&predicate.val)) {
        if (unary->op != sql::unary_op_kind::is_null &&
            unary->op != sql::unary_op_kind::is_not_null) {
            return std::unexpected(bulk_mutation_error("Invalid bulk NULL operator"));
        }
        return validate_bulk_operand<T>(unary->operand);
    }
    const auto& binary = std::get<sql::binary_predicate>(predicate.val);
    if (!binary.left || !binary.right ||
        (binary.op != sql::op_kind::and_ && binary.op != sql::op_kind::or_)) {
        return std::unexpected(bulk_mutation_error("Invalid bulk logical predicate"));
    }
    auto left = validate_bulk_predicate<T>(*binary.left);
    if (!left) {
        return left;
    }
    return validate_bulk_predicate<T>(*binary.right);
}

template <typename Session> constexpr bool supports_repository_stream() {
    if constexpr (requires { typename Session::connection_type; }) {
        using connection = typename Session::connection_type;
        if constexpr (requires(connection& native, std::string_view statement,
                               const query_parameters& parameters) {
                          connection::supports_single_row_stream;
                          connection::sql_dialect;
                          { native.single_row_ready() } noexcept -> std::same_as<bool>;
                          native.open_single_row(statement, parameters);
                      }) {
            return connection::supports_single_row_stream &&
                   connection::sql_dialect == sql::dialect::postgres &&
                   has_stream_cleanup<connection>;
        }
    }
    return false;
}

template <typename Session> constexpr bool supports_repository_cursor() {
    if constexpr (requires { typename Session::connection_type; }) {
        using connection = typename Session::connection_type;
        if constexpr (requires(connection& native, std::string_view statement,
                               const query_parameters& parameters) {
                          connection::supports_cursor_stream;
                          connection::sql_dialect;
                          { native.cursor_ready() } noexcept -> std::same_as<bool>;
                          native.open_cursor(statement, parameters, std::size_t{});
                      }) {
            return connection::supports_cursor_stream &&
                   connection::sql_dialect == sql::dialect::postgres &&
                   has_stream_cleanup<connection>;
        }
    }
    return false;
}

template <typename T>
result<void> validate_stream_predicate(const sql::predicate_ast& predicate, std::size_t& nodes,
                                       std::size_t depth = 0) {
    if (depth > 64 || ++nodes > 1024) {
        return std::unexpected(
            bulk_mutation_error("Stream predicate exceeds its depth/node limit"));
    }
    if (const auto* binary = std::get_if<sql::binary_predicate>(&predicate.val)) {
        if (!binary->left || !binary->right ||
            (binary->op != sql::op_kind::and_ && binary->op != sql::op_kind::or_)) {
            return std::unexpected(bulk_mutation_error("Invalid stream logical predicate"));
        }
        auto left = validate_stream_predicate<T>(*binary->left, nodes, depth + 1);
        if (!left) {
            return left;
        }
        return validate_stream_predicate<T>(*binary->right, nodes, depth + 1);
    }
    return validate_bulk_predicate<T>(predicate); // checked mapped comparison/NULL leaves
}

template <typename T> result<void> validate_stream_source(const sql::select_ast& source) {
    constexpr auto width = std::tuple_size_v<decltype(entity_traits<T>::columns)>;
    if (width == 0 || source.table != entity_traits<T>::table || source.table_alias ||
        source.columns.size() != width || !source.items.empty() || !source.joins.empty() ||
        !source.group_bys.empty() || source.having_clause ||
        source.locking.strength != sql::select_lock_strength::none ||
        source.locking.wait != sql::select_lock_wait::block) {
        return std::unexpected(
            bulk_mutation_error("Streaming requires a full mapped single-table SELECT"));
    }
    std::size_t index = 0;
    result<void> valid;
    std::apply(
        [&](const auto&... column) {
            (
                [&] {
                    if (!valid) {
                        return;
                    }
                    if (source.columns[index] != column.name ||
                        std::find(source.columns.begin(),
                                  source.columns.begin() + static_cast<std::ptrdiff_t>(index),
                                  source.columns[index]) !=
                            source.columns.begin() + static_cast<std::ptrdiff_t>(index)) {
                        valid = std::unexpected(
                            bulk_mutation_error("Stream projection must match mapping order"));
                        return;
                    }
                    valid = validate_bulk_column<T>(source.columns[index++]);
                }(),
                ...);
        },
        entity_traits<T>::columns);
    if (!valid) {
        return valid;
    }
    for (const auto& order : source.order_bys) {
        valid = validate_bulk_column<T>(order.column);
        if (!valid) {
            return valid;
        }
    }
    if (source.where_clause) {
        std::size_t nodes = 0;
        return validate_stream_predicate<T>(*source.where_clause, nodes);
    }
    return {};
}

template <typename T>
result<void> validate_bulk_arithmetic(const sql::arithmetic_operand& operand,
                                      std::size_t& visited_nodes, std::size_t depth = 0) {
    if (depth > sql::max_arithmetic_depth || ++visited_nodes > sql::max_arithmetic_nodes) {
        return std::unexpected(
            bulk_mutation_error("Assignment arithmetic exceeds the depth/node limit"));
    }
    if (const auto* column = std::get_if<sql::column_expr>(&operand)) {
        return validate_bulk_column<T>(column->name);
    }
    if (const auto* parameter = std::get_if<sql::param_expr>(&operand)) {
        if (parameter->encoded_value.error) {
            return std::unexpected(*parameter->encoded_value.error);
        }
        return {};
    }
    const auto& expression = std::get<sql::arithmetic_ptr>(operand);
    if (!expression || (expression->op != sql::arithmetic_op::add &&
                        expression->op != sql::arithmetic_op::multiply)) {
        return std::unexpected(bulk_mutation_error("Invalid assignment arithmetic node/operator"));
    }
    auto left = validate_bulk_arithmetic<T>(expression->left, visited_nodes, depth + 1);
    if (!left) {
        return left;
    }
    return validate_bulk_arithmetic<T>(expression->right, visited_nodes, depth + 1);
}

template <typename T>
result<void> validate_bulk_assignments(
    const std::vector<std::pair<std::string, sql::value_source>>& assignments) {
    if (assignments.empty()) {
        return std::unexpected(
            bulk_mutation_error("update_where requires at least one assignment"));
    }
    std::vector<std::string_view> seen;
    for (const auto& [name, value] : assignments) {
        auto valid = validate_bulk_column<T>(name);
        if (!valid) {
            return valid;
        }
        if (std::find(seen.begin(), seen.end(), name) != seen.end()) {
            return std::unexpected(bulk_mutation_error("Duplicate bulk assignment: " + name));
        }
        seen.push_back(name);
        bool writable = true;
        if constexpr (requires { entity_traits<T>::primary_key; }) {
            writable = !is_primary_key_column<T>(name);
        }
        std::apply(
            [&](const auto&... column) {
                ((writable = writable && !(column.name == name && column.gen == generated::always)),
                 ...);
            },
            entity_traits<T>::columns);
        if (!writable) {
            return std::unexpected(bulk_mutation_error("Bulk assignment is not writable: " + name));
        }
        if (const auto* parameter = std::get_if<sql::param_expr>(&value);
            parameter && parameter->encoded_value.error) {
            return std::unexpected(*parameter->encoded_value.error);
        }
        if (const auto* arithmetic = std::get_if<sql::arithmetic_ptr>(&value)) {
            std::size_t visited_nodes = 0;
            auto arithmetic_valid =
                validate_bulk_arithmetic<T>(sql::arithmetic_operand{*arithmetic}, visited_nodes);
            if (!arithmetic_valid) {
                return arithmetic_valid;
            }
        }
    }
    return {};
}

template <typename T, auto MemberPointer> asterorm::result<std::string> column_name_for_member() {
    std::optional<std::string> resolved_column_name;
    using traits = entity_traits<T>;
    auto inspect_columns =
        [&]<std::size_t... ColumnIndexes>(std::index_sequence<ColumnIndexes...>) {
            (..., [&]() {
                const auto column_metadata = std::get<ColumnIndexes>(traits::columns);
                if constexpr (std::is_same_v<std::decay_t<decltype(column_metadata.member_ptr)>,
                                             std::decay_t<decltype(MemberPointer)>>) {
                    if (column_metadata.member_ptr == MemberPointer) {
                        resolved_column_name = std::string(column_metadata.name);
                    }
                }
            }());
        };
    inspect_columns(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
    if (!resolved_column_name) {
        db_error error;
        error.kind = db_error_kind::parse_failed;
        error.message = "No mapped column for relationship member";
        return std::unexpected(error);
    }
    return *resolved_column_name;
}

inline db_error projection_error(std::string message) {
    db_error error;
    error.kind = db_error_kind::parse_failed;
    error.message = std::move(message);
    return error;
}

template <typename T>
asterorm::result<void>
validate_projection_columns(const std::vector<std::string>& selected_columns) {
    if (selected_columns.empty()) {
        return std::unexpected(projection_error("projection requires at least one column"));
    }
    for (const auto& selected_column : selected_columns) {
        if (!mapped_column_exists<T>(selected_column)) {
            return std::unexpected(
                projection_error("Unknown column in projection: " + selected_column));
        }
    }
    return {};
}

template <typename T> asterorm::result<void> validate_order_columns(const query_options& options) {
    for (const auto& requested_order : options.order_by) {
        if (!mapped_column_exists<T>(requested_order.column)) {
            return std::unexpected(
                projection_error("Unknown column in order_by: " + requested_order.column));
        }
    }
    return {};
}

template <typename Projection>
asterorm::result<void> validate_projection_shape(std::size_t selected_column_count) {
    if constexpr (is_tuple_like_v<Projection>) {
        constexpr std::size_t projection_field_count = std::tuple_size_v<std::decay_t<Projection>>;
        if (selected_column_count != projection_field_count) {
            return std::unexpected(
                projection_error("tuple projection field count must match selected column count"));
        }
    } else {
        if (selected_column_count != 1) {
            return std::unexpected(
                projection_error("scalar projection requires exactly one selected column"));
        }
    }
    return {};
}

template <typename Value, typename Result>
asterorm::result<Value> decode_scalar_projection(const Result& query_result, int row_index) {
    Value decoded_value{};
    auto decode_result = asterorm::decode(query_result.get_string(row_index, 0), decoded_value);
    if (!decode_result) {
        return std::unexpected(decode_result.error());
    }
    return decoded_value;
}

template <typename Tuple, typename Result, std::size_t... FieldIndexes>
asterorm::result<Tuple> decode_tuple_projection(const Result& query_result, int row_index,
                                                std::index_sequence<FieldIndexes...>) {
    Tuple decoded_tuple{};
    asterorm::result<void> decode_result;
    (..., [&]() {
        if (!decode_result) {
            return;
        }
        auto field_decode_result =
            asterorm::decode(query_result.get_string(row_index, static_cast<int>(FieldIndexes)),
                             std::get<FieldIndexes>(decoded_tuple));
        if (!field_decode_result) {
            decode_result = std::move(field_decode_result);
        }
    }());
    if (!decode_result) {
        return std::unexpected(decode_result.error());
    }
    return decoded_tuple;
}

template <typename Projection, typename Result>
asterorm::result<Projection> decode_projection(const Result& query_result, int row_index) {
    if constexpr (is_tuple_like_v<Projection>) {
        return decode_tuple_projection<Projection>(
            query_result, row_index,
            std::make_index_sequence<std::tuple_size_v<std::decay_t<Projection>>>{});
    } else {
        return decode_scalar_projection<Projection>(query_result, row_index);
    }
}

// Accumulates one statement and quotes every identifier through sql::compiler.
// After the first quoting failure, later appends are ignored. Call finish()
// before executing the text.
class quoted_sql {
  public:
    explicit quoted_sql(sql::dialect dialect) : dialect_(dialect) {}

    quoted_sql& raw(std::string_view text) {
        if (!error_)
            sql_ += text;
        return *this;
    }

    quoted_sql& ident(std::string_view name) {
        append(name, false);
        return *this;
    }

    quoted_sql& qualified(std::string_view name) {
        append(name, true);
        return *this;
    }

    [[nodiscard]] asterorm::result<std::string> finish() {
        if (error_)
            return std::unexpected(*error_);
        return std::move(sql_);
    }

  private:
    void append(std::string_view name, bool qualified) {
        if (error_)
            return;
        sql::compiler compiler{dialect_};
        auto quoted = qualified ? compiler.quote_qualified_ident(name) : compiler.quote_ident(name);
        if (!quoted)
            error_ = quoted.error();
        else
            sql_ += *quoted;
    }

    sql::dialect dialect_;
    std::string sql_;
    std::optional<db_error> error_;
};

inline void append_csv(quoted_sql& sql, const std::vector<std::string>& names) {
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i != 0)
            sql.raw(", ");
        sql.ident(names[i]);
    }
}

template <typename T> void append_primary_key_csv(quoted_sql& sql) {
    append_csv(sql, primary_key_names<T>());
}

template <typename T>
void append_entity_pk_predicate(quoted_sql& sql, query_parameters& params, const T& entity,
                                std::size_t& param_idx) {
    bool first = true;
    for_each_pk(entity_traits<T>::primary_key, [&](const auto& pk) {
        if (!first)
            sql.raw(" AND ");
        first = false;
        sql.ident(pk.name).raw(" = $" + std::to_string(param_idx++));
        params.push_back(asterorm::encode_parameter(entity.*(pk.member_ptr)));
    });
}

template <typename T, typename Key, std::size_t... Is>
void append_tuple_key_predicate(quoted_sql& sql, query_parameters& params, const Key& key,
                                std::size_t& param_idx, std::index_sequence<Is...>) {
    constexpr auto primary_key_metadata = entity_traits<T>::primary_key;
    bool first = true;
    (..., [&]() {
        if (!first)
            sql.raw(" AND ");
        first = false;
        const auto& primary_key_part = std::get<Is>(primary_key_metadata);
        sql.ident(primary_key_part.name).raw(" = $" + std::to_string(param_idx++));
        params.push_back(asterorm::encode_parameter(std::get<Is>(key)));
    }());
}

template <typename T, typename Key>
void append_key_predicate(quoted_sql& sql, query_parameters& params, const Key& key,
                          std::size_t& param_idx) {
    constexpr auto primary_key_metadata = entity_traits<T>::primary_key;
    if constexpr (is_tuple_like_v<decltype(primary_key_metadata)>) {
        static_assert(is_tuple_like_v<Key>,
                      "Composite primary keys require a tuple-like key value");
        static_assert(std::tuple_size_v<std::decay_t<Key>> ==
                          std::tuple_size_v<std::decay_t<decltype(primary_key_metadata)>>,
                      "Composite key tuple size must match entity_traits<T>::primary_key");
        append_tuple_key_predicate<T>(
            sql, params, key, param_idx,
            std::make_index_sequence<
                std::tuple_size_v<std::decay_t<decltype(primary_key_metadata)>>>{});
    } else {
        sql.ident(primary_key_metadata.name).raw(" = $" + std::to_string(param_idx++));
        params.push_back(asterorm::encode_parameter(key));
    }
}

template <typename T> void append_returning_all(quoted_sql& sql) {
    sql.raw(" RETURNING ");
    append_csv(sql, column_names<T>());
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

template <typename T>
asterorm::result<sql::compiled_query>
build_versioned_update(const T& entity, bool include_returning, sql::dialect dialect) {
    using traits = entity_traits<T>;
    constexpr auto version_col = traits::version_column;
    sql::compiled_query compiled;
    quoted_sql statement{dialect};
    statement.raw("UPDATE ").qualified(traits::table).raw(" SET ");

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
                statement.raw(", ");
            statement.ident(col.name).raw(" = $" + std::to_string(param_idx++));
            compiled.params.push_back(asterorm::encode_parameter(entity.*(col.member_ptr)));
            first = false;
        }());
    };
    collect_update(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

    if (!first)
        statement.raw(", ");
    statement.ident(version_col.name).raw(" = ").ident(version_col.name).raw(" + 1 WHERE ");
    detail::append_entity_pk_predicate(statement, compiled.params, entity, param_idx);
    statement.raw(" AND ").ident(version_col.name).raw(" = $" + std::to_string(param_idx));
    compiled.params.push_back(asterorm::encode_parameter(entity.*(version_col.member_ptr)));
    if (include_returning)
        detail::append_returning_all<T>(statement);
    auto sql = statement.finish();
    if (!sql)
        return std::unexpected(sql.error());
    compiled.sql = std::move(*sql);
    return compiled;
}
} // namespace detail

template <typename Session> class repository {
  public:
    explicit repository(Session& sess) : session_(&sess) {}

    template <typename T>
    asterorm::result<row_stream<T>> stream(sql::select_ast source, stream_options options = {}) {
        if constexpr (!detail::supports_repository_stream<Session>() &&
                      !detail::supports_repository_cursor<Session>()) {
            return std::unexpected(detail::stream_ownership_error(
                "This session does not support repository streaming"));
        } else {
            if (options.chunk_size == 0 || options.chunk_size > 65536) {
                return std::unexpected(
                    detail::bulk_mutation_error("Stream chunk size must be between 1 and 65536"));
            }
            if (options.mode != stream_mode::single_row && options.mode != stream_mode::cursor) {
                return std::unexpected(detail::bulk_mutation_error("Invalid stream mode"));
            }
            if (options.mode == stream_mode::cursor &&
                !detail::supports_repository_cursor<Session>()) {
                return std::unexpected(detail::stream_ownership_error(
                    "This session does not support repository cursor streaming"));
            }
            if (options.mode == stream_mode::single_row &&
                !detail::supports_repository_stream<Session>()) {
                return std::unexpected(detail::stream_ownership_error(
                    "This session does not support repository single-row streaming"));
            }
            auto valid = detail::validate_stream_source<T>(source);
            if (!valid) {
                return std::unexpected(std::move(valid.error()));
            }
            sql::compiler compiler{sql::dialect_for_session<Session>()};
            auto compiled = compiler.compile(source);
            if (!compiled) {
                return std::unexpected(std::move(compiled.error()));
            }
            if (compiled->params.size() >
                static_cast<std::size_t>(std::numeric_limits<int>::max())) {
                return std::unexpected(
                    detail::stream_ownership_error("Too many stream parameters"));
            }
            for (const auto& parameter : compiled->params) {
                if (parameter.error) {
                    return std::unexpected(*parameter.error);
                }
                if (parameter.value && parameter.value->contains('\0')) {
                    return std::unexpected(detail::stream_ownership_error(
                        "Stream text parameter contains a NUL byte"));
                }
            }
            if constexpr (detail::supports_repository_cursor<Session>()) {
                if (options.mode == stream_mode::cursor) {
                    return detail::row_stream_access::open<T, true>(
                        *session_, std::move(compiled->sql), std::move(compiled->params),
                        options.chunk_size);
                }
            }
            if constexpr (detail::supports_repository_stream<Session>()) {
                return detail::row_stream_access::open<T>(*session_, std::move(compiled->sql),
                                                          std::move(compiled->params));
            } else {
                return std::unexpected(detail::stream_ownership_error(
                    "This session does not support repository single-row streaming"));
            }
        }
    }

    // Batch insert. All rows go into a single INSERT statement; skipped
    // generated columns are handled per-row (rows must agree on which
    // generated-by-default columns are set, since the column list is shared).
    // PKs on inserted entities are NOT refreshed from RETURNING — use the
    // single-row insert() when you need that.
    template <typename T> asterorm::result<std::size_t> insert_many(const std::vector<T>& rows) {
        using traits = entity_traits<T>;
        if (rows.empty())
            return std::size_t{0};

        auto layout = detail::collect_bulk_insert_layout<T>(
            rows, std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
        if (!layout)
            return std::unexpected(layout.error());
        auto col_names = std::move(layout->col_names);
        auto include_mask = std::move(layout->include_mask);

        query_parameters params;
        params.reserve(rows.size() * col_names.size());

        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("INSERT INTO ").qualified(traits::table).raw(" (");
        detail::append_csv(statement, col_names);
        statement.raw(") VALUES ");

        std::size_t param_idx = 1;
        for (std::size_t r = 0; r < rows.size(); ++r) {
            statement.raw("(");
            bool first_in_row = true;
            auto bind = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (..., [&]() {
                    auto col = std::get<Is>(traits::columns);
                    if (!include_mask[Is])
                        return;
                    if (!first_in_row)
                        statement.raw(", ");
                    first_in_row = false;
                    const auto& val = rows[r].*(col.member_ptr);
                    statement.raw("$" + std::to_string(param_idx++));
                    params.push_back(asterorm::encode_parameter(val));
                }());
            };
            bind(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
            statement.raw(")");
            if (r + 1 < rows.size())
                statement.raw(", ");
        }

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, *sql, params); });
        if (!res)
            return std::unexpected(res.error());
        return detail::checked_row_count<std::size_t>(res->affected_rows());
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

        query_parameters params;
        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("UPDATE ").qualified(traits::table).raw(" SET ");
        std::size_t idx = 1;
        bool first = true;
        std::vector<bool> matched(changed.size(), false);

        auto apply = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                for (std::size_t i = 0; i < changed.size(); ++i) {
                    if (std::string_view(col.name) == changed[i]) {
                        matched[i] = true;
                        if constexpr (detail::has_version_column<T>::value) {
                            if (col.name == traits::version_column.name)
                                continue;
                        }
                        if (!first)
                            statement.raw(", ");
                        first = false;
                        statement.ident(col.name).raw(" = $" + std::to_string(idx++));
                        params.push_back(asterorm::encode_parameter(entity.*(col.member_ptr)));
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

        if constexpr (detail::has_version_column<T>::value) {
            if (!first)
                statement.raw(", ");
            statement.ident(traits::version_column.name)
                .raw(" = ")
                .ident(traits::version_column.name)
                .raw(" + 1");
        }

        statement.raw(" WHERE ");
        detail::append_entity_pk_predicate(statement, params, entity, idx);
        if constexpr (detail::has_version_column<T>::value) {
            statement.raw(" AND ")
                .ident(traits::version_column.name)
                .raw(" = $" + std::to_string(idx));
            params.push_back(
                asterorm::encode_parameter(entity.*(traits::version_column.member_ptr)));
        }

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, *sql, params); });
        if (!res)
            return std::unexpected(res.error());
        if (res->affected_rows() == 0) {
            if constexpr (detail::has_version_column<T>::value)
                return std::unexpected(detail::stale_write_error());
            return std::unexpected(detail::no_rows_affected_error());
        }
        return {};
    }

    template <typename T> asterorm::result<void> insert(T& entity) {
        using traits = entity_traits<T>;
        query_parameters params;
        std::vector<std::string> col_names;
        std::vector<std::string> placeholders;

        detail::collect_insert_params(
            entity, traits::columns, params, col_names, placeholders,
            std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("INSERT INTO ").qualified(traits::table).raw(" (");
        detail::append_csv(statement, col_names);
        statement.raw(") VALUES (");
        for (size_t i = 0; i < placeholders.size(); ++i) {
            if (i != 0)
                statement.raw(", ");
            statement.raw(placeholders[i]);
        }
        statement.raw(")");
        if constexpr (supports_returning)
            detail::append_returning_all<T>(statement);

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = execute_returning(entity, *sql, params, true);

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
        query_parameters params;
        std::vector<std::string> col_names;
        std::vector<std::string> placeholders;

        detail::collect_insert_params(
            entity, traits::columns, params, col_names, placeholders,
            std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("INSERT INTO ").qualified(traits::table).raw(" (");
        detail::append_csv(statement, col_names);
        statement.raw(") VALUES (");
        for (size_t i = 0; i < placeholders.size(); ++i) {
            if (i != 0)
                statement.raw(", ");
            statement.raw(placeholders[i]);
        }
        statement.raw(")");
        bool has_update_column = false;
        for (const auto& column : col_names)
            has_update_column = has_update_column || !detail::is_primary_key_column<T>(column);
        if (!has_update_column)
            return std::unexpected(
                detail::projection_error("upsert() requires at least one updatable column"));
        auto clause = upsert_clause<T>(col_names);
        if (!clause)
            return std::unexpected(clause.error());
        statement.raw(*clause);

        if constexpr (supports_returning)
            detail::append_returning_all<T>(statement);

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = execute_returning(entity, *sql, params, false);

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
        if constexpr (!supports_returning) {
            if (!rows.empty())
                return std::unexpected(detail::projection_error(
                    "Bulk upsert with entity refresh requires RETURNING; use insert_many or "
                    "individual upsert calls in a transaction"));
        }
        if (rows.empty())
            return std::size_t{0};

        auto layout = detail::collect_bulk_insert_layout<T>(
            rows, std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
        if (!layout)
            return std::unexpected(layout.error());
        auto col_names = std::move(layout->col_names);
        auto include_mask = std::move(layout->include_mask);

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

        query_parameters params;
        params.reserve(rows.size() * col_names.size());

        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("INSERT INTO ").qualified(traits::table).raw(" (");
        detail::append_csv(statement, col_names);
        statement.raw(") VALUES ");

        std::size_t param_idx = 1;
        for (std::size_t r = 0; r < rows.size(); ++r) {
            statement.raw("(");
            bool first_in_row = true;
            auto bind = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (..., [&]() {
                    auto col = std::get<Is>(traits::columns);
                    if (!include_mask[Is])
                        return;
                    if (!first_in_row)
                        statement.raw(", ");
                    first_in_row = false;
                    const auto& val = rows[r].*(col.member_ptr);
                    statement.raw("$" + std::to_string(param_idx++));
                    params.push_back(asterorm::encode_parameter(val));
                }());
            };
            bind(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
            statement.raw(")");
            if (r + 1 < rows.size())
                statement.raw(", ");
        }

        auto clause = upsert_clause<T>(col_names);
        if (!clause)
            return std::unexpected(clause.error());
        statement.raw(*clause);
        detail::append_returning_all<T>(statement);

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, *sql, params); });
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
        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("SELECT ");

        std::vector<std::string> col_names;
        auto collect_select = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                col_names.push_back(std::string(col.name));
            }());
        };
        collect_select(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
        detail::append_csv(statement, col_names);
        statement.raw(" FROM ").qualified(traits::table).raw(" WHERE ");

        query_parameters params;
        std::size_t param_idx = 1;
        detail::append_key_predicate<T>(statement, params, pk_val, param_idx);

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, *sql, params); });

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
        auto order_validation_result = detail::validate_order_columns<T>(options);
        if (!order_validation_result) {
            return std::unexpected(order_validation_result.error());
        }

        std::vector<std::string> col_names;
        auto collect = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (col_names.push_back(std::string(std::get<Is>(traits::columns).name)), ...);
        };
        collect(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        auto builder = sql::select(col_names);
        builder.from(traits::table);
        builder.where(std::move(pred));
        for (const auto& order : options.order_by) {
            builder.order_by(order.column, order.ascending);
        }
        if (options.limit)
            builder.limit(*options.limit);
        if (options.offset)
            builder.offset(*options.offset);
        auto ast = builder.build();

        sql::compiler c{sql::dialect_for_session<Session>()};
        auto compiled = c.compile(ast);
        if (!compiled)
            return std::unexpected(compiled.error());

        auto res = session_->with_connection([&](auto& conn) {
            return session_->observed_execute(conn, compiled->sql, compiled->params);
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
        auto order_validation_result = detail::validate_order_columns<T>(options);
        if (!order_validation_result) {
            return std::unexpected(order_validation_result.error());
        }

        std::vector<std::string> col_names;
        auto collect = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (col_names.push_back(std::string(std::get<Is>(traits::columns).name)), ...);
        };
        collect(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        auto builder = sql::select(col_names);
        builder.from(traits::table);
        for (const auto& order : options.order_by) {
            builder.order_by(order.column, order.ascending);
        }
        if (options.limit)
            builder.limit(*options.limit);
        if (options.offset)
            builder.offset(*options.offset);

        sql::compiler c{sql::dialect_for_session<Session>()};
        auto compiled = c.compile(builder.build());
        if (!compiled)
            return std::unexpected(compiled.error());
        return session_->template native<T>(compiled->sql);
    }

    template <typename T> asterorm::result<std::optional<T>> find_one_by(sql::predicate_ast pred) {
        auto res = find_by<T>(std::move(pred), 1);
        if (!res)
            return std::unexpected(res.error());
        if (res->empty())
            return std::optional<T>{};
        return std::optional<T>{std::move((*res)[0])};
    }

    template <typename T, typename Projection>
    asterorm::result<std::vector<Projection>>
    select_projection(std::vector<std::string> selected_columns, query_options options = {}) {
        auto compiled_query_result =
            compile_projection_query<T>(selected_columns, std::move(options));
        if (!compiled_query_result) {
            return std::unexpected(compiled_query_result.error());
        }
        return execute_projection_query<Projection>(*compiled_query_result,
                                                    selected_columns.size());
    }

    template <typename T, typename Projection>
    asterorm::result<std::vector<Projection>>
    select_projection_by(sql::predicate_ast predicate, std::vector<std::string> selected_columns,
                         query_options options = {}) {
        auto compiled_query_result = compile_projection_query_by<T>(
            std::move(predicate), selected_columns, std::move(options));
        if (!compiled_query_result) {
            return std::unexpected(compiled_query_result.error());
        }
        return execute_projection_query<Projection>(*compiled_query_result,
                                                    selected_columns.size());
    }

    template <typename T, typename Mapper>
    auto select_map(std::vector<std::string> selected_columns, Mapper&& mapper,
                    query_options options = {})
        -> asterorm::result<std::vector<std::invoke_result_t<Mapper&, const projection_row&>>> {
        auto compiled_query_result =
            compile_projection_query<T>(selected_columns, std::move(options));
        if (!compiled_query_result) {
            return std::unexpected(compiled_query_result.error());
        }
        return execute_projection_map(*compiled_query_result, selected_columns.size(),
                                      std::forward<Mapper>(mapper));
    }

    template <typename T, typename Mapper>
    auto select_map_by(sql::predicate_ast predicate, std::vector<std::string> selected_columns,
                       Mapper&& mapper, query_options options = {})
        -> asterorm::result<std::vector<std::invoke_result_t<Mapper&, const projection_row&>>> {
        auto compiled_query_result = compile_projection_query_by<T>(
            std::move(predicate), selected_columns, std::move(options));
        if (!compiled_query_result) {
            return std::unexpected(compiled_query_result.error());
        }
        return execute_projection_map(*compiled_query_result, selected_columns.size(),
                                      std::forward<Mapper>(mapper));
    }

    template <std::size_t RelationshipIndex, typename T>
    auto load_related_many(const T& entity, query_options options = {}) {
        static_assert(has_relationships_v<T>,
                      "load_related_many() requires entity_traits<T>::relationships");
        static_assert(RelationshipIndex < relationship_count_v<T>,
                      "Relationship index is out of range for entity_traits<T>::relationships");

        using relationship_type =
            std::decay_t<decltype(std::get<RelationshipIndex>(entity_traits<T>::relationships))>;
        using related_entity_type = typename relationship_type::related_entity;

        auto related_column_name =
            detail::column_name_for_member<related_entity_type,
                                           relationship_type::related_member_ptr>();
        if (!related_column_name) {
            return asterorm::result<std::vector<related_entity_type>>{
                std::unexpected(related_column_name.error())};
        }

        auto relationship_predicate = sql::col(*related_column_name) ==
                                      sql::val(entity.*(relationship_type::local_member_ptr));
        return find_by<related_entity_type>(std::move(relationship_predicate), std::move(options));
    }

    template <std::size_t RelationshipIndex, typename T> auto load_related_one(const T& entity) {
        static_assert(has_relationships_v<T>,
                      "load_related_one() requires entity_traits<T>::relationships");
        static_assert(RelationshipIndex < relationship_count_v<T>,
                      "Relationship index is out of range for entity_traits<T>::relationships");

        using relationship_type =
            std::decay_t<decltype(std::get<RelationshipIndex>(entity_traits<T>::relationships))>;
        static_assert(relationship_type::kind != relationship_kind::one_to_many,
                      "load_related_one() requires a one-to-one or many-to-one relationship");
        using related_entity_type = typename relationship_type::related_entity;

        query_options options;
        options.limit = 1;
        auto related_rows = load_related_many<RelationshipIndex>(entity, std::move(options));
        if (!related_rows) {
            return asterorm::result<std::optional<related_entity_type>>{
                std::unexpected(related_rows.error())};
        }
        if (related_rows->empty()) {
            return asterorm::result<std::optional<related_entity_type>>{
                std::optional<related_entity_type>{}};
        }
        return asterorm::result<std::optional<related_entity_type>>{
            std::optional<related_entity_type>{std::move((*related_rows)[0])}};
    }

    template <typename T>
    asterorm::result<std::size_t>
    update_where(sql::predicate_ast predicate,
                 std::vector<std::pair<std::string, sql::value_source>> assignments) {
        if constexpr (!detail::supports_bulk_predicate_dml<Session>() ||
                      detail::has_version_column<T>::value) {
            return std::unexpected(
                detail::bulk_mutation_error("Bulk predicate DML requires an explicit supported "
                                            "capability and an unversioned entity",
                                            db_error_kind::query_failed));
        } else {
            auto valid = detail::validate_bulk_assignments<T>(assignments);
            if (!valid) {
                return std::unexpected(valid.error());
            }
            valid = detail::validate_bulk_predicate<T>(predicate);
            if (!valid) {
                return std::unexpected(valid.error());
            }
            sql::update_ast mutation;
            mutation.table = entity_traits<T>::table;
            mutation.assignments = std::move(assignments);
            mutation.where_clause = std::move(predicate);
            return execute_bulk_mutation(mutation);
        }
    }

    template <typename T> asterorm::result<std::size_t> delete_where(sql::predicate_ast predicate) {
        if constexpr (!detail::supports_bulk_predicate_dml<Session>() ||
                      detail::has_version_column<T>::value) {
            return std::unexpected(
                detail::bulk_mutation_error("Bulk predicate DML requires an explicit supported "
                                            "capability and an unversioned entity",
                                            db_error_kind::query_failed));
        } else {
            auto valid = detail::validate_bulk_predicate<T>(predicate);
            if (!valid) {
                return std::unexpected(valid.error());
            }
            sql::delete_ast mutation;
            mutation.table = entity_traits<T>::table;
            mutation.where_clause = std::move(predicate);
            return execute_bulk_mutation(mutation);
        }
    }

    template <typename T> asterorm::result<std::size_t> insert_select(sql::select_ast source) {
        if constexpr (!detail::supports_bulk_predicate_dml<Session>()) {
            return std::unexpected(detail::bulk_mutation_error(
                "INSERT SELECT requires an explicit supported relational capability",
                db_error_kind::query_failed));
        } else {
            sql::insert_select_ast mutation;
            mutation.table = entity_traits<T>::table;
            std::apply(
                [&](const auto&... column) {
                    (
                        [&] {
                            if (column.gen == generated::none) {
                                mutation.columns.emplace_back(column.name);
                            }
                        }(),
                        ...);
                },
                entity_traits<T>::columns);
            mutation.source = std::move(source);
            return execute_bulk_mutation(mutation);
        }
    }

    template <typename T> asterorm::result<void> update(T& entity) {
        if constexpr (detail::has_version_column<T>::value) {
            return update_versioned(entity);
        } else {
            return update_returning(entity);
        }
    }

    template <typename T> asterorm::result<void> update(const T& entity) {
        if constexpr (detail::has_version_column<T>::value) {
            return update_versioned_const(entity);
        } else {
            return update_const(entity);
        }
    }

    template <typename T, typename PkType> asterorm::result<void> erase(const PkType& pk_val) {
        using traits = entity_traits<T>;
        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("DELETE FROM ").qualified(traits::table).raw(" WHERE ");

        query_parameters params;
        std::size_t param_idx = 1;
        detail::append_key_predicate<T>(statement, params, pk_val, param_idx);

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, *sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->affected_rows() == 0)
            return std::unexpected(detail::no_rows_affected_error());
        return {};
    }

  private:
    template <typename Mutation>
    asterorm::result<std::size_t> execute_bulk_mutation(const Mutation& mutation) {
        sql::compiler compiler{sql::dialect_for_session<Session>()};
        auto compiled = compiler.compile(mutation);
        if (!compiled) {
            return std::unexpected(compiled.error());
        }
        auto written = session_->with_connection([&](auto& connection) {
            return session_->observed_execute(connection, compiled->sql, compiled->params);
        });
        if (!written) {
            return std::unexpected(written.error());
        }
        return detail::checked_row_count<std::size_t>(written->affected_rows());
    }

    static constexpr bool supports_returning = [] {
        if constexpr (requires { typename Session::connection_type; })
            return detail::supports_returning_for<typename Session::connection_type>();
        return true;
    }();

    template <typename T>
    asterorm::result<std::string> upsert_clause(const std::vector<std::string>& columns) {
        sql::compiler compiler{sql::dialect_for_session<Session>()};
        std::optional<std::string_view> version_column;
        if constexpr (detail::has_version_column<T>::value)
            version_column = entity_traits<T>::version_column.name;
        return compiler.upsert_clause(entity_traits<T>::table, columns,
                                      detail::primary_key_names<T>(), version_column);
    }

    // Backends without RETURNING use an explicitly scoped write + primary-key
    // read. Never infer consecutive IDs for bulk writes or release the lease
    // between the mutation and refresh. A savepoint protects caller transactions.
    template <typename T>
    auto execute_returning(const T& entity, std::string_view sql, const query_parameters& params,
                           bool inserting) {
        return session_->with_connection([&](auto& connection) {
            using execution_result = decltype(session_->observed_execute(connection, sql, params));
            if constexpr (supports_returning) {
                return session_->observed_execute(connection, sql, params);
            } else {
                query_parameters key_params;
                std::optional<std::size_t> generated_key_index;
                bool invalid_key = false;
                const auto key_names = detail::primary_key_names<T>();
                detail::for_each_pk(entity_traits<T>::primary_key, [&](const auto& key) {
                    auto parameter = asterorm::encode_parameter(entity.*(key.member_ptr));
                    bool omitted = false;
                    bool numeric_generated_key = false;
                    std::apply(
                        [&](const auto&... columns) {
                            (
                                [&] {
                                    if (columns.name != key.name)
                                        return;
                                    using field_type =
                                        std::decay_t<decltype(entity.*(columns.member_ptr))>;
                                    using value_type =
                                        typename detail::refresh_key_value<field_type>::type;
                                    omitted =
                                        (inserting && columns.gen == generated::always) ||
                                        (columns.gen == generated::by_default && !parameter.value);
                                    numeric_generated_key = columns.gen != generated::none &&
                                                            std::is_integral_v<value_type> &&
                                                            !std::is_same_v<value_type, bool>;
                                }(),
                                ...);
                        },
                        entity_traits<T>::columns);
                    if (omitted || !parameter.value) {
                        if (!inserting || key_names.size() != 1 || !numeric_generated_key)
                            invalid_key = true;
                        generated_key_index = key_params.size();
                    }
                    if (parameter.error)
                        invalid_key = true;
                    key_params.push_back(std::move(parameter));
                });
                if (invalid_key)
                    return execution_result{std::unexpected(
                        detail::projection_error("Entity refresh requires explicit primary keys or "
                                                 "one generated integral key on insert"))};

                static std::atomic<std::uint64_t> next_savepoint{0};
                auto savepoint = "asterorm_refresh_" + std::to_string(++next_savepoint);
                const bool nested = connection.in_transaction();
                auto started = nested ? connection.execute("SAVEPOINT " + savepoint)
                                      : connection.execute("START TRANSACTION");
                if (!started)
                    return execution_result{std::unexpected(started.error())};
                detail::write_refresh_guard guard{connection, nested, std::move(savepoint)};
                auto execute_and_refresh = [&]() -> execution_result {
                    auto written = session_->observed_execute(connection, sql, params);
                    if (!written)
                        return std::unexpected(written.error());
                    if (written->affected_rows() == 0)
                        return written;
                    if (generated_key_index) {
                        if (written->last_insert_id() == 0)
                            return std::unexpected(
                                detail::projection_error("Insert did not produce a generated key"));
                        key_params[*generated_key_index] =
                            std::to_string(written->last_insert_id());
                    }
                    sql::select_ast refresh;
                    refresh.table = entity_traits<T>::table;
                    refresh.columns = detail::column_names<T>();
                    for (std::size_t index = 0; index < key_names.size(); ++index) {
                        auto predicate =
                            sql::col(key_names[index]) == sql::param_expr{key_params[index]};
                        if (refresh.where_clause)
                            refresh.where_clause =
                                std::move(*refresh.where_clause) && std::move(predicate);
                        else
                            refresh.where_clause = std::move(predicate);
                    }
                    sql::compiler compiler{sql::dialect_for_session<Session>()};
                    const auto query = compiler.compile(refresh);
                    if (!query)
                        return execution_result{std::unexpected(query.error())};
                    auto refreshed =
                        session_->observed_execute(connection, query->sql, query->params);
                    if (refreshed && refreshed->rows() != 1)
                        return std::unexpected(detail::projection_error(
                            "Write could not be refreshed by primary key"));
                    return refreshed;
                };
                auto refreshed = execute_and_refresh();
                if (!refreshed)
                    return refreshed;
                auto committed = guard.commit();
                if (!committed)
                    return execution_result{std::unexpected(committed.error())};
                return refreshed;
            }
        });
    }

    template <typename T>
    asterorm::result<sql::compiled_query>
    compile_projection_query(const std::vector<std::string>& selected_columns,
                             query_options options) {
        auto validation_result = detail::validate_projection_columns<T>(selected_columns);
        if (!validation_result) {
            return std::unexpected(validation_result.error());
        }
        auto order_validation_result = detail::validate_order_columns<T>(options);
        if (!order_validation_result) {
            return std::unexpected(order_validation_result.error());
        }

        using traits = entity_traits<T>;
        auto query_builder = sql::select(selected_columns);
        query_builder.from(traits::table);
        for (const auto& requested_order : options.order_by) {
            query_builder.order_by(requested_order.column, requested_order.ascending);
        }
        if (options.limit) {
            query_builder.limit(*options.limit);
        }
        if (options.offset) {
            query_builder.offset(*options.offset);
        }

        sql::compiler query_compiler{sql::dialect_for_session<Session>()};
        return query_compiler.compile(query_builder.build());
    }

    template <typename T>
    asterorm::result<sql::compiled_query>
    compile_projection_query_by(sql::predicate_ast predicate,
                                const std::vector<std::string>& selected_columns,
                                query_options options) {
        auto validation_result = detail::validate_projection_columns<T>(selected_columns);
        if (!validation_result) {
            return std::unexpected(validation_result.error());
        }
        auto order_validation_result = detail::validate_order_columns<T>(options);
        if (!order_validation_result) {
            return std::unexpected(order_validation_result.error());
        }

        using traits = entity_traits<T>;
        auto query_builder = sql::select(selected_columns);
        query_builder.from(traits::table);
        query_builder.where(std::move(predicate));
        for (const auto& requested_order : options.order_by) {
            query_builder.order_by(requested_order.column, requested_order.ascending);
        }
        if (options.limit) {
            query_builder.limit(*options.limit);
        }
        if (options.offset) {
            query_builder.offset(*options.offset);
        }

        sql::compiler query_compiler{sql::dialect_for_session<Session>()};
        return query_compiler.compile(query_builder.build());
    }

    template <typename Projection>
    asterorm::result<std::vector<Projection>>
    execute_projection_query(const sql::compiled_query& compiled_query,
                             std::size_t selected_column_count) {
        auto shape_validation_result =
            detail::validate_projection_shape<Projection>(selected_column_count);
        if (!shape_validation_result) {
            return std::unexpected(shape_validation_result.error());
        }

        auto query_result = session_->with_connection([&](auto& connection) {
            return session_->observed_execute(connection, compiled_query.sql,
                                              compiled_query.params);
        });
        if (!query_result) {
            return std::unexpected(query_result.error());
        }

        std::vector<Projection> projected_rows;
        projected_rows.reserve(static_cast<std::size_t>(query_result->rows()));
        for (int row_index = 0; row_index < query_result->rows(); ++row_index) {
            auto decoded_projection =
                detail::decode_projection<Projection>(*query_result, row_index);
            if (!decoded_projection) {
                return std::unexpected(decoded_projection.error());
            }
            projected_rows.push_back(std::move(*decoded_projection));
        }
        return projected_rows;
    }

    template <typename Mapper>
    auto execute_projection_map(const sql::compiled_query& compiled_query,
                                std::size_t selected_column_count, Mapper&& mapper)
        -> asterorm::result<std::vector<std::invoke_result_t<Mapper&, const projection_row&>>> {
        auto query_result = session_->with_connection([&](auto& connection) {
            return session_->observed_execute(connection, compiled_query.sql,
                                              compiled_query.params);
        });
        using mapped_type = std::invoke_result_t<Mapper&, const projection_row&>;
        if (!query_result) {
            return std::unexpected(query_result.error());
        }

        std::vector<mapped_type> projected_rows;
        projected_rows.reserve(static_cast<std::size_t>(query_result->rows()));
        for (int row_index = 0; row_index < query_result->rows(); ++row_index) {
            projection_row row_values;
            row_values.reserve(selected_column_count);
            for (std::size_t column_index = 0; column_index < selected_column_count;
                 ++column_index) {
                row_values.push_back(
                    query_result->get_string(row_index, static_cast<int>(column_index)));
            }
            projected_rows.push_back(std::invoke(mapper, row_values));
        }
        return projected_rows;
    }

    template <typename T> asterorm::result<void> update_const(const T& entity) {
        using traits = entity_traits<T>;
        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("UPDATE ").qualified(traits::table).raw(" SET ");

        query_parameters params;
        bool has_assignment = false;

        auto collect_update = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            size_t param_idx = 1;
            bool first = true;
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (!detail::is_primary_key_column<T>(col.name) && col.gen != generated::always) {
                    if (!first)
                        statement.raw(", ");
                    statement.ident(col.name).raw(" = $" + std::to_string(param_idx++));
                    params.push_back(asterorm::encode_parameter(entity.*(col.member_ptr)));
                    first = false;
                    has_assignment = true;
                }
            }());
            statement.raw(" WHERE ");
            detail::append_entity_pk_predicate(statement, params, entity, param_idx);
        };
        collect_update(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        if (!has_assignment) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "update() requires at least one writable column";
            return std::unexpected(err);
        }

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = session_->with_connection(
            [&](auto& conn) { return session_->observed_execute(conn, *sql, params); });

        if (!res)
            return std::unexpected(res.error());

        if (res->affected_rows() == 0)
            return std::unexpected(detail::no_rows_affected_error());
        return {};
    }

    template <typename T> asterorm::result<void> update_returning(T& entity) {
        using traits = entity_traits<T>;
        detail::quoted_sql statement{sql::dialect_for_session<Session>()};
        statement.raw("UPDATE ").qualified(traits::table).raw(" SET ");

        query_parameters params;
        bool has_assignment = false;
        auto collect_update = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            size_t param_idx = 1;
            bool first = true;
            (..., [&]() {
                auto col = std::get<Is>(traits::columns);
                if (!detail::is_primary_key_column<T>(col.name) && col.gen != generated::always) {
                    if (!first)
                        statement.raw(", ");
                    statement.ident(col.name).raw(" = $" + std::to_string(param_idx++));
                    params.push_back(asterorm::encode_parameter(entity.*(col.member_ptr)));
                    first = false;
                    has_assignment = true;
                }
            }());
            statement.raw(" WHERE ");
            detail::append_entity_pk_predicate(statement, params, entity, param_idx);
        };
        collect_update(std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});

        if (!has_assignment) {
            db_error err;
            err.kind = db_error_kind::parse_failed;
            err.message = "update() requires at least one writable column";
            return std::unexpected(err);
        }

        if constexpr (supports_returning)
            detail::append_returning_all<T>(statement);

        auto sql = statement.finish();
        if (!sql)
            return std::unexpected(sql.error());
        auto res = execute_returning(entity, *sql, params, false);

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
        auto compiled = detail::build_versioned_update(entity, supports_returning,
                                                       sql::dialect_for_session<Session>());
        if (!compiled)
            return std::unexpected(compiled.error());

        auto res = execute_returning(entity, compiled->sql, compiled->params, false);

        if (!res)
            return std::unexpected(res.error());

        if (res->rows() == 0)
            return std::unexpected(detail::stale_write_error());

        auto decoded = detail::hydrate_entity(*res, 0, entity);
        if (!decoded)
            return std::unexpected(decoded.error());
        return {};
    }

    template <typename T> asterorm::result<void> update_versioned_const(const T& entity) {
        auto compiled =
            detail::build_versioned_update(entity, false, sql::dialect_for_session<Session>());
        if (!compiled)
            return std::unexpected(compiled.error());
        auto res = session_->with_connection([&](auto& conn) {
            return session_->observed_execute(conn, compiled->sql, compiled->params);
        });
        if (!res)
            return std::unexpected(res.error());
        if (res->affected_rows() == 0)
            return std::unexpected(detail::stale_write_error());
        return {};
    }

    Session* session_;
};

} // namespace asterorm
