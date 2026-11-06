#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "asterorm/core/result.hpp"
#include "asterorm/core/session.hpp"

namespace asterorm {

struct migration {
    std::int64_t version{};
    std::string name;
    std::string up_sql;
    std::string down_sql;
};

struct migration_status {
    std::int64_t current_version{};
    std::size_t applied_count{};
};

template <typename Session> class schema_migrator {
  public:
    explicit schema_migrator(Session& session,
                             std::string table_name = "asterorm_schema_migrations")
        : session_(&session), table_name_(std::move(table_name)),
          table_sql_(quote_qualified_identifier(table_name_)) {}

    asterorm::result<migration_status> apply(std::vector<migration> migrations) {
        return serialized([&] { return apply_unlocked(std::move(migrations)); });
    }

    asterorm::result<migration_status> rollback_to(std::int64_t target_version,
                                                   std::vector<migration> migrations) {
        return serialized(
            [&] { return rollback_to_unlocked(target_version, std::move(migrations)); });
    }

    asterorm::result<std::int64_t> current_version() {
        return serialized([&] { return current_version_unlocked(); });
    }

  private:
    // Stable across processes, table spellings, and search paths. Reserve the
    // bigint key spelling "ASTERORM" for all migrators in the same database.
    static constexpr std::int64_t migration_lock_key = 0x41535445524f524d;

    template <typename Callback> auto serialized(Callback&& callback) -> decltype(callback()) {
        using return_type = decltype(callback());
        auto supported = validate_backend_and_table();
        if (!supported) {
            return return_type{std::unexpected(supported.error())};
        }
        using connection_type = typename Session::connection_type;
        if constexpr (detail::has_advisory_scope<connection_type>) {
            if (session_->has_active_transaction()) {
                return return_type{std::unexpected(active_transaction_error())};
            }
            return session_->with_advisory_lock(
                migration_lock_key, [&](auto& locked) -> return_type {
                    // A caller-owned transaction could retain uncommitted DDL or
                    // an old snapshot beyond release of the migration lock.
                    if (locked.has_active_transaction()) {
                        return std::unexpected(active_transaction_error());
                    }
                    return std::forward<Callback>(callback)();
                });
        } else {
            return std::forward<Callback>(callback)();
        }
    }

    asterorm::result<migration_status> apply_unlocked(std::vector<migration> migrations) {
        std::ranges::sort(migrations, {}, &migration::version);
        auto current = current_version_unlocked();
        if (!current)
            return std::unexpected(current.error());

        migration_status status{.current_version = *current, .applied_count = 0};
        for (const auto& m : migrations) {
            if (m.version <= status.current_version)
                continue;

            auto applied = session_->transact([&](auto& tx_session) -> asterorm::result<void> {
                auto run_up = execute_sql(tx_session, m.up_sql);
                if (!run_up)
                    return std::unexpected(run_up.error());
                auto record = record_migration(tx_session, m);
                if (!record)
                    return std::unexpected(record.error());
                return {};
            });
            if (!applied)
                return std::unexpected(applied.error());

            status.current_version = m.version;
            ++status.applied_count;
        }
        return status;
    }

    asterorm::result<migration_status> rollback_to_unlocked(std::int64_t target_version,
                                                            std::vector<migration> migrations) {
        std::ranges::sort(migrations, [](const migration& lhs, const migration& rhs) {
            return lhs.version > rhs.version;
        });

        auto current = current_version_unlocked();
        if (!current)
            return std::unexpected(current.error());

        migration_status status{.current_version = *current, .applied_count = 0};
        for (const auto& m : migrations) {
            if (m.version > status.current_version || m.version <= target_version)
                continue;
            if (m.down_sql.empty()) {
                db_error err;
                err.kind = db_error_kind::query_failed;
                err.message = "Migration has no down SQL: ";
                err.message += m.name;
                return std::unexpected(err);
            }

            auto rolled_back = session_->transact([&](auto& tx_session) -> asterorm::result<void> {
                auto run_down = execute_sql(tx_session, m.down_sql);
                if (!run_down)
                    return std::unexpected(run_down.error());
                auto remove = remove_migration(tx_session, m.version);
                if (!remove)
                    return std::unexpected(remove.error());
                return {};
            });
            if (!rolled_back)
                return std::unexpected(rolled_back.error());

            ++status.applied_count;
        }

        auto refreshed = current_version_unlocked();
        if (!refreshed)
            return std::unexpected(refreshed.error());
        status.current_version = *refreshed;
        return status;
    }

    asterorm::result<std::int64_t> current_version_unlocked() {
        auto ensure = ensure_table();
        if (!ensure)
            return std::unexpected(ensure.error());

        auto version = session_->template native_scalar<std::int64_t>(
            "SELECT COALESCE(MAX(version), 0) FROM " + *table_sql_);
        if (!version)
            return std::unexpected(version.error());
        return version->value_or(0);
    }

    [[nodiscard]] asterorm::result<void> validate_backend_and_table() const {
        using connection_type = typename Session::connection_type;
        if constexpr (requires { connection_type::supports_atomic_migrations; }) {
            if constexpr (!connection_type::supports_atomic_migrations) {
                db_error error;
                error.kind = db_error_kind::query_failed;
                error.message = "This backend cannot guarantee atomic schema migrations; use "
                                "native SQL with explicit migration tracking";
                return std::unexpected(error);
            }
        }
        if (!table_sql_)
            return std::unexpected(invalid_table_error());
        return {};
    }

    asterorm::result<void> ensure_table() {
        std::string sql = "CREATE TABLE IF NOT EXISTS ";
        sql += *table_sql_;
        sql += " (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL "
               "DEFAULT ";
        sql += migration_timestamp_default();
        sql += ")";
        return session_->with_connection([&](auto& conn) -> asterorm::result<void> {
            auto res = conn.execute(sql);
            if (!res)
                return std::unexpected(res.error());
            return {};
        });
    }

    template <typename TxSession>
    asterorm::result<void> execute_sql(TxSession& session, std::string_view sql) {
        return session.with_connection([&](auto& conn) -> asterorm::result<void> {
            auto res = conn.execute(sql);
            if (!res)
                return std::unexpected(res.error());
            return {};
        });
    }

    template <typename TxSession>
    asterorm::result<void> record_migration(TxSession& session, const migration& m) {
        query_parameters params{asterorm::encode_parameter(m.version),
                                asterorm::encode_parameter(m.name)};
        std::string sql = "INSERT INTO ";
        sql += *table_sql_;
        sql += " (version, name) VALUES ($1, $2)";
        return session.with_connection([&](auto& conn) -> asterorm::result<void> {
            auto res = session.observed_execute(conn, sql, params);
            if (!res)
                return std::unexpected(res.error());
            return {};
        });
    }

    template <typename TxSession>
    asterorm::result<void> remove_migration(TxSession& session, std::int64_t version) {
        query_parameters params{asterorm::encode_parameter(version)};
        std::string sql = "DELETE FROM ";
        sql += *table_sql_;
        sql += " WHERE version = $1";
        return session.with_connection([&](auto& conn) -> asterorm::result<void> {
            auto res = session.observed_execute(conn, sql, params);
            if (!res)
                return std::unexpected(res.error());
            return {};
        });
    }

    static bool is_identifier_start(char c) {
        auto ch = static_cast<unsigned char>(c);
        return std::isalpha(ch) || c == '_';
    }

    static std::string migration_timestamp_default() {
        using connection_type = typename Session::connection_type;
        if constexpr (requires { connection_type::migration_timestamp_default(); }) {
            return connection_type::migration_timestamp_default();
        } else {
            return "now()";
        }
    }

    static bool is_identifier_continue(char c) {
        auto ch = static_cast<unsigned char>(c);
        return std::isalnum(ch) || c == '_';
    }

    static std::optional<std::string> quote_qualified_identifier(std::string_view name) {
        std::string out;
        std::size_t pos = 0;
        while (pos < name.size()) {
            auto dot = name.find('.', pos);
            auto part =
                name.substr(pos, dot == std::string_view::npos ? name.size() - pos : dot - pos);
            if (part.empty() || !is_identifier_start(part.front()))
                return std::nullopt;

            for (char c : part) {
                if (!is_identifier_continue(c))
                    return std::nullopt;
            }

            if (!out.empty())
                out += '.';
            out += '"';
            out.append(part);
            out += '"';

            if (dot == std::string_view::npos)
                break;
            pos = dot + 1;
        }
        return out.empty() ? std::nullopt : std::optional<std::string>{out};
    }

    static db_error active_transaction_error() {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Schema migrations require a session without an active transaction";
        return error;
    }

    static db_error invalid_table_error() {
        db_error err;
        err.kind = db_error_kind::query_failed;
        err.message = "Invalid migration table name";
        return err;
    }

    Session* session_;
    std::string table_name_;
    std::optional<std::string> table_sql_;
};

} // namespace asterorm
