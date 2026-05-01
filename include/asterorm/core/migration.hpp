#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
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
        auto ensure = ensure_table();
        if (!ensure)
            return std::unexpected(ensure.error());

        std::ranges::sort(migrations, {}, &migration::version);
        auto current = current_version();
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

    asterorm::result<migration_status> rollback_to(std::int64_t target_version,
                                                   std::vector<migration> migrations) {
        auto ensure = ensure_table();
        if (!ensure)
            return std::unexpected(ensure.error());

        std::ranges::sort(migrations, [](const migration& lhs, const migration& rhs) {
            return lhs.version > rhs.version;
        });

        auto current = current_version();
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

            status.current_version = target_version;
            ++status.applied_count;
        }

        auto refreshed = current_version();
        if (!refreshed)
            return std::unexpected(refreshed.error());
        status.current_version = *refreshed;
        return status;
    }

    asterorm::result<std::int64_t> current_version() {
        auto ensure = ensure_table();
        if (!ensure)
            return std::unexpected(ensure.error());

        auto version = session_->template native_scalar<std::int64_t>(
            "SELECT COALESCE(MAX(version), 0) FROM " + *table_sql_);
        if (!version)
            return std::unexpected(version.error());
        return version->value_or(0);
    }

  private:
    asterorm::result<void> ensure_table() {
        if (!table_sql_)
            return std::unexpected(invalid_table_error());

        std::string sql = "CREATE TABLE IF NOT EXISTS ";
        sql += *table_sql_;
        sql += " (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL "
               "DEFAULT now())";
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
        std::vector<std::optional<std::string>> params{asterorm::encode(m.version),
                                                       asterorm::encode(m.name)};
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
        std::vector<std::optional<std::string>> params{asterorm::encode(version)};
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
