#pragma once
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "asterorm/session.hpp"

namespace asterorm::test {
struct reference_metrics {
    int preparations{};
    int schema_statements{};
    int identity_statements{};
    int validations{};
    int bodies{};
    int connect_attempts{};
    int native_connects{};
};
inline db_error reference_error(const char* message) {
    return {.kind = db_error_kind::connection_failed, .message = message};
}

// Serial fixture driver: never replace the physical connection owning TEMP data.
template <typename Driver> struct reference_driver {
    Driver native;
    reference_metrics* metrics;
    auto connect(const std::string& settings) -> decltype(native.connect(settings)) {
        ++metrics->connect_attempts;
        if (metrics->native_connects != 0) {
            return std::unexpected(
                reference_error("Reference fixture connection lost; reconnect disabled"));
        }
        ++metrics->native_connects;
        return native.connect(settings);
    }
};

template <typename Driver> class reference_fixture {
  public:
    using pool_type = connection_pool<reference_driver<Driver>>;
    using session_type = asterorm::session<pool_type>;
    reference_metrics metrics;
    pool_type pool;
    session_type database{pool};

    reference_fixture(Driver driver, std::string settings)
        : pool(reference_driver<Driver>{std::move(driver), &metrics}, [&] {
              pool_config config;
              config.conninfo = std::move(settings);
              config.min_size = 0;
              config.max_size = 1;
              return config;
          }()) {}
    auto& session() {
        return database;
    }
    result<void> execute_schema(std::string_view sql) {
        ++metrics.schema_statements;
        return database.with_connection([&](auto& connection) -> result<void> {
            auto executed = connection.execute(sql);
            if (!executed) {
                return std::unexpected(std::move(executed.error()));
            }
            return {};
        });
    }
    result<void> prepare_identity() {
        ++metrics.preparations;
        return database.with_connection([&](auto& connection) -> result<void> {
            ++metrics.identity_statements;
            auto created = connection.execute(
                "CREATE TEMP TABLE asterorm_reference_identity(token INTEGER PRIMARY KEY)");
            if (!created) {
                return std::unexpected(std::move(created.error()));
            }
            auto seeded = connection.execute("INSERT INTO asterorm_reference_identity VALUES(1)");
            if (!seeded) {
                return std::unexpected(std::move(seeded.error()));
            }
            return {};
        });
    }
    result<void> validate_identity(session_type& owner) {
        ++metrics.validations;
        // Qualify TEMP explicitly so an unrelated permanent table cannot satisfy readiness.
        const auto query = [] {
            if constexpr (sql::dialect_for<typename session_type::connection_type>() ==
                          sql::dialect::postgres) {
                return "SELECT count(*) FROM pg_temp.asterorm_reference_identity WHERE token=1";
            } else {
                return "SELECT count(*) FROM temp.asterorm_reference_identity WHERE token=1";
            }
        }();
        auto checked = owner.template native_scalar<int>(query);
        if (!checked) {
            return std::unexpected(std::move(checked.error()));
        }
        if (*checked != 1) {
            return std::unexpected(reference_error("Reference fixture identity is missing"));
        }
        return {};
    }
};

inline void print_reference_receipt(const char* backend,
                                    const reference_metrics& metrics) noexcept {
    // Portable, nonthrowing machine-readable test receipt without <print> requirements.
    // NOLINTNEXTLINE(modernize-use-std-print)
    std::printf(
        "reference backend=%s preparations=%d schema=%d identity=%d validations=%d bodies=%d "
        "connect_attempts=%d native_connects=%d\n",
        backend, metrics.preparations, metrics.schema_statements, metrics.identity_statements,
        metrics.validations, metrics.bodies, metrics.connect_attempts, metrics.native_connects);
}
} // namespace asterorm::test
