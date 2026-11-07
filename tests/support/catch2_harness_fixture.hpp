#pragma once
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "asterorm/sqlite/driver.hpp"
#include "asterorm/testing/catch2.hpp"

namespace harness_test {
struct counters {
    int factories{};
    int constructed{};
    int prepares{};
    int validations{};
    int connects{};
    int closes{};
    int begins{};
    int savepoints{};
    int rollback_to{};
    int releases{};
    int rollbacks{};
    int bodies{};
    int unwinds{};
    bool injected{};
};
inline counters observed;
inline std::string_view fault_mode() {
    const auto* configured = std::getenv("ASTERORM_HARNESS_FAULT");
    return configured ? configured : "";
}
inline asterorm::db_error fault() {
    return {.kind = asterorm::db_error_kind::query_failed,
            .sqlstate = "XX901",
            .message = "injected harness failure",
            .detail = "original diagnostic retained"};
}
class connection : public asterorm::sqlite::connection {
  public:
    explicit connection(asterorm::sqlite::connection&& native)
        : asterorm::sqlite::connection(std::move(native)) {}
    void close() noexcept {
        if (is_open()) {
            ++observed.closes;
        }
        asterorm::sqlite::connection::close();
    }
    asterorm::result<asterorm::sqlite::result> execute(std::string_view statement) {
        if (statement == "BEGIN") {
            ++observed.begins;
            std::puts("event=begin");
        } else if (statement.starts_with("SAVEPOINT ")) {
            ++observed.savepoints;
        } else if (statement.starts_with("ROLLBACK TO ")) {
            ++observed.rollback_to;
            std::puts("event=rollback_to");
        } else if (statement.starts_with("RELEASE ")) {
            ++observed.releases;
        } else if (statement == "ROLLBACK") {
            ++observed.rollbacks;
            std::puts("event=rollback");
        }
        if (!observed.injected) {
            const auto mode = fault_mode();
            if ((mode == "entry_error" && statement.starts_with("SAVEPOINT ")) ||
                (mode == "cleanup_error" && statement.starts_with("ROLLBACK TO "))) {
                observed.injected = true;
                return std::unexpected(fault());
            }
            if (mode == "cleanup_alloc" && statement.starts_with("ROLLBACK TO ")) {
                observed.injected = true;
                throw std::bad_alloc{};
            }
            if (mode == "cleanup_throw" && statement.starts_with("ROLLBACK TO ")) {
                observed.injected = true;
                throw std::runtime_error("injected cleanup exception");
            }
        }
        return asterorm::sqlite::connection::execute(statement);
    }
};
struct driver {
    asterorm::result<connection> connect(const std::string& settings) {
        ++observed.connects;
        auto native = asterorm::sqlite::driver{}.connect(settings);
        if (!native) {
            return std::unexpected(std::move(native.error()));
        }
        return connection{std::move(*native)};
    }
};
struct fixture {
    asterorm::connection_pool<driver> pool{driver{}, [] {
                                               asterorm::pool_config config;
                                               config.conninfo = ":memory:";
                                               config.min_size = 0;
                                               config.max_size = 1;
                                               return config;
                                           }()};
    asterorm::session<decltype(pool)> database{pool};
    fixture() {
        ++observed.constructed;
    }
    auto& session() {
        return database;
    }
    asterorm::result<void> prepare(decltype(database)& owner) {
        ++observed.prepares;
        if (fault_mode() == "prepare_throw") {
            throw std::runtime_error("injected preparation exception");
        }
        if (fault_mode() == "prepare_error") {
            return std::unexpected(fault());
        }
        return owner.with_connection([](auto& native) -> asterorm::result<void> {
            auto prepared =
                native.execute("CREATE TEMP TABLE harness_rows(id INTEGER PRIMARY KEY)");
            if (!prepared) {
                return std::unexpected(std::move(prepared.error()));
            }
            auto inserted = native.execute("INSERT INTO harness_rows VALUES(1)");
            if (!inserted) {
                return std::unexpected(std::move(inserted.error()));
            }
            return {};
        });
    }
    asterorm::result<void> validate(decltype(database)& owner) {
        ++observed.validations;
        if (!owner.has_active_transaction()) {
            return std::unexpected(fault());
        }
        if (fault_mode() == "validate_error") {
            return std::unexpected(fault());
        }
        if (fault_mode() == "validate_throw") {
            throw std::runtime_error("injected validation exception");
        }
        if (fault_mode() == "validate_alloc") {
            throw std::bad_alloc{};
        }
        auto baseline = owner.native_scalar<int>("SELECT count(*) FROM harness_rows");
        if (!baseline) {
            return std::unexpected(std::move(baseline.error()));
        }
        if (*baseline != 1) {
            return std::unexpected(fault());
        }
        return {};
    }
};
struct other_fixture : fixture {};
struct unsupported_fixture {
    struct unsupported_session {
        struct connection_type {};
    } database;
    unsupported_fixture() {
        ++observed.constructed;
    }
    auto& session() {
        return database;
    }
    asterorm::result<void> prepare([[maybe_unused]] unsupported_session& owner) {
        ++observed.prepares;
        return {};
    }
};
struct body_marker {
    body_marker() {
        ++observed.bodies;
    }
    ~body_marker() {
        ++observed.unwinds;
        std::puts("event=body_unwind");
    }
};
} // namespace harness_test
