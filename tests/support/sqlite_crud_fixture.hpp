#pragma once
#include "asterorm/sqlite/driver.hpp"
#include "reference_fixture.hpp"

namespace asterorm::test {
struct sqlite_crud_fixture : reference_fixture<sqlite::driver> {
    static inline sqlite_crud_fixture* instance{};
    int reference_user_id{};
    sqlite_crud_fixture() : reference_fixture(sqlite::driver{}, ":memory:") {
        instance = this;
    }
    result<void> prepare([[maybe_unused]] session_type& owner) {
        auto identity = prepare_identity();
        if (!identity) {
            return identity;
        }
        auto created = execute_schema("CREATE TEMP TABLE reference_users(id INTEGER PRIMARY KEY "
                                      "AUTOINCREMENT,name TEXT NOT NULL)");
        if (!created) {
            return created;
        }
        auto seeded = database.native_scalar<int>(
            "INSERT INTO temp.reference_users(name) VALUES($1) RETURNING id", "Reference");
        if (!seeded) {
            return std::unexpected(std::move(seeded.error()));
        }
        if (!*seeded) {
            return std::unexpected(reference_error("Reference insert returned no identifier"));
        }
        reference_user_id = **seeded;
        return {};
    }
    result<void> validate(session_type& owner) {
        auto identity = validate_identity(owner);
        if (!identity) {
            return identity;
        }
        auto baseline = owner.native_scalar<bool>(
            "SELECT (SELECT count(*) FROM temp.reference_users)=1 AND "
            "EXISTS(SELECT 1 FROM temp.reference_users WHERE id=$1 AND name='Reference')",
            reference_user_id);
        if (!baseline) {
            return std::unexpected(std::move(baseline.error()));
        }
        if (*baseline != true) {
            return std::unexpected(reference_error("SQLite reference baseline changed"));
        }
        return {};
    }
};
} // namespace asterorm::test
