#pragma once
#include <array>
#include <cstdlib>

#include "asterorm/postgres/driver.hpp"
#include "reference_fixture.hpp"

namespace asterorm::test {
inline std::string reference_pg_settings() {
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    return configured ? configured
                      : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                        "password=orm_test sslmode=disable";
}
struct pg_crud_fixture : reference_fixture<pg::driver> {
    static inline pg_crud_fixture* instance{}; // Process-owned by the registered provider.
    int reference_user_id{};
    pg_crud_fixture() : reference_fixture(pg::driver{}, reference_pg_settings()) {
        instance = this;
    }
    result<void> prepare([[maybe_unused]] session_type& owner) {
        auto identity = prepare_identity();
        if (!identity) {
            return identity;
        }
        constexpr std::array statements{
            "CREATE TEMP TABLE crud_users (id SERIAL PRIMARY KEY, email TEXT, name TEXT, active "
            "BOOLEAN)",
            "CREATE TEMP TABLE generated_users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, slug "
            "TEXT GENERATED ALWAYS AS (lower(name)) STORED)",
            "CREATE TEMP TABLE locked_users (id SERIAL PRIMARY KEY, email TEXT UNIQUE, name TEXT, "
            "version INT NOT NULL DEFAULT 0)",
            "CREATE TEMP TABLE order_lines (order_id INT NOT NULL, line_no INT NOT NULL, sku TEXT "
            "NOT NULL, qty INT NOT NULL, PRIMARY KEY (order_id,line_no))",
            "CREATE TEMP TABLE relationship_authors (id SERIAL PRIMARY KEY, email TEXT NOT NULL)",
            "CREATE TEMP TABLE relationship_profiles (id SERIAL PRIMARY KEY, author_id INT NOT "
            "NULL, display_name TEXT NOT NULL)",
            "CREATE TEMP TABLE relationship_posts (id SERIAL PRIMARY KEY, author_id INT NOT NULL, "
            "title TEXT NOT NULL)",
        };
        for (const auto* statement : statements) {
            auto created = execute_schema(statement);
            if (!created) {
                return created;
            }
        }
        auto seeded = database.native_scalar<int>(
            "INSERT INTO pg_temp.crud_users(email,name,active) VALUES($1,$2,TRUE) RETURNING id",
            "reference@example.test", "Reference");
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
            "SELECT (SELECT count(*) FROM pg_temp.crud_users)=1 AND "
            "EXISTS(SELECT 1 FROM pg_temp.crud_users WHERE id=$1 AND "
            "email='reference@example.test' AND name='Reference' AND active=TRUE) AND "
            "(SELECT count(*) FROM pg_temp.generated_users)=0 AND "
            "(SELECT count(*) FROM pg_temp.locked_users)=0 AND "
            "(SELECT count(*) FROM pg_temp.order_lines)=0 AND "
            "(SELECT count(*) FROM pg_temp.relationship_authors)=0 AND "
            "(SELECT count(*) FROM pg_temp.relationship_profiles)=0 AND "
            "(SELECT count(*) FROM pg_temp.relationship_posts)=0",
            reference_user_id);
        if (!baseline) {
            return std::unexpected(std::move(baseline.error()));
        }
        if (*baseline != true) {
            return std::unexpected(reference_error("PostgreSQL reference baseline changed"));
        }
        return {};
    }
};
} // namespace asterorm::test
