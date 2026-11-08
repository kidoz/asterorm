#pragma once
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string_view>
#include <type_traits>

#include "asterorm/testing/catch2.hpp"

namespace asterorm::test {
inline std::string_view reference_damage_mode() {
    const auto* mode = std::getenv("ASTERORM_REFERENCE_DAMAGE");
    return mode ? mode : "close";
}
template <typename Fixture> void check_reference_initialized() {
    auto& owner = testing::catch2::session<Fixture>();
    REQUIRE(owner.has_active_transaction());
    REQUIRE(Fixture::instance != nullptr);
    CHECK(Fixture::instance->metrics.preparations == 1);
}
template <typename Fixture> void damage_idle_reference() {
    REQUIRE(Fixture::instance != nullptr);
    auto& owner = Fixture::instance->database;
    REQUIRE_FALSE(owner.has_active_transaction());
    auto damaged = owner.with_connection([](auto& connection) -> result<void> {
        if (reference_damage_mode() == "marker") {
            auto removed = connection.execute("DROP TABLE asterorm_reference_identity");
            if (!removed) {
                return std::unexpected(std::move(removed.error()));
            }
        } else if (reference_damage_mode() == "baseline") {
            const auto statement = [] {
                using connection_type = std::remove_reference_t<decltype(connection)>;
                if constexpr (sql::dialect_for<connection_type>() == sql::dialect::postgres) {
                    return "UPDATE pg_temp.crud_users SET name='Corrupted reference'";
                } else {
                    return "UPDATE temp.reference_users SET name='Corrupted reference'";
                }
            }();
            auto changed = connection.execute(statement);
            if (!changed) {
                return std::unexpected(std::move(changed.error()));
            }
        } else {
            connection.close();
        }
        return {};
    });
    REQUIRE(damaged.has_value());
}
template <typename Fixture> void check_reference_protected() {
    CHECK_THROWS_AS(testing::catch2::session<Fixture>(), std::logic_error);
    REQUIRE(Fixture::instance != nullptr);
    CHECK(Fixture::instance->metrics.preparations == 1);
}
} // namespace asterorm::test
