#pragma once
#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <type_traits>

#include "asterorm/repository.hpp"
#include "asterorm/sql/typed.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/testing/transactional_session.hpp"

struct transactional_scope_row {
    int id{};
    std::string note;
    bool active{};
};
template <> struct asterorm::entity_traits<transactional_scope_row> {
    static constexpr const char* table = "transactional_scope_rows";
    static constexpr auto primary_key = asterorm::pk<&transactional_scope_row::id>("id");
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&transactional_scope_row::id>("id"),
                        asterorm::column<&transactional_scope_row::note>("note"),
                        asterorm::column<&transactional_scope_row::active>("active"));
};
namespace asterorm::test {
template <typename Value> Value scope_take(result<Value> outcome) {
    REQUIRE(outcome.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}
template <typename Driver> void check_transactional_session(Driver driver, std::string settings) {
    pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    repository repo{db};
    using scope_type = testing::transactional_session<decltype(db)>;
    auto execute = [&](const char* sql) {
        scope_take(db.with_connection([&](auto& connection) { return connection.execute(sql); }));
    };
    execute("CREATE TEMP TABLE transactional_scope_rows(id INTEGER PRIMARY KEY,note TEXT "
            "UNIQUE,active BOOLEAN)");
    transactional_scope_row reference{1, "reference", true};
    scope_take(repo.insert(reference));
    int baseline_events = 0;
    int body_events = 0;
    db.set_observer([&](const auto&) { ++baseline_events; });
    {
        auto scope = scope_take(scope_type::create(db));
        REQUIRE(baseline_events == 0);
        REQUIRE_FALSE(db.begin().has_value());
        REQUIRE_FALSE(db.commit_transaction().has_value());
        REQUIRE_FALSE(db.rollback_transaction().has_value());
        REQUIRE_FALSE(scope_type::create(db).has_value());
        db.set_observer([&](const auto&) { ++body_events; });
        transactional_scope_row inserted{2, "bound ' ; --", false};
        scope_take(repo.insert(inserted));
        auto baseline = scope_take(repo.template find<transactional_scope_row>(1));
        baseline.note = "modified";
        scope_take(repo.update(baseline));
        auto query = sql::typed::select<transactional_scope_row>();
        query.where(sql::typed::col<&transactional_scope_row::id> == sql::typed::val(2));
        REQUIRE(scope_take(repo.template find_all<transactional_scope_row>()).size() == 2);
        REQUIRE(scope_take(sql::typed::query(db, query)).size() == 1);
        scope_take(db.savepoint("body_boundary"));
        execute("DELETE FROM transactional_scope_rows WHERE id=2");
        scope_take(db.rollback_to_savepoint("body_boundary"));
        scope_take(db.release_savepoint("body_boundary"));
        REQUIRE(scope_take(repo.template find<transactional_scope_row>(2)).note == "bound ' ; --");
        execute("CREATE TEMP TABLE transactional_scope_transient(id INTEGER)");
        const auto observed = body_events;
        scope_take(scope.finish());
        scope_take(scope.finish());
        REQUIRE(body_events == observed);
        REQUIRE(baseline_events == 0);
        REQUIRE_THROWS_AS(scope.session(), std::logic_error);
    }
    REQUIRE_FALSE(db.has_active_transaction());
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE(pool.stats().total == 1);
    REQUIRE(scope_take(repo.template find<transactional_scope_row>(1)).note == "reference");
    REQUIRE(baseline_events == 1);
    db.set_observer({});
    REQUIRE_FALSE(repo.template find<transactional_scope_row>(2).has_value());
    REQUIRE_FALSE(
        db.template native_scalar<int>("SELECT count(*) FROM transactional_scope_transient")
            .has_value());
    {
        auto scope = scope_take(scope_type::create(db));
        auto failed = db.with_connection([](auto& connection) {
            return connection.execute(
                "INSERT INTO transactional_scope_rows VALUES(1,'duplicate',TRUE)");
        });
        REQUIRE_FALSE(failed.has_value());
        const auto original = failed.error();
        scope_take(
            scope.finish()); // PostgreSQL failed-statement recovery uses the private boundary
        REQUIRE(failed.error().message == original.message);
        REQUIRE(failed.error().sqlstate == original.sqlstate);
    }
    {
        auto scope = scope_take(scope_type::create(db));
        execute("INSERT INTO transactional_scope_rows VALUES(3,'destructor',TRUE)");
    }
    REQUIRE(scope_take(repo.template find_all<transactional_scope_row>()).size() == 1);
    REQUIRE(scope_take(db.with_connection(
        [](auto& connection) -> result<bool> { return connection.is_transaction_idle(); })));
    {
        auto scope = scope_take(scope_type::create(db));
        // Test-only raw tampering to inject loss of the private boundary.
        execute("RELEASE SAVEPOINT \"_asterorm_test_4\"");
        const auto failed = scope.finish();
        REQUIRE_FALSE(failed.has_value());
        const auto error = failed.error();
        REQUIRE(scope.finish().error().message == error.message);
        REQUIRE_THROWS_AS(scope.session(), std::logic_error);
    }
    REQUIRE_FALSE(db.has_active_transaction());
    REQUIRE(pool.stats().total == 0);
    REQUIRE_FALSE(scope_type::create(db).has_value());
    REQUIRE_FALSE(repo.template find_all<transactional_scope_row>().has_value());
    session independent{pool};
    REQUIRE(scope_take(independent.template native_scalar<int>("SELECT 1")) == 1);
}
} // namespace asterorm::test
