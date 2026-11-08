#pragma once
#include "asterorm/repository.hpp"
#include "asterorm/sql/typed_query.hpp"
#include <catch2/catch_test_macros.hpp>

struct pool_policy_row {
    int id{};
    std::string label;
};
template <> struct asterorm::entity_traits<pool_policy_row> {
    static constexpr const char* table = "asterorm_pool_policy_rows";
    static constexpr auto primary_key = asterorm::pk<&pool_policy_row::id>("id");
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&pool_policy_row::id>("id"),
                        asterorm::column<&pool_policy_row::label>("label"));
};
namespace asterorm::test {
template <typename Driver> void check_pool_policy(Driver driver, const std::string& settings) {
    for (const auto policy :
         {statement_cache_policy::off, statement_cache_policy::per_connection}) {
        for (const std::size_t capacity : {std::size_t{0}, std::size_t{2}}) {
            pool_config config;
            config.min_size = 0;
            config.max_size = 1;
            config.conninfo = settings;
            config.statement_caching = policy;
            config.prepared_statement_cache_size = capacity;
            connection_pool<Driver> pool{driver, config};
            session db{pool};
            int events{};
            db.set_observer([&](const auto&) { ++events; });
            for (int index = 0; index < 2; ++index) {
                auto selected = db.template native_scalar<int>("SELECT $1", 41);
                REQUIRE(selected);
                REQUIRE(*selected == 41);
            }
            REQUIRE(events == 2);
            const bool cached = policy == statement_cache_policy::per_connection && capacity > 0;
            auto configured = db.with_connection([&](auto& connection) -> result<void> {
                REQUIRE(connection.prepared_statement_count() == (cached ? 1 : 0));
                REQUIRE(connection.execute("CREATE TEMPORARY TABLE asterorm_pool_policy_rows (id "
                                           "INTEGER PRIMARY KEY, label TEXT NOT NULL)"));
                return {};
            });
            REQUIRE(configured);
            repository repo{db};
            pool_policy_row row{7, "first"};
            REQUIRE(repo.insert(row));
            row.label = "changed";
            REQUIRE(repo.update(row));
            auto found = repo.template find<pool_policy_row>(7);
            REQUIRE(found);
            REQUIRE(found->label == "changed");
            auto typed = sql::typed::query(
                db, sql::typed::select<pool_policy_row>().where(
                        sql::typed::col<&pool_policy_row::id> == sql::typed::val(7)));
            REQUIRE(typed);
            REQUIRE(typed->size() == 1);
            REQUIRE(typed->front().label == "changed");
            REQUIRE(repo.template erase<pool_policy_row>(7));
            REQUIRE(db.with_connection([&](auto& connection) -> result<void> {
                REQUIRE(connection.prepared_statement_count() <= (cached ? capacity : 0));
                return {};
            }));
        }
    }
    using connection_type = typename connection_pool<Driver>::connection_type;
    if constexpr (!requires { requires connection_type::supports_transaction_pool_profile; }) {
        for (const std::size_t minimum : {std::size_t{0}, std::size_t{1}}) {
            pool_config unsupported;
            unsupported.min_size = minimum;
            unsupported.conninfo = settings;
            unsupported.statement_caching = statement_cache_policy::off;
            unsupported.profile = pool_profile::transaction_pool;
            connection_pool<Driver> rejected{driver, unsupported};
            REQUIRE(rejected.warmup_error());
            REQUIRE(rejected.warmup_error()->message.contains("transaction_pool"));
            REQUIRE_FALSE(rejected.acquire());
        }
    }
}
} // namespace asterorm::test
