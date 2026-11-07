#pragma once
#include <catch2/catch_test_macros.hpp>
#include <stdexcept>

#include "asterorm/repository.hpp"
#include "asterorm/sql/typed.hpp"

struct bulk_live_row {
    int tenant{};
    int id{};
    int cutoff{};
    int price{};
    bool active{};
    std::optional<std::string> note;
    int derived{};
    int defaulted{};
};
template <> struct asterorm::entity_traits<bulk_live_row> {
    static constexpr const char* table = "bulk_predicate_rows";
    static constexpr auto primary_key = std::make_tuple(
        asterorm::pk<&bulk_live_row::tenant>("tenant"), asterorm::pk<&bulk_live_row::id>("id"));
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&bulk_live_row::tenant>("tenant"),
        asterorm::column<&bulk_live_row::id>("id"),
        asterorm::column<&bulk_live_row::cutoff>("cutoff"),
        asterorm::column<&bulk_live_row::price>("price"),
        asterorm::column<&bulk_live_row::active>("active"),
        asterorm::column<&bulk_live_row::note>("note"),
        asterorm::column<&bulk_live_row::derived>("derived", asterorm::generated::always),
        asterorm::column<&bulk_live_row::defaulted>("defaulted", asterorm::generated::by_default));
};

namespace asterorm::test {
template <typename Session> void check_bulk_arithmetic(Session& db) {
    namespace sql = asterorm::sql;
    asterorm::repository repo{db};
    int events = 0;
    int errors = 0;
    db.set_observer([&](const query_event& event) {
        ++events;
        if (event.error) {
            ++errors;
        }
    });
    auto transaction = db.begin();
    REQUIRE(transaction.has_value());
    auto adjusted = repo.template update_where<bulk_live_row>(
        sql::col("tenant") == sql::val(7),
        {
            {"price", sql::multiply(sql::add(sql::col("price"), sql::val(5)), sql::val(2))},
        });
    REQUIRE(adjusted == 2);
    auto prices = db.with_connection([](auto& connection) {
        return connection.execute("SELECT price FROM bulk_predicate_rows ORDER BY tenant,id");
    });
    REQUIRE(prices.has_value());
    REQUIRE(prices->get_int64(0, 0) == 210);
    REQUIRE(prices->get_int64(1, 0) == 410);
    REQUIRE(prices->get_int64(2, 0) == 300);
    REQUIRE(repo.template update_where<bulk_live_row>(
                sql::col("tenant") == sql::val(7),
                {
                    {"price", sql::multiply(sql::col("price"), sql::val(2))},
                    {"defaulted", sql::add(sql::col("price"), sql::val(5))},
                }) == 2);
    auto dependent = db.with_connection([](auto& connection) {
        return connection.execute(
            "SELECT defaulted FROM bulk_predicate_rows WHERE tenant=7 ORDER BY id");
    });
    REQUIRE(dependent.has_value());
    // Preserve native SET evaluation: MySQL reads the earlier updated target.
    const bool left_to_right = sql::dialect_for_session<Session>() == sql::dialect::mysql;
    REQUIRE(dependent->get_int64(0, 0) == (left_to_right ? 425 : 215));
    REQUIRE(dependent->get_int64(1, 0) == (left_to_right ? 825 : 415));
    REQUIRE(repo.template update_where<bulk_live_row>(
                sql::col("tenant") == sql::val(7),
                {
                    {"defaulted", sql::add(sql::col("derived"), sql::val(1))},
                }) == 2);
    REQUIRE(repo.template update_where<bulk_live_row>(
                sql::col("tenant") == sql::val(7),
                {
                    {"defaulted", sql::add(sql::col("defaulted"), sql::val(std::optional<int>{}))},
                }) == 2);
    auto nulls = db.with_connection([](auto& connection) {
        return connection.execute(
            "SELECT COUNT(*) FROM bulk_predicate_rows WHERE defaulted IS NULL");
    });
    REQUIRE(nulls.has_value());
    REQUIRE(nulls->get_int64(0, 0) == 2);
    REQUIRE(repo.template update_where<bulk_live_row>(
                sql::col("id") == sql::val(999),
                {
                    {"price", sql::multiply(sql::col("price"), sql::val(2))},
                }) == 0);
    auto invalid = repo.template update_where<bulk_live_row>(
        sql::col("tenant") == sql::val(7),
        {
            {"price", sql::add(sql::col("price"), sql::val(std::optional<int>{}))},
        });
    REQUIRE_FALSE(invalid.has_value());
    REQUIRE(invalid.error().kind == db_error_kind::constraint_violation);
    REQUIRE(events == 6);
    REQUIRE(errors == 1);
    REQUIRE(transaction->rollback().has_value());
    if constexpr (sql::dialect_for_session<Session>() == sql::dialect::postgres) {
        auto type_transaction = db.begin();
        REQUIRE(type_transaction.has_value());
        auto type_error = repo.template update_where<bulk_live_row>(
            sql::col("tenant") == sql::val(7),
            {
                {"price", sql::add(sql::col("price"), sql::val("not-an-integer"))},
            });
        REQUIRE_FALSE(type_error.has_value());
        REQUIRE(type_error.error().sqlstate == "22P02");
        REQUIRE(type_transaction->rollback().has_value());
        REQUIRE(events == 7);
        REQUIRE(errors == 2);
    }
    db.set_observer({});
    auto restored = db.with_connection([](auto& connection) {
        return connection.execute(
            "SELECT COUNT(*) FROM bulk_predicate_rows WHERE price=cutoff AND defaulted=11");
    });
    REQUIRE(restored.has_value());
    REQUIRE(restored->get_int64(0, 0) == 3);
}

template <typename Driver> void check_bulk_predicate(Driver driver, std::string conninfo) {
    namespace sql = asterorm::sql;
    asterorm::pool_config config;
    config.conninfo = std::move(conninfo);
    config.max_size = 1;
    config.acquire_timeout = std::chrono::milliseconds{100};
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    auto created = db.with_connection([](auto& connection) {
        return connection.execute("CREATE TEMPORARY TABLE bulk_predicate_rows (tenant INTEGER, id "
                                  "INTEGER, cutoff INTEGER, price INTEGER NOT NULL, "
                                  "active BOOLEAN NOT NULL, note VARCHAR(255) UNIQUE, "
                                  "derived INTEGER GENERATED ALWAYS AS (id + tenant) STORED, "
                                  "defaulted INTEGER DEFAULT 11, PRIMARY KEY (tenant, id))");
    });
    REQUIRE(created.has_value());
    auto seeded = db.with_connection([](auto& connection) {
        return connection.execute(
            "INSERT INTO bulk_predicate_rows (tenant,id,cutoff,price,active,note) VALUES "
            "(7,1,100,100,TRUE,'old'),(7,2,200,200,TRUE,NULL),(8,1,300,300,TRUE,'other')");
    });
    REQUIRE(seeded.has_value());
    check_bulk_arithmetic(db);
    int events = 0;
    int errors = 0;
    db.set_observer([&](const query_event& event) {
        ++events;
        REQUIRE(pool.stats().in_use == 1);
        if (event.error) {
            ++errors;
        }
    });
    auto transaction = db.begin();
    REQUIRE(transaction.has_value());
    auto typed_predicate = sql::typed::col<&bulk_live_row::tenant> == sql::typed::val(7);
    REQUIRE(repo.template update_where<bulk_live_row>(
                std::move(typed_predicate.inner),
                {{"active", sql::val(false)}, {"defaulted", sql::val(17)}}) == 2);
    REQUIRE(events == 1);
    REQUIRE(repo.template update_where<bulk_live_row>(sql::col("tenant") == sql::val(7),
                                                      {{"active", sql::val(false)}}) == 2);
    REQUIRE(repo.template update_where<bulk_live_row>(sql::col("id") == sql::val(999),
                                                      {{"note", sql::val("absent")}}) == 0);
    REQUIRE(repo.template delete_where<bulk_live_row>(sql::col("cutoff") < sql::val(150) &&
                                                      sql::col("tenant") == sql::val(7)) == 1);
    auto changed = db.with_connection([](auto& connection) {
        return connection.execute("SELECT COUNT(*) FROM bulk_predicate_rows WHERE tenant=7 AND "
                                  "active=FALSE AND defaulted=17");
    });
    REQUIRE(changed.has_value());
    REQUIRE(changed->get_string(0, 0) == "1");
    auto rejected = repo.template update_where<bulk_live_row>(sql::col("tenant") == sql::val(7),
                                                              {{"derived", sql::val(2)}});
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(events == 4);
    auto duplicate = repo.template update_where<bulk_live_row>(sql::col("tenant") >= sql::val(7),
                                                               {{"note", sql::val("duplicate")}});
    REQUIRE_FALSE(duplicate.has_value());
    REQUIRE(duplicate.error().kind == db_error_kind::constraint_violation);
    REQUIRE(events == 5);
    REQUIRE(errors == 1);
    REQUIRE(transaction->rollback().has_value());
    auto restored = db.with_connection([](auto& connection) {
        return connection.execute(
            "SELECT COUNT(*) FROM bulk_predicate_rows WHERE active=TRUE AND defaulted=11");
    });
    REQUIRE(restored.has_value());
    REQUIRE(restored->get_string(0, 0) == "3");
    REQUIRE(repo.template delete_where<bulk_live_row>(sql::is_null(sql::col("note"))) == 1);
    REQUIRE(repo.template delete_where<bulk_live_row>(sql::is_null(sql::col("note"))) == 0);
    const std::string hostile = "quote '; DROP TABLE bulk_predicate_rows; --";
    auto committed = db.begin();
    REQUIRE(committed.has_value());
    REQUIRE(repo.template update_where<bulk_live_row>(sql::col("tenant") == sql::val(7),
                                                      {{"note", sql::val(hostile)}}) == 1);
    REQUIRE(committed->commit().has_value());
    auto read = repo.template find_one_by<bulk_live_row>(sql::col("tenant") == sql::val(7));
    REQUIRE(read.has_value());
    REQUIRE(read->has_value());
    REQUIRE((*read)->note == hostile);
    REQUIRE((*read)->derived == 8);
    REQUIRE((*read)->defaulted == 11);
    int throwing_events = 0;
    db.set_observer([&](const query_event&) {
        ++throwing_events;
        throw std::runtime_error("bulk observer identity");
    });
    try {
        (void)repo.template update_where<bulk_live_row>(sql::col("tenant") == sql::val(8),
                                                        {{"active", sql::val(false)}});
        FAIL("Observer exception must propagate");
    } catch (const std::runtime_error& failure) {
        REQUIRE(std::string{failure.what()} == "bulk observer identity");
    }
    REQUIRE(throwing_events == 1);
    db.set_observer({});
    auto persisted = db.with_connection([](auto& connection) {
        return connection.execute(
            "SELECT COUNT(*) FROM bulk_predicate_rows WHERE tenant=8 AND active=FALSE");
    });
    REQUIRE(persisted.has_value());
    REQUIRE(persisted->get_string(0, 0) == "1");
}
} // namespace asterorm::test
