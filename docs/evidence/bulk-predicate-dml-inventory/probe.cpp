#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "asterorm/mysql/driver.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/sqlite/driver.hpp"

struct InventoryItem {
    int id{};
    int price{};
    bool active{true};
};

template <> struct asterorm::entity_traits<InventoryItem> {
    static constexpr const char* table = "bulk_inventory_source";
    static constexpr auto primary_key = asterorm::pk<&InventoryItem::id>("id");
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&InventoryItem::id>("id"),
                        asterorm::column<&InventoryItem::price>("price"),
                        asterorm::column<&InventoryItem::active>("active"));
};

namespace {
template <typename Repository>
concept has_update_where =
    requires(Repository& repo, asterorm::sql::predicate_ast predicate,
             std::vector<std::pair<std::string, asterorm::sql::value_source>> assignments) {
        repo.template update_where<InventoryItem>(std::move(predicate), std::move(assignments));
    };
template <typename Repository>
concept has_delete_where = requires(Repository& repo, asterorm::sql::predicate_ast predicate) {
    repo.template delete_where<InventoryItem>(std::move(predicate));
};
template <typename Repository>
concept has_insert_select = requires(Repository& repo, asterorm::sql::select_ast source) {
    repo.template insert_select<InventoryItem>(std::move(source));
};
using PgSession = asterorm::session<asterorm::connection_pool<asterorm::pg::driver>>;
using PgRepository = asterorm::repository<PgSession>;
static_assert(!has_update_where<PgRepository>);
static_assert(!has_delete_where<PgRepository>);
static_assert(!has_insert_select<PgRepository>);
static_assert(std::variant_size_v<asterorm::sql::value_source> == 2);
static_assert(!std::is_constructible_v<asterorm::sql::value_source, asterorm::sql::column_expr>);

void check(bool condition, const std::string& description) {
    if (!condition) {
        throw std::runtime_error(description);
    }
}

template <typename Value> Value take(asterorm::result<Value> outcome) {
    if (!outcome) {
        // Connection settings and vendor diagnostics are intentionally not printed.
        throw std::runtime_error("Inventory operation failed; check local test services");
    }
    return std::move(*outcome);
}

void take(asterorm::result<void> outcome) {
    if (!outcome) {
        throw std::runtime_error("Inventory operation failed; check local test services");
    }
}

void inspect_compiler() {
    namespace sql = asterorm::sql;
    for (auto dialect : {sql::dialect::postgres, sql::dialect::sqlite, sql::dialect::mysql}) {
        sql::compiler compiler{dialect};
        auto mutation = sql::update("bulk_inventory_source")
                            .set("price", sql::val(50))
                            .set("active", sql::val(false))
                            .where(sql::col("id") > sql::val(0))
                            .build();
        auto compiled = take(compiler.compile(mutation));
        check(compiled.params.size() == 3 && compiled.params[0] == "50" &&
                  compiled.params[1].type == asterorm::parameter_type::boolean &&
                  compiled.params[2] == "0",
              "Assignment parameters must precede predicate parameters");
        if (dialect == sql::dialect::mysql) {
            check(compiled.sql ==
                      "UPDATE `bulk_inventory_source` SET `price` = ?, `active` = ? WHERE `id` > ?",
                  "MySQL placeholder/identifier shape");
        } else {
            check(compiled.sql == "UPDATE \"bulk_inventory_source\" SET \"price\" = $1, \"active\" "
                                  "= $2 WHERE \"id\" > $3",
                  "PostgreSQL/SQLite placeholder/identifier shape");
        }
        // These are current gaps, recorded without executing invalid statements.
        check(compiler.compile(sql::update("bulk_inventory_source").build()).has_value(),
              "Inventory changed: empty assignment list now rejects");
        check(
            compiler
                .compile(sql::update("bulk_inventory_source").set("unmapped", sql::val(1)).build())
                .has_value(),
            "The compiler is schema-independent");
        sql::insert_ast missing_source;
        missing_source.table = "bulk_inventory_archive";
        check(!compiler.compile(missing_source), "INSERT has no SELECT source today");
    }
    std::cout << "compiler: 3 dialects; ordered typed parameters; empty SET/mapping gaps; no "
                 "INSERT SELECT source\n";
}

template <typename Driver>
void inspect_backend(const std::string& backend, Driver driver, const std::string& conninfo) {
    namespace sql = asterorm::sql;
    asterorm::pool_config config;
    config.min_size = 1;
    config.max_size = 1;
    config.conninfo = conninfo;
    asterorm::connection_pool<Driver> pool{driver, config};
    asterorm::session db{pool};
    take(db.with_connection([&](auto& connection) -> asterorm::result<void> {
        auto version = take(connection.execute(backend == "sqlite" ? "SELECT sqlite_version()"
                                                                   : "SELECT version()"));
        auto version_text = version.get_string(0, 0);
        check(version_text.has_value(), backend + ": server version");
        std::cout << backend << ": version=" << *version_text << '\n';
        for (const auto& statement : {
                 "CREATE TEMPORARY TABLE bulk_inventory_source (id INTEGER PRIMARY KEY, price "
                 "INTEGER NOT NULL, active BOOLEAN NOT NULL)",
                 "CREATE TEMPORARY TABLE bulk_inventory_archive (id INTEGER PRIMARY KEY, price "
                 "INTEGER NOT NULL, active BOOLEAN NOT NULL)",
                 "INSERT INTO bulk_inventory_source VALUES (1, 10, TRUE), (2, 20, TRUE)",
             }) {
            auto outcome = connection.execute(statement);
            if (!outcome) {
                return std::unexpected(outcome.error());
            }
        }
        return {};
    }));
    int events = 0;
    int error_events = 0;
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        if (event.error) {
            ++error_events;
        }
    });
    sql::compiler compiler{sql::dialect_for_session<decltype(db)>()};
    auto transaction = take(db.begin());
    take(db.with_connection([&](auto& connection) -> asterorm::result<void> {
        auto mutation = take(compiler.compile(sql::update("bulk_inventory_source")
                                                  .set("active", sql::val(false))
                                                  .where(sql::col("id") > sql::val(0))
                                                  .build()));
        for (int execution = 0; execution < 2; ++execution) {
            auto changed = take(db.observed_execute(connection, mutation.sql, mutation.params));
            check(changed.affected_rows() == 2, backend + ": update/no-change count");
        }
        auto no_match = take(compiler.compile(sql::update("bulk_inventory_source")
                                                  .set("price", sql::val(50))
                                                  .where(sql::col("id") > sql::val(99))
                                                  .build()));
        check(
            take(db.observed_execute(connection, no_match.sql, no_match.params)).affected_rows() ==
                0,
            backend + ": no-match count");
        auto removal = take(compiler.compile(sql::delete_from("bulk_inventory_source")
                                                 .where(sql::col("id") == sql::val(1))
                                                 .build()));
        check(take(db.observed_execute(connection, removal.sql, removal.params)).affected_rows() ==
                  1,
              backend + ": delete count");
        auto source = take(compiler.compile(sql::select({"id", "price", "active"})
                                                .from("bulk_inventory_source")
                                                .where(sql::col("id") > sql::val(0))
                                                .build()));
        // Native fixed prefix proves server support, not a future repository API.
        const std::string archive =
            "INSERT INTO bulk_inventory_archive (id, price, active) " + source.sql;
        check(take(db.observed_execute(connection, archive, source.params)).affected_rows() == 1,
              backend + ": native INSERT SELECT count");
        auto duplicate = db.observed_execute(connection, archive, source.params);
        check(!duplicate, backend + ": duplicate must fail");
        check(duplicate.error().kind == asterorm::db_error_kind::constraint_violation,
              backend + ": duplicate constraint diagnostic");
        return {};
    }));
    take(transaction.rollback());
    check(events == 6 && error_events == 1, backend + ": one observed event per execution");
    take(db.with_connection([&](auto& connection) -> asterorm::result<void> {
        auto restored = take(
            connection.execute("SELECT COUNT(*) FROM bulk_inventory_source WHERE active = TRUE AND "
                               "((id = 1 AND price = 10) OR (id = 2 AND price = 20))"));
        auto archived = take(connection.execute("SELECT COUNT(*) FROM bulk_inventory_archive"));
        check(restored.get_string(0, 0) == "2" && archived.get_string(0, 0) == "0",
              backend + ": caller rollback restores both tables");
        return {};
    }));
    std::cout << backend
              << ": update=2; unchanged=2; no-match=0; delete=1; native-insert-select=1; "
                 "events=6/errors=1; rollback=restored\n";
}

std::string configured(const char* name, const char* fallback) {
    const auto* value = std::getenv(name);
    return value ? value : fallback;
}
} // namespace

int main() {
    try {
        inspect_compiler();
        inspect_backend("sqlite", asterorm::sqlite::driver{}, ":memory:");
        inspect_backend("postgres", asterorm::pg::driver{},
                        configured("ASTERORM_TEST_CONNINFO",
                                   "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                   "password=orm_test sslmode=disable"));
        inspect_backend(
            "mysql", asterorm::mysql::driver{},
            configured("ASTERORM_MYSQL_TEST_CONNINFO",
                       "host=127.0.0.1 port=3307 dbname=orm_test user=orm_test password=orm_test"));
        std::cout << "Inventory probe passed; proposed repository methods remain unimplemented\n";
        return 0;
    } catch (const std::exception& failure) {
        std::cerr << failure.what() << '\n';
        return 1;
    }
}
