#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"
#include "asterorm/sql/typed_query.hpp"

namespace {
using std::chrono::milliseconds;
template <typename Value> Value timeout_take(asterorm::result<Value> result) {
    INFO((result ? "success" : result.error().message));
    REQUIRE(result.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*result);
    }
}
asterorm::pool_config pg_timeout_config() {
    const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    asterorm::pool_config config;
    config.conninfo = configured ? configured
                                 : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                   "password=orm_test sslmode=disable";
    config.max_size = 1;
    config.acquire_timeout = milliseconds{100};
    return config;
}
} // namespace

struct bulk_timeout_row {
    int id{};
    bool active{true};
    int price{1};
};
template <> struct asterorm::entity_traits<bulk_timeout_row> {
    static constexpr const char* table = "bulk_timeout_rows";
    static constexpr auto primary_key = asterorm::pk<&bulk_timeout_row::id>("id");
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&bulk_timeout_row::id>("id"),
                        asterorm::column<&bulk_timeout_row::active>("active"),
                        asterorm::column<&bulk_timeout_row::price>("price"));
};

struct bulk_timeout_archive_row : bulk_timeout_row {};
template <>
struct asterorm::entity_traits<bulk_timeout_archive_row>
    : asterorm::entity_traits<bulk_timeout_row> {
    static constexpr const char* table = "bulk_timeout_archive";
};

TEST_CASE("PG Integration: bulk predicate mutations honor pool and scoped timeouts",
          "[pg][bulk-dml][statement-timeout]") {
    const bool scoped = GENERATE(false, true);
    const int operation = GENERATE(0, 1, 2);
    auto config = pg_timeout_config();
    config.statement_timeout = milliseconds{scoped ? 1000 : 100};
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    timeout_take(db.with_connection([](auto& connection) -> asterorm::result<void> {
        for (const auto* statement : {
                 "CREATE TEMPORARY TABLE bulk_timeout_rows (id INTEGER PRIMARY KEY, active BOOLEAN "
                 "NOT NULL, price INTEGER NOT NULL)",
                 "CREATE TEMPORARY TABLE bulk_timeout_archive (id INTEGER PRIMARY KEY, active "
                 "BOOLEAN NOT NULL, price INTEGER NOT NULL)",
                 "INSERT INTO bulk_timeout_rows VALUES (1,TRUE,1)",
                 "CREATE FUNCTION pg_temp.bulk_timeout_delay() RETURNS trigger LANGUAGE plpgsql AS "
                 "$$ BEGIN PERFORM pg_sleep(0.3); IF TG_OP='DELETE' THEN RETURN OLD; END IF; "
                 "RETURN NEW; END $$",
                 "CREATE TRIGGER bulk_timeout_delay BEFORE UPDATE OR DELETE ON bulk_timeout_rows "
                 "FOR EACH ROW EXECUTE FUNCTION pg_temp.bulk_timeout_delay()",
                 "CREATE TRIGGER bulk_archive_delay BEFORE INSERT ON bulk_timeout_archive FOR EACH "
                 "ROW EXECUTE FUNCTION pg_temp.bulk_timeout_delay()",
             }) {
            auto executed = connection.execute(statement);
            if (!executed) {
                return std::unexpected(executed.error());
            }
        }
        return {};
    }));
    const auto original_pid = timeout_take(db.with_connection([](auto& connection) {
                                  return connection.execute("SELECT pg_backend_pid()");
                              })).get_int64(0, 0);
    int events = 0;
    int errors = 0;
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        if (event.error) {
            ++errors;
            REQUIRE(event.error->sqlstate == "57014");
        }
    });
    auto transaction = timeout_take(db.begin());
    auto cancel_and_rollback = [&](auto&) -> asterorm::result<void> {
        auto cancelled = [&] {
            if (operation == 2) {
                return repo.insert_select<bulk_timeout_archive_row>(
                    asterorm::sql::select({"id", "active", "price"})
                        .from("bulk_timeout_rows")
                        .where(asterorm::sql::col("id") == asterorm::sql::val(1))
                        .build());
            }
            if (operation == 1) {
                return repo.delete_where<bulk_timeout_row>(asterorm::sql::col("id") ==
                                                           asterorm::sql::val(1));
            }
            return repo.update_where<bulk_timeout_row>(
                asterorm::sql::col("id") == asterorm::sql::val(1),
                {
                    {
                        "price",
                        asterorm::sql::multiply(asterorm::sql::col("price"), asterorm::sql::val(2)),
                    },
                });
        }();
        REQUIRE_FALSE(cancelled.has_value());
        REQUIRE(cancelled.error().sqlstate == "57014");
        REQUIRE(cancelled.error().kind == asterorm::db_error_kind::query_failed);
        return transaction.rollback();
    };
    if (scoped) {
        timeout_take(db.with_statement_timeout(milliseconds{20}, cancel_and_rollback));
    } else {
        timeout_take(cancel_and_rollback(db));
    }
    REQUIRE(events == 1);
    REQUIRE(errors == 1);
    auto restored = timeout_take(db.with_connection([](auto& connection) {
        return connection.execute("SELECT pg_backend_pid(),COUNT(*) FROM bulk_timeout_rows WHERE "
                                  "active=TRUE AND price=1");
    }));
    REQUIRE(restored.get_int64(0, 0) == original_pid);
    REQUIRE(restored.get_int64(0, 1) == 1);
    auto empty_archive = timeout_take(db.with_connection([](auto& connection) {
        return connection.execute("SELECT COUNT(*) FROM bulk_timeout_archive");
    }));
    REQUIRE(empty_archive.get_int64(0, 0) == 0);
    timeout_take(db.with_connection([](auto& connection) {
        return connection.execute("DROP TRIGGER bulk_timeout_delay ON bulk_timeout_rows");
    }));
    REQUIRE(repo.update_where<bulk_timeout_row>(asterorm::sql::col("id") == asterorm::sql::val(1),
                                                {{"active", asterorm::sql::val(false)}}) == 1);
    REQUIRE(events == 2);
}

TEST_CASE("PG Integration: statement timeout adapter validates context and exact values",
          "[pg][pool-timeout]") {
    auto connection = timeout_take(asterorm::pg::driver{}.connect(pg_timeout_config().conninfo));
    REQUIRE(connection.is_transaction_idle());
    REQUIRE_FALSE(connection.set_local_statement_timeout(milliseconds{10}).has_value());
    for (auto count : {0LL, 60000LL, 2147483647LL}) {
        timeout_take(connection.set_statement_timeout(milliseconds{count}));
        REQUIRE(timeout_take(connection.statement_timeout()) == milliseconds{count});
    }
    for (auto count : {-1LL, 2147483648LL}) {
        auto invalid = connection.set_statement_timeout(milliseconds{count});
        REQUIRE_FALSE(invalid.has_value());
        REQUIRE(invalid.error().kind == asterorm::db_error_kind::query_failed);
        REQUIRE_FALSE(connection.set_local_statement_timeout(milliseconds{count}).has_value());
    }
    REQUIRE(timeout_take(connection.statement_timeout()) == milliseconds{2147483647});
    timeout_take(connection.set_statement_timeout(milliseconds{500}));
    timeout_take(connection.execute("BEGIN"));
    REQUIRE_FALSE(connection.is_transaction_idle());
    REQUIRE_FALSE(connection.set_statement_timeout(milliseconds{99}).has_value());
    timeout_take(connection.set_local_statement_timeout(milliseconds{250}));
    REQUIRE(timeout_take(connection.statement_timeout()) == milliseconds{250});
    timeout_take(connection.set_local_statement_timeout(milliseconds{100}));
    REQUIRE(timeout_take(connection.statement_timeout()) == milliseconds{100});
    timeout_take(connection.execute("COMMIT"));
    REQUIRE(timeout_take(connection.statement_timeout()) == milliseconds{500});
    timeout_take(connection.execute("BEGIN"));
    timeout_take(connection.set_local_statement_timeout(milliseconds{250}));
    REQUIRE_FALSE(connection.execute("SELECT 1/0").has_value());
    REQUIRE_FALSE(connection.is_transaction_idle());
    REQUIRE_FALSE(connection.set_local_statement_timeout(milliseconds{100}).has_value());
    auto aborted_read = connection.statement_timeout();
    REQUIRE_FALSE(aborted_read.has_value());
    REQUIRE(aborted_read.error().sqlstate == "25P02");
    timeout_take(connection.execute("ROLLBACK"));
    REQUIRE(timeout_take(connection.statement_timeout()) == milliseconds{500});
    connection.close();
    REQUIRE_FALSE(connection.is_transaction_idle());
    REQUIRE_FALSE(connection.statement_timeout().has_value());
    REQUIRE_FALSE(connection.set_statement_timeout(milliseconds{100}).has_value());
}

TEST_CASE("PG Integration: pool timeout baseline covers warmup, lazy connect and reuse",
          "[pg][pool-timeout]") {
    const auto minimum = GENERATE(0, 1);
    const auto count = GENERATE(0LL, 500LL, 2147483647LL);
    auto config = pg_timeout_config();
    config.min_size = minimum;
    config.statement_timeout = milliseconds{count};
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    REQUIRE_FALSE(pool.warmup_error().has_value());
    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    REQUIRE(timeout_take((*lease)->statement_timeout()) == milliseconds{count});
    auto pid = timeout_take((*lease)->execute("SELECT pg_backend_pid()"));
    const auto original_pid = pid.get_value(0, 0);
    timeout_take((*lease)->set_statement_timeout(milliseconds{999}));
    lease->release_to_pool();
    auto reused = pool.acquire();
    REQUIRE(reused.has_value());
    REQUIRE(timeout_take((*reused)->statement_timeout()) == milliseconds{count});
    auto current = timeout_take((*reused)->execute("SELECT pg_backend_pid()"));
    REQUIRE(current.get_value(0, 0) == original_pid);
}

TEST_CASE("PG Integration: absent pool timeout preserves borrower setting", "[pg][pool-timeout]") {
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{},
                                                         pg_timeout_config()};
    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    timeout_take((*lease)->set_statement_timeout(milliseconds{350}));
    lease->release_to_pool();
    auto reused = pool.acquire();
    REQUIRE(reused.has_value());
    REQUIRE(timeout_take((*reused)->statement_timeout()) == milliseconds{350});
}

TEST_CASE("PG Integration: configured pool discards returned active and failed transactions",
          "[pg][pool-timeout]") {
    const bool aborted = GENERATE(false, true);
    auto config = pg_timeout_config();
    config.statement_timeout = milliseconds{500};
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    auto lease = pool.acquire();
    REQUIRE(lease.has_value());
    auto pid = timeout_take((*lease)->execute("SELECT pg_backend_pid()"));
    const auto original_pid = pid.get_value(0, 0);
    timeout_take((*lease)->execute("BEGIN"));
    if (aborted) {
        REQUIRE_FALSE((*lease)->execute("SELECT 1/0").has_value());
    }
    lease->release_to_pool();
    auto rejected = pool.acquire();
    REQUIRE(rejected.has_value()); // Dirty work was discarded on return, before checkout.
    auto replacement = std::move(rejected);
    REQUIRE(replacement.has_value());
    auto current = timeout_take((*replacement)->execute("SELECT pg_backend_pid()"));
    REQUIRE(current.get_value(0, 0) != original_pid);
    REQUIRE(timeout_take((*replacement)->statement_timeout()) == milliseconds{500});
}

TEST_CASE("PG Integration: pool timeout cancels execution without extra observer events",
          "[pg][pool-timeout]") {
    auto config = pg_timeout_config();
    config.statement_timeout = milliseconds{50};
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    const auto original_pid = timeout_take(db.native_scalar<int>("SELECT pg_backend_pid()"));
    int events = 0;
    std::string observed_state;
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        if (event.error) {
            observed_state = event.error->sqlstate;
        }
    });
    auto cancelled = db.native_scalar<int>("SELECT 1 FROM pg_sleep($1)", 0.2);
    REQUIRE_FALSE(cancelled.has_value());
    REQUIRE(cancelled.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE(cancelled.error().sqlstate == "57014");
    REQUIRE(events == 1);
    REQUIRE(observed_state == "57014");
    REQUIRE(timeout_take(db.native_scalar<int>("SELECT pg_backend_pid()")) == original_pid);
    REQUIRE(events == 2);
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("PG Integration: pool baseline does not replace transaction-local values mid-lease",
          "[pg][pool-timeout]") {
    auto config = pg_timeout_config();
    config.statement_timeout = milliseconds{500};
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session db{pool};
    auto transaction = timeout_take(db.begin());
    timeout_take(db.with_connection([](auto& connection) {
        return connection.set_local_statement_timeout(milliseconds{250});
    }));
    REQUIRE(
        timeout_take(db.native_scalar<int>(
            "SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'")) ==
        250);
    REQUIRE(pool.stats().in_use == 1);
    timeout_take(transaction.commit());
    REQUIRE(pool.stats().in_use == 0);
    REQUIRE(
        timeout_take(db.native_scalar<int>(
            "SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'")) ==
        500);
}

struct scoped_timeout_row {
    int id{};
};
template <> struct asterorm::entity_traits<scoped_timeout_row> {
    static constexpr const char* table = "timeout_scope_rows";
    static constexpr auto primary_key = asterorm::pk<&scoped_timeout_row::id>("id");
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&scoped_timeout_row::id>("id"));
};

namespace {
struct pg_timeout_fixture {
    asterorm::pool_config config = [] {
        auto value = pg_timeout_config();
        value.statement_timeout = milliseconds{500};
        return value;
    }();
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    asterorm::session<decltype(pool)> db{pool};
    std::int64_t key{-std::chrono::steady_clock::now().time_since_epoch().count()};

    milliseconds current() {
        return timeout_take(
            db.with_connection([](auto& connection) { return connection.statement_timeout(); }));
    }
};
} // namespace

TEST_CASE("PG Integration: timeout scopes restore prior locals on all healthy exits",
          "[pg][timeout-scope]") {
    const int outcome = GENERATE(0, 1, 2);
    pg_timeout_fixture fixture;
    auto transaction = timeout_take(fixture.db.begin());
    timeout_take(fixture.db.with_connection([](auto& connection) {
        return connection.set_local_statement_timeout(milliseconds{250});
    }));
    int events = 0;
    fixture.db.set_observer([&](const auto&) { ++events; });
    auto body = [&](auto& session) -> asterorm::result<int> {
        REQUIRE(fixture.current() == milliseconds{200});
        timeout_take(session.with_connection([](auto& connection) {
            return connection.execute(
                "CREATE TEMP TABLE timeout_scope_rows (id INTEGER PRIMARY KEY)");
        }));
        timeout_take(session.with_connection([](auto& connection) {
            return connection.copy_in("COPY timeout_scope_rows (id) FROM STDIN", {"1"});
        }));
        auto rows = timeout_take(asterorm::sql::typed::query(
            session, asterorm::sql::typed::select<scoped_timeout_row>()));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front().id == 1);
        timeout_take(session.with_connection([](auto& connection) {
            return connection.copy_out("COPY timeout_scope_rows TO STDOUT");
        }));
        timeout_take(session.with_statement_timeout(
            milliseconds{0}, [&](auto& nested) -> asterorm::result<void> {
                REQUIRE(fixture.current() == milliseconds{0});
                REQUIRE(timeout_take(nested.template native_scalar<int>("SELECT 1")) == 1);
                return {};
            }));
        REQUIRE(fixture.current() == milliseconds{200});
        if (outcome == 1) {
            asterorm::db_error error;
            error.kind = asterorm::db_error_kind::query_failed;
            error.message = "healthy callback failed";
            return std::unexpected(std::move(error));
        }
        if (outcome == 2) {
            throw std::runtime_error("healthy callback threw");
        }
        return 42;
    };
    if (outcome == 2) {
        REQUIRE_THROWS_WITH(fixture.db.with_statement_timeout(milliseconds{200}, body),
                            "healthy callback threw");
    } else {
        auto result = fixture.db.with_statement_timeout(milliseconds{200}, body);
        if (outcome == 0) {
            REQUIRE(result.has_value());
            REQUIRE(*result == 42);
        } else {
            REQUIRE_FALSE(result.has_value());
            REQUIRE(result.error().message == "healthy callback failed");
        }
    }
    REQUIRE(events == 2); // Typed/native body queries only; no configuration/COPY/DDL events.
    REQUIRE(fixture.current() == milliseconds{250});
    REQUIRE(fixture.db.has_active_transaction());
    REQUIRE(fixture.pool.stats().in_use == 1);
    timeout_take(transaction.rollback());
    REQUIRE(fixture.current() == milliseconds{500});
}

TEST_CASE("PG Integration: completed timeout frames block work while advisory ownership continues",
          "[pg][timeout-scope]") {
    const bool rollback = GENERATE(false, true);
    pg_timeout_fixture fixture;
    auto result =
        fixture.db.with_advisory_lock(fixture.key, [&](auto& session) -> asterorm::result<void> {
            auto transaction = timeout_take(session.begin());
            timeout_take(session.with_connection([](auto& connection) {
                return connection.set_local_statement_timeout(milliseconds{250});
            }));
            timeout_take(session.with_statement_timeout(
                milliseconds{200}, [&](auto& outer) -> asterorm::result<void> {
                    return outer.with_statement_timeout(
                        milliseconds{100}, [&](auto& inner) -> asterorm::result<void> {
                            timeout_take(rollback ? transaction.rollback() : transaction.commit());
                            REQUIRE(fixture.pool.stats().in_use == 1);
                            REQUIRE_FALSE(inner.begin().has_value());
                            REQUIRE_FALSE(
                                inner.template native_scalar<int>("SELECT 1").has_value());
                            REQUIRE_THROWS_AS(inner.with_connection([](auto&) {}),
                                              std::logic_error);
                            REQUIRE_FALSE(inner
                                              .with_statement_timeout(
                                                  milliseconds{10},
                                                  [](auto&) -> asterorm::result<void> {
                                                      FAIL("completed timeout callback invoked");
                                                      return {};
                                                  })
                                              .has_value());
                            REQUIRE_FALSE(inner
                                              .with_advisory_lock(
                                                  fixture.key,
                                                  [](auto&) -> asterorm::result<void> {
                                                      FAIL("completed advisory callback invoked");
                                                      return {};
                                                  })
                                              .has_value());
                            return {};
                        });
                }));
            // The advisory pin prevents checkout reapplication from hiding a bad restore.
            REQUIRE(fixture.current() == milliseconds{500});
            auto fresh = timeout_take(session.begin());
            REQUIRE(timeout_take(session.template native_scalar<int>("SELECT 1")) == 1);
            timeout_take(fresh.commit());
            return {};
        });
    REQUIRE(result.has_value());
    REQUIRE(fixture.pool.stats().in_use == 0);
    auto next_transaction = timeout_take(fixture.db.begin());
    auto nested_cleanup = fixture.db.with_statement_timeout(milliseconds{100}, [&](auto& session) {
        return session.with_advisory_lock(fixture.key,
                                          [&](auto&) { return next_transaction.commit(); });
    });
    REQUIRE(nested_cleanup.has_value());
    REQUIRE(fixture.pool.stats().in_use == 0);
}

TEST_CASE(
    "PG Integration: scoped server timeouts cover native, typed, DDL, COPY and advisory waits",
    "[pg][timeout-scope]") {
    const int path = GENERATE(0, 1, 2, 3, 4, 5);
    pg_timeout_fixture fixture;
    auto peer = timeout_take(asterorm::pg::driver{}.connect(pg_timeout_config().conninfo));
    timeout_take(peer.set_statement_timeout(milliseconds{3000}));
    if (path == 4) {
        timeout_take(peer.advisory_lock(fixture.key));
    }
    const auto original_pid =
        timeout_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()"));
    auto transaction = timeout_take(fixture.db.begin());
    if (path == 1) {
        timeout_take(fixture.db.with_connection([](auto& connection) {
            return connection.execute(
                "CREATE TEMP VIEW timeout_scope_rows AS SELECT 1::int AS id FROM pg_sleep(0.2)");
        }));
    }
    if (path == 5) {
        timeout_take(fixture.db.with_connection([](auto& connection) {
            return connection.execute(
                "CREATE TEMP TABLE timeout_scope_rows (id INTEGER PRIMARY KEY)");
        }));
        timeout_take(fixture.db.with_connection([](auto& connection) {
            return connection.execute(
                "CREATE FUNCTION pg_temp.timeout_scope_delay() RETURNS trigger LANGUAGE plpgsql "
                "AS $$ BEGIN PERFORM pg_catalog.pg_sleep(0.2); RETURN NEW; END $$");
        }));
        timeout_take(fixture.db.with_connection([](auto& connection) {
            return connection.execute(
                "CREATE TRIGGER timeout_scope_delay BEFORE INSERT ON timeout_scope_rows "
                "FOR EACH ROW EXECUTE FUNCTION pg_temp.timeout_scope_delay()");
        }));
    }
    int events = 0;
    std::string observed_state;
    fixture.db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        if (event.error) {
            observed_state = event.error->sqlstate;
        }
    });
    auto result = fixture.db.with_statement_timeout(
        milliseconds{40}, [&](auto& session) -> asterorm::result<void> {
            REQUIRE(fixture.current() == milliseconds{40});
            if (path == 0) {
                auto query = session.template native_scalar<int>("SELECT 1 FROM pg_sleep($1)", 0.2);
                if (!query) {
                    return std::unexpected(query.error());
                }
            } else if (path == 1) {
                auto query = asterorm::sql::typed::query(
                    session, asterorm::sql::typed::select<scoped_timeout_row>());
                if (!query) {
                    return std::unexpected(query.error());
                }
            } else if (path == 2) {
                auto query = session.with_connection([](auto& connection) {
                    return connection.execute("CREATE TEMP TABLE timeout_scope_created AS SELECT 1 "
                                              "AS id FROM pg_sleep(0.2)");
                });
                if (!query) {
                    return std::unexpected(query.error());
                }
            } else if (path == 3) {
                auto query = session.with_connection([](auto& connection) {
                    return connection.copy_out("COPY (SELECT 1 FROM pg_sleep(0.2)) TO STDOUT");
                });
                if (!query) {
                    return std::unexpected(query.error());
                }
            } else if (path == 4) {
                return session.with_advisory_lock(fixture.key, [](auto&) -> asterorm::result<void> {
                    FAIL("contended lock acquired");
                    return {};
                });
            } else {
                auto query = session.with_connection([](auto& connection) {
                    return connection.copy_in("COPY timeout_scope_rows (id) FROM STDIN", {"1"});
                });
                if (!query) {
                    return std::unexpected(query.error());
                }
            }
            return {};
        });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE(result.error().sqlstate == "57014");
    REQUIRE(result.error().detail.has_value());
    REQUIRE(result.error().detail->contains("cleanup"));
    REQUIRE(events == (path < 2 ? 1 : 0));
    if (path < 2) {
        REQUIRE(observed_state == "57014");
    }
    REQUIRE_FALSE(fixture.db.has_active_transaction());
    REQUIRE(fixture.pool.stats().total == 0);
    fixture.db.set_observer({});
    REQUIRE(timeout_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()")) != original_pid);
    auto fresh = timeout_take(fixture.db.begin());
    REQUIRE_FALSE(transaction.rollback().has_value());
    timeout_take(fresh.commit());
    if (path == 4) {
        timeout_take(peer.advisory_unlock(fixture.key));
    }
}

TEST_CASE("PG Integration: explicit rollback inside timeout callback recovers without discard",
          "[pg][timeout-scope]") {
    pg_timeout_fixture fixture;
    const auto original_pid =
        timeout_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()"));
    auto transaction = timeout_take(fixture.db.begin());
    auto result = fixture.db.with_statement_timeout(
        milliseconds{40}, [&](auto& session) -> asterorm::result<void> {
            auto failed = session.template native_scalar<int>("SELECT 1 FROM pg_sleep($1)", 0.2);
            REQUIRE_FALSE(failed.has_value());
            timeout_take(transaction.rollback());
            return std::unexpected(failed.error());
        });
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().sqlstate == "57014");
    REQUIRE(timeout_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()")) == original_pid);
    REQUIRE(fixture.current() == milliseconds{500});
}

TEST_CASE("PG Integration: snapshot failure and savepoint rejection preserve caller ownership",
          "[pg][timeout-scope]") {
    pg_timeout_fixture fixture;
    const auto original_pid =
        timeout_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()"));
    auto transaction = timeout_take(fixture.db.begin());
    timeout_take(fixture.db.savepoint("before_scope"));
    timeout_take(fixture.db.with_statement_timeout(
        milliseconds{100}, [&](auto& session) -> asterorm::result<void> {
            timeout_take(session.savepoint("inside_scope"));
            timeout_take(session.release_savepoint("inside_scope"));
            REQUIRE_FALSE(session.rollback_to_savepoint("before_scope").has_value());
            REQUIRE(fixture.current() == milliseconds{100});
            REQUIRE(timeout_take(session.template native_scalar<int>("SELECT 1")) == 1);
            return {};
        }));
    timeout_take(fixture.db.rollback_to_savepoint("before_scope"));
    REQUIRE_FALSE(fixture.db.native_scalar<int>("SELECT 1/0").has_value());
    bool called = false;
    auto rejected =
        fixture.db.with_statement_timeout(milliseconds{100}, [&](auto&) -> asterorm::result<void> {
            called = true;
            return {};
        });
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(rejected.error().sqlstate == "25P02");
    REQUIRE_FALSE(called);
    REQUIRE(fixture.db.has_active_transaction());
    REQUIRE(fixture.pool.stats().in_use == 1);
    timeout_take(transaction.rollback());
    REQUIRE(timeout_take(fixture.db.native_scalar<int>("SELECT pg_backend_pid()")) == original_pid);
}

TEST_CASE("PG Integration: timeout observer exceptions preserve identity on healthy and aborted "
          "transactions",
          "[pg][timeout-scope]") {
    const bool aborted = GENERATE(false, true);
    pg_timeout_fixture fixture;
    auto transaction = timeout_take(fixture.db.begin());
    fixture.db.set_observer(
        [](const auto&) { throw std::runtime_error("timeout observer threw"); });
    bool called = false;
    REQUIRE_THROWS_WITH(
        fixture.db.with_statement_timeout(milliseconds{100},
                                          [&](auto& session) -> asterorm::result<void> {
                                              called = true;
                                              (void)session.template native_scalar<int>(
                                                  aborted ? "SELECT 1/0" : "SELECT 1");
                                              return {};
                                          }),
        "timeout observer threw");
    REQUIRE(called);
    fixture.db.set_observer({});
    if (aborted) {
        REQUIRE(fixture.pool.stats().total == 0);
        REQUIRE_FALSE(fixture.db.has_active_transaction());
        REQUIRE_FALSE(transaction.rollback().has_value());
    } else {
        REQUIRE(fixture.db.has_active_transaction());
        timeout_take(transaction.commit());
    }
    REQUIRE(fixture.current() == milliseconds{500});
}
