#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "asterorm/repository.hpp"
#include "asterorm/sql/typed_query.hpp"

struct stream_entity {
    int id{};
};
template <> struct asterorm::entity_traits<stream_entity> {
    static constexpr const char* table = "stream_rows";
    static constexpr auto columns = std::make_tuple(asterorm::column<&stream_entity::id>("id"));
};

namespace {
using asterorm::detail::session_stream_access;
using std::chrono::milliseconds;

asterorm::db_error reader_error() {
    return {
        .kind = asterorm::db_error_kind::constraint_violation,
        .sqlstate = "23505",
        .message = "reader diagnostic",
        .detail = "original detail",
    };
}

struct reader_state {
    int connects{};
    int closes{};
    int queries{};
    int controls{};
    bool fail_connect{};
    std::vector<std::string> commands;
};

struct reader_result {
    std::string text{"1"};
    std::string tag;
    [[nodiscard]] int rows() const {
        return 1;
    }
    [[nodiscard]] int columns() const {
        return 1;
    }
    [[nodiscard]] std::string_view command_tag() const {
        return tag;
    }
    [[nodiscard]] std::string_view column_name([[maybe_unused]] int column) const {
        return "id";
    }
    [[nodiscard]] std::optional<std::string> get_string([[maybe_unused]] int row,
                                                        [[maybe_unused]] int column) const {
        return text;
    }
};

struct reader_connection {
    reader_state* state;
    int id{};
    bool open{true};
    bool transaction{};
    [[nodiscard]] bool is_open() const {
        return open;
    }
    [[nodiscard]] bool is_transaction_idle() const noexcept {
        return !transaction;
    }
    void close() noexcept {
        if (open) {
            ++state->closes;
            open = false;
            transaction = false;
        }
    }
    asterorm::result<reader_result> execute(std::string_view sql) {
        state->commands.emplace_back(sql);
        if (sql.starts_with("BEGIN")) {
            transaction = true;
        } else if (sql == "COMMIT" || sql == "ROLLBACK") {
            transaction = false;
        }
        return reader_result{.text = std::to_string(id), .tag = std::string{sql}};
    }
    [[nodiscard]] asterorm::result<reader_result>
    execute_prepared(std::string_view sql,
                     [[maybe_unused]] const asterorm::query_parameters& parameters) const {
        ++state->queries;
        if (sql == "deadlock") {
            auto error = reader_error();
            error.kind = asterorm::db_error_kind::deadlock_detected;
            error.sqlstate = "40P01";
            return std::unexpected(std::move(error));
        }
        return reader_result{.text = std::to_string(id), .tag = {}};
    }
    [[nodiscard]] asterorm::result<milliseconds> statement_timeout() const {
        ++state->controls;
        return milliseconds{100};
    }
    [[nodiscard]] asterorm::result<void>
    set_statement_timeout([[maybe_unused]] milliseconds duration) const {
        ++state->controls;
        return {};
    }
    [[nodiscard]] asterorm::result<void>
    set_local_statement_timeout([[maybe_unused]] milliseconds duration) const {
        ++state->controls;
        return {};
    }
    [[nodiscard]] asterorm::result<void> advisory_lock([[maybe_unused]] std::int64_t key) const {
        ++state->controls;
        return {};
    }
    [[nodiscard]] asterorm::result<void> advisory_unlock([[maybe_unused]] std::int64_t key) const {
        ++state->controls;
        return {};
    }
};

struct reader_driver {
    reader_state* state;
    [[nodiscard]] asterorm::result<reader_connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        if (state->fail_connect) {
            return std::unexpected(reader_error());
        }
        return reader_connection{.state = state, .id = ++state->connects};
    }
};

struct reader_fixture {
    reader_state state;
    asterorm::pool_config config = [] {
        asterorm::pool_config options;
        options.min_size = 0;
        options.max_size = 2;
        options.acquire_timeout = milliseconds{20};
        return options;
    }();
    asterorm::connection_pool<reader_driver> pool{reader_driver{&state}, config};
    asterorm::session<decltype(pool)> db{pool};
};

template <typename Value> Value take(asterorm::result<Value> outcome) {
    REQUIRE(outcome.has_value());
    return std::move(*outcome);
}

void rejected(const auto& outcome) {
    REQUIRE_FALSE(outcome.has_value());
    REQUIRE(outcome.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE(outcome.error().sqlstate.empty());
}

TEST_CASE("Stream reservation pins one connection independently of other sessions",
          "[core][stream]") {
    reader_fixture fixture;
    auto reservation = take(session_stream_access::acquire(fixture.db));
    static_assert(!std::is_copy_constructible_v<decltype(reservation)>);
    static_assert(std::is_nothrow_move_constructible_v<decltype(reservation)>);
    static_assert(std::is_nothrow_move_assignable_v<decltype(reservation)>);
    REQUIRE(fixture.pool.stats().in_use == 1);
    auto moved = std::move(reservation);
    // Deliberately exercise the documented moved-from error path.
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    rejected(reservation.with_connection([](auto&) -> asterorm::result<void> {
        FAIL("Moved-from reservation entered its callback");
        return {};
    }));
    auto identity =
        moved.with_connection([](auto& connection) -> asterorm::result<std::unique_ptr<int>> {
            return std::make_unique<int>(connection.id);
        });
    REQUIRE(identity.has_value());
    REQUIRE(**identity == 1);
    asterorm::session peer{fixture.pool};
    REQUIRE(peer.native_scalar<int>("SELECT 1") == 2);
    REQUIRE(fixture.pool.stats().in_use == 1);
    REQUIRE(moved.complete().has_value());
    REQUIRE(moved.complete().has_value());
    REQUIRE(fixture.pool.stats().in_use == 0);
    REQUIRE(fixture.state.closes == 0);
}

TEST_CASE("Stream reservation blocks ordinary session entry points before work", "[core][stream]") {
    reader_fixture fixture;
    auto transaction = take(fixture.db.begin());
    auto reservation = take(session_stream_access::acquire(fixture.db, true));
    int events = 0;
    fixture.db.set_observer([&](const auto&) { ++events; });
    rejected(session_stream_access::acquire(fixture.db));
    rejected(fixture.db.begin());
    rejected(transaction.commit());
    rejected(transaction.rollback());
    rejected(fixture.db.commit_transaction());
    rejected(fixture.db.rollback_transaction());
    rejected(fixture.db.savepoint("point"));
    rejected(fixture.db.release_savepoint("point"));
    rejected(fixture.db.rollback_to_savepoint("point"));
    rejected(fixture.db.with_advisory_lock(1, [](auto&) -> asterorm::result<void> {
        FAIL("Busy advisory callback ran");
        return {};
    }));
    rejected(
        fixture.db.with_statement_timeout(milliseconds{1}, [](auto&) -> asterorm::result<void> {
            FAIL("Busy timeout callback ran");
            return {};
        }));
    rejected(fixture.db.with_connection([](auto&) -> asterorm::result<void> {
        FAIL("Busy connection callback ran");
        return {};
    }));
    REQUIRE_THROWS_AS(fixture.db.with_connection([](auto&) { FAIL("Busy void callback ran"); }),
                      std::logic_error);
    reader_connection external{.state = &fixture.state};
    rejected(fixture.db.observed_execute(external, "SELECT 1", {}));
    rejected(fixture.db.native<stream_entity>("SELECT id FROM stream_rows"));
    rejected(
        asterorm::sql::typed::query(fixture.db, asterorm::sql::typed::select<stream_entity>()));
    asterorm::repository repo{fixture.db};
    rejected(repo.find_all<stream_entity>());
    REQUIRE(fixture.db.has_active_transaction());
    REQUIRE(fixture.state.connects == 1);
    REQUIRE(fixture.state.commands.size() == 1); // BEGIN only
    REQUIRE(fixture.state.queries == 0);
    REQUIRE(fixture.state.controls == 0);
    REQUIRE(events == 0);
    REQUIRE(reservation.complete().has_value());
    REQUIRE(fixture.pool.stats().in_use == 1); // transaction pin survives
    REQUIRE(transaction.commit().has_value());
    REQUIRE(fixture.pool.stats().in_use == 0);
}

TEST_CASE("Stream reservation admission respects transactions scopes and callbacks",
          "[core][stream]") {
    reader_fixture fixture;
    rejected(session_stream_access::acquire(fixture.db, true));
    REQUIRE(fixture.state.connects == 0);
    REQUIRE(fixture.db
                .with_connection([&](auto&) -> asterorm::result<void> {
                    rejected(session_stream_access::acquire(fixture.db));
                    return {};
                })
                .has_value());
    REQUIRE(fixture.db
                .with_advisory_lock(1,
                                    [&](auto&) -> asterorm::result<void> {
                                        rejected(session_stream_access::acquire(fixture.db));
                                        return {};
                                    })
                .has_value());
    auto transaction = take(fixture.db.begin());
    REQUIRE(fixture.db
                .with_statement_timeout(milliseconds{1},
                                        [&](auto&) -> asterorm::result<void> {
                                            rejected(session_stream_access::acquire(fixture.db));
                                            return {};
                                        })
                .has_value());
    fixture.db.set_observer(
        [&](const auto&) { rejected(session_stream_access::acquire(fixture.db)); });
    REQUIRE(fixture.db.native_scalar<int>("SELECT 1") == 1);
    REQUIRE(fixture.db.native_scalar<int>("deadlock").error().sqlstate == "40P01");
    rejected(session_stream_access::acquire(fixture.db));
    REQUIRE(transaction.rollback().has_value());
    REQUIRE(fixture.state.connects == 1);
    auto reservation = take(session_stream_access::acquire(fixture.db));
    REQUIRE(reservation.complete().has_value());
}

TEST_CASE("Stream reservation abandonment discards without SQL or observers", "[core][stream]") {
    reader_fixture fixture;
    fixture.db.set_observer([](const auto&) { FAIL("Abandonment invoked an observer"); });
    {
        auto reservation = take(session_stream_access::acquire(fixture.db));
        REQUIRE(fixture.pool.stats().in_use == 1);
    }
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.state.commands.empty());
    REQUIRE(fixture.pool.stats().in_use == 0);
    REQUIRE(fixture.pool.stats().total == 0);
    auto replacement = take(session_stream_access::acquire(fixture.db));
    REQUIRE(fixture.state.connects == 2);
    REQUIRE(replacement.complete().has_value());
}

TEST_CASE("Stream reservation move replacement discards its previous owner", "[core][stream]") {
    reader_fixture first;
    reader_fixture second;
    auto destination = take(session_stream_access::acquire(first.db));
    auto source = take(session_stream_access::acquire(second.db));
    destination = std::move(source);
    REQUIRE(first.state.closes == 1);
    REQUIRE(first.pool.stats().in_use == 0);
    REQUIRE(second.pool.stats().in_use == 1);
    REQUIRE(destination.complete().has_value());
    REQUIRE(second.state.closes == 0);
}

TEST_CASE("Reader errors and exceptions preserve diagnostics while releasing ownership",
          "[core][stream]") {
    const bool throwing = GENERATE(false, true);
    reader_fixture fixture;
    auto old_transaction = take(fixture.db.begin());
    auto old_reservation = take(session_stream_access::acquire(fixture.db));
    auto read = [&] -> asterorm::result<void> {
        return old_reservation.with_connection([&](auto&) -> asterorm::result<void> {
            if (throwing) {
                throw std::runtime_error("reader exception identity");
            }
            return std::unexpected(reader_error());
        });
    };
    if (throwing) {
        REQUIRE_THROWS_WITH(read(), "reader exception identity");
    } else {
        auto failure = read();
        REQUIRE_FALSE(failure.has_value());
        REQUIRE(failure.error().sqlstate == "23505");
        REQUIRE(failure.error().message == "reader diagnostic");
        REQUIRE(failure.error().detail == "original detail");
    }
    REQUIRE_FALSE(fixture.db.has_active_transaction());
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.pool.stats().in_use == 0);
    auto fresh_transaction = take(fixture.db.begin());
    auto fresh_reservation = take(session_stream_access::acquire(fixture.db));
    rejected(old_reservation.with_connection([](auto&) -> asterorm::result<void> {
        FAIL("Stale reader entered its callback");
        return {};
    }));
    rejected(old_reservation.complete());
    rejected(old_transaction.commit());
    old_reservation.discard();
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fresh_reservation.complete().has_value());
    rejected(old_transaction.rollback());
    REQUIRE(fresh_transaction.commit().has_value());
}

TEST_CASE("Transaction guard destruction invalidates a live reader without rollback SQL",
          "[core][stream]") {
    reader_fixture fixture;
    using guard_type = asterorm::transaction_guard<decltype(fixture.db)>;
    std::optional<guard_type> transaction{take(fixture.db.begin())};
    auto reservation = take(session_stream_access::acquire(fixture.db));
    transaction.reset();
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fixture.state.commands.size() == 1);
    REQUIRE_FALSE(fixture.db.has_active_transaction());
    REQUIRE(fixture.pool.stats().in_use == 0);
    auto fresh = take(session_stream_access::acquire(fixture.db));
    rejected(reservation.complete());
    reservation.discard();
    REQUIRE(fixture.state.closes == 1);
    REQUIRE(fresh.complete().has_value());
}

TEST_CASE("Transaction guard move replacement abandons its associated reader", "[core][stream]") {
    reader_fixture first;
    reader_fixture second;
    auto replaced = take(first.db.begin());
    auto incoming = take(second.db.begin());
    auto reader = take(session_stream_access::acquire(first.db));
    replaced = std::move(incoming);
    REQUIRE(first.state.closes == 1);
    REQUIRE_FALSE(first.db.has_active_transaction());
    rejected(reader.complete());
    REQUIRE(second.db.has_active_transaction());
    REQUIRE(replaced.rollback().has_value());
}

TEST_CASE("Stream callbacks cannot reenter or release their live connection", "[core][stream]") {
    reader_fixture fixture;
    auto reservation = take(session_stream_access::acquire(fixture.db));
    REQUIRE(reservation
                .with_connection([&](auto&) -> asterorm::result<void> {
                    rejected(reservation.with_connection([](auto&) -> asterorm::result<void> {
                        FAIL("Reentrant reader callback ran");
                        return {};
                    }));
                    rejected(reservation.complete());
                    return {};
                })
                .has_value());
    auto invalidated = reservation.with_connection([&](auto& connection) -> asterorm::result<void> {
        reservation.discard();
        REQUIRE_FALSE(connection.is_open()); // object retained until callback returns
        REQUIRE(fixture.pool.stats().in_use == 1);
        rejected(fixture.db.with_connection([](auto&) -> asterorm::result<void> {
            FAIL("Ordinary work entered a deferred release");
            return {};
        }));
        return {};
    });
    rejected(invalidated);
    REQUIRE(fixture.pool.stats().in_use == 0);
    REQUIRE(fixture.state.closes == 1);
}

TEST_CASE("Stream acquisition failure leaves the session available", "[core][stream]") {
    reader_fixture fixture;
    fixture.state.fail_connect = true;
    auto failed = session_stream_access::acquire(fixture.db);
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().sqlstate == "23505");
    REQUIRE(fixture.pool.stats().in_use == 0);
    fixture.state.fail_connect = false;
    auto reservation = take(session_stream_access::acquire(fixture.db));
    REQUIRE(reservation.complete().has_value());
}

TEST_CASE("Stream admission from a direct observer cannot borrow another connection",
          "[core][stream]") {
    reader_fixture fixture;
    int events = 0;
    fixture.db.set_observer([&](const auto&) {
        ++events;
        rejected(session_stream_access::acquire(fixture.db));
    });
    reader_connection external{.state = &fixture.state};
    REQUIRE(fixture.db.observed_execute(external, "SELECT 1", {}).has_value());
    REQUIRE(events == 1);
    REQUIRE(fixture.state.connects == 0);
    auto reservation = take(session_stream_access::acquire(fixture.db));
    REQUIRE(reservation.complete().has_value());
}

TEST_CASE("Transaction abandonment inside a reader callback defers lease release",
          "[core][stream]") {
    reader_fixture fixture;
    using guard_type = asterorm::transaction_guard<decltype(fixture.db)>;
    std::optional<guard_type> transaction{take(fixture.db.begin())};
    auto reservation = take(session_stream_access::acquire(fixture.db));
    auto outcome = reservation.with_connection([&](auto& connection) -> asterorm::result<void> {
        transaction.reset();
        REQUIRE_FALSE(connection.is_open());
        REQUIRE(fixture.pool.stats().in_use == 1);
        rejected(session_stream_access::acquire(fixture.db));
        return {};
    });
    rejected(outcome);
    REQUIRE(fixture.pool.stats().in_use == 0);
    REQUIRE(fixture.state.commands.size() == 1);
    REQUIRE(fixture.state.closes == 1);
}

struct unsafe_reader_connection {
    [[nodiscard]] bool is_open() const {
        return true;
    }
    void close() {} // deliberately does not offer the noexcept cleanup contract
};
struct unsafe_reader_driver {
    reader_state* state;
    [[nodiscard]] asterorm::result<unsafe_reader_connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        ++state->connects;
        return unsafe_reader_connection{};
    }
};

TEST_CASE("Stream reservation requires nonthrowing cleanup before acquiring", "[core][stream]") {
    static_assert(!asterorm::detail::has_stream_cleanup<unsafe_reader_connection>);
    reader_state state;
    asterorm::pool_config config;
    config.min_size = 0;
    asterorm::connection_pool<unsafe_reader_driver> pool{unsafe_reader_driver{&state}, config};
    asterorm::session db{pool};
    rejected(session_stream_access::acquire(db));
    REQUIRE(state.connects == 0);
}

TEST_CASE("Stream admission recovers after ordinary callback and observer exceptions",
          "[core][stream]") {
    reader_fixture fixture;
    REQUIRE_THROWS_WITH(fixture.db.with_connection([](auto&) -> asterorm::result<void> {
        throw std::runtime_error("ordinary callback identity");
    }),
                        "ordinary callback identity");
    fixture.db.set_observer([](const auto&) { throw std::runtime_error("observer identity"); });
    REQUIRE_THROWS_WITH(fixture.db.native_scalar<int>("SELECT 1"), "observer identity");
    fixture.db.set_observer({});
    auto reservation = take(session_stream_access::acquire(fixture.db));
    REQUIRE(reservation.complete().has_value());
}

TEST_CASE("Stream ownership does not hold the session mutex between reader calls",
          "[core][stream]") {
    reader_fixture fixture;
    auto reservation = take(session_stream_access::acquire(fixture.db));
    auto peer_call = std::async(std::launch::async, [&] {
        return fixture.db.with_connection([](auto&) -> asterorm::result<void> {
            // A different diagnostic distinguishes an incorrectly invoked callback.
            return std::unexpected(reader_error());
        });
    });
    const auto readiness = peer_call.wait_for(std::chrono::seconds{1});
    // Release ownership before assertions so even a failed ready check can unwind.
    REQUIRE(reservation.complete().has_value());
    REQUIRE(readiness == std::future_status::ready);
    rejected(peer_call.get());
    REQUIRE(fixture.pool.stats().in_use == 0);
}
} // namespace
