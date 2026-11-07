#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "asterorm/testing/transactional_session.hpp"

namespace {
using namespace std::chrono_literals;
namespace sql = asterorm::sql;
asterorm::db_error fault() {
    return {
        .kind = asterorm::db_error_kind::query_failed,
        .sqlstate = "XX001",
        .message = "control identity",
        .detail = "control detail",
    };
}
struct state {
    int connects{};
    int closes{};
    int controls{};
    int fail_at{-1};
    int throw_at{-1};
    bool fail_connect{};
    bool native_transaction{};
    bool bad_final_idle{};
    std::exception_ptr exception;
    std::vector<std::string> commands;
    std::function<void(std::string_view)> on_execute;
};
struct physical {
    state* observed;
    bool open{true};
    bool transaction{};
    std::string last;
};
struct native_result {
    std::string tag;
    [[nodiscard]] int rows() const {
        return 1;
    }
    [[nodiscard]] int columns() const {
        return 1;
    }
    [[nodiscard]] std::string_view column_name([[maybe_unused]] int column) const {
        return "value";
    }
    [[nodiscard]] std::optional<std::string> get_string([[maybe_unused]] int row,
                                                        [[maybe_unused]] int column) const {
        return "1";
    }
    [[nodiscard]] std::string_view command_tag() const {
        return tag;
    }
};
template <bool Supported = true, sql::dialect Dialect = sql::dialect::postgres> struct connection {
    static constexpr bool supports_transactional_test_scope = Supported;
    static constexpr auto sql_dialect = Dialect;
    std::shared_ptr<physical> native;
    explicit connection(state& observed)
        : native(std::make_shared<physical>(physical{
              .observed = &observed,
              .open = true,
              .transaction = observed.native_transaction,
              .last = {},
          })) {}
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;
    connection(connection&&) noexcept = default;
    connection& operator=(connection&&) noexcept = default;
    ~connection() {
        close();
    }
    [[nodiscard]] bool is_open() const {
        return native && native->open;
    }
    [[nodiscard]] bool is_transaction_idle() const noexcept {
        return native && native->open && !native->transaction &&
               !(native->observed->bad_final_idle && native->last == "ROLLBACK");
    }
    void close() noexcept {
        if (native && native->open) {
            ++native->observed->closes;
            native->open = false;
            native->transaction = false;
        }
    }
    asterorm::result<native_result> execute(std::string_view statement) {
        auto& observed = *native->observed;
        observed.commands.emplace_back(statement);
        native->last = statement;
        const bool control = statement == "BEGIN" || statement == "ROLLBACK" ||
                             statement == "COMMIT" || statement.starts_with("SAVEPOINT ") ||
                             statement.starts_with("ROLLBACK TO ") ||
                             statement.starts_with("RELEASE ");
        if (control) {
            ++observed.controls;
        }
        if (statement == "BEGIN") {
            native->transaction = true;
        }
        if (observed.on_execute) {
            observed.on_execute(statement);
        }
        if (control && observed.controls == observed.throw_at) {
            std::rethrow_exception(observed.exception);
        }
        if (control && observed.controls == observed.fail_at) {
            return std::unexpected(fault());
        }
        if (statement == "ROLLBACK" || statement == "COMMIT") {
            native->transaction = false;
        }
        return native_result{std::string{statement}};
    }
    asterorm::result<native_result>
    execute_prepared(std::string_view statement,
                     [[maybe_unused]] const asterorm::query_parameters& parameters) {
        return execute(statement);
    }
    asterorm::result<void> advisory_lock([[maybe_unused]] std::int64_t key) {
        return {};
    }
    asterorm::result<void> advisory_unlock([[maybe_unused]] std::int64_t key) {
        return {};
    }
    asterorm::result<std::chrono::milliseconds> statement_timeout() {
        return 0ms;
    }
    asterorm::result<void>
    set_local_statement_timeout([[maybe_unused]] std::chrono::milliseconds timeout) {
        return {};
    }
    asterorm::result<void>
    set_statement_timeout([[maybe_unused]] std::chrono::milliseconds timeout) {
        return {};
    }
};
template <typename Connection> struct driver {
    state* observed;
    asterorm::result<Connection> connect([[maybe_unused]] const std::string& settings) {
        ++observed->connects;
        if (observed->fail_connect) {
            return std::unexpected(fault());
        }
        return Connection{*observed};
    }
};
template <typename Connection = connection<>> struct fixture {
    state observed;
    asterorm::connection_pool<driver<Connection>> pool{
        driver<Connection>{&observed},
        [] {
            asterorm::pool_config config;
            config.min_size = 0;
            config.max_size = 1;
            return config;
        }(),
    };
    asterorm::session<decltype(pool)> db{pool};
    using scope_type = asterorm::testing::transactional_session<decltype(db)>;
};
template <typename Value> Value take(asterorm::result<Value> outcome) {
    REQUIRE(outcome.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}
struct marker {
    int value{73};
};
struct copying_observer {
    bool* fail;
    std::exception_ptr exception;
    copying_observer(bool& enabled, std::exception_ptr error)
        : fail(&enabled), exception(std::move(error)) {}
    copying_observer(const copying_observer& other) : fail(other.fail), exception(other.exception) {
        if (*fail) {
            std::rethrow_exception(exception);
        }
    }
    copying_observer(copying_observer&&) noexcept = default;
    void operator()([[maybe_unused]] const asterorm::query_event& event) const {}
};
} // namespace

TEST_CASE("Transactional scope owns root/savepoint and restores the entering observer",
          "[core][transactional-testing]") {
    fixture test;
    using scope_type = fixture<>::scope_type;
    STATIC_REQUIRE(!std::is_copy_constructible_v<scope_type>);
    STATIC_REQUIRE(std::is_nothrow_move_constructible_v<scope_type>);
    STATIC_REQUIRE(!std::is_move_assignable_v<scope_type>);
    STATIC_REQUIRE(std::is_nothrow_destructible_v<scope_type>);
    int baseline = 0;
    int body = 0;
    test.db.set_observer([&](const auto&) { ++baseline; });
    auto scope = take(scope_type::create(test.db));
    REQUIRE(&scope.session() == &test.db);
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().in_use == 1);
    auto lifetime = std::make_shared<int>(1);
    const std::weak_ptr<int> weak = lifetime;
    test.db.set_observer([&, retained = std::move(lifetime)](const auto&) { ++body; });
    REQUIRE(take(test.db.native_scalar<int>("SELECT value")) == 1);
    take(scope.finish());
    take(scope.finish());
    REQUIRE(weak.expired());
    REQUIRE(body == 1);
    REQUIRE(baseline == 0);
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().in_use == 0);
    REQUIRE(test.observed.closes == 0);
    REQUIRE(test.observed.commands ==
            std::vector<std::string>{"BEGIN", "SAVEPOINT \"_asterorm_test_1\"", "SELECT value",
                                     "ROLLBACK TO SAVEPOINT \"_asterorm_test_1\"",
                                     "RELEASE SAVEPOINT \"_asterorm_test_1\"", "ROLLBACK"});
    REQUIRE_THROWS_AS(scope.session(), std::logic_error);
    REQUIRE(take(test.db.native_scalar<int>("SELECT value")) == 1);
    REQUIRE(baseline == 1);
    auto fresh = take(scope_type::create(test.db));
    take(scope.finish()); // completed handles never affect a newer generation
    REQUIRE(test.db.has_active_transaction());
    take(fresh.finish());
}

TEST_CASE("Transactional scope rejects competing ownership and protects private savepoints",
          "[core][transactional-testing]") {
    fixture test;
    auto old = take(test.db.begin());
    take(test.db.commit_transaction()); // keep a stale guard intentionally
    auto scope = take(fixture<>::scope_type::create(test.db));
    const auto before = test.observed.commands.size();
    REQUIRE_FALSE(test.db.commit_transaction().has_value());
    REQUIRE_FALSE(test.db.rollback_transaction().has_value());
    REQUIRE_FALSE(old.commit().has_value());
    REQUIRE_FALSE(old.rollback().has_value());
    REQUIRE_FALSE(test.db.begin().has_value());
    REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
    REQUIRE_FALSE(asterorm::detail::session_stream_access::acquire(test.db).has_value());
    bool invoked = false;
    REQUIRE_FALSE(test.db
                      .with_advisory_lock(1,
                                          [&](auto&) -> asterorm::result<void> {
                                              invoked = true;
                                              return {};
                                          })
                      .has_value());
    REQUIRE_FALSE(test.db
                      .with_statement_timeout(1ms,
                                              [&](auto&) -> asterorm::result<void> {
                                                  invoked = true;
                                                  return {};
                                              })
                      .has_value());
    REQUIRE_FALSE(invoked);
    REQUIRE_FALSE(test.db.savepoint("_asterorm_test_1").has_value());
    REQUIRE_FALSE(test.db.rollback_to_savepoint("_asterorm_test_1").has_value());
    REQUIRE_FALSE(test.db.release_savepoint("_asterorm_test_1").has_value());
    REQUIRE(test.observed.commands.size() == before);
    take(test.db.savepoint("user"));
    take(test.db.rollback_to_savepoint("user"));
    take(test.db.release_savepoint("user"));
    take(scope.finish());
    auto fresh = take(test.db.begin());
    REQUIRE_FALSE(old.rollback().has_value());
    REQUIRE(test.db.has_active_transaction());
    take(fresh.rollback());
}

TEST_CASE("Transactional scope admission respects callbacks and existing scopes",
          "[core][transactional-testing]") {
    fixture test;
    auto existing = take(test.db.begin());
    const auto before = test.observed.controls;
    REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
    REQUIRE(test.observed.controls == before);
    take(existing.rollback());
    take(test.db.with_connection([&](auto&) -> asterorm::result<void> {
        REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
        return {};
    }));
    take(test.db.with_advisory_lock(2, [&](auto&) -> asterorm::result<void> {
        REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
        return {};
    }));
    auto transaction = take(test.db.begin());
    take(test.db.with_statement_timeout(1ms, [&](auto&) -> asterorm::result<void> {
        REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
        return {};
    }));
    take(transaction.rollback());
    auto reservation = take(asterorm::detail::session_stream_access::acquire(test.db));
    REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
    take(reservation.complete());
    auto scope = take(fixture<>::scope_type::create(test.db));
    take(test.db.with_connection([&](auto&) -> asterorm::result<void> {
        const auto commands = test.observed.commands.size();
        REQUIRE_FALSE(scope.finish().has_value());
        REQUIRE(test.observed.commands.size() == commands);
        return {};
    }));
    take(scope.finish());
}

TEST_CASE("Transactional scope capability rejection happens before connecting",
          "[core][transactional-testing]") {
    SECTION("disabled") {
        fixture<connection<false>> test;
        REQUIRE_FALSE(decltype(test)::scope_type::create(test.db).has_value());
        REQUIRE(test.observed.connects == 0);
    }
    SECTION("mysql") {
        fixture<connection<true, sql::dialect::mysql>> test;
        REQUIRE_FALSE(decltype(test)::scope_type::create(test.db).has_value());
        REQUIRE(test.observed.connects == 0);
    }
    SECTION("clickhouse") {
        fixture<connection<true, sql::dialect::clickhouse>> test;
        REQUIRE_FALSE(decltype(test)::scope_type::create(test.db).has_value());
        REQUIRE(test.observed.connects == 0);
    }
}

TEST_CASE("Transactional scope acquisition and snapshot exceptions leave the session reusable",
          "[core][transactional-testing]") {
    fixture test;
    test.observed.fail_connect = true;
    REQUIRE(fixture<>::scope_type::create(test.db).error().detail == "control detail");
    test.observed.fail_connect = false;
    bool fail = false;
    const auto exception = std::make_exception_ptr(marker{});
    test.db.set_observer(copying_observer{fail, exception});
    // Read through the copied std::function target, which the analyzer does not model.
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    fail = true;
    // Used by the assertion after exceptional control flow.
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    const auto connects = test.observed.connects;
    try {
        (void)fixture<>::scope_type::create(test.db);
        FAIL("copy must throw");
    } catch (const marker&) {
        REQUIRE(std::current_exception() == exception);
    }
    REQUIRE(test.observed.connects == connects);
    REQUIRE(test.observed.controls == 0);
    REQUIRE_FALSE(test.db.has_active_transaction());
    fail = false;
    auto scope = take(fixture<>::scope_type::create(test.db));
    take(scope.finish());
}

TEST_CASE(
    "Transactional scope SQL failures retain diagnostics poison the session and release capacity",
    "[core][transactional-testing]") {
    const int failure = GENERATE(1, 2, 3, 4, 5);
    fixture test;
    test.observed.fail_at = failure;
    if (failure <= 2) {
        const auto opened = fixture<>::scope_type::create(test.db);
        REQUIRE_FALSE(opened.has_value());
        REQUIRE(opened.error().sqlstate == "XX001");
        REQUIRE(opened.error().detail == "control detail");
    } else {
        auto scope = take(fixture<>::scope_type::create(test.db));
        const auto finished = scope.finish();
        REQUIRE_FALSE(finished.has_value());
        REQUIRE(finished.error().message == "control identity");
        REQUIRE(finished.error().detail == "control detail");
        const auto controls = test.observed.controls;
        REQUIRE(scope.finish().error().sqlstate == "XX001");
        REQUIRE(test.observed.controls == controls);
        REQUIRE_THROWS_AS(scope.session(), std::logic_error);
    }
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
    const auto connects = test.observed.connects;
    REQUIRE(fixture<>::scope_type::create(test.db).error().sqlstate == "XX001");
    REQUIRE_FALSE(test.db.native_scalar<int>("SELECT value").has_value());
    REQUIRE(test.observed.connects == connects);
    asterorm::session independent{test.pool};
    auto next =
        take(asterorm::testing::transactional_session<decltype(independent)>::create(independent));
    take(next.finish());
    REQUIRE(test.observed.connects == connects + 1);
}

TEST_CASE("Transactional scope exceptions preserve identity and destructor fallback never throws",
          "[core][transactional-testing]") {
    const int failure = GENERATE(1, 2, 3, 4, 5);
    const bool destructor = GENERATE(false, true);
    fixture test;
    test.observed.exception = std::make_exception_ptr(std::bad_alloc{});
    test.observed.throw_at = failure;
    if (failure <= 2) {
        try {
            (void)fixture<>::scope_type::create(test.db);
            FAIL("entry must throw");
        } catch (const std::bad_alloc&) {
            REQUIRE(std::current_exception() == test.observed.exception);
        }
    } else {
        auto scope = take(fixture<>::scope_type::create(test.db));
        if (!destructor) {
            try {
                (void)scope.finish();
                FAIL("finish must throw");
            } catch (const std::bad_alloc&) {
                REQUIRE(std::current_exception() == test.observed.exception);
            }
            REQUIRE_FALSE(scope.finish().has_value());
        }
    }
    REQUIRE(test.observed.controls == failure);
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE_FALSE(test.db.has_active_transaction());
}

TEST_CASE("Transactional scope destruction during a callback defers physical discard",
          "[core][transactional-testing]") {
    fixture test;
    std::optional<fixture<>::scope_type> scope;
    scope.emplace(take(fixture<>::scope_type::create(test.db)));
    take(test.db.with_connection([&](auto& conn) -> asterorm::result<void> {
        scope.reset();
        REQUIRE(conn.is_open());
        REQUIRE(test.observed.closes == 0);
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE_FALSE(test.db.native_scalar<int>("blocked").has_value());
        REQUIRE(conn.execute("callback still owns the object").has_value());
        return {};
    }));
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE(test.observed.controls == 2); // no nested rollback SQL
    REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
}

TEST_CASE("Transactional scope moves and active-control handle destruction transfer once",
          "[core][transactional-testing]") {
    fixture test;
    auto original = take(fixture<>::scope_type::create(test.db));
    std::optional<fixture<>::scope_type> moved;
    moved.emplace(std::move(original));
    // Deliberately verify the defined moved-from finish contract.
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    take(original.finish());
    REQUIRE_THROWS_AS(original.session(), std::logic_error);
    test.observed.on_execute = [&](std::string_view command) {
        if (command.starts_with("ROLLBACK TO")) {
            moved.reset();
        }
    };
    auto finished = moved->finish();
    REQUIRE(finished.has_value());
    REQUIRE_FALSE(moved.has_value());
    REQUIRE(test.observed.controls == 5);
    REQUIRE(test.observed.closes == 0);
}

TEST_CASE("Transactional scope rejects uncertain native state and checks final idle",
          "[core][transactional-testing]") {
    const bool initial = GENERATE(false, true);
    fixture test;
    if (initial) {
        test.observed.native_transaction = true;
        REQUIRE_FALSE(fixture<>::scope_type::create(test.db).has_value());
        REQUIRE(test.observed.controls == 0);
    } else {
        auto scope = take(fixture<>::scope_type::create(test.db));
        test.observed.bad_final_idle = true;
        REQUIRE_FALSE(scope.finish().has_value());
        REQUIRE(test.observed.controls == 5);
    }
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE_FALSE(test.db.has_active_transaction());
}

namespace {
struct throwing_cleanup : connection<> {
    using connection<>::connection;
    // Deliberately weaken cleanup to verify pre-lease protocol rejection.
    // NOLINTNEXTLINE(bugprone-derived-method-shadowing-base-method)
    void close() {
        connection<>::close();
    }
};
struct legacy_connection : connection<> {
    using connection<>::connection;

  private:
    using connection<>::supports_transactional_test_scope;
};
struct returning_pool {
    using connection_type = connection<>;
    asterorm::connection_pool<driver<connection_type>> underlying;
    std::exception_ptr release_failure;
    struct lease_type {
        decltype(underlying)::lease_type owned;
        std::exception_ptr* failure;
        lease_type(decltype(owned)&& source, std::exception_ptr& error)
            : owned(std::move(source)), failure(&error) {}
        lease_type(lease_type&&) noexcept = default;
        lease_type& operator=(lease_type&&) noexcept = default;
        lease_type(const lease_type&) = delete;
        connection_type& operator*() {
            return *owned;
        }
        connection_type* operator->() {
            return owned.operator->();
        }
        void release_to_pool() {
            if (*failure) {
                auto exception = std::exchange(*failure, {});
                std::rethrow_exception(exception);
            }
            owned.release_to_pool();
        }
    };
    explicit returning_pool(state& observed)
        : underlying(driver<connection_type>{&observed}, [] {
              asterorm::pool_config config;
              config.min_size = 0;
              config.max_size = 1;
              return config;
          }()) {}
    asterorm::result<lease_type> acquire() {
        auto acquired = underlying.acquire();
        if (!acquired) {
            return std::unexpected(acquired.error());
        }
        return lease_type{std::move(*acquired), release_failure};
    }
};
} // namespace

TEST_CASE("Transactional scope refuses legacy and throwing-cleanup protocols before lease",
          "[core][transactional-testing]") {
    SECTION("legacy") {
        fixture<legacy_connection> test;
        REQUIRE_FALSE(decltype(test)::scope_type::create(test.db).has_value());
        REQUIRE(test.observed.connects == 0);
    }
    SECTION("throwing close") {
        fixture<throwing_cleanup> test;
        REQUIRE_FALSE(decltype(test)::scope_type::create(test.db).has_value());
        REQUIRE(test.observed.connects == 0);
    }
}

TEST_CASE("Transactional scope catches pool return allocation failure before lease destruction",
          "[core][transactional-testing]") {
    state observed;
    returning_pool pool{observed};
    asterorm::session db{pool};
    auto scope = take(asterorm::testing::transactional_session<decltype(db)>::create(db));
    const auto exception = std::make_exception_ptr(std::bad_alloc{});
    pool.release_failure = exception;
    try {
        (void)scope.finish();
        FAIL("pool return must throw");
    } catch (const std::bad_alloc&) {
        REQUIRE(std::current_exception() == exception);
    }
    REQUIRE(observed.controls == 5);
    REQUIRE(observed.closes == 1);
    REQUIRE(pool.underlying.stats().total == 0);
    REQUIRE_FALSE(scope.finish().has_value());
    REQUIRE_FALSE(db.has_active_transaction());
}

TEST_CASE("Transactional scope detects externally lost native roots before invoking more work",
          "[core][transactional-testing]") {
    fixture test;
    auto scope = take(fixture<>::scope_type::create(test.db));
    take(test.db.with_connection([](auto& conn) { return conn.execute("ROLLBACK"); }));
    bool called = false;
    REQUIRE_FALSE(test.db
                      .with_connection([&](auto& conn) {
                          called = true;
                          return conn.execute("escaped");
                      })
                      .has_value());
    REQUIRE_FALSE(called);
    REQUIRE(test.observed.controls == 3);
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE_FALSE(scope.finish().has_value());
}

TEST_CASE("Transactional scope destruction from query observation restores callbacks safely",
          "[core][transactional-testing]") {
    fixture test;
    std::optional<fixture<>::scope_type> scope;
    scope.emplace(take(fixture<>::scope_type::create(test.db)));
    int events = 0;
    test.db.set_observer([&](const auto&) {
        ++events;
        scope.reset();
        REQUIRE(test.observed.closes == 0);
        REQUIRE(test.pool.stats().in_use == 1);
    });
    REQUIRE(take(test.db.native_scalar<int>("SELECT value")) == 1);
    REQUIRE(events == 1);
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.observed.controls == 2);
    REQUIRE(test.pool.stats().total == 0);
}

TEST_CASE("Transactional testing rejects old session facades without invoking them",
          "[core][transactional-testing]") {
    struct old_session {
        int operations{};
    };
    old_session db;
    const auto refused = asterorm::testing::transactional_session<old_session>::create(db);
    REQUIRE_FALSE(refused.has_value());
    REQUIRE(refused.error().kind == asterorm::db_error_kind::query_failed);
    REQUIRE(db.operations == 0);
}

TEST_CASE("Ordinary transaction guard cleanup exceptions discard without escaping",
          "[core][transactional-testing]") {
    fixture test;
    {
        auto transaction = take(test.db.begin());
        test.observed.throw_at = 2;
        test.observed.exception = std::make_exception_ptr(std::bad_alloc{});
    }
    REQUIRE(test.observed.controls == 2);
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE_FALSE(test.db.has_active_transaction());
    test.observed.throw_at = -1;
    auto scope = take(fixture<>::scope_type::create(test.db));
    take(scope.finish()); // an ordinary failed guard does not poison a testing baseline
}

TEST_CASE("Ordinary guard abandonment during callbacks cannot free a borrowed connection",
          "[core][transactional-testing]") {
    fixture test;
    using guard_type = asterorm::transaction_guard<decltype(test.db)>;
    std::optional<guard_type> transaction;
    transaction.emplace(take(test.db.begin()));
    take(test.db.with_connection([&](auto& conn) -> asterorm::result<void> {
        transaction.reset();
        REQUIRE(conn.is_open());
        REQUIRE(test.observed.closes == 0);
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE_FALSE(test.db.native_scalar<int>("blocked").has_value());
        return {};
    }));
    REQUIRE(test.observed.controls == 1);
    REQUIRE(test.observed.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
    REQUIRE(take(test.db.native_scalar<int>("SELECT value")) == 1);
}

TEST_CASE("Exceptional ordinary guard cleanup cannot discard a newer transaction",
          "[core][transactional-testing]") {
    fixture test;
    using guard_type = asterorm::transaction_guard<decltype(test.db)>;
    std::optional<guard_type> fresh;
    const auto error = std::make_exception_ptr(marker{});
    bool replaced = false;
    {
        auto old = take(test.db.begin());
        test.observed.on_execute = [&](std::string_view command) {
            if (command == "ROLLBACK" && !replaced) {
                replaced = true;
                take(test.db.commit_transaction());
                fresh.emplace(take(test.db.begin()));
                std::rethrow_exception(error);
            }
        };
    }
    REQUIRE(replaced);
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.observed.closes == 0);
    take(fresh->rollback());
    REQUIRE_FALSE(test.db.has_active_transaction());
}

TEST_CASE("Ordinary guard abandonment catches pool return allocation failures",
          "[core][transactional-testing]") {
    state observed;
    returning_pool pool{observed};
    asterorm::session db{pool};
    {
        auto guard = take(db.begin());
        pool.release_failure = std::make_exception_ptr(std::bad_alloc{});
    }
    REQUIRE(observed.controls == 2);
    REQUIRE(observed.closes == 1);
    REQUIRE(pool.underlying.stats().total == 0);
    REQUIRE_FALSE(db.has_active_transaction());
}
