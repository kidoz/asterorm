#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <chrono>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "asterorm/session.hpp"
#include "asterorm/sql/typed_query.hpp"

namespace {
struct observed_user {
    int id{};
    std::string email;
    bool active{};
    std::optional<std::string> note;
};
} // namespace

template <> struct asterorm::entity_traits<observed_user> {
    static constexpr const char* table = "observed_users";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&observed_user::id>("id"),
                        asterorm::column<&observed_user::email>("email"),
                        asterorm::column<&observed_user::active>("active"),
                        asterorm::column<&observed_user::note>("note"));
};

namespace {
namespace typed = asterorm::sql::typed;
namespace sql = asterorm::sql;

struct query_result {
    std::string tag;
    [[nodiscard]] int rows() const {
        return 1;
    }
    [[nodiscard]] std::string_view command_tag() const {
        return tag;
    }
    [[nodiscard]] std::optional<std::string> get_string([[maybe_unused]] int row_index,
                                                        int column) const {
        switch (column) {
        case 0:
            return "7";
        case 1:
            return "alice'o@example.com";
        case 2:
            return "t";
        default:
            return std::nullopt;
        }
    }
};

struct execution_state {
    int calls{};
    std::string sql;
    asterorm::query_parameters params;
    std::optional<asterorm::db_error> failure;
};

struct query_connection {
    execution_state* state;
    bool open{true};
    [[nodiscard]] bool is_open() const {
        return open;
    }
    void close() {
        open = false;
    }
    [[nodiscard]] asterorm::result<query_result> execute(std::string_view statement) const {
        return query_result{.tag = std::string{statement}};
    }
    [[nodiscard]] asterorm::result<query_result>
    execute_prepared(std::string_view statement, const asterorm::query_parameters& params) const {
        ++state->calls;
        state->sql = statement;
        state->params = params;
        if (state->failure) {
            return std::unexpected(*state->failure);
        }
        return query_result{};
    }
};

struct query_driver {
    execution_state* state;
    [[nodiscard]] asterorm::result<query_connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        return query_connection{.state = state};
    }
};

// Intentionally has neither connection_type nor observed_execute.
struct direct_session {
    query_connection connection;
    int lease_calls{};
    template <typename Callback> auto with_connection(Callback&& callback) {
        ++lease_calls;
        return std::forward<Callback>(callback)(connection);
    }
};

struct observing_session : direct_session {
    int observed_calls{};
    asterorm::result<query_result> observed_execute(query_connection& leased,
                                                    const std::string& statement,
                                                    const asterorm::query_parameters& params) {
        ++observed_calls;
        return leased.execute_prepared(statement, params);
    }
};

struct unrelated_session : direct_session {
    // A coincidentally named method with an incompatible result is not a helper.
    void observed_execute([[maybe_unused]] query_connection& connection,
                          [[maybe_unused]] const std::string& statement,
                          [[maybe_unused]] const asterorm::query_parameters& params) {
        FAIL("incompatible observed_execute must not be called");
    }
};
} // namespace

TEST_CASE("Typed query: session observation preserves SQL, parameters and projections",
          "[typed-query][observer]") {
    const bool observe = GENERATE(false, true);
    execution_state state;
    asterorm::pool_config config;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{.state = &state}, config};
    asterorm::session db{pool};
    int events = 0;
    if (observe) {
        db.set_observer([&](const asterorm::query_event& event) {
            ++events;
            REQUIRE(event.sql == state.sql);
            REQUIRE(event.param_count == 2);
            REQUIRE(event.error == nullptr);
            REQUIRE(event.elapsed >= std::chrono::nanoseconds::zero());
            REQUIRE(pool.stats().in_use == 1);
        });
    }

    auto string_ast = sql::select({"id", "email", "active", "note"})
                          .from("observed_users")
                          .where((sql::col("active") == sql::val(true)) &&
                                 (sql::col("email") != sql::val("blocked'o@example.com")))
                          .order_by("id", false)
                          .limit(3)
                          .build();
    auto expected = sql::compiler{}.compile(string_ast);
    REQUIRE(expected.has_value());
    auto selection =
        typed::select_cols(typed::col<&observed_user::id>, typed::col<&observed_user::email>,
                           typed::col<&observed_user::active>, typed::col<&observed_user::note>);
    selection
        .where((typed::col<&observed_user::active> == typed::val(true)) &&
               (typed::col<&observed_user::email> != typed::val("blocked'o@example.com")))
        .order_by(typed::col<&observed_user::id>, typed::desc)
        .limit(3);
    auto rows = typed::query(db, selection);
    REQUIRE(rows.has_value());
    REQUIRE(*rows == std::vector<std::tuple<int, std::string, bool, std::optional<std::string>>>{
                         {7, "alice'o@example.com", true, std::nullopt}});
    REQUIRE(state.calls == 1);
    REQUIRE(state.sql == expected->sql);
    REQUIRE(state.params == expected->params);
    REQUIRE(events == (observe ? 1 : 0));
}

TEST_CASE("Typed query: observed failures preserve diagnostics and transaction invalidation",
          "[typed-query][observer][tx]") {
    execution_state state;
    state.failure = asterorm::db_error{
        .kind = asterorm::db_error_kind::serialization_failure,
        .sqlstate = "40001",
        .message = "retry the transaction",
        .detail = "conflicting write",
    };
    asterorm::pool_config config;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{.state = &state}, config};
    asterorm::session db{pool};
    int events = 0;
    std::optional<asterorm::db_error> observed_error;
    db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        REQUIRE(event.error != nullptr);
        observed_error = *event.error;
        REQUIRE(event.sql == state.sql);
        REQUIRE(event.param_count == 1);
        REQUIRE(pool.stats().in_use == 1);
    });
    auto transaction = db.begin();
    REQUIRE(transaction.has_value());
    auto rows = typed::query(
        db, typed::select<observed_user>().where(typed::col<&observed_user::id> == typed::val(7)));
    REQUIRE_FALSE(rows.has_value());
    REQUIRE(observed_error.has_value());
    REQUIRE(observed_error->sqlstate == rows.error().sqlstate);
    REQUIRE(observed_error->kind == rows.error().kind);
    REQUIRE(observed_error->message == rows.error().message);
    REQUIRE(observed_error->detail == rows.error().detail);
    REQUIRE(rows.error().sqlstate == "40001");
    REQUIRE(state.calls == 1);
    REQUIRE(events == 1);
    auto rejected = typed::query(db, typed::select<observed_user>());
    REQUIRE_FALSE(rejected.has_value());
    REQUIRE(rejected.error().message.contains("invalidated"));
    REQUIRE(state.calls == 1);
    REQUIRE(events == 1);
    REQUIRE_FALSE(transaction->commit().has_value());
    REQUIRE(transaction->rollback().has_value());
    REQUIRE(pool.stats().in_use == 0);
}

TEST_CASE("Typed query: custom sessions retain direct execution", "[typed-query][compatibility]") {
    const bool fail = GENERATE(false, true);
    execution_state state;
    if (fail) {
        state.failure = asterorm::db_error{
            .kind = asterorm::db_error_kind::query_failed,
            .sqlstate = "42000",
            .message = "custom execution failed",
        };
    }
    direct_session db{.connection = query_connection{.state = &state}};
    auto rows = typed::query(db, typed::select<observed_user>());
    REQUIRE(state.calls == 1);
    if (fail) {
        REQUIRE_FALSE(rows.has_value());
        REQUIRE(rows.error().message == state.failure->message);
        REQUIRE(rows.error().sqlstate == state.failure->sqlstate);
    } else {
        REQUIRE(rows.has_value());
        REQUIRE(rows->size() == 1);
        REQUIRE(rows->front().id == 7);
        REQUIRE(rows->front().email == "alice'o@example.com");
        REQUIRE(rows->front().active);
        REQUIRE_FALSE(rows->front().note);
    }
}

TEST_CASE("Typed query: observer exceptions release ephemeral leases", "[typed-query][observer]") {
    execution_state state;
    asterorm::pool_config config;
    config.max_size = 1;
    asterorm::connection_pool<query_driver> pool{query_driver{.state = &state}, config};
    asterorm::session db{pool};
    db.set_observer(
        [](const asterorm::query_event&) { throw std::runtime_error("observer failed"); });
    REQUIRE_THROWS_AS(typed::query(db, typed::select<observed_user>()), std::runtime_error);
    REQUIRE(pool.stats().in_use == 0);
    db.set_observer({});
    REQUIRE(typed::query(db, typed::select<observed_user>()).has_value());
    REQUIRE(state.calls == 2);
}

TEST_CASE("Typed query: custom compatible helpers are used", "[typed-query][compatibility]") {
    execution_state state;
    observing_session db{{query_connection{.state = &state}}};
    REQUIRE(typed::query(db, typed::select<observed_user>()).has_value());
    REQUIRE(db.observed_calls == 1);
    REQUIRE(state.calls == 1);
}

TEST_CASE("Typed query: incompatible custom helpers retain the direct fallback",
          "[typed-query][compatibility]") {
    execution_state state;
    unrelated_session db{{query_connection{.state = &state}}};
    REQUIRE(typed::query(db, typed::select<observed_user>()).has_value());
    REQUIRE(state.calls == 1);
}

TEST_CASE("SELECT locking: unknown custom backends reject before leasing", "[select-locking]") {
    execution_state state;
    SECTION("facade without a connection type") {
        direct_session db{.connection = query_connection{.state = &state}};
        auto result = typed::query(db, typed::select<observed_user>().for_update());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().kind == asterorm::db_error_kind::parse_failed);
        REQUIRE(db.lease_calls == 0);
    }
    SECTION("connection type without an explicit dialect") {
        asterorm::pool_config config;
        config.min_size = 0;
        config.max_size = 1;
        asterorm::connection_pool<query_driver> pool{query_driver{.state = &state}, config};
        asterorm::session db{pool};
        auto result = typed::query(db, typed::select<observed_user>().for_share());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().kind == asterorm::db_error_kind::parse_failed);
        REQUIRE(pool.stats().total == 0);
    }
    REQUIRE(state.calls == 0);
}
