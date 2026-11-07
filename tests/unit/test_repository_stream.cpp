#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "asterorm/repository.hpp"
#include "asterorm/sql/typed.hpp"

struct streamed_entity {
    int id{};
    std::optional<std::string> name;
    bool active{};
};
template <> struct asterorm::entity_traits<streamed_entity> {
    static constexpr const char* table = "streamed_rows";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&streamed_entity::id>("id"),
                        asterorm::column<&streamed_entity::name>("name"),
                        asterorm::column<&streamed_entity::active>("active"));
};
struct throwing_stream_entity {
    int id{};
    throwing_stream_entity() {
        throw std::runtime_error("entity exception identity");
    }
};
template <> struct asterorm::entity_traits<throwing_stream_entity> {
    static constexpr const char* table = "streamed_rows";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&throwing_stream_entity::id>("id"));
};

namespace {
namespace sql = asterorm::sql;
using namespace std::chrono_literals;

asterorm::db_error stream_failure() {
    return {
        .kind = asterorm::db_error_kind::query_failed,
        .sqlstate = "57014",
        .message = "native failure identity",
        .detail = "native detail",
    };
}
struct fake_state {
    int connects{};
    int closes{};
    int opens{};
    int reads{};
    int ordinary{};
    bool ready{true};
    int fail_after{-1};
    bool fail_open{};
    bool fail_connect{};
    bool throw_read{};
    bool fail_close{};
    int cursor_closes{};
    std::string statement;
    asterorm::query_parameters parameters;
    std::vector<std::string> names{"id", "name", "active"};
    std::vector<std::vector<std::optional<std::string>>> rows{
        {"1", "first", "t"},
        {"2", std::nullopt, "f"},
    };
};
struct physical {
    fake_state* state;
    bool open{true};
    bool active{};
    void close() noexcept {
        if (open) {
            ++state->closes;
            open = false;
            active = false;
        }
    }
};
struct fake_result {
    std::vector<std::string> names;
    std::vector<std::optional<std::string>> values;
    std::string tag;
    std::vector<std::vector<std::optional<std::string>>> batch_values;
    [[nodiscard]] int rows() const {
        return batch_values.empty() ? 1 : static_cast<int>(batch_values.size());
    }
    [[nodiscard]] int columns() const {
        return static_cast<int>(names.size());
    }
    [[nodiscard]] std::string_view column_name(int index) const {
        return names.at(index);
    }
    [[nodiscard]] std::optional<std::string> get_string(int row, int column) const {
        return batch_values.empty() ? values.at(column) : batch_values.at(row).at(column);
    }
    [[nodiscard]] std::string_view command_tag() const {
        return tag;
    }
};
struct fake_reader {
    std::shared_ptr<physical> owner;
    std::size_t index{};
    bool complete{};
    fake_reader(const fake_reader&) = delete;
    fake_reader& operator=(const fake_reader&) = delete;
    explicit fake_reader(std::shared_ptr<physical> connection) : owner(std::move(connection)) {}
    fake_reader(fake_reader&&) noexcept = default;
    fake_reader& operator=(fake_reader&&) noexcept = default;
    ~fake_reader() {
        close();
    }
    asterorm::result<std::optional<fake_result>> next() {
        ++owner->state->reads;
        if (owner->state->throw_read) {
            throw std::runtime_error("native exception identity");
        }
        if (std::cmp_equal(owner->state->fail_after, index)) {
            owner->close();
            return std::unexpected(stream_failure());
        }
        if (index == owner->state->rows.size()) {
            complete = true;
            owner->active = false;
            return std::nullopt;
        }
        return std::optional<fake_result>{
            fake_result{
                .names = owner->state->names,
                .values = owner->state->rows[index++],
                .tag = {},
            },
        };
    }
    void close() const noexcept {
        if (owner && !complete && owner->active) {
            owner->close();
        }
    }
};
struct fake_cursor_reader {
    std::shared_ptr<physical> owner;
    std::size_t chunk_size;
    std::size_t index{};
    bool complete{};
    fake_cursor_reader(std::shared_ptr<physical> connection, std::size_t chunk)
        : owner(std::move(connection)), chunk_size(chunk) {}
    fake_cursor_reader(const fake_cursor_reader&) = delete;
    fake_cursor_reader& operator=(const fake_cursor_reader&) = delete;
    fake_cursor_reader(fake_cursor_reader&&) noexcept = default;
    fake_cursor_reader& operator=(fake_cursor_reader&&) noexcept = default;
    ~fake_cursor_reader() {
        if (owner && !complete) {
            owner->close();
        }
    }
    asterorm::result<void> close() {
        ++owner->state->cursor_closes;
        if (owner->state->fail_close) {
            return std::unexpected(stream_failure());
        }
        complete = true;
        owner->active = false;
        return {};
    }
    asterorm::result<std::optional<fake_result>> next() {
        ++owner->state->reads;
        if (index == owner->state->rows.size()) {
            auto closed = close();
            if (!closed) {
                return std::unexpected(closed.error());
            }
            return std::nullopt;
        }
        fake_result batch{.names = owner->state->names, .values = {}, .tag = {}};
        for (std::size_t count = 0; count < chunk_size && index < owner->state->rows.size();
             ++count) {
            batch.batch_values.push_back(owner->state->rows[index++]);
        }
        return std::optional<fake_result>{std::move(batch)};
    }
};
template <sql::dialect Dialect = sql::dialect::postgres, bool Supported = true, bool Cursor = false>
struct fake_connection {
    static constexpr auto sql_dialect = Dialect;
    static constexpr bool supports_single_row_stream = Supported;
    static constexpr bool supports_cursor_stream = Cursor;
    std::shared_ptr<physical> owner;
    fake_connection(const fake_connection&) = delete;
    fake_connection& operator=(const fake_connection&) = delete;
    explicit fake_connection(fake_state& state) : owner(std::make_shared<physical>(&state)) {}
    fake_connection(fake_connection&&) noexcept = default;
    fake_connection& operator=(fake_connection&& other) noexcept {
        if (this != &other) {
            close();
            owner = std::move(other.owner);
        }
        return *this;
    }
    ~fake_connection() {
        close();
    }
    [[nodiscard]] bool is_open() const {
        return owner && owner->open;
    }
    [[nodiscard]] bool is_pool_reusable() const noexcept {
        return is_open() && !owner->active;
    }
    void close() noexcept {
        if (owner) {
            owner->close();
        }
    }
    [[nodiscard]] bool single_row_ready() const noexcept {
        return is_open() && !owner->active && owner->state->ready;
    }
    [[nodiscard]] bool cursor_ready() const noexcept {
        return single_row_ready();
    }
    asterorm::result<fake_cursor_reader> open_cursor(std::string_view text,
                                                     const asterorm::query_parameters& parameters,
                                                     std::size_t chunk_size) {
        ++owner->state->opens;
        owner->state->statement = text;
        owner->state->parameters = parameters;
        owner->active = true;
        return fake_cursor_reader{owner, chunk_size};
    }
    asterorm::result<fake_reader> open_single_row(std::string_view text,
                                                  const asterorm::query_parameters& parameters) {
        ++owner->state->opens;
        owner->state->statement = text;
        owner->state->parameters = parameters;
        if (owner->state->fail_open) {
            return std::unexpected(stream_failure());
        }
        owner->active = true;
        return fake_reader{owner};
    }
    asterorm::result<fake_result> execute(std::string_view text) {
        return fake_result{.names = {"id"}, .values = {"1"}, .tag = std::string{text}};
    }
    asterorm::result<fake_result>
    execute_prepared([[maybe_unused]] std::string_view text,
                     [[maybe_unused]] const asterorm::query_parameters& parameters) {
        ++owner->state->ordinary;
        return execute("SELECT");
    }
};
template <typename Connection> struct fake_driver {
    fake_state* state;
    [[nodiscard]] asterorm::result<Connection>
    connect([[maybe_unused]] const std::string& conninfo) const {
        if (state->fail_connect) {
            return std::unexpected(stream_failure());
        }
        ++state->connects;
        return Connection{*state};
    }
};
template <typename Connection = fake_connection<>> struct fixture {
    fake_state state;
    asterorm::pool_config config = [] {
        asterorm::pool_config options;
        options.min_size = 0;
        options.max_size = 1;
        return options;
    }();
    asterorm::connection_pool<fake_driver<Connection>> pool{
        fake_driver<Connection>{&state},
        config,
    };
    asterorm::session<decltype(pool)> db{pool};
    asterorm::repository<decltype(db)> repo{db};
};

auto source() {
    return sql::select({"id", "name", "active"}).from("streamed_rows").build();
}
template <typename Value> Value take(asterorm::result<Value> outcome) {
    REQUIRE(outcome.has_value());
    if constexpr (!std::is_void_v<Value>) {
        return std::move(*outcome);
    }
}

TEST_CASE("Repository stream owns rows and observes only terminal completion",
          "[core][repository-stream]") {
    fixture test;
    int events = 0;
    std::chrono::nanoseconds elapsed{};
    const auto started = std::chrono::steady_clock::now();
    std::chrono::steady_clock::duration paused{};
    test.db.set_observer([&](const asterorm::query_event& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.sql ==
                R"(SELECT "id", "name", "active" FROM "streamed_rows" WHERE "active" = $1)");
        REQUIRE(event.param_count == 1);
        REQUIRE(event.error == nullptr);
        elapsed = event.elapsed;
        REQUIRE(elapsed <= std::chrono::steady_clock::now() - started - paused);
    });
    auto query = source();
    query.where_clause = sql::col("active") == sql::val(true);
    auto stream = take(test.repo.stream<streamed_entity>(std::move(query)));
    REQUIRE(test.state.opens == 1);
    REQUIRE(test.state.reads == 0);
    REQUIRE(test.state.parameters[0].type == asterorm::parameter_type::boolean);
    auto first = take(stream.next());
    REQUIRE(first->id == 1);
    REQUIRE(first->name == "first");
    REQUIRE(first->active);
    test.state.rows[0][1] = "changed native buffer";
    auto second = take(stream.next());
    REQUIRE(second->id == 2);
    REQUIRE_FALSE(second->name.has_value());
    REQUIRE_FALSE(second->active);
    REQUIRE(first->name == "first");
    REQUIRE(events == 0);
    auto pause_started = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(10ms);
    paused = std::chrono::steady_clock::now() - pause_started;
    REQUIRE_FALSE(take(stream.next()).has_value());
    REQUIRE_FALSE(take(stream.next()).has_value());
    take(stream.close());
    REQUIRE(events == 1);
    REQUIRE(test.state.reads == 3);
    REQUIRE(test.state.ordinary == 0);
    REQUIRE(test.pool.stats().in_use == 0);
    REQUIRE(test.state.closes == 0);
}

TEST_CASE("Repository stream reports native and decode failures once",
          "[core][repository-stream]") {
    fixture test;
    bool decoding = false;
    SECTION("late native error") {
        test.state.fail_after = 1;
    }
    SECTION("bad codec") {
        decoding = true;
        test.state.rows[1][0] = "not an integer";
    }
    SECTION("bad projection") {
        decoding = true;
        test.state.names[1] = "wrong";
    }
    int events = 0;
    std::optional<asterorm::db_error> diagnostic;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.error != nullptr);
        diagnostic = *event.error;
    });
    auto stream = take(test.repo.stream<streamed_entity>(source()));
    if (test.state.names[1] != "wrong") {
        REQUIRE(take(stream.next()).has_value());
    }
    auto failed = stream.next();
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(failed.error().kind == (decoding ? asterorm::db_error_kind::parse_failed
                                             : asterorm::db_error_kind::query_failed));
    REQUIRE(diagnostic->message == failed.error().message);
    if (!decoding) {
        REQUIRE(failed.error().sqlstate == "57014");
    }
    REQUIRE(stream.next().error().message == failed.error().message);
    take(stream.close());
    REQUIRE(events == 1);
    REQUIRE(test.state.closes == 1);
    REQUIRE(test.pool.stats().in_use == 0);
}

TEST_CASE("Repository close observes cancellation while abandonment stays quiet",
          "[core][repository-stream]") {
    fixture test;
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.error != nullptr);
        REQUIRE(event.error->sqlstate.empty());
    });
    {
        auto abandoned = take(test.repo.stream<streamed_entity>(source()));
    }
    REQUIRE(events == 0);
    auto closed = take(test.repo.stream<streamed_entity>(source()));
    take(closed.close());
    take(closed.close());
    REQUIRE_FALSE(take(closed.next()).has_value());
    REQUIRE(events == 1);
    REQUIRE(test.state.closes == 2);
    REQUIRE(test.pool.stats().total == 0);
}

TEST_CASE("Repository stream snapshots observers and keeps transactions pinned",
          "[core][repository-stream]") {
    fixture test;
    int original = 0;
    int replacement = 0;
    test.db.set_observer([&](const auto&) { ++original; });
    auto transaction = take(test.db.begin());
    auto stream = take(test.repo.stream<streamed_entity>(source()));
    test.db.set_observer([&](const auto&) { ++replacement; });
    REQUIRE_FALSE(transaction.commit().has_value());
    while (take(stream.next())) {
    }
    REQUIRE(original == 1);
    REQUIRE(replacement == 0);
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().in_use == 1);
    take(transaction.commit());
    REQUIRE(test.pool.stats().in_use == 0);
}

TEST_CASE("Repository streams preserve C++ exception identity and never replay",
          "[core][repository-stream]") {
    fixture test;
    int events = 0;
    bool native_throw = false;
    SECTION("native throws") {
        native_throw = true;
        test.state.throw_read = true;
    }
    SECTION("EOF observer throws") {
        test.state.rows.clear();
    }
    SECTION("error observer throws") {
        test.state.fail_after = 0;
    }
    SECTION("close observer throws") {}
    test.db.set_observer([&](const auto&) {
        ++events;
        throw std::runtime_error("observer identity");
    });
    auto stream = take(test.repo.stream<streamed_entity>(source()));
    if (!native_throw && !test.state.rows.empty() && test.state.fail_after == -1) {
        REQUIRE_THROWS_WITH(stream.close(), "observer identity");
    } else {
        REQUIRE_THROWS_WITH(stream.next(),
                            native_throw ? "native exception identity" : "observer identity");
    }
    REQUIRE_FALSE(stream.next().has_value());
    REQUIRE(events == 1);
    REQUIRE(test.state.opens == 1);
    REQUIRE(test.state.closes == 1);
    REQUIRE(test.pool.stats().total == 0);
}

TEST_CASE("Repository stream observers may replace the active handle safely",
          "[core][repository-stream]") {
    fixture test;
    test.state.rows.clear();
    asterorm::row_stream<streamed_entity> stream;
    int events = 0;
    test.db.set_observer([&](const auto&) {
        ++events;
        REQUIRE_FALSE(stream.next().has_value());
        REQUIRE_FALSE(stream.close().has_value());
        stream = {};
    });
    stream = take(test.repo.stream<streamed_entity>(source()));
    REQUIRE_FALSE(take(stream.next()).has_value());
    REQUIRE(events == 1);
    REQUIRE(test.pool.stats().in_use == 0);
    REQUIRE(test.state.closes == 0);
}

TEST_CASE("Repository streams validate sources and options before acquiring",
          "[core][repository-stream]") {
    fixture test;
    auto reject = [&](sql::select_ast query, asterorm::stream_options options = {}) {
        auto failed = test.repo.stream<streamed_entity>(std::move(query), options);
        REQUIRE_FALSE(failed.has_value());
        REQUIRE(test.state.connects == 0);
        REQUIRE(test.state.opens == 0);
    };
    SECTION("wrong table") {
        auto query = source();
        query.table = "other";
        reject(std::move(query));
    }
    SECTION("wildcard") {
        auto query = source();
        query.columns = {"*"};
        reject(std::move(query));
    }
    SECTION("order") {
        auto query = source();
        std::swap(query.columns[0], query.columns[1]);
        reject(std::move(query));
    }
    SECTION("alias") {
        auto query = source();
        query.table_alias = "s";
        reject(std::move(query));
    }
    SECTION("items") {
        auto query = source();
        query.items.push_back({});
        reject(std::move(query));
    }
    SECTION("grouping") {
        auto query = source();
        query.group_bys = {"id"};
        reject(std::move(query));
    }
    SECTION("locking") {
        auto query = source();
        query.locking.strength = sql::select_lock_strength::update;
        reject(std::move(query));
    }
    SECTION("bad order column") {
        auto query = source();
        query.order_bys.push_back({.column = "missing", .ascending = true});
        reject(std::move(query));
    }
    SECTION("bad predicate") {
        auto query = source();
        query.where_clause = sql::col("other.id") == sql::val(1);
        reject(std::move(query));
    }
    SECTION("NULL child") {
        auto query = source();
        query.where_clause = sql::predicate_ast{
            sql::binary_predicate{
                .op = sql::op_kind::and_,
                .left = nullptr,
                .right = nullptr,
            },
        };
        reject(std::move(query));
    }
    SECTION("functions") {
        auto query = source();
        query.where_clause = sql::expr_ast{std::shared_ptr<sql::func_expr>{}} == sql::val(1);
        reject(std::move(query));
    }
    SECTION("encoded error") {
        auto query = source();
        auto value = sql::val(1);
        value.encoded_value.error = stream_failure();
        query.where_clause = sql::col("id") == value;
        reject(std::move(query));
    }
    SECTION("text NUL") {
        auto query = source();
        query.where_clause = sql::col("name") == sql::val(std::string{"a\0b", 3});
        reject(std::move(query));
    }
    SECTION("zero chunk") {
        reject(source(), {.chunk_size = 0});
    }
    SECTION("large chunk") {
        reject(source(), {.chunk_size = 65537});
    }
    SECTION("cursor") {
        reject(source(), {.mode = asterorm::stream_mode::cursor});
    }
    SECTION("invalid mode") {
        // Deliberately validate a value outside the named enumerators.
        // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
        reject(source(), {.mode = static_cast<asterorm::stream_mode>(255)});
    }
}

sql::predicate_ast balanced(int depth) {
    if (depth == 0) {
        return sql::col("id") == sql::val(1);
    }
    return balanced(depth - 1) && balanced(depth - 1);
}
TEST_CASE("Repository stream bounds predicate depth and visits", "[core][repository-stream]") {
    fixture test;
    auto node_boundary = source();
    node_boundary.where_clause = balanced(9); // 1023 nodes, depth 9
    auto accepted = take(test.repo.stream<streamed_entity>(std::move(node_boundary)));
    take(accepted.close());
    auto too_many_nodes = source();
    too_many_nodes.where_clause = balanced(9) && (sql::col("id") == sql::val(1)); // 1025 nodes
    const auto rejected_nodes = test.repo.stream<streamed_entity>(std::move(too_many_nodes));
    REQUIRE_FALSE(rejected_nodes.has_value());
    auto depth_boundary_source = source();
    auto allowed = sql::col("id") == sql::val(1);
    for (int depth = 0; depth < 64; ++depth) {
        allowed = std::move(allowed) && (sql::col("id") == sql::val(1));
    }
    depth_boundary_source.where_clause = std::move(allowed);
    auto depth_boundary = take(test.repo.stream<streamed_entity>(std::move(depth_boundary_source)));
    take(depth_boundary.close());
    auto too_deep = source();
    auto predicate = sql::col("id") == sql::val(1);
    for (int depth = 0; depth < 65; ++depth) {
        predicate = std::move(predicate) && (sql::col("id") == sql::val(1));
    }
    too_deep.where_clause = std::move(predicate);
    const auto rejected_depth = test.repo.stream<streamed_entity>(std::move(too_deep));
    REQUIRE_FALSE(rejected_depth.has_value());
    REQUIRE(test.state.opens == 2);
}

TEST_CASE("Repository streaming requires an explicit supported capability",
          "[core][repository-stream]") {
    fixture<fake_connection<sql::dialect::sqlite>> sqlite;
    fixture<fake_connection<sql::dialect::mysql>> mysql;
    fixture<fake_connection<sql::dialect::clickhouse>> clickhouse;
    fixture<fake_connection<sql::dialect::postgres, false>> disabled;
    REQUIRE_FALSE(sqlite.repo.stream<streamed_entity>(source()).has_value());
    REQUIRE_FALSE(mysql.repo.stream<streamed_entity>(source()).has_value());
    REQUIRE_FALSE(clickhouse.repo.stream<streamed_entity>(source()).has_value());
    REQUIRE_FALSE(disabled.repo.stream<streamed_entity>(source()).has_value());
    REQUIRE(sqlite.state.connects + mysql.state.connects + clickhouse.state.connects +
                disabled.state.connects ==
            0);
    struct legacy {};
    legacy facade;
    asterorm::repository old{facade};
    REQUIRE_FALSE(old.stream<streamed_entity>(source()).has_value());
}

TEST_CASE("Repository stream opening failures preserve ownership and observation rules",
          "[core][repository-stream]") {
    fixture test;
    int events = 0;
    test.db.set_observer([&](const auto&) { ++events; });
    SECTION("acquisition") {
        test.state.fail_connect = true;
    }
    SECTION("admission") {
        test.state.ready = false;
    }
    SECTION("dispatch") {
        test.state.fail_open = true;
    }
    auto failed = test.repo.stream<streamed_entity>(source());
    REQUIRE_FALSE(failed.has_value());
    REQUIRE(events == (test.state.fail_open ? 1 : 0));
    REQUIRE(test.pool.stats().in_use == 0);
    if (!test.state.ready) {
        REQUIRE(test.state.closes == 0);
    }
}

TEST_CASE("Repository stream moves and temporary builders retain independent ownership",
          "[core][repository-stream]") {
    fixture first;
    fixture second;
    int events = 0;
    first.db.set_observer([&](const auto&) { ++events; });
    auto stream = [&] {
        asterorm::repository temporary{first.db};
        return take(
            temporary.stream<streamed_entity>(sql::typed::select<streamed_entity>().build()));
    }();
    STATIC_REQUIRE(!std::is_copy_constructible_v<decltype(stream)>);
    STATIC_REQUIRE(std::is_nothrow_move_assignable_v<decltype(stream)>);
    auto moved = std::move(stream);
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    REQUIRE_FALSE(stream.next().has_value());
    REQUIRE(take(moved.next())->id == 1);
    auto replacement = take(second.repo.stream<streamed_entity>(source()));
    moved = std::move(replacement);
    REQUIRE(events == 0);
    REQUIRE(first.pool.stats().in_use == 0);
    REQUIRE(first.state.closes == 1);
    REQUIRE(take(moved.next())->id == 1);
    take(moved.close());
    REQUIRE(second.pool.stats().in_use == 0);
}

TEST_CASE("Repository admission failure keeps a caller transaction and emits no event",
          "[core][repository-stream]") {
    fixture test;
    auto transaction = take(test.db.begin());
    test.state.ready = false;
    int events = 0;
    test.db.set_observer([&](const auto&) { ++events; });
    REQUIRE_FALSE(test.repo.stream<streamed_entity>(source()).has_value());
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().in_use == 1);
    REQUIRE(test.state.closes == 0);
    REQUIRE(events == 0);
    take(transaction.rollback());
}

struct copy_observer {
    bool* fail;
    explicit copy_observer(bool& flag) : fail(&flag) {}
    copy_observer(copy_observer&&) noexcept = default;
    copy_observer(const copy_observer& other) : fail(other.fail) {
        if (*fail) {
            throw std::runtime_error("observer copy identity");
        }
    }
    void operator()([[maybe_unused]] const asterorm::query_event& event) const {}
};
TEST_CASE("Repository observer snapshot exceptions release only the new pin",
          "[core][repository-stream]") {
    fixture test;
    bool fail = false;
    auto transaction = take(test.db.begin());
    test.db.set_observer(copy_observer{fail});
    // Read by the callable's copy constructor through std::function type erasure.
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    fail = true;
    REQUIRE_THROWS_WITH(test.repo.stream<streamed_entity>(source()), "observer copy identity");
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.pool.stats().in_use == 1);
    REQUIRE(test.state.opens == 0);
    REQUIRE(test.state.closes == 0);
    take(transaction.rollback());
}

TEST_CASE("Repository entity exceptions preserve identity and discard",
          "[core][repository-stream]") {
    fixture test;
    test.state.names = {"id"};
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.error != nullptr);
        throw std::runtime_error("secondary observer failure");
    });
    auto stream = take(test.repo.stream<throwing_stream_entity>(
        sql::select({"id"}).from("streamed_rows").build()));
    REQUIRE_THROWS_WITH(stream.next(), "entity exception identity");
    REQUIRE_FALSE(stream.next().has_value());
    REQUIRE(events == 1);
    REQUIRE(test.pool.stats().total == 0);
}

TEST_CASE("Invalidated repository streams cannot discard a newly opened generation",
          "[core][repository-stream]") {
    fixture test;
    using guard_type = asterorm::transaction_guard<decltype(test.db)>;
    std::optional<guard_type> transaction{take(test.db.begin())};
    int events = 0;
    asterorm::row_stream<streamed_entity> fresh;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error != nullptr);
        REQUIRE(test.pool.stats().in_use == 0); // parent already discarded the old pin
        test.db.set_observer({});
        fresh = take(test.repo.stream<streamed_entity>(source()));
    });
    auto old = take(test.repo.stream<streamed_entity>(source()));
    transaction.reset();
    REQUIRE_FALSE(old.next().has_value());
    REQUIRE(events == 1);
    REQUIRE(test.pool.stats().in_use == 1);
    take(old.close());
    REQUIRE(take(fresh.next())->id == 1);
    take(fresh.close());
    REQUIRE(test.pool.stats().total == 0);
}
} // namespace

TEST_CASE("Repository cursor requires a managed transaction before leasing",
          "[core][repository-cursor]") {
    fixture<fake_connection<sql::dialect::postgres, true, true>> test;
    const auto refused =
        test.repo.stream<streamed_entity>(source(), {.mode = asterorm::stream_mode::cursor});
    REQUIRE_FALSE(refused.has_value());
    REQUIRE(test.state.connects == 0);
    REQUIRE(test.state.opens == 0);
    auto transaction = take(test.db.begin());
    test.state.ready = false;
    const auto unhealthy =
        test.repo.stream<streamed_entity>(source(), {.mode = asterorm::stream_mode::cursor});
    REQUIRE_FALSE(unhealthy.has_value());
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.state.opens == 0);
    take(transaction.rollback());
}

TEST_CASE("Repository cursor decodes batches without native reads between their rows",
          "[core][repository-cursor]") {
    fixture<fake_connection<sql::dialect::postgres, true, true>> test;
    test.state.rows = {{"1", "first", "t"}, {"2", std::nullopt, "f"}, {"3", "third", "t"}};
    auto transaction = take(test.db.begin());
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error == nullptr);
        REQUIRE(test.pool.stats().in_use == 1);
        REQUIRE(event.sql == "SELECT \"id\", \"name\", \"active\" FROM \"streamed_rows\"");
    });
    auto stream = take(test.repo.stream<streamed_entity>(
        source(), {.mode = asterorm::stream_mode::cursor, .chunk_size = 2}));
    auto first = take(stream.next());
    REQUIRE(first->id == 1);
    REQUIRE(test.state.reads == 1);
    REQUIRE(take(stream.next())->id == 2);
    REQUIRE(test.state.reads == 1);
    REQUIRE(take(stream.next())->id == 3);
    REQUIRE(test.state.reads == 2);
    REQUIRE_FALSE(take(stream.next()).has_value());
    REQUIRE(test.state.reads == 3);
    REQUIRE(events == 1);
    REQUIRE(test.state.cursor_closes == 1);
    REQUIRE(first->name == "first");
    REQUIRE(test.db.has_active_transaction());
    take(stream.close());
    REQUIRE(events == 1);
    REQUIRE(test.state.closes == 0);
    test.db.set_observer({});
    take(transaction.commit());
}

TEST_CASE("Repository cursor explicit close retains transaction and quiet abandonment discards",
          "[core][repository-cursor]") {
    fixture<fake_connection<sql::dialect::postgres, true, true>> test;
    auto transaction = take(test.db.begin());
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error != nullptr);
        REQUIRE(event.error->sqlstate.empty());
        REQUIRE(test.pool.stats().in_use == 1);
    });
    {
        auto stream = take(
            test.repo.stream<streamed_entity>(source(), {.mode = asterorm::stream_mode::cursor}));
        REQUIRE(take(stream.next())->id == 1);
        take(stream.close());
        REQUIRE_FALSE(take(stream.next()).has_value());
        take(stream.close());
    }
    REQUIRE(test.state.cursor_closes == 1);
    REQUIRE(events == 1);
    REQUIRE(test.db.has_active_transaction());
    REQUIRE(test.state.closes == 0);
    {
        auto abandoned = take(
            test.repo.stream<streamed_entity>(source(), {.mode = asterorm::stream_mode::cursor}));
        REQUIRE(take(abandoned.next())->id == 1);
    }
    REQUIRE(test.state.cursor_closes == 1);
    REQUIRE(events == 1);
    REQUIRE(test.state.closes == 1);
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE_FALSE(transaction.commit().has_value());
}

TEST_CASE("Repository cursor cleanup failures and observer exceptions preserve identity",
          "[core][repository-cursor]") {
    const bool throw_observer = GENERATE(false, true);
    const bool eof = GENERATE(false, true);
    fixture<fake_connection<sql::dialect::postgres, true, true>> test;
    auto transaction = take(test.db.begin());
    test.state.fail_close = true;
    int events = 0;
    test.db.set_observer([&](const auto& event) {
        ++events;
        REQUIRE(event.error->sqlstate == "57014");
        REQUIRE(test.pool.stats().in_use == 1);
        if (throw_observer) {
            throw std::runtime_error("cursor observer identity");
        }
    });
    auto stream =
        take(test.repo.stream<streamed_entity>(source(), {.mode = asterorm::stream_mode::cursor}));
    if (eof) {
        take(stream.next());
        take(stream.next());
    }
    auto finish = [&] -> asterorm::result<void> {
        if (eof) {
            auto next = stream.next();
            return next ? asterorm::result<void>{} : std::unexpected(next.error());
        }
        return stream.close();
    };
    if (throw_observer) {
        REQUIRE_THROWS_WITH(finish(), "cursor observer identity");
    } else {
        REQUIRE(finish().error().sqlstate == "57014");
    }
    REQUIRE(events == 1);
    REQUIRE(stream.next().error().message == "native failure identity");
    take(stream.close());
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE(test.state.closes == 1);
    REQUIRE_FALSE(transaction.commit().has_value());
}

TEST_CASE("Repository cursor capability is independent from single-row support",
          "[core][repository-cursor]") {
    fixture<fake_connection<sql::dialect::postgres, false, true>> test;
    const auto single_row = test.repo.stream<streamed_entity>(source());
    REQUIRE_FALSE(single_row.has_value());
    REQUIRE(test.state.connects == 0);
    auto transaction = take(test.db.begin());
    auto stream =
        take(test.repo.stream<streamed_entity>(source(), {.mode = asterorm::stream_mode::cursor}));
    REQUIRE(take(stream.next())->id == 1);
    take(stream.close());
    take(transaction.rollback());
}

TEST_CASE("Repository cursor terminal observer exceptions discard a clean portal",
          "[core][repository-cursor]") {
    const bool eof = GENERATE(false, true);
    fixture<fake_connection<sql::dialect::postgres, true, true>> test;
    auto transaction = take(test.db.begin());
    int events = 0;
    test.db.set_observer([&](const auto&) {
        ++events;
        REQUIRE(test.pool.stats().in_use == 1);
        throw std::runtime_error("clean cursor observer identity");
    });
    auto stream =
        take(test.repo.stream<streamed_entity>(source(), {.mode = asterorm::stream_mode::cursor}));
    if (eof) {
        take(stream.next());
        take(stream.next());
        REQUIRE_THROWS_WITH(stream.next(), "clean cursor observer identity");
    } else {
        REQUIRE_THROWS_WITH(stream.close(), "clean cursor observer identity");
    }
    REQUIRE(events == 1);
    REQUIRE(test.state.cursor_closes == 1);
    REQUIRE(test.state.closes == 1);
    REQUIRE_FALSE(test.db.has_active_transaction());
    REQUIRE_FALSE(transaction.commit().has_value());
    REQUIRE_FALSE(stream.next().has_value());
    take(stream.close());
    REQUIRE(events == 1);
}
