# Use transactional fixtures with Catch2

Include `<asterorm/testing/catch2.hpp>` and link Catch2 **without its main**.
Call `asterorm::testing::catch2::run(argc, argv)` from your main after registering
fixtures. The optional header requires Catch2 3 with partial-invocation callbacks;
the local verification uses Catch2 3.16.1. AsterORM's normal headers, libraries and
installed package dependencies do not acquire a Catch2 dependency.

This complete SQLite example prepares connection-local reference data once per
process. Each generator/section path receives a new rollback-only scope and sees
the reference row. Preparation must return a checked `result<void>`; it runs
outside the per-invocation transaction.

```cpp
#include <asterorm/testing/catch2.hpp>
#include <asterorm/sqlite/driver.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

struct example_fixture {
    asterorm::connection_pool<asterorm::sqlite::driver> pool{
        asterorm::sqlite::driver{}, [] {
            asterorm::pool_config config;
            config.conninfo = ":memory:";
            config.min_size = 0;
            config.max_size = 1;
            return config;
        }()};
    asterorm::session<decltype(pool)> database{pool};

    auto& session() { return database; }
    asterorm::result<void> prepare(decltype(database)& owner) {
        return owner.with_connection([](auto& connection) -> asterorm::result<void> {
            auto created = connection.execute("CREATE TEMP TABLE fixture_rows(id INTEGER PRIMARY KEY)");
            if (!created) return std::unexpected(std::move(created.error()));
            auto seeded = connection.execute("INSERT INTO fixture_rows VALUES(1)");
            if (!seeded) return std::unexpected(std::move(seeded.error()));
            return {};
        });
    }
    asterorm::result<void> validate(decltype(database)& owner) {
        auto baseline = owner.native_scalar<bool>(
            "SELECT (SELECT count(*) FROM temp.fixture_rows)=1 AND "
            "EXISTS(SELECT 1 FROM temp.fixture_rows WHERE id=1)");
        if (!baseline) return std::unexpected(std::move(baseline.error()));
        if (*baseline != true) return std::unexpected(asterorm::db_error{
            .kind = asterorm::db_error_kind::query_failed, .message = "Reference baseline changed"});
        return {};
    }
};

TEST_CASE("Reference rows survive each path", "[transactional:example]") {
    auto& database = asterorm::testing::catch2::session<example_fixture>();
    const auto generated = GENERATE(2, 3);
    auto count = database.native_scalar<int>("SELECT count(*) FROM fixture_rows");
    REQUIRE(count.has_value());
    REQUIRE(*count == 1);
    SECTION("insert") {
        REQUIRE(database.with_connection([&](auto& connection) {
            return connection.execute_params("INSERT INTO fixture_rows VALUES($1)",
                {asterorm::encode_parameter(generated)});
        }).has_value());
    }
    SECTION("delete") {
        REQUIRE(database.with_connection([](auto& connection) {
            return connection.execute("DELETE FROM fixture_rows");
        }).has_value());
    }
}

int main(int argc, char* argv[]) {
    auto registered = asterorm::testing::catch2::register_fixture<example_fixture>("example");
    if (!registered) return 1;
    return asterorm::testing::catch2::run(argc, argv);
}
```

With installed AsterORM and a separately supplied Catch2, a CMake client links
`AsterORM::asterorm` and `Catch2::Catch2`. A pkg-config client adds both `asterorm`
and `catch2`. Do not link `Catch2::Catch2WithMain` or `catch2-with-main`. The
adapter's listener is registered once by `run()`, so the header may appear in
multiple translation units. Use one runner invocation per process.

Registration accepts a unique lowercase ASCII name containing letters, digits,
`-` or `_`, and a unique fixture type. It must happen before `run()`. For configured
or non-default-constructible fixtures, pass a copyable factory returning
`std::unique_ptr<Fixture>` as the second argument. The factory is lazy: untagged
cases, filters that select no tagged case, listing and help do not construct or
prepare fixtures. The registry owns each constructed fixture until process exit.
The fixture must own its pool/session, return its borrowed session from `session()`,
and have nonthrowing destruction. Use one connection and checked connection-local
schema/reference setup; the adapter does not inspect or enforce pool configuration.
Unsupported session/connection capabilities reject before fixture construction or
preparation, so rejected backends cannot execute setup SQL through this adapter.

An optional checked `validate(Session&) -> result<void>` runs after the scope pins
its root/private savepoint and before typed body access becomes available. It runs
on every matching partial invocation, including the first. Use it to check
connection-local identity and reference baselines without reconnecting or reseeding.
Validation errors/exceptions poison the provider and trigger scope cleanup while
retaining the first setup diagnostic. Fixtures without the hook retain their
existing behavior. A hook with the wrong return type is a compile error.

Exactly one `[transactional:name]` tag selects a fixture. Selection follows Catch2's
ASCII case-insensitive tag matching. Unknown, empty and multiple fixture tags fail
closed. The typed accessor rejects a different fixture, an untagged invocation or
an unrelated worker thread. Registration errors are retained by the runner even
when the caller ignores their returned diagnostic. Registration is frozen during
execution.

The listener enters at `testCasePartialStarting` and explicitly finishes at
`testCasePartialEnded`, after body locals unwind. It also cleans up after fatal
REQUIRE, SKIP, ordinary body exceptions and Catch2's early-abort option. It uses no
assertion macros and lets no setup/cleanup exceptions escape its callbacks.
Preparation, entry, ownership or cleanup failure poisons the provider; later paths
cannot silently reconstruct or reseed it. Other providers remain independent.

The runner emits a separate stderr harness summary containing the failure count
and first diagnostic, including SQLSTATE/detail or a captured C++ exception when
available. Existing Catch2 nonzero statuses are preserved. Harness failures promote
a successful Catch2 exit to status 1, including failures in tagged bodies that
never request their session. Catch2's per-case totals and JUnit/XML can still show
success for listener cleanup failures; consume process status and the harness
summary as well.

The [generic scope restrictions](transactional-test-scopes.md) still apply:
managed completion and stream/advisory/timeout composition are gated; raw transaction
control and external side effects are outside isolation. This is serial test
infrastructure, with no promise of cleanup after fatal signals/process termination
or a whole-scope deadline. The [worked reference fixtures](worked-reference-fixtures.md)
show a driver that forbids replacement connections, the migrated seven-path
PostgreSQL CRUD case, and four SQLite generator/section paths. The
[combined local review](../transactional-test-harness-acceptance.md) records
acceptance within the revised v1 contract and the remaining release boundaries.
