# Roll back an isolated test scope

Include `<asterorm/testing/transactional_session.hpp>` to use the optional
`asterorm::testing::transactional_session<Session>` helper. It supports PostgreSQL
and SQLite and has no Catch2 dependency. A separately included
[Catch2 adapter](catch2-transactional-tests.md) provides lazy registered fixtures
and isolation for each partial invocation.

Prepare connection-local schema/reference data outside the scope, then create a
new rollback-only scope on an idle session. It owns a root transaction and private
savepoint; ordinary repository, typed and native work can use `scope.session()`.

```cpp
#include <asterorm/testing/transactional_session.hpp>
#include <asterorm/sqlite/driver.hpp>
#include <string>

template <typename Driver>
asterorm::result<int> count_after_test_rollback(Driver driver, std::string conninfo) {
    asterorm::pool_config config;
    config.conninfo = std::move(conninfo);
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    asterorm::session db{pool};
    auto prepared = db.with_connection([](auto& connection) -> asterorm::result<void> {
        auto created = connection.execute(
            "CREATE TEMP TABLE scope_example(id INTEGER PRIMARY KEY,note TEXT)");
        if (!created) return std::unexpected(created.error());
        auto seeded = connection.execute("INSERT INTO scope_example VALUES(1,'reference')");
        if (!seeded) return std::unexpected(seeded.error());
        return {};
    });
    if (!prepared) return std::unexpected(prepared.error());

    using test_scope = asterorm::testing::transactional_session<decltype(db)>;
    auto scope = test_scope::create(db);
    if (!scope) return std::unexpected(scope.error());
    auto changed = scope->session().with_connection([](auto& connection) {
        return connection.execute("INSERT INTO scope_example VALUES(2,'temporary')");
    });
    if (!changed) return std::unexpected(changed.error());
    auto finished = scope->finish();
    if (!finished) return std::unexpected(finished.error());

    auto count = db.template native_scalar<int>("SELECT count(*) FROM scope_example");
    if (!count) return std::unexpected(count.error());
    return count->value_or(0); // 1: the reference row survives, this scope's row does not
}

asterorm::result<int> sqlite_scope_example() {
    return count_after_test_rollback(asterorm::sqlite::driver{}, ":memory:");
}
```

The scope and borrowed repositories must not outlive the session/pool. Move
construction transfers ownership; copy and move assignment are disabled.
`session()` throws `logic_error` after finish, poisoning or move. `finish()` is
idempotent; terminal cleanup failures retain their first diagnostic and do not
retry. A moved-from `finish()` succeeds without work.

Normal finish performs ROLLBACK TO, RELEASE and root ROLLBACK, verifies native idle
state and releases the lease for reuse. It restores the observer snapshotted at
entry; control SQL invokes no observer callback. Ordinary body queries still emit
their normal events. The destructor attempts the same cleanup without throwing;
call explicit finish to observe cleanup failures. C++ exceptions from explicit
entry/finish preserve identity, including allocation failure.
Cleanup performs synchronous SQL and has no whole-scope deadline.

An existing transaction, another testing scope, connection/observer callback,
stream, advisory scope or timeout scope rejects admission. Streams and advisory/
timeout scopes also reject inside an active testing scope. Managed commit/rollback
and use of `_asterorm_test_` savepoint names reject before SQL. User savepoints with
other names remain available. MySQL, ClickHouse and unknown/legacy capabilities
reject before acquiring a connection.

Custom connections need an explicit constant boolean
`supports_transactional_test_scope = true`, a PostgreSQL/SQLite dialect, and
nonthrowing close/native-idle methods. Legacy session facades without the internal
ownership bridge reject without calling their executors. Generic core headers
expose no vendor API or test-framework dependency.

Unexpected non-idle checkout, connection/transaction loss or cleanup failure
discards and poisons the session. Further scope/query access reports the stored
failure; it does not silently reconnect or recreate reference data. SQLite native
automatic rollback is detected before another statement can escape into autocommit.
If destruction occurs during an active connection callback, discard and lease
release wait until its safe unwind; cleanup sends no nested rollback SQL.

This is trusted test infrastructure, not an SQL sandbox. Raw transaction control,
independent connections, pool shutdown and session-level external effects are
outside its isolation contract. It does not rewind generated identifiers or C++
objects. Keep transaction, migration, locking/timeout, streaming and connection-loss
integration cases outside these scopes unless they explicitly test rejection.

The [design and delivery contract](../transactional-test-harness-design.md) records
the delivered adapter/worked fixtures and remaining combined acceptance review.
