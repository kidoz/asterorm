# Choose pool cache policy and a transaction-pool profile

`pool_config::statement_caching` defaults to
`statement_cache_policy::per_connection`, bounded by
`prepared_statement_cache_size` (default 128). Set `statement_cache_policy::off`
to disable retained statement caching regardless of that capacity. A capacity of
zero also disables caching with the default policy. Per-session caching is deferred
and is not an enum value or alias.

This complete example runs with caching off on PostgreSQL, SQLite and MySQL.
Passing `transaction_pool = true` selects the restricted PostgreSQL profile;
unsupported adapters return a configuration error before connecting:

```cpp
#include <asterorm/session.hpp>

template <typename Driver>
asterorm::result<int> pool_cache_example(Driver driver, std::string settings,
                                       bool transaction_pool = false) {
    asterorm::pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 2;
    config.statement_caching = asterorm::statement_cache_policy::off;
    if (transaction_pool)
        config.profile = asterorm::pool_profile::transaction_pool;
    asterorm::connection_pool<Driver> pool{std::move(driver), config};
    if (auto failure = pool.warmup_error())
        return std::unexpected(*failure);
    asterorm::session db{pool};
    auto transaction = db.begin();
    if (!transaction)
        return std::unexpected(transaction.error());
    auto answer = db.template native_scalar<int>("SELECT $1", 42);
    if (!answer)
        return std::unexpected(answer.error());
    auto committed = transaction->commit();
    if (!committed)
        return std::unexpected(committed.error());
    if (*answer != 42) {
        asterorm::db_error error;
        error.kind = asterorm::db_error_kind::query_failed;
        error.message = "Unexpected cache policy example result";
        return std::unexpected(std::move(error));
    }
    return **answer;
}
```

Off routes PostgreSQL's ordinary bound `execute_prepared` entry point through
`execute_params`/`PQexecParams`, with unnamed protocol preparation and no retained
named plans. This includes session observation, native scalar/hydration,
repository and typed query paths. Control/raw SQL still uses its existing APIs;
streams retain their uncached protocols. SQLite/MySQL still prepare ephemeral
native statements to bind values, and retain no cached entries in off mode.
Do not reconfigure cache/profile hooks manually on pooled native connections.

`pool_profile::direct` is the default. The explicit PostgreSQL-only
`pool_profile::transaction_pool` requires effective cache off (the off policy or
zero capacity), plus an **absent** pool `statement_timeout` baseline. An explicit
zero baseline is still conflicting. Invalid enum values, unsupported explicit
capabilities and conflicts reject before warmup or lazy connect. Unsupported
custom adapters reject explicit off unless their cache-limit hook has the normal
adapter contract (`void set_max_prepared_statements(std::size_t)`); default legacy
behavior is preserved. Profile opt-in additionally requires the compile-time
`supports_transaction_pool_profile` marker and a nonthrowing
`set_transaction_pool_profile(bool)` hook. The standard pool supplies the optional
advisory-admission metadata consumed by sessions.

Profile metadata reaches session admission before borrowing. Session advisory
scopes and schema migrator apply/rollback/current-version operations reject
before callback, connection acquisition, observation or SQL. PostgreSQL native
session advisory lock/unlock and session timeout setters also reject before SQL;
profile state survives native connection moves. It forces the bound unnamed path
throughout the profile. `SET LOCAL` timeout scopes in a managed transaction and
WITHOUT HOLD declared cursors remain supported within that transaction.

The profile constrains these library-managed operations. Raw SQL, persistent TEMP
state, LISTEN, caller session settings, native hook mutation, and other
session-dependent features remain the caller's responsibility. There is no SQL
parser/sandbox, session-state reset or general proxy compatibility guarantee.
PgBouncer's [feature matrix](https://www.pgbouncer.org/features.html) lists the
transaction-mode restrictions; modern named-plan support depends on
[its prepared-statement setting](https://www.pgbouncer.org/config.html#max_prepared_statements).
This conservative profile does not depend on that named-plan support.

For the checked local setup, run `just test-pgbouncer`. It starts only the opt-in
proxy and its PostgreSQL dependency using [the Compose overlay](../../compose.pgbouncer.yaml).
The proxy is PgBouncer 1.24.1, pinned by image digest, with transaction mode, two
backing connections and `max_prepared_statements=0`. The hidden `[pgbouncer]` test
performs six rounds of simultaneous explicit transactions on two distinct backend
PIDs, bound repository/typed CRUD, local timeout/cursor work, zero named plans and
rejected session APIs. The ordinary regression suite does not require this proxy.
Services remain running; remove only the proxy with
`docker compose -f compose.yaml -f compose.pgbouncer.yaml --profile pgbouncer rm -sf pgbouncer`
when finished. Test credentials and the loopback listener are local fixtures.

The [evidence](../evidence/pool-policy/README.md) records this scoped direct/proxy
verification, controlled real-libpq packet checks and installed consumers. It does
not establish other proxy versions/configurations, arbitrary session SQL, remote
CI or release readiness. Rebuild library and consumers for appended configuration
fields and PostgreSQL connection layout/API changes. Combined production-pool
acceptance is next.
