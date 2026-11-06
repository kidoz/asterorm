# Run the test suites

Run commands from the repository root. Tests require Catch2 with a main function;
Meson can resolve it through the supplied wrap. Use a new build directory for
each independent configuration below.

## Test without a server

```sh
meson setup build-test-sqlite -Dpostgres=disabled -Dsqlite=enabled \
  -Dmysql=disabled -Dclickhouse=disabled -Dtests=true
meson test -C build-test-sqlite --print-errorlogs
```

This runs core unit tests, typed DSL compile checks, and SQLite integration
tests. SQLite uses in-memory databases.

## Test PostgreSQL

```sh
docker compose up -d --wait postgres
meson setup build-test-pg -Dpostgres=enabled -Dsqlite=enabled -Dtests=true
meson test -C build-test-pg --print-errorlogs
```

PostgreSQL tests use `ASTERORM_TEST_CONNINFO`, falling back to the Compose
database (`orm_test` user/password/database at `127.0.0.1:5432`). Use a disposable
database: integration tests create and drop tables.

To select one registered test:

```sh
meson test -C build-test-pg 'pg integration' --print-errorlogs
meson test -C build-test-pg 'sqlite integration' --print-errorlogs
```

## Test MySQL

```sh
docker compose --profile mysql up -d --wait mysql
meson setup build-test-mysql -Dpostgres=disabled -Dsqlite=enabled \
  -Dmysql=enabled -Dtests=true
meson test -C build-test-mysql --print-errorlogs
```

MySQL integration tests use `ASTERORM_MYSQL_TEST_CONNINFO` or the Compose
defaults at `127.0.0.1:3307`. They fail when the configured server is unavailable.

The repository also supplies `just test-sqlite`, `just test-pg`, and
`just test-mysql` for the shared `buildDir` configuration. `just test` runs all
enabled suites; ensure their services are available. `just test-all` removes
Compose containers on success through `just db-down`, which runs
`docker compose down -v`.

For code style checks, use `just format-check` and `just lint`. For performance
work, follow the [benchmark guide](../../benchmarks/README.md).
