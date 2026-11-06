# Run the PostgreSQL example

This lesson runs the repository's [quickstart program](../../examples/quickstart.cpp)
against a local PostgreSQL database. It prints the SQL timing events and the
results of inserting, loading, updating, and erasing a user.

You need Docker Compose, the build tools from the
[SQLite lesson](first-application.md), and libpq development headers. Port 5432
must be available for the development service.

## 1. Start the development database

From the repository root:

```sh
docker compose up -d --wait postgres
export ASTERORM_TEST_CONNINFO='host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test sslmode=disable'
```

These credentials and the disabled TLS setting belong to the local development
service. The example drops and recreates `asterorm_quickstart_users` each time;
use the disposable development database for this lesson.

## 2. Build the example

```sh
meson setup build-postgres-tutorial -Dpostgres=enabled -Dsqlite=disabled \
  -Dmysql=disabled -Dclickhouse=disabled -Dtests=false -Dexamples=true
meson compile -C build-postgres-tutorial quickstart
./build-postgres-tutorial/examples/quickstart
```

The output includes SQL timing lines, `inserted user id=1`, a loaded user,
`after update: Alice Cooper`, and `erased`. Timings vary between runs.

Open the example and find its three main parts: `entity_traits<user>` maps the
entity, `pool_config` supplies connection settings, and `repository` executes
the CRUD operations. `db.set_observer()` prints events for these operations.

## 3. Stop the service

```sh
docker compose stop postgres
```

Next, use [build and install](../how-to/build-and-install.md) to link AsterORM
into your own application, or [query data](../how-to/query-data.md) to add filters
and projections.
