# Your first SQLite application

In this lesson you will store a user in an in-memory database, read it back,
change its name, query its email, and delete it. The database disappears when
the program exits.

## 1. Build and install AsterORM

You need a C++23 compiler and standard library with `std::expected`, Meson 1.1.0
or newer, a build backend such as Ninja, Python 3, pkg-config, and SQLite 3.35 or
newer with development headers. Run these commands from the repository root.
Use a new build directory for this lesson:

```sh
meson setup build-tutorial -Dpostgres=disabled -Dsqlite=enabled \
  -Dmysql=disabled -Dclickhouse=disabled -Dtests=false -Dexamples=false \
  --prefix="$PWD/build-tutorial/install" --libdir=lib
meson compile -C build-tutorial
meson install -C build-tutorial
export PKG_CONFIG_PATH="$PWD/build-tutorial/install/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
```

The installed package now supplies headers and the core and SQLite libraries.

## 2. Write the application

Save the following as `build-tutorial/first_app.cpp`. The entity mapping names
the table and columns; the `CREATE TABLE` statement creates the actual schema.

```cpp
#include <asterorm/repository.hpp>
#include <asterorm/session.hpp>
#include <asterorm/sql/typed_query.hpp>
#include <asterorm/sqlite/driver.hpp>

#include <iostream>
#include <optional>
#include <string>
#include <tuple>

struct user {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
};

template <> struct asterorm::entity_traits<user> {
    static constexpr const char* table = "users";
    static constexpr auto primary_key = asterorm::pk<&user::id>("id");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&user::id>("id", asterorm::generated::by_default),
        asterorm::column<&user::email>("email"),
        asterorm::column<&user::name>("name"),
        asterorm::column<&user::active>("active"));
};

asterorm::result<void> run_application() {
    asterorm::pool_config cfg;
    cfg.conninfo = ":memory:";
    cfg.min_size = 1;
    cfg.max_size = 1;
    asterorm::connection_pool<asterorm::sqlite::driver> pool{
        asterorm::sqlite::driver{}, cfg};
    asterorm::session db{pool};
    asterorm::repository repo{db};

    auto schema = db.with_connection([](auto& conn) -> asterorm::result<void> {
        auto created = conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, "
            "email TEXT NOT NULL UNIQUE, name TEXT NOT NULL, "
            "active INTEGER NOT NULL)");
        if (!created)
            return std::unexpected(created.error());
        return {};
    });
    if (!schema)
        return std::unexpected(schema.error());

    user alice{.email = "alice@example.com", .name = "Alice"};
    auto inserted = repo.insert(alice);
    if (!inserted)
        return std::unexpected(inserted.error());
    std::cout << "inserted id=" << alice.id.value() << '\n';

    auto loaded = repo.find<user>(*alice.id);
    if (!loaded)
        return std::unexpected(loaded.error());
    loaded->name = "Alice Cooper";
    auto updated = repo.update(*loaded);
    if (!updated)
        return std::unexpected(updated.error());

    namespace typed = asterorm::sql::typed;
    auto contacts = typed::query(db,
        typed::select_cols(typed::col<&user::email>, typed::col<&user::name>)
            .where(typed::col<&user::active> == typed::val(true)));
    if (!contacts)
        return std::unexpected(contacts.error());
    for (const auto& [email, name] : *contacts)
        std::cout << name << " <" << email << ">\n";

    auto erased = repo.erase<user>(*alice.id);
    if (!erased)
        return std::unexpected(erased.error());
    std::cout << "erased\n";
    return {};
}

int main() {
    auto outcome = run_application();
    if (!outcome) {
        std::cerr << outcome.error().message << '\n';
        return 1;
    }
}
```

The pool has exactly one connection because each `:memory:` connection has its
own database. The schema callback releases its lease before CRUD starts.

## 3. Compile and run

From the same shell and repository root, compile with the installed package:

```sh
c++ -std=c++23 build-tutorial/first_app.cpp \
  $(pkg-config --cflags --libs asterorm) \
  -Wl,-rpath,"$PWD/build-tutorial/install/lib" \
  -o build-tutorial/first_app
./build-tutorial/first_app
```

Expected output:

```text
inserted id=1
Alice Cooper <alice@example.com>
erased
```

You have now mapped a plain struct, received a database-generated key, updated a
row, and hydrated a typed projection. Continue with the
[PostgreSQL tutorial](postgresql.md), or learn how to
[group operations in a transaction](../how-to/transactions.md).
