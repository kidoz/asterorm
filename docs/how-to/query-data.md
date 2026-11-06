# Query and project data

The examples below assume the `user` mapping and session `db` from the
[first application](../tutorials/first-application.md). Include
`<asterorm/sql/typed_query.hpp>` to execute typed builders.

## Filter and order whole entities

```cpp
namespace typed = asterorm::sql::typed;
auto active_users = typed::query(db,
    typed::select<user>()
        .where(typed::col<&user::active> == typed::val(true))
        .order_by(typed::col<&user::email>, typed::asc)
        .limit(20));
if (!active_users)
    return std::unexpected(active_users.error());
```

Use this in a function returning `asterorm::result<...>`. The result contains a
`std::vector<user>`. For nullable columns, use `typed::is_null(column)` and
`typed::is_not_null(column)`; SQL NULL checks are distinct from ordinary equality.

## Select only the fields you need

Omit the result type to infer a tuple in selection order:

```cpp
auto contacts = typed::query(db,
    typed::select_cols(typed::col<&user::email>, typed::col<&user::active>));
// result<vector<tuple<string, bool>>>
```

For a named result, supply an aggregate that can be initialized from those fields:

```cpp
struct contact {
    std::string email;
    bool active;
};
auto contacts = typed::query(db,
    typed::select_cols<contact>(typed::col<&user::email>, typed::col<&user::active>));
```

These are alternative snippets. At least one item must identify the mapped
entity; all entity-bound items must refer to the same entity.

## Count rows by a field

```cpp
auto totals = typed::query(db,
    typed::select_cols(typed::col<&user::active>, typed::count_all())
        .group_by(typed::col<&user::active>));
// result<vector<tuple<bool, int64_t>>>
```

Aggregate types and NULL behavior are listed in the
[SQL reference](../reference/sql.md).

## Use runtime column names

For a predicate assembled at runtime, use the string AST with a repository:

```cpp
asterorm::repository repo{db};
auto active_users = repo.find_by<user>(
    asterorm::sql::col("active") == asterorm::sql::val(true));
```

See [entity and repository reference](../reference/entities-and-repositories.md)
for pagination and other repository operations.

## Hydrate a native query

Pass values separately from SQL text:

```cpp
auto active_users = db.native<user>(
    "SELECT id, email, name, active FROM users WHERE active = $1", true);
```

`native<T>()` requires every mapped column by name. Alias expressions to their
mapped names. For deliberately partial rows, choose
`db.native_as<user>(asterorm::native_hydration::lenient, sql, args...)`;
omitted fields retain their value-initialized defaults. Prefer a projection
when partial rows should have their own type.

To read a scalar:

```cpp
auto total = db.native_scalar<std::int64_t>("SELECT COUNT(*) FROM users");
// result<optional<int64_t>>; the optional is empty when there are no rows.
```

The SQL text remains caller-owned. Use trusted statement templates and bind
values; raw SQL is not checked by the typed DSL.
