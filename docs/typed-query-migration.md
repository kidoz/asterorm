# Migrating string queries to the typed DSL

Convert one SELECT at a time. Keep your existing pool, session, repository,
entity traits, and write operations. Include `<asterorm/sql/typed_query.hpp>` for
the typed builder and execution helper; `<asterorm/repository.hpp>` supplies the
repository used by the examples below.

The complete [runnable example](../examples/typed_query_migration.cpp) uses a
temporary table and checks these queries against known rows on PostgreSQL,
SQLite, and MySQL. The snippets below are checked against that source by Meson.

## Keep the mapping

This example uses explicit IDs and one optional column. Existing generated-key
and other column metadata can stay in your own mappings. The equality operator
only lets the example compare results; persistence does not require it.

<!-- guide:mapping -->
```cpp
struct User {
    std::int64_t id{};
    std::string email;
    int age{};
    bool active{true};
    std::optional<std::string> deleted_at;

    bool operator==(const User&) const = default;
};

template <> struct asterorm::entity_traits<User> {
    static constexpr const char* table = "typed_query_migration_users";
    static constexpr auto primary_key = asterorm::pk<&User::id>("id");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&User::id>("id"), asterorm::column<&User::email>("email"),
        asterorm::column<&User::age>("age"), asterorm::column<&User::active>("active"),
        asterorm::column<&User::deleted_at>("deleted_at"));
};

namespace sql = asterorm::sql;
namespace typed = asterorm::sql::typed;
```

`typed::col<&User::email>` is an inline variable template: there are no trailing
parentheses. It resolves the SQL column name from `entity_traits<User>::columns`.
Renaming a C++ member makes old member references fail compilation; renaming a
database column only requires updating its mapping and schema.

## Handle errors as before

`typed::query` returns `asterorm::result<std::vector<Result>>`. Check it before
using the rows. The example's `take` helper throws on an error to keep the query
snippets short; application code can inspect `error().kind`, `message`, and
backend diagnostics and return the error instead.

<!-- guide:errors -->
```cpp
template <typename T> T take(asterorm::result<T> result) {
    if (!result) {
        throw std::runtime_error(result.error().message);
    }
    return std::move(*result);
}
void take(asterorm::result<void> result) {
    if (!result) {
        throw std::runtime_error(result.error().message);
    }
}
```

## Convert a whole-entity query

Start with the existing string predicate and query options. Here `repo` is an
`asterorm::repository` over session `db`. These fragments live in a function
templated on the driver, hence the dependent-call `template` keyword; omit that
keyword in ordinary non-template application code.

<!-- guide:legacy -->
```cpp
asterorm::query_options options;
options.order_by = {{.column = "email", .ascending = true}};
options.limit = 10;
options.offset = 0;
auto legacy_rows = take(repo.template find_by<User>(
    (sql::col("age") >= sql::val(18)) && (sql::col("active") == sql::val(true)), options));
```

Replace the read with `typed::select<User>()`, replace names with member
references, and move options onto the builder:

<!-- guide:entities -->
```cpp
auto typed_rows =
    take(typed::query(db, typed::select<User>()
                              .where((typed::col<&User::age> >= typed::val(18)) &&
                                     (typed::col<&User::active> == typed::val(true)))
                              .order_by(typed::col<&User::email>, typed::asc)
                              .limit(10)
                              .offset(0)));
```

Both queries return `std::vector<User>` after checking the result. Values remain
bound parameters. Ordering, limit, and offset have the same meaning. `order_by`
defaults to ascending order; use `typed::desc` for descending order.

Typed and string predicates have different C++ types. Convert the complete
predicate for this query, including its `&&`, `||`, and `!` combinations; a
string `sql::predicate_ast` cannot be passed directly to typed `.where()`.
Other queries can keep using `repo.find_by`, string projections, and native SQL
while this query uses `typed::query`.

Builders are single-use: both `build()` and `typed::query(db, builder)` consume
the builder, including a named lvalue. Construct a fresh builder for each query.

## Select only the columns you need

Omit the DTO type to infer a tuple in selection order. Optional mapped members
stay optional, and a SQL NULL becomes `std::nullopt`.

<!-- guide:tuples -->
```cpp
auto tuples =
    take(typed::query(db, typed::select_cols(typed::col<&User::email>, typed::col<&User::age>,
                                             typed::col<&User::deleted_at>)
                              .where((typed::col<&User::age> >= typed::val(18)) &&
                                     (typed::col<&User::active> == typed::val(true)))
                              .order_by(typed::col<&User::email>)));
using UserTuple = std::tuple<std::string, int, std::optional<std::string>>;
static_assert(std::is_same_v<decltype(tuples), std::vector<UserTuple>>);
```

Use an explicit DTO for a named result. Fields must be brace-initializable from
the projected values in the same order; there is no result-field name matching.
Changing the selection order also changes the tuple or DTO initialization order.

<!-- guide:dto -->
```cpp
struct UserSummary {
    std::string email;
    int age;
};
auto summaries = take(typed::query(
    db, typed::select_cols<UserSummary>(typed::col<&User::email>, typed::col<&User::age>)
            .where(typed::col<&User::email> == typed::val("alice@example.com"))));
```

At least one selection must identify the mapped entity. For example,
`count_all()` alone cannot infer a table; `count(typed::col<&User::id>)` can.
All entity-bound items, predicates, grouping columns, and ordering columns in a
typed query must refer to the same entity. Joins and cross-entity projections
remain work for the string AST or native SQL.

## Preserve SQL NULL semantics

Use `is_null` and `is_not_null` for SQL NULL predicates:

<!-- guide:nulls -->
```cpp
auto live = take(typed::query(db, typed::select<User>()
                                      .where(typed::is_null(typed::col<&User::deleted_at>) &&
                                             (typed::col<&User::active> == typed::val(true)))
                                      .order_by(typed::col<&User::id>)));
auto archived = take(typed::query(
    db, typed::select<User>().where(typed::is_not_null(typed::col<&User::deleted_at>))));
```

Do not use `== typed::null`, `!= typed::null`, or a bound optional value as a
replacement for an `IS NULL` predicate. Comparison with a non-NULL value follows
SQL's three-valued logic: rows with NULL do not satisfy ordinary comparisons.
Optional columns compare using their underlying C++ value type.

Boolean columns require boolean operands (`typed::val(true)` or `false`).
`typed::val("false")` and `typed::val(0)` are rejected for a boolean column.
Text columns accept string literals as text parameters. Numeric comparison
compatibility is a C++ type check, not a proof against database overflow,
precision loss, collation differences, or backend coercions.

## Add grouped projections

Aggregates can be mixed with mapped columns. This query groups by `active` and
uses a predicate on that grouped column in `HAVING`:

<!-- guide:aggregates -->
```cpp
auto groups = take(typed::query(
    db, typed::select_cols(
            typed::col<&User::active>, typed::count_all(), typed::count(typed::col<&User::id>),
            typed::sum(typed::col<&User::age>), typed::avg(typed::col<&User::age>),
            typed::min(typed::col<&User::age>), typed::max(typed::col<&User::age>))
            .group_by(typed::col<&User::active>)
            .having(typed::col<&User::active> == typed::val(true))));
using Group = std::tuple<bool, std::int64_t, std::int64_t, std::int64_t, double, int, int>;
static_assert(std::is_same_v<decltype(groups), std::vector<Group>>);
```

Projected types follow these current rules:

| Selection | Projected C++ type |
|---|---|
| `count_all()`, `count(column)` | `std::int64_t` |
| `sum(integral column)` | `std::int64_t` |
| `sum(floating column)` | `double` |
| `avg(arithmetic column)` | `double` |
| `sum(numeric column)`, `avg(numeric column)` | `asterorm::numeric` |
| `min(column)`, `max(column)` | Underlying member type, with `optional` removed |

Aggregates currently project nonoptional values even when SQL can return NULL.
For example, SUM/AVG/MIN/MAX over an empty input or an all-NULL group can return
a decoding error. COUNT returns zero. Keep a string/native projection with an
optional result where nullable aggregate output is required. AVG of integers
projects to `double`, including when the server returns decimal text; consider
precision requirements before migrating an exact decimal calculation.

Typed aggregate objects currently have no predicate operators. An expression
such as `typed::count_all() > typed::val(1)` is unsupported. Keep aggregate-result
HAVING conditions on the string/native path. The typed builder also does not
validate SQL grouping legality: grouping, nonaggregated selections, and backend
function support must still form valid SQL.

## Keep an escape hatch

Existing `sql::col("name")`, runtime AST builders, repository methods, and native
queries remain available. For a construct outside the typed surface, keep that
whole query on its existing path and bind its values:

<!-- guide:escape -->
```cpp
auto adult_count = take(db.template native_scalar<std::int64_t>(
    "SELECT COUNT(*) FROM typed_query_migration_users WHERE age >= $1", 18));
```

`native_scalar<T>` returns a result containing `std::optional<T>`: no row means
an empty optional, and a decoding failure is an error. `native<T>` hydrates mapped
entities, while `native_map` supports custom row decoding. Caller-owned SQL is
not parsed or compile-time checked. Never concatenate untrusted values into it.

Advanced callers can lower a typed builder with `build()` and then modify the
returned `sql::select_ast` for the existing compiler. That leaves the typed
contract; the caller must preserve valid SQL and result order/types and provide
the execution/mapping path. A runtime AST cannot be passed back to
`typed::query` as a typed builder.

The current `typed::query` calls the connection's `execute_prepared` through
`session.with_connection`; it does not use the session's observed-execute helper.
Check observer/timeout requirements before moving a query that depends on that
helper. Connection-level behavior still applies. This guide does not change
that execution contract.

## What compilation checks

The typed surface checks mapped member pointers, entity consistency, operand
compatibility, and whether the selected values can initialize the result type.
Unmapped members and entities without traits fail in templates with the relevant
C++ member/type in the instantiation context. The compile-check suite exercises
these failures and the boolean, NULL, DTO, and projection boundaries.

It cannot verify that a mapped table/column actually exists in a running
database, parse arbitrary SQL, enforce database constraints, or guarantee every
backend accepts a SQL expression. Keep schema migrations and integration tests.
Runtime errors are still returned as `db_error`.

## Run the example

With the default PostgreSQL and SQLite adapters and examples enabled:

```sh
meson compile -C buildDir typed_query_migration
./buildDir/examples/typed_query_migration sqlite
./buildDir/examples/typed_query_migration postgres
meson test -C buildDir 'typed query guide*' --print-errorlogs
```

The PostgreSQL run uses `ASTERORM_TEST_CONNINFO` or the local compose database
defaults. Enable MySQL with `meson configure buildDir -Dmysql=enabled`, compile
again, and pass `mysql`; its connection setting is
`ASTERORM_MYSQL_TEST_CONNINFO`. See [compose.yaml](../compose.yaml) for the local
test services. SQLite uses `:memory:` and needs no service. Shared-library builds
may require the same runtime library search path as other project examples.

The example uses a one-connection pool so every query sees the same temporary
table; it checks entities, tuple/DTO values, NULL filters, grouping, and native
fallback results. Meson registers runtime checks for each enabled adapter when
both `tests` and `examples` are enabled. The snippet consistency check runs even
when examples are disabled.

See the [acceptance review](typed-query-acceptance.md) for the tested compatibility
contract and the scope of the existing runtime-cost evidence.
