# Entities and repositories

Declarations: [entity_traits.hpp](../../include/asterorm/core/entity_traits.hpp)
and [repository.hpp](../../include/asterorm/core/repository.hpp).

## Mapping contract

Specialize `asterorm::entity_traits<T>` with a table name, primary key mapping,
and tuple of column mappings. Names describe the existing database schema;
traits do not create or migrate tables.

| Declaration | Meaning |
| --- | --- |
| `column<&T::field>(name)` | Writable mapped field |
| `column<&T::field>(name, generated::always)` | Database-generated field omitted from insert values |
| `column<&T::field>(name, generated::by_default)` | Omitted from inserts when its optional value is empty |
| `pk<&T::field>(name)` | Single-column primary key |
| `std::make_tuple(pk<...>(...), pk<...>(...))` | Composite primary key, in tuple order |
| `version<&T::field>(name)` assigned to `version_column` | Optimistic-lock version field |

`std::optional<T>` represents nullable values. For generated keys, an empty
optional distinguishes an omitted key from an explicit value. See the
[tutorial mapping](../tutorials/first-application.md).

## Repository operations

Construct `asterorm::repository repo{db}` with an existing session.

| Call | Result payload | Behavior |
| --- | --- | --- |
| `insert(entity)` | `void` | Inserts and refreshes the mutable entity |
| `find<T>(key)` | `T` | Fetches one row; missing row is `query_failed` |
| `update(entity)` | `void` | Updates by primary key; mutable overload refreshes fields |
| `erase<T>(key)` | `void` | Deletes by primary key; zero affected rows is an error |
| `patch(entity, changed_columns)` | `void` | Updates named mapped fields; does not refresh the entity |
| `upsert(entity)` | `void` | Inserts or updates by conflict handling and refreshes the entity |
| `insert_many(const vector<T>&)` | `size_t` | One multi-row insert; leaves generated IDs unset |
| `upsert_many(vector<T>&)` | `size_t` | Bulk upsert with refresh; unsupported on MySQL |
| `find_all<T>(options)` | `vector<T>` | Fetches mapped entities |
| `find_by<T>(predicate, options)` | `vector<T>` | Filters using a runtime AST predicate |
| `find_one_by<T>(predicate)` | `optional<T>` | Empty optional when no row matches |

All payloads are wrapped in `asterorm::result<...>`. Composite keys passed to
`find` and `erase` use a tuple matching the mapping order. Bulk insert rows must
agree on the set of included generated columns.

`query_options` contains `order_by` (`vector<query_order>`), optional `limit`,
and optional `offset`. Each `query_order` has a mapped `column` name and
`ascending` flag, defaulting to `true`.

Projection helpers include `select_projection<T, Projection>`, `select_projection_by<T, Projection>`,
`select_map<T>`, and `select_map_by<T>`. Consult the header for their column-list
and mapper arguments. Typed projections are documented in [SQL reference](sql.md).

## Optimistic locking

For a mapped `version_column`, updates add the old version to the WHERE clause
and increment the database version. No matching row produces `stale_write`.
Mutable `update()` refreshes the version. Const updates and `patch()` cannot
refresh the caller's entity; reload it before another versioned write.

## Explicit relationships

An optional `relationships` tuple can contain `one_to_one`, `one_to_many`, and
`many_to_one` mappings, each parameterized by the related entity and local and
related member pointers. `load_related_many<Index>(entity, options)` returns a
related entity vector; `load_related_one<Index>(entity)` returns an optional
related entity. Each is an explicit query. Automatic eager/lazy loading and an
identity map are not implemented.

See [backend capabilities](backends.md) for MySQL refresh and upsert restrictions.
