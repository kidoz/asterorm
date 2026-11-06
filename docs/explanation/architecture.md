# Architecture and entity mapping

AsterORM uses a data mapper design. Domain entities are plain structs, while
`entity_traits<T>` supplies table, column, and key metadata separately. An entity
therefore has no base class, hidden database handle, or persistence methods.
The same type can be used without a database connection.

Mapping uses pointers to members, allowing the compiler to connect fields to
their C++ types. This is explicit metadata rather than runtime reflection.
Database schema creation remains a separate concern: changing a mapping does
not change an existing table. Migrations or native DDL must bring the schema
and mapping into agreement.

## Responsibilities

| Layer | Responsibility |
| --- | --- |
| Entity traits and codecs | Describe fields and convert values |
| Repository | Implement entity CRUD and explicit relationship queries |
| Typed SQL | Check supported expressions and determine result types |
| Runtime AST and compiler | Represent statements and produce dialect SQL and parameters |
| Session | Acquire connections, hold transaction state, hydrate native queries |
| Connection pool | Lease exclusive connections and control their lifecycle |
| Backend adapter | Execute through the vendor client and expose results |

The generic layers rely on the shape of a driver's connection and result types.
Adapter-specific capabilities handle differences such as transaction syntax,
SQL dialect, and generated-value refresh. MySQL's write-plus-read refresh is
an example of why a common C++ API does not imply identical database execution.

The source layout mirrors these boundaries: core, pool, SQL, and CRUD live in
`src/`, while adapters live in `src/database/`. Installed public includes retain
paths such as `asterorm/postgres/driver.hpp` and `asterorm/sqlite/driver.hpp`.

## Explicit object and query behavior

Hydration produces independent C++ values. Loading the same row twice does not
establish shared object identity, and editing one object does not update another.
There is no automatic change tracking or identity map.

Relationships are metadata plus explicit `load_related_one()` or
`load_related_many()` calls. Accessing a struct field does not trigger a query.
This makes connection use visible, but callers must choose when to load related
rows and account for the cost of repeated queries.

See [entity reference](../reference/entities-and-repositories.md) for mapping
declarations and [backend reference](../reference/backends.md) for portability limits.
