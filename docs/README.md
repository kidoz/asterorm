# AsterORM documentation

AsterORM maps plain C++23 structs to database rows. PostgreSQL is the primary
backend, SQLite supports embedded applications and server-free tests, and MySQL
is available as an optional adapter. ClickHouse support is experimental.

Choose a section based on what you want to do:

| Your goal | Start here |
| --- | --- |
| Learn by building a working program | [Tutorials](tutorials/README.md) |
| Complete a specific task | [How-to guides](how-to/README.md) |
| Look up APIs, options, or backend capabilities | [Reference](reference/README.md) |
| Understand the design and its tradeoffs | [Explanation](explanation/README.md) |

New to the library? Start with [your first SQLite application](tutorials/first-application.md).
It covers mapping, schema creation, CRUD, and typed queries without a database server.

These pages describe the current source tree. See the [changelog](../CHANGELOG.md)
for release changes and the [benchmark guide](../benchmarks/README.md) for
performance measurements.
