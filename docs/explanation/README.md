# Explanation

These pages explain the ideas and tradeoffs behind the API:

- [Architecture and entity mapping](architecture.md)
- [Connection and transaction lifetimes](connections-and-transactions.md)
- [Choosing a query interface](query-interfaces.md)

The [statement timeout contract](../statement-timeout-design.md) distinguishes
PostgreSQL pool defaults from transaction-bound session scopes and their v1 limits.
The [local locking/timeout review](../locking-timeout-acceptance.md) records accepted
scope, current evidence, and release boundaries.

The [bulk predicate DML contract](../bulk-predicate-dml-design.md) records the
implemented predicate UPDATE/DELETE, arithmetic and INSERT-from-SELECT slices
and their limits. The [local acceptance review](../bulk-predicate-dml-acceptance.md)
maps the revised v1 requirements to current code and fresh verification.

The [streaming inventory and contract](../streaming-design.md) defines the portable
reader/error interface, connection ownership, cancellation limits and memory checks.
Session ownership, the native PostgreSQL single-row reader and repository entity
streaming are implemented, including declared-cursor batches. Combined acceptance
remains open. See the [entity streaming guide](../how-to/stream-entities.md).

For executable lessons, start with [tutorials](../tutorials/README.md).

[Documentation home](../README.md)
