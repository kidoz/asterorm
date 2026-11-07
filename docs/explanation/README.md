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
implemented predicate UPDATE/DELETE slice and its limits. Structured bound
arithmetic and INSERT-from-SELECT remain planned.

For executable lessons, start with [tutorials](../tutorials/README.md).

[Documentation home](../README.md)
