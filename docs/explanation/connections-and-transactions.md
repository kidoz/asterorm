# Connection and transaction lifetimes

A pool owns reusable database connections. An acquisition transfers exclusive
use of one connection into a move-only lease. Returning the lease makes that
connection available again. This bounds concurrency without exposing a global
connection singleton.

Outside a transaction, a session operation borrows a connection for that
operation. Consecutive operations may use different physical connections.
Connection-local state therefore belongs to a lease or a transaction, not to
an assumed sequence of independent session calls.

Inside a transaction, the session retains one lease. All operations through
that session use the pinned connection until commit or rollback. Savepoints
also operate on that connection. Constructing another session over the same
pool does not join the transaction.

## Object ownership

Create the pool first, then sessions, repositories, and transaction guards.
Destroy dependent objects before their owners: a repository borrows a session,
a session borrows a pool, and a transaction guard refers back to its session.
Use separate sessions for independent units of concurrent work. Sharing a
session also shares its transaction state.

An explicit lease can exhaust a small pool while code waits for another lease.
For example, retaining a schema-creation lease in a one-connection SQLite pool
prevents the repository from acquiring that connection. Scoping direct access
through `with_connection()` makes release happen at the callback boundary.

Private SQLite `:memory:` databases illustrate the importance of connection
identity: a second connection opens a separate database. A one-connection pool
lets schema creation and later CRUD observe the same in-memory database.

## Database state and C++ state

Rollback restores database transaction state. It does not restore C++ objects
already hydrated or modified by an operation. An object can retain a generated
ID after its insert was rolled back. Retrying a transaction with that object
can therefore behave differently from the original attempt.

Build or reload transaction-local objects inside retry callbacks. Keep external
side effects outside the callback unless they are safe to repeat. A successful
body still requires a successful commit; connection loss and transaction
invalidation can make commit fail.

Prepared statements are likewise connection-local. The bounded cache lives on
each adapter connection, allowing reuse across leases without assuming that a
statement prepared on one connection exists on another.

For concrete code, see [transactions](../how-to/transactions.md) and
[pool configuration](../how-to/pooling-and-observability.md).
