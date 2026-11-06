# Use transactions and retries

Use one session for all operations in a transaction. `begin()` pins a pool
connection until commit or rollback. These snippets assume the `user` entity
from the [tutorial](../tutorials/first-application.md) and a function returning
`asterorm::result<void>`.

## Commit a unit of work

```cpp
auto transaction = db.begin();
if (!transaction)
    return std::unexpected(transaction.error());

asterorm::repository repo{db};
user alice{.email = "alice@example.com", .name = "Alice"};
auto inserted = repo.insert(alice);
if (!inserted)
    return std::unexpected(inserted.error());

return transaction->commit();
```

The guard rolls back when it leaves scope without a successful commit. Check
commit results: successful statements do not by themselves establish a
successful transaction. Nested `begin()` calls fail; use `savepoint(name)`,
`rollback_to_savepoint(name)`, and `release_savepoint(name)` for partial rollback.

## Use a callback

```cpp
return db.transact([](auto& transaction_db) -> asterorm::result<void> {
    asterorm::repository repo{transaction_db};
    user alice{.email = "alice@example.com", .name = "Alice"};
    return repo.insert(alice);
});
```

The callback returns an expected-like result. Success commits, an error rolls
back, and an exception unwinds the guard and propagates to the caller.

## Retry transient conflicts

Use `db.retry_transaction(callback, max_attempts, options)` to retry complete
transactions on `serialization_failure` or `deadlock_detected`. The default is
three attempts, with a short increasing delay between attempts. A zero attempt
count is treated as one attempt. Other errors return immediately.

Construct or reload mutable entities inside the callback. A rollback does not
undo changes already made to C++ objects, including populated generated IDs.
The callback can run more than once; keep external actions such as sending
messages outside it or make them independently idempotent.

## Choose transaction options

Set `transaction_options::isolation`, `read_only`, and `deferrable`, then pass
the options to `begin()`, `transact()`, or `retry_transaction()`.

- PostgreSQL uses its transaction syntax and server validation.
- SQLite uses `BEGIN IMMEDIATE` for repeatable-read or serializable requests and
  `BEGIN` otherwise. Read-only and deferrable flags are accepted but not enforced.
- MySQL supports isolation and read-only settings and rejects deferrable mode.
  Use transactional tables such as InnoDB.

See [connection lifetimes](../explanation/connections-and-transactions.md) for
ownership and concurrency considerations.
