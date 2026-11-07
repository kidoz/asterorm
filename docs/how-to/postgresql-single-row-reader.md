# Read PostgreSQL results one row at a time

PostgreSQL 16+ connections expose `open_single_row(sql, parameters)`, returning a
move-only `pg::single_row_reader`. `next()` returns
`result<optional<pg::result>>`: one owned row, clean EOF, or a structured error.
This is a native adapter API. The [repository stream](stream-entities.md) adds
entity hydration, terminal session observation and declared-cursor batching.

With an existing standalone PostgreSQL connection:

```cpp
#include <asterorm/postgres/connection.hpp>
#include <iostream>
#include <utility>

asterorm::result<void> stream_values(asterorm::pg::connection& connection) {
    auto opened = connection.open_single_row(
        "SELECT value FROM generate_series(1,$1::integer) AS value",
        {asterorm::encode_parameter(3)});
    if (!opened) return std::unexpected(opened.error());
    auto reader = std::move(*opened);
    for (;;) {
        auto row = reader.next();
        if (!row) return std::unexpected(row.error());
        if (!*row) break;
        std::cout << (*row)->get_int64(0, 0).value() << '\n';
    }
    return {};
}
```

Values are bound in text format. SQL and parameter buffers need only survive the
opening call. Empty/NUL-containing SQL, stored codec errors and text parameters
with NUL bytes reject before dispatch. A healthy idle connection or healthy active
transaction is required. No transaction is implicitly started or committed.

Consume through the EOF result to permit reuse. Seeing the final row alone does
not finish the protocol. The implementation requires PostgreSQL's empty terminal
tuple result and final null result before releasing its native busy state; see
[single-row mode](https://www.postgresql.org/docs/16/libpq-single-row-mode.html) and
[asynchronous processing](https://www.postgresql.org/docs/16/libpq-async.html).
The public API remains synchronous and can block on a read.

While a reader is active, ordinary execution, another reader, COPY and adapter
control SQL reject. Cache mutation methods returning `void` throw `logic_error`.
`close()` or destruction of an unfinished reader closes the physical connection
without draining or issuing SQL. This may roll back all earlier uncommitted work;
consume through EOF to retain a caller transaction. EOF and explicit close are
idempotent. A failed reader retains its original diagnostic on repeated `next()`;
a moved-from/default reader returns a local `query_failed` instead of reading.

Owned row results remain valid after the next read or connection destruction.
Do not retain all rows when bounded client memory is the goal. Rows already
delivered cannot be retracted if a later SQL error arrives. Native errors preserve
available SQLSTATE, message, detail, hint, table, column and constraint fields;
failure discards the connection without automatic replay.

SQL remains caller-owned. The native entry point does not parse it or enforce
read-only behavior. A non-row command rejects when its result arrives, after its
effects may already have committed in autocommit. Use row-returning queries and
choose transaction boundaries explicitly.

The reader's shared record follows connection moves and invalidates safely when
its connection closes or is destroyed. An active reader makes
`connection::is_pool_reusable()` false. The pool's optional nonthrowing boolean
reuse hook prevents a reader escaped from an ordinary, unreserved
`session::with_connection` callback from leaving unread results in the idle pool:
the connection is discarded and that escaped reader becomes invalid.

For session integration, readers must use the existing internal stream
reservation and run native reads through its authorized callback, completing the
reservation only after native EOF. Reservation failure/abandonment invalidates the
transaction and releases capacity. The current native reader emits no query events;
the [repository bridge](stream-entities.md) owns hydration and terminal observation. Native
connections/readers are single-consumer objects; concurrent access needs external
synchronization.

The connection layout changed. Rebuild the library and consumers together. The
[streaming contract](../streaming-design.md) records current coverage, limits and
remaining work, including combined repository memory acceptance.
