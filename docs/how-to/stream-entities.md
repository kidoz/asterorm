# Stream mapped PostgreSQL entities

`repository::stream<T>(select_ast, stream_options)` returns
`result<row_stream<T>>`. `next()` yields one owned entity, clean EOF, or a structured
error. PostgreSQL 16+ supports single-row mode and declared-cursor batches. Other
backends reject before acquiring a connection.

The typed whole-entity builder can lower through `build()`:

```cpp
#include <asterorm/row_stream.hpp>
#include <asterorm/repository.hpp>
#include <asterorm/postgres/driver.hpp>
#include <asterorm/sql/typed.hpp>

struct StreamJob {
    int id{};
    std::optional<std::string> name;
    bool active{};
};
template <> struct asterorm::entity_traits<StreamJob> {
    static constexpr const char* table = "stream_jobs";
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&StreamJob::id>("id"),
        asterorm::column<&StreamJob::name>("name"),
        asterorm::column<&StreamJob::active>("active"));
};
using StreamSession = asterorm::session<asterorm::connection_pool<asterorm::pg::driver>>;

asterorm::result<std::size_t> count_active_jobs(StreamSession& db) {
    namespace typed = asterorm::sql::typed;
    asterorm::repository repo{db};
    auto opened = repo.stream<StreamJob>(typed::select<StreamJob>()
        .where(typed::col<&StreamJob::active> == typed::val(true))
        .order_by(typed::col<&StreamJob::id>).build());
    if (!opened) return std::unexpected(opened.error());
    auto stream = std::move(*opened);
    std::size_t count = 0;
    for (;;) {
        auto row = stream.next();
        if (!row) return std::unexpected(row.error());
        if (!*row) break;
        ++count; // process **row here; retain entities only when needed
    }
    return count;
}
```

The runtime source must use the entity's mapped table and all mapped bare columns
in mapping order. No wildcard, aliases, joins, function projections, grouping,
HAVING or row-lock clauses are accepted. Mapped-column ordering and PostgreSQL
LIMIT/OFFSET are supported. Predicates accept mapped bare columns, bound values,
comparisons, AND/OR and NULL tests; traversal is limited to depth 64 and 1024 nodes.
Malformed shapes, codec errors and embedded text NULs reject before leasing.
Entities must be default-constructible and movable, with codec-supported fields.
Rows also validate their native width/names before positional hydration.

Default options select `stream_mode::single_row`. `chunk_size` must be between
1 and 65536; it does not alter single-row granularity. Cursor mode uses that value
as the maximum native batch size, releases each batch before the next fetch and
hydrates one owned entity per `next()`. It requires an existing healthy managed
transaction and never adds an implicit BEGIN, commit or retry. A short batch is
followed by an empty fetch to establish EOF and close the private portal.

Here the application explicitly manages the transaction:

```cpp
asterorm::result<std::size_t> count_active_jobs_in_batches(StreamSession& db) {
    auto transaction = db.begin();
    if (!transaction) return std::unexpected(transaction.error());
    namespace typed = asterorm::sql::typed;
    asterorm::repository repo{db};
    auto opened = repo.stream<StreamJob>(typed::select<StreamJob>()
        .where(typed::col<&StreamJob::active> == typed::val(true))
        .order_by(typed::col<&StreamJob::id>).build(),
        {.mode = asterorm::stream_mode::cursor, .chunk_size = 256});
    if (!opened) return std::unexpected(opened.error());
    auto stream = std::move(*opened);
    std::size_t count = 0;
    for (;;) {
        auto row = stream.next();
        if (!row) return std::unexpected(row.error());
        if (!*row) break;
        ++count;
    }
    auto committed = transaction->commit();
    if (!committed) return std::unexpected(committed.error());
    return count;
}
```

SQLite, MySQL, ClickHouse, unknown capabilities and legacy custom facades reject
explicitly. Custom adapters need an explicit PostgreSQL dialect and nonthrowing
connection cleanup/readiness, plus `supports_single_row_stream` / `open_single_row`
or `supports_cursor_stream` / `open_cursor` for the selected strategy. Cursor
readers return owned batches and provide fallible explicit close and nonthrowing
discard on destruction; a dialect fallback is insufficient.

Opening dispatches immediately and pins one connection. Calls to `next()` fetch
and hydrate lazily; no collection query or prepared-result materialization sits
under the stream. The session rejects unrelated work and transaction completion
until EOF or close. A successful EOF retains a caller's transaction pin and permits
connection reuse. Cursor `close()` sends CLOSE, releases buffered rows and retains
the caller transaction when cleanup succeeds. A failed native-transaction admission leaves caller rollback
ownership intact. Scoped advisory/timeout composition rejects; pool timeout defaults
apply to the server query. Cursor DECLARE/FETCH/CLOSE commands use that timeout
separately; this is not a whole-stream deadline. No implicit transaction or retry is added.

Consume through EOF, including the call after the final row, to keep the connection.
Explicit `close()` discards an unfinished single-row connection and returns success
when cleanup completes. Cursor close preserves prior uncommitted writes if
successful. Destruction/move replacement of either unfinished strategy discards
without SQL. Discard can
roll back all earlier uncommitted work on that connection. SQL/decode failures
retain their primary diagnostic on later reads; close is idempotent. A default or
moved-from handle returns local `query_failed` from `next()`. Session/pool must
outlive the handle; the repository and source builder need not.

The observer is snapshotted at opening and emits once on EOF, error or explicit
close. Explicit early close reports local cancellation without a fabricated
SQLSTATE. Normal terminal callbacks run with the reservation pin held. If a parent
guard already invalidated the reservation, the next call reports that invalidation
after its pin has gone. Destruction emits no event and invokes no user callback.
Callback exceptions preserve identity, discard ownership and are never retried;
a secondary observer exception does not replace an existing reader/decoder exception.
Elapsed time sums adapter open/read/close calls, excluding hydration and consumer
pauses. Copy error fields during the callback if retaining them.

Only one consumer may operate a stream at a time; reentrant next/close rejects.
Returned entities own their fields and survive the next read or discard. A later
failure cannot retract previously delivered rows or consumer side effects.

The [streaming contract](../streaming-design.md) records remaining combined-acceptance
work. Each strategy has a separate-process 100k-row test enforcing a 32 MiB RSS
growth budget for its fixed 1 KiB payload fixture; that is not a byte bound for
arbitrary row widths or caller-retained entities. See the
[native reader guide](postgresql-single-row-reader.md) for the lower-level API.
Native cursor users include `<asterorm/postgres/cursor.hpp>` and call
`connection.open_cursor(sql, parameters, chunk_size)` within a transaction;
`cursor_reader::next()` returns `result<optional<pg::result>>` with an owned batch.
Explicit `close()` returns `result<void>`; `abandon()` and destruction discard.
Both native readers follow connection moves and invalidate safely on owner loss.
