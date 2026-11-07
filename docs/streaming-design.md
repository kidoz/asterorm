# Streaming cursors: inventory and implementation contract

Reviewed on 2026-11-07. **Both PostgreSQL streaming strategies are locally accepted
within the revised v1 contract.** The [acceptance review](streaming-acceptance.md)
records current source-bound evidence and limits. This document retains the v1
boundary, inventory and implementation history.
The [native/toolchain evidence](evidence/streaming-inventory/README.md) freezes the
pre-implementation observations; the follow-up below records current ownership code.

## Inventory before streaming implementation

| Area | Observed code | Consequence |
| --- | --- | --- |
| Repository reads | [repository.hpp](../include/asterorm/core/repository.hpp) hydrates collections into `vector<T>`; `find_all` delegates to the materialized native path | An iterator over the resulting vector would still allocate the entire result |
| Native/typed reads | [session.hpp](../include/asterorm/core/session.hpp) and [typed_query.hpp](../include/asterorm/sql/typed_query.hpp) collect rows after execution | Keep these APIs compatible; introduce a separate streaming path |
| PostgreSQL adapter | [connection.cpp](../src/database/postgres/connection.cpp) uses `PQexec`, `PQexecParams` and `PQexecPrepared`; [result factory](../src/database/postgres/result.cpp) admits command/tuple completion statuses | A native reader must handle `PGRES_SINGLE_TUPLE` separately and distinguish rows, terminal success, failure and protocol completion |
| Other adapters | SQLite steps into owned vectors, MySQL stores/fetches results into owned vectors, and ClickHouse accumulates result blocks | These are not implementations of either planned PostgreSQL strategy; do not simulate streaming by slicing their vectors |
| Lease/session lifetime | [Pool](../include/asterorm/pool/connection_pool.hpp) leases move connections back on release. Session scopes pin a lease, but `with_connection` still permits another callback against it | Add stream ownership independently of transaction ownership; the existing callback lifetime cannot represent an open reader |
| Transactions/scopes | Session transaction generations, advisory/timeout pins and poisoning already exist | Stream admission, transaction completion and exceptional abandonment must compose with these gates |
| Hydration | [hydrate_entity](../include/asterorm/core/hydration.hpp) decodes one row into a caller-provided entity | Reuse one-row decoding into `T{}`; never call a collection API from the reader |
| Observation | [query_event](../include/asterorm/core/observer.hpp) describes synchronous dispatch-to-response; `observed_execute` returns a complete result | Streaming needs a terminal observation path; calling the current executor would defeat streaming |
| Toolchain | C++23 is configured, but the actual Apple Clang 21 toolchain compiles `<coroutine>` and fails `<generator>` with “file not found” | Do not add a mandatory `<generator>` dependency to installed core headers |

The [libc++ status page](https://libcxx.llvm.org/Status/Cxx23.html) also leaves
P2502's implementation/release fields blank as of this review. The compile probe,
rather than the language-standard flag or compiler version alone, establishes
availability on this host. The Ubuntu CI configuration was inspected, not executed
or treated as proof of its standard-library capabilities.

## Reconcile the original generator proposal

The plan proposes `repository::stream<T>(query, chunk_size)` returning
`std::generator<T>`. Two issues require an explicit revision: the configured
library lacks that header, and an SQL/decode failure may arrive after some rows
have already been consumed. An outer `result<generator<T>>` cannot carry that
later failure. The standard generator is a coroutine view, not a database error
channel ([C++ draft](https://eel.is/c++draft/coro.generator)).

Use a portable move-only pull stream as the required v1 surface. These declarations
are implemented for single-row and cursor modes:

```cpp
enum class stream_mode { single_row, cursor };
struct stream_options {
    stream_mode mode = stream_mode::single_row;
    std::size_t chunk_size = 256;
};

// repository<Session> member:
template <typename T>
result<row_stream<T>> stream(sql::select_ast query, stream_options options = {});

// row_stream<T>: noncopyable, movable, single consumer
result<std::optional<T>> next(); // value, clean EOF, or terminal db_error
result<void> close();           // explicit, idempotent cleanup
```

An optional, separately included `std::generator<result<T>>` adapter can later wrap
this primitive on toolchains that compile the actual feature probe. It yields a
late error once and stops; a plain generator of `T` would instead require a stated
exception policy. Neither adapter nor a private replacement added to namespace
`std` is required for the first portable implementation. This revises the original
exact return type; do not report that original criterion delivered unchanged.

Creation validates and compiles, reserves the session/lease, then starts the native
operation. Rows are fetched/decoded only by `next()`. Dispatch can therefore occur
before the first `next()`; creating a stream already makes the session busy.
Compilation/admission/acquisition failures return from `stream`; SQL errors may
return either there or from `next`, depending on when the server reports them.
No rows means successful EOF. After EOF/explicit closure, `next()` returns EOF
without SQL; after failure it retains the primary error without retrying. Moving
transfers ownership once; a moved-from `next()` returns a local `query_failed`.

## Query and backend boundary

This shape policy applies to the repository API. The delivered native
adapter accepts caller-owned SQL as described in its guide.

Initially accept a full mapped-entity `select_ast`: its table and explicit column
projection must match `entity_traits<T>` in mapping order. No wildcard, aliases,
joins, function projections, grouping/HAVING or row locking. Ordering uses mapped
bare columns; LIMIT/OFFSET retain PostgreSQL semantics. WHERE accepts mapped bare
columns/encoded parameters, comparisons, AND/OR and NULL tests. Validate malformed
nodes, codec errors and a depth-64/node-1024 predicate budget before acquisition.
This keeps hydration positional and predictable. Runtime source SQL types still
belong to the backend. A typed whole-entity SELECT can lower through `build()`;
DTO/tuple projection streams and typed assignment work are separate extensions.

PostgreSQL 16+ is the initial backend. Both strategies below are **PostgreSQL
transport strategies**, not two database adapters. SQLite, MySQL, ClickHouse and
unknown custom capabilities reject with `query_failed` before leasing. Opt-in
requires an explicit reader capability/protocol; the ordinary dialect fallback
does not imply streaming support. Invalid local shapes/options use `parse_failed`;
codec and native diagnostics retain their identity and available SQLSTATEs.

| Property | Single-row strategy | Declared-cursor strategy |
| --- | --- | --- |
| Dispatch | `PQsendQueryParams`, immediately followed by `PQsetSingleRowMode` | Parameterized `DECLARE <private name> NO SCROLL CURSOR WITHOUT HOLD FOR <compiled SELECT>` |
| Iteration | Own/release each single-tuple result; require empty terminal `PGRES_TUPLES_OK` and final null result | `FETCH FORWARD N` into one batch, decode incrementally, release before the next fetch; empty batch is EOF |
| Transactions | Autocommit or an existing healthy managed transaction | Existing healthy managed transaction required; no hidden BEGIN, commit or holdable cursor |
| Batch bound | One native row at a time; no decoded prefetch | At most `chunk_size` native rows and one decoded entity |
| Options | Validate `1 <= chunk_size <= 65536`; it does not change single-row granularity | Same bound, used as the FETCH row limit; it is a row count, not a byte budget |
| Normal completion | Consume through final null before release | CLOSE the private cursor, then release the stream pin; caller transaction stays active |

The adapter owns quoting of generated cursor names and renders only validated
numeric fetch sizes. SELECT parameters stay in their original vector; values
never become SQL fragments. Do not route either reader through the existing
materializing `execute_prepared`. Native single-row dispatch bypasses the prepared
cache; cached/asynchronous preparation is a later optimization, with no
automatic replay after any row has been exposed.

PostgreSQL documents the activation order, empty completion result and possible
rows-before-error behavior in [single-row mode](https://www.postgresql.org/docs/16/libpq-single-row-mode.html).
[Asynchronous processing](https://www.postgresql.org/docs/16/libpq-async.html)
requires reading through null before another command on that connection. This
protocol is used by a synchronous pull API here; it does not introduce background
threads, pipeline mode or an async public API. The [DECLARE rules](https://www.postgresql.org/docs/16/sql-declare.html)
require a transaction for a non-holdable cursor; holdable cursors can materialize
their contents and are excluded. [FETCH](https://www.postgresql.org/docs/16/sql-fetch.html)
supplies the requested batch limit, not a fixed number of bytes or a server-memory bound.

## Ownership and session gates

A session-owned stream control record carries a unique generation, independent
stream pin and cancellation state. The row stream holds its move-only ownership
token and reader state, not a reference to a stack-local lease or result. Keep
backend implementation behind the adapter; `row_stream<T>` can use a private
type-erased reader without exposing libpq in generic core headers.

Only one stream is active per session. Before invoking callbacks or obtaining
another lease, ordinary native/typed/repository work, `with_connection`, direct
observed execution, BEGIN, advisory/timeout admission, savepoints and explicit
transaction commit/rollback reject while that stream is active. Use the existing
typed `db_error` (`query_failed`, no fabricated SQLSTATE), not a new error enum.
The existing void-callback fallback may throw `logic_error`, consistent with the
session API. Read-only status/statistics and observer configuration remain usable.
Independent sessions on the same pool continue normally, subject to capacity.

The stream's internal read/close path alone carries authority to use its reserved
connection. Normal completion releases the stream pin while retaining a caller's
transaction pin. Locks protect each transition; no session mutex stays locked
between `next()` calls or across consumer processing. Concurrent/reentrant `next`
is unsupported; ordinary work must not silently interleave.

For initial v1, stream creation inside advisory or scoped-timeout callbacks rejects
before dispatch, and such scopes cannot start during streaming. Pool timeout
baselines still apply. This prevents a reader escaping a callback with a live
scope pin/override; broader scope composition needs a separate lifetime design.
Explicit transaction guard destruction while a stream lives needs a dedicated
nonthrowing abandonment path: discard/invalidate the reader and transaction,
rather than ignore a busy error and leave an ownerless transaction. A subsequent
`next()` reports invalidation without dereferencing a released connection.

Session and pool must outlive their stream, as with existing borrowed repository
and transaction handles. Destroying a repository alone is safe because the stream
owns its reader/query/parameters. Row values returned by `next()` own their decoded
fields; no result-buffer views survive release of a native row or batch.

## Cleanup, errors and observation

| Outcome | Required behavior |
| --- | --- |
| Clean EOF | Verify native completion/close cursor; release stream pin and busy state; retain the same healthy connection and any caller transaction |
| Explicit early cursor `close()` | Free buffered rows, issue CLOSE on the reserved connection, release stream ownership; keep the caller transaction if cleanup succeeds |
| Explicit early single-row `close()` | Discard the physical connection; do not drain an arbitrarily long query. Invalidate an associated transaction and stale guards |
| Destructor abandonment of either strategy | Nonthrowing discard of an unfinished reader's connection, release stream ownership and invalidate transaction generation; no SQL, drain, cancel round trip or user callback in the destructor |
| SQL/decode/protocol failure or cursor cleanup failure | Preserve the first structured diagnostic, discard/invalidate conservatively in initial v1, release ownership, and never retry. No connection with unread results or an owned portal returns to idle reuse |

Early discard may roll back **all** earlier uncommitted work on that connection.
It is not a savepoint rollback limited to the SELECT. Consumers needing to retain
a cursor transaction must call `close()` successfully or consume through EOF.
Already delivered rows cannot be retracted when a later error arrives; consumers
must arrange their own atomicity for external side effects.

The native probe demonstrates graceful cancel/drain and reuse on this host, but
does not establish a bounded cleanup deadline. [PQcancel](https://www.postgresql.org/docs/16/libpq-cancel.html)
reports request dispatch, not guaranteed cancellation. Initial destructor cleanup
therefore closes the connection instead of depending on cancellation/draining.
The server releases session resources after detecting disconnection; no hard
server-side release deadline is promised. Normal reads and explicit cursor CLOSE
can block; pool/statement timeouts are not an end-to-end stream deadline.

Emit one logical SELECT `query_event` on normal EOF, terminal failure, or explicit
close after dispatch. Normal terminal callbacks run while the stream pin is held.
If parent-guard abandonment already invalidated the reservation, its next read or
close reports invalidation after pin loss; a stale reservation cannot affect a
new generation opened by that observer. Internal DECLARE/FETCH/
CLOSE commands produce no extra query events. Local preflight rejection produces
none. Snapshot the observer at stream creation; mark the event delivered before
invoking it so a throwing callback is never retried. Exceptions preserve identity
and cleanup still releases/discards ownership. Destructor abandonment invokes no
observer; explicit close is required for its terminal event. Document this
stream-specific exception to the existing synchronous event contract. Elapsed
time accumulates dispatch/read/close waits, excluding hydration and time spent
between consumer calls; consumer think time is not query latency. An explicit
early-close event carries a local `query_failed` cancellation diagnostic without
an invented SQLSTATE; a genuine server cancellation retains `57014`.

## Evidence and acceptance plan

The reproducible [capture](evidence/streaming-inventory/capture.py) compiled with
configured C++23 flags and exited 0 against PostgreSQL 16.14/libpq 18.6. It verifies
seven single-row results plus one empty terminal result, four rows before `22012`,
bound cursor declaration, batches `3,3,1,0`, CLOSE removing the cursor, and native
cancel/drain yielding `57014` followed by connection reuse. Coroutine compile exit
was 0; `<generator>` compile exit was 1 (the expected inventory finding).

Each scan ran in its own process with 100,000 rows of 1,024-byte payload:

| Native strategy | Largest result in rows | Peak RSS growth above post-connect baseline |
| --- | --- | --- |
| Buffered control | 100,000 | 134.33 MiB |
| Single-row | 1 | 0.09 MiB |
| Cursor, chunk 256 | 256 | 0.45 MiB |

These are local native feasibility measurements, not an ORM memory guarantee or
throughput benchmark. Large individual rows, libpq I/O buffers, server plans and
caller-retained entities remain outside the row-count bound.

| Delivery | Required executable acceptance |
| --- | --- |
| Session ownership foundation — **implemented** | [No-server ownership cases](../tests/unit/test_stream_reservation.cpp): one lease, independent transaction/stream pins, busy gates, second stream rejection, moves, stale generations, guard destruction, completion, failure/discard and capacity return. Ordinary SQLite/MySQL paths remain green |
| PostgreSQL single-row native reader — **implemented** | [Adapter unit cases](../tests/unit/test_postgres_single_row.cpp) and [live cases](../tests/integration/test_single_row.cpp): checked single-tuple ownership, parameters, empty/100k-row/late-error reads, diagnostics, moves, abandonment, timeout/loss and reuse; [guide](how-to/postgresql-single-row-reader.md) compiles in the suite |
| Repository hydration + terminal observation — **implemented** | [No-server cases](../tests/unit/test_repository_stream.cpp), [live cases and separate-process memory fixtures](../tests/integration/test_repository_stream.cpp), and [compiled public guide](how-to/stream-entities.md): explicit backend/query admission, bounded predicates, owned hydration, typed-source lowering, observer/exception lifecycle, real session-pool integration without a materializing executor |
| Declared cursor strategy — **implemented** | [Native cursor tests](../tests/integration/test_cursor.cpp) and [repository tests](../tests/integration/test_repository_stream.cpp): existing transaction required, parameterized declaration, batch bounds/tail/empty fetch, CLOSE and prior-write retention, failed CLOSE, quiet abandonment, stale guards and independent backend capability rejection |
| Timeout/cancellation and release review — **locally accepted** | [Combined review](streaming-acceptance.md): pool defaults, `57014`, deliberate read error, connection loss, quiet discard, independently observed backend/transaction-lock cleanup and fresh three-backend regressions |
| Memory and public contract — **locally accepted** | [Combined review](streaming-acceptance.md): five fresh-process repository scenarios, 100k-row/1 KiB streaming gates, buffered control, 64 KiB payload cases, compiled/installed public guide paths and fresh optional-generator feature probe |

The 32 MiB growth budget is proposed for the fixed-schema acceptance fixture and
must be reported with compiler, allocator/platform, actual row width and baseline.
If it fails, diagnose and revise with evidence rather than silently disabling it.
It is not a universal row-width-independent promise. The combined review now
closes these scoped local criteria; native probes alone never established
production ownership, decoder, observer or destructor acceptance.

## Verification of this review

| Command | Observed result |
| --- | --- |
| `python3 docs/evidence/streaming-inventory/capture.py --build-root buildDir` | Compile/native probes exit 0; coroutine compile 0, generator compile 1 (missing header, recorded finding) |
| `just test` | `Ok: 14`, `Fail: 0`, including PostgreSQL/SQLite/MySQL and compiled guides |
| `meson compile -C buildDir` | Exit 0; no work remaining |
| `just format-check` | Exit 0, including the new native probe |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: 53 errors and 12 warnings, identical to the pre-review baseline; no findings in the three edited canonical contexts |

That inventory review changed documentation and probes only. Its results remain
historical and do not test the proposed streaming API. The implementation below
has its own verification. No remote CI, full streaming acceptance or release
conclusion is claimed.

## Implementation follow-up: session stream ownership (2026-11-07)

[stream_reservation.hpp](../include/asterorm/core/stream_reservation.hpp) adds a
move-only internal reservation entered through `detail::session_stream_access`.
It advertises no backend streaming capability and sends no SQL. A connection must
offer nonthrowing close before admission can acquire a lease. The future reader
path must validate backend/query admission and enforce native protocol completion.

[session.hpp](../include/asterorm/core/session.hpp) now owns an independent stream
generation and pin. A reservation shares the existing transaction lease or takes
one lease itself. Its internal callback is the only authorized connection path;
ordinary session/native/typed/repository work, another reservation, transaction
completion, savepoints, advisory and timeout admission reject while it is active.
Reservation from an ordinary connection callback/observer or scoped callback also
rejects, so no nested operation silently borrows another connection. Mutexes are
held per transition/callback, not between reads. Observer configuration and status
remain usable.

`complete()` is an internal assertion that the future reader has already reached
a clean native terminal state or closed its cursor. It releases the stream pin
while retaining a caller transaction. No native EOF or cursor cleanup is implemented
by this method. `discard()`/destruction closes the physical connection, releases
capacity and invalidates transaction ownership without SQL or observer callbacks.
The reservation and pool/session lifetime precondition remains as specified above.

Callback errors preserve the primary `db_error`; callback exceptions preserve
identity. Both discard. Transaction-guard destruction or move replacement with a
live reservation uses the abandonment path rather than a blocked rollback. Stale
reservations/guards cannot act on a fresh generation. If abandonment occurs inside
the authorized callback, the closed connection object and busy gate remain until
that callback unwinds, preventing a dangling reference or premature pool reuse.

The [15 ownership test cases](../tests/unit/test_stream_reservation.cpp) use fake
connections and the real pool/session. They cover gates with zero SQL/events,
independent sessions, move-only results, normal transaction-pin retention, error/
exception identity, stale handles, guard destruction/replacement, deferred release,
acquisition failure, cleanup-capability rejection, callback/observer exception
recovery and cross-thread busy rejection between reads. Session layout changed;
rebuild library and consumers together.

Fresh verification of the ownership implementation:

| Command | Observed result |
| --- | --- |
| `buildDir/tests/core_tests '[stream]'` | 252 assertions passed in 15 test cases, without a database server |
| `just test` | `Ok: 14`, `Fail: 0`, including PostgreSQL/SQLite/MySQL regression paths |
| `meson compile -C buildDir` and `just format-check` | Exit 0 |
| `just lint` | Exit 0 across 51 translation units |
| `python3 docs/evidence/bulk-predicate-dml-acceptance/check_consumers.py` | Fresh temporary-prefix build/install; CMake and shared pkg-config consumers compile/run with the new installed header |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: unchanged 53 errors and 12 warnings; no findings on edited canonical contexts |

New fixture style diagnostics from the full lint run were corrected. Final targeted
lint also exited 0, retaining a diagnostic originating in Catch2's enum-flag macro:

```sh
clang-tidy -p buildDir --extra-arg=-isysroot \
  --extra-arg=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk \
  tests/unit/test_stream_reservation.cpp
```

At this point the native reader and repository streaming were still absent. The
native reader is delivered in the follow-up below. Preserve the historical native
capture rather than rewriting its hashes.

## Implementation follow-up: PostgreSQL single-row native reader (2026-11-07)

`pg::connection::open_single_row(sql, parameters)` now returns the move-only
[`pg::single_row_reader`](../include/asterorm/postgres/single_row.hpp). Its synchronous
`next()` returns an owned one-row `pg::result`, clean EOF, or a retained structured
error. The [guide](how-to/postgresql-single-row-reader.md) distinguishes this native
escape hatch from the repository shape/typing contract implemented below.

The implementation stays in the existing PostgreSQL adapter `.cpp` boundary.
Buffers and a shared ownership record are allocated before dispatch; it calls
`PQsendQueryParams` and immediately activates single-row mode. Every single-tuple
result passes a dedicated checked factory; ordinary `result::from_native` still
rejects that status. Empty terminal tuples, final null and a healthy protocol state
are required before reuse. This path bypasses the prepared cache and does not
collect rows, hydrate entities, emit query events, retry or start transactions.

The connection rejects competing execution/COPY/cache-control operations while
active. Reader moves transfer ownership, connection moves update the shared record,
and connection destruction invalidates it before releasing the native handle.
Closing or destroying an unfinished reader discards its connection without a drain
or SQL. SQL/protocol failures preserve primary diagnostics and discard; returned
row handles survive that discard. Failed-transaction admission rejects without
taking over caller rollback. The connection layout changed: rebuild consumers.

The optional pool reuse protocol requires a nonthrowing boolean readiness check
and nonthrowing close. PostgreSQL reports unread streams as non-reusable. Discard
closes outside the pool mutex before capacity is freed, including explicit
`release_to_pool()` while the lease object survives. Tests cover both a reader
escaped from an unreserved callback and an explicitly released active lease.
Adapters without the opt-in retain the existing open-connection reuse contract.

Session integration uses the ownership reservation from the previous delivery:
read callbacks run through its authority, EOF is followed by reservation completion,
and failure/transaction abandonment invalidates both layers. At this point repository
`row_stream<T>` construction, per-row hydration, terminal observation and declared
cursor batching were still absent. These were not closed by native test results.

Fresh adapter tests pass without a server (3 cases, 41 assertions). Thirteen live
PostgreSQL cases pass (100,198 assertions), including parameter/null/text binding,
exactly one row per result across 100k rows, empty completion, four rows before a
late error, full diagnostic fields, unchanged cache/PID after clean EOF, competing
operation rejection, early close/destruction, reader/connection moves, owner loss,
failed-transaction admission, reservation pins, explicit release and callback
escape, `57014` and backend termination. The guide has a dedicated server-free
syntax-check target. Native API scans here do not establish repository RSS bounds;
the separate-process memory acceptance remains outstanding.

| Fresh command/check | Observed result |
| --- | --- |
| `buildDir/tests/postgres_unit_tests '[pg-single-row-unit]'` | 3 cases, 41 assertions passed without a server |
| `buildDir/tests/pg_integration '[single-row]'` | 13 cases, 100,198 assertions passed |
| `just test` | `Ok: 15`, `Fail: 0`, including PostgreSQL/SQLite/MySQL regressions and the native guide compile target |
| `meson compile -C buildDir`, `just format-check`, `just lint` | Exit 0; lint covered 53 translation units, with existing diagnostics retained |
| Fresh temporary-prefix build/install and CMake/shared pkg-config consumers | Exit 0 for existing mutation consumers and the native guide; both installed guide programs emitted `1,2,3` and reached reusable EOF |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: 53 errors and 12 warnings, identical to the previous ownership baseline; no findings on edited canonical contexts |

These are local results, without a remote-CI, sanitizer, all-platform or release
claim. The existing governance findings are not waived. Historical inventory and
prior acceptance captures remain unchanged.

At that point repository streaming was still absent. The follow-up below delivers
the portable single-row bridge; declared-cursor batching and combined feature
acceptance remain open.

## Implementation follow-up: repository single-row streams (2026-11-07)

The public [`row_stream<T>` facade](../include/asterorm/row_stream.hpp) and
[`repository::stream<T>`](../include/asterorm/core/repository.hpp) now implement
the pull interface above. The [guide](how-to/stream-entities.md) is compiled as a
server-free test. Runtime and typed whole-entity sources share mapped SELECT and
depth-64/node-1024 predicate validation before leasing. Unsupported capabilities,
cursor mode, malformed shapes, codec errors and text NULs reject locally. The
native capability includes a nonthrowing readiness check; a failed-transaction
admission retains caller rollback ownership and emits no event. Observer snapshot
copy failure also releases the new stream pin without dispatch or transaction loss.

The stream owns SQL, parameters, observer and native reader. Each `next()` validates
native row width/names and reuses shared entity hydration into an owned `T{}`;
there is no collection executor or decoded prefetch. EOF completes the reservation;
SQL/decode failures preserve their first diagnostic and discard. Explicit early
close reports local cancellation once and discards; destructor/move replacement
discards without invoking user code. Shared internal state keeps a terminal call
alive if its observer replaces or destroys the public handle. Reentrant calls
reject; only a single consumer is supported. Exceptions retain their identity,
including an original reader/decoder exception when reporting it also throws.

Terminal events use the opening observer snapshot and mark delivery before calling
it. Ordinary EOF/error/close callbacks retain the pin; already-invalidated parent
ownership follows the exception documented above. Adapter-call elapsed time excludes
hydration and consumer pauses. The error pointer is callback-scoped.

Fresh focused unit tests pass 250 assertions in 15 cases. Six ordinary live cases
pass 112 assertions, covering typed rows/NULLs, reuse and transaction retention,
codec failure, rows before `22012`, observer exceptions, explicit/destructor close,
failed-transaction admission, scope rejection and pool-default timeout `57014`.
The separate-process hidden case incrementally consumes 100,000 rows with a 1,024-byte
text field and two scalar fields, retaining no collection (100,009 assertions).
On this macOS arm64 / Apple Clang 21 / system-allocator host its last measured peak
RSS growth above the post-connect/view-setup baseline was 327,680 bytes (0.3125 MiB),
under the fixture's 32 MiB bound. Native batch size is one. This is a local fixture
result, not a universal row-size or server-memory guarantee. Repository cursor
bounds, buffered control and larger-row measurements remain combined-acceptance work.

| Fresh command/check | Observed result |
| --- | --- |
| `buildDir/tests/core_tests '[repository-stream]'` | 15 cases, 250 assertions passed without a server, including the final fixture cleanup |
| `buildDir/tests/pg_integration '[repository-stream]'` | 6 cases, 112 assertions passed |
| `python3 tests/check_repository_stream_memory.py buildDir/tests/pg_integration` | Fresh process, 100,009 assertions passed; RSS growth 327,680 bytes |
| `just test` | `Ok: 17`, `Fail: 0`, including PostgreSQL/SQLite/MySQL, both compiled guides and the fresh-process memory target |
| `meson compile -C buildDir`, `just format-check` | Exit 0 |
| `just lint` | Exit 0 across 55 translation units; existing diagnostics retained. New fixture diagnostics corrected; final targeted lint exits 0 with only Catch2's existing enum-flag diagnostic |
| Fresh temporary-prefix build/install and CMake/shared pkg-config consumers | Existing mutation consumers pass; both installed entity-guide programs return two mapped rows, emit one pinned terminal event and release the lease at EOF |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: 53 errors, 12 warnings, 0 infos; JSON report exactly matches the previous native-reader baseline, including no findings on edited canonical contexts |

These checks are local. No remote-CI, sanitizer, cross-platform or release result
is implied, and existing governance findings remain open. At this point cursor
batches were the next implementation; they are delivered in the follow-up below.

## Implementation follow-up: declared-cursor batches (2026-11-07)

[`pg::cursor_reader`](../include/asterorm/postgres/cursor.hpp) now owns a private
quoted NO SCROLL / WITHOUT HOLD portal. `open_cursor` binds the original SELECT
parameters to DECLARE; each `next()` performs a validated numeric FETCH into one
owned batch of at most `chunk_size` rows. An empty batch triggers CLOSE and EOF;
short batches are consumed before that final empty fetch. Normal EOF and successful
explicit close retain the healthy caller transaction. Destructor/move replacement
of an unfinished reader discards without SQL. Declaration/fetch/close failures
retain the primary diagnostic and discard conservatively. A disconnected backend
can produce either the server SQLSTATE or a libpq socket diagnostic; no SQLSTATE
is fabricated. Both readers share connection move/invalidation and reuse gates.

The generic entity stream selects an explicitly opted-in transport, owns only one
native batch and decodes its rows incrementally using the existing hydrator. The
batch is freed before another fetch or explicit close. Cursor admission requires a
managed transaction before acquiring any new lease, plus native healthy-transaction
readiness before dispatch. Single-row and cursor capability flags are independent.
Repository source validation, terminal observation, exception precedence and parent
invalidation apply to both modes. Internal DECLARE/FETCH/CLOSE commands emit no
extra events. Pool statement timeouts apply separately to each native command;
there is no automatic transaction, retry or whole-stream deadline.

Native tests exercise chunk sizes 1, 2, 3, 7, 256 and 65536, parameters/NULLs,
empty/tail/full batches, NO SCROLL / WITHOUT HOLD, portal disappearance, cache/PID
retention, early close, failed CLOSE, late division errors, declaration failure,
connection/reader moves, owner loss and backend termination. Repository cases
verify prior writes survive EOF/explicit close, busy gates, no-transaction and
failed-transaction admission, decode/late errors, timeout, quiet abandonment,
parent-guard destruction and one terminal event. No-server fake transports also
verify no native fetch between rows of a batch, independent capabilities and
cleanup/observer exception precedence.

Each strategy now has its own process for the 100k-row fixture with 1,024-byte text
plus integer/boolean fields and no retained collection. The last full-suite run
on macOS arm64 / Apple Clang 21 / system allocator recorded:

| Strategy | Native row bound | Post-connect/setup RSS baseline | Peak RSS | Growth |
| --- | --- | --- | --- | --- |
| Single-row | 1 | 21,250,048 bytes | 21,577,728 bytes | 327,680 bytes |
| Cursor | 256 | 21,217,280 bytes | 21,954,560 bytes | 737,280 bytes |

Both passed 100,011 assertions and the 32 MiB fixture growth bound. This does not
bound arbitrary row widths, caller retention or server memory. The next step is
combined acceptance: add repository buffered-control and larger-row measurements,
reconcile the remaining runtime/cleanup matrix and publish the source-bound local
review. Historical native inventory captures remain unchanged.

| Fresh command/check | Observed result |
| --- | --- |
| `buildDir/tests/core_tests '[repository-cursor],[repository-stream]'` | 21 cases, 388 assertions pass; cursor-only filter passes 6 cases / 138 assertions |
| `buildDir/tests/postgres_unit_tests '[pg-cursor-unit]'` | 1 case, 19 assertions pass without a server |
| `buildDir/tests/pg_integration '[cursor],[repository-cursor]'` | 16 cases, 692 assertions pass; final native-only `[cursor]` check also passes 10 cases / 464 assertions after fixture cleanup |
| `just test` | `Ok: 18`, `Fail: 0`, including PostgreSQL/SQLite/MySQL regression paths, both guides and both fresh-process RSS targets |
| `meson compile -C buildDir`, `just format-check` | Exit 0 |
| `just lint` | Exit 0 across 56 translation units; new diagnostics corrected and final targeted checks pass. Existing adapter style, typed-DSL analyzer and Catch2 diagnostics retained |
| Fresh temporary-prefix build/install and CMake/shared pkg-config consumers | Both guide functions compile/run, return two rows, emit one pinned SELECT event each and leave no private cursor portal after EOF |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: 53 errors, 12 warnings, 0 infos; JSON report exactly matches the previous repository-stream baseline, with no findings on the edited canonical contexts |

These are local checks, without remote-CI, sanitizer, all-platform or release
claims. Governance findings remain open; combined streaming acceptance is the
next delivery.

## Combined local acceptance (2026-11-07)

The [acceptance review](streaming-acceptance.md) reconciles the original criteria
with the delivered portable pull API, two PostgreSQL transport strategies and
conservative cleanup/observation policy. Five fresh-process repository scenarios
now include the buffered control and 64 KiB text payloads. An independent monitor
observes backend disappearance and transaction-lock release after abandonment,
parent-guard destruction and late errors in both modes. The fixture's observation
deadline does not become a public server-cleanup guarantee.

Fresh `just test` reports `Ok: 19`, `Fail: 0`; both public guide functions compile
and run from fresh CMake/pkg-config installations. The configured coroutine probe
passes; optional `<generator>` remains absent. Source, test, document and receipt
hashes are bound in the [evidence package](evidence/streaming-acceptance/README.md).
The runtime implementation is unchanged by this acceptance work. Existing strict
standards findings remain open; no remote-CI or release conclusion is implied.

Next follow the plan with transactional test harness inventory and contract work.
