# Streaming cursors: inventory and implementation contract

Reviewed on 2026-11-07. **Session ownership foundation implemented; native readers
and repository streaming remain unimplemented.** This document defines the v1
boundary and remaining delivery sequence. It does not establish feature acceptance.
The [native/toolchain evidence](evidence/streaming-inventory/README.md) freezes the
pre-implementation observations; the follow-up below records current ownership code.

## Current implementation

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

Use a portable move-only pull stream as the required v1 surface. Proposed names
below are design targets, not current declarations:

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
materializing `execute_prepared`. Initial single-row dispatch may bypass the
prepared cache; cached/asynchronous preparation is a later optimization, with no
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

Plan one logical SELECT `query_event` on normal EOF, terminal failure, or explicit
close after dispatch, while the stream pin is still held. Internal DECLARE/FETCH/
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
| PostgreSQL single-row reader + repository hydration — **next** | Compiler/adapter unit cases without a server, malformed/codec/source admission, partial and empty results, late SQL/decode errors, native status ownership, observer exceptions, zero hidden materialization/retry; live full/early/failing scans and idle-pool recovery |
| Declared cursor strategy | Existing transaction required, parameterized declaration, exact batch bounds/tail/empty fetch, CLOSE and transaction retention, failed CLOSE, destructor abandonment, stale guards and backend capability rejection |
| Timeout/cancellation and release review | Pool defaults, `57014`, deliberate read error, loss of connection, destructor discard, no leaked portal/session after eventual server cleanup, fresh no-server + PostgreSQL/SQLite/MySQL regression checks |
| Memory and public contract | Separate-process **repository** scans of 100k fixed-width rows, incremental consumption with no retained collection, max native batches 1/N, peak RSS growth at most 32 MiB over the recorded post-connect baseline for the 1 KiB fixture; include buffered control and larger-row case. Compile/run installed consumers and public examples; feature-probe optional generator facade before claiming it |

The 32 MiB growth budget is proposed for the fixed-schema acceptance fixture and
must be reported with compiler, allocator/platform, actual row width and baseline.
If it fails, diagnose and revise with evidence rather than silently disabling it.
It is not a universal row-width-independent promise. The feature remains partial
until its remaining runtime slices and combined review close; native probes alone satisfy
none of the production ownership, decoder, observer or destructor criteria.

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

Next implement the PostgreSQL single-row adapter reader, then connect it to owned
per-row repository hydration and terminal observation. `row_stream<T>`, native
single-tuple reading, cursor execution and the repository `stream` method are still
absent. Preserve the historical native capture rather than rewriting its hashes.
