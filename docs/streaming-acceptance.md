# Streaming acceptance

Reviewed on 2026-11-07 against the Git base and source hashes in
[metadata.json](evidence/streaming-acceptance/metadata.json).
**Outcome: locally accepted within the revised v1 contract.** This covers the
portable pull API, both PostgreSQL transport strategies, repository hydration,
session ownership, terminal observation and the specified memory fixtures.
The [contract](streaming-design.md) retains the inventory and implementation history;
the [guide](how-to/stream-entities.md) describes the delivered API.

## Requirement-to-evidence matrix

| Requirement | Implementation and executable evidence | Accepted boundary |
| --- | --- | --- |
| Lazy mapped-entity reads with late errors | [Public handle](../include/asterorm/row_stream.hpp), [core model](../include/asterorm/core/row_stream.hpp), [repository](../include/asterorm/core/repository.hpp) and [unit cases](../tests/unit/test_repository_stream.cpp) check owned rows, moves, temporary repositories/builders, typed-source lowering, decoder exceptions and errors after delivered rows | Move-only `result<row_stream<T>>`; `next()` returns `result<optional<T>>`. Previously delivered rows and external consumer side effects cannot be retracted |
| Validate before acquiring a lease | Repository unit cases check full mapped SELECT shape, table/column order, duplicate/unknown names, invalid nodes/options, parameter codecs/NULs, explicit capabilities and depth/node boundaries | One mapped table, all bare mapped columns in mapping order, mapped WHERE/order and PostgreSQL LIMIT/OFFSET. No aliases, joins, grouping, projections or row locking. Predicate depth 64 / 1024 nodes |
| Native PostgreSQL single-row transport | [Adapter unit cases](../tests/unit/test_postgres_single_row.cpp), [native live cases](../tests/integration/test_single_row.cpp) and [adapter implementation](../src/database/postgres/connection.cpp) check parameters, one owned tuple per read, terminal empty result/final null, empty/100k-row scans, protocol rejection and retained diagnostics | `PQsendQueryParams` followed immediately by single-row activation. No materializing or prepared executor beneath the stream; autocommit or healthy caller transaction |
| Declared-cursor batches | [Cursor live cases](../tests/integration/test_cursor.cpp) check quoted private NO SCROLL / WITHOUT HOLD portals, bound declaration, chunks 1/2/3/7/256/65536, full/tail/empty fetch, portal disappearance and owned results | Existing healthy managed transaction required by repository admission, with native readiness checked before dispatch. One native batch at most `chunk_size`, released before another fetch; no hidden BEGIN or holdable cursor |
| Exclusive stream ownership and safe reuse | [Reservation](../include/asterorm/core/stream_reservation.hpp), [session](../include/asterorm/core/session.hpp), [pool](../include/asterorm/pool/connection_pool.hpp) and [ownership tests](../tests/unit/test_stream_reservation.cpp) check independent pins/generations, moves, deferred release, capacity return, stale handles and competing sessions | Ordinary work, second streams, transaction completion, savepoints and advisory/timeout scopes reject while active. Pool/session must outlive the handle. Single consumer; reentrant stream calls reject |
| EOF and explicit cleanup | [Repository live cases](../tests/integration/test_repository_stream.cpp) and native tests check same-connection/cache reuse, retained transactions and prior writes, empty/tail completion, idempotent close and injected CLOSE failure | Clean EOF retains a caller transaction. Successful cursor close sends CLOSE and retains prior writes. Early single-row close discards; SQL/decode/cleanup failures conservatively discard and invalidate associated transactions |
| Quiet abandonment, including parent destruction | Ownership/core/native/repository cases check stream/connection moves, owner loss, guard destruction/replacement and no destructor SQL or observer callbacks. The new `[streaming-acceptance]` live case observes both strategies from an independent connection | Unfinished destruction discards without drain, cancel round trip, retry or callback. It can roll back all prior uncommitted work. The fixture observes backend disappearance and transaction-lock availability within five seconds; this observation deadline is not a public server-release guarantee |
| Errors, timeouts and lost connections | Native/repository cases cover division errors after delivered rows/batches, rich SQLSTATE fields, malformed data, declaration/CLOSE failure, failed-transaction admission, pool timeout `57014` and backend termination | Preserve primary structured diagnostics and C++ exception identity; no automatic replay. A socket failure may have no server SQLSTATE. Failed-transaction/readiness rejection retains caller rollback ownership. Pool timeouts apply per native command, not to the entire stream |
| One terminal SELECT observation | Core/repository cases check opening snapshots, EOF/error/explicit-close events, callback exceptions, observer replacement of the active handle, invalidation and no event for local rejection/destruction | Event delivered at most once and marked delivered before callback. Normal callbacks hold the stream pin; a parent-invalidated stream reports after pin loss. Internal DECLARE/FETCH/CLOSE produce no extra events. Elapsed time excludes hydration and consumer pauses; error pointers are callback-scoped |
| Bounded client memory with a materialization control | [RSS fixtures](../tests/integration/test_repository_stream.cpp), [fresh-process runner](../tests/check_repository_stream_memory.py) and [measurements](evidence/streaming-acceptance/memory.json) exercise actual repository streaming and `find_all`, checksum IDs/payloads and include 64 KiB values | The 32 MiB growth gate applies to each 100k-row/1 KiB streaming fixture. Buffered control must exceed it. Large-row streams use a separate anti-materialization sanity gate, below half of their 256 MiB payload. Neither gate is an arbitrary-row-width or server-memory promise |
| Public packaging and backend boundaries | Both guide snippets compile in the suite. The [consumer probe](evidence/streaming-acceptance/check_consumers.py) freshly installs enabled PostgreSQL/SQLite/MySQL libraries and compiles/runs both guide functions through CMake and shared pkg-config | PostgreSQL 16+ only for streaming; explicit independent strategy capabilities. SQLite/MySQL/ClickHouse/unknown and legacy facades reject before leasing unless explicitly opted into the PostgreSQL protocol. No live ClickHouse claim |
| Existing APIs remain usable | Fresh PostgreSQL/SQLite/MySQL suites pass, including ordinary CRUD, transactions, typed queries, migrations, locking and timeout behavior | Materializing APIs retain their existing behavior. New streaming reads bypass the prepared cache; no asynchronous, pipeline or coroutine scheduler API is introduced |

## Recorded v1 revisions

1. The original `std::generator<T>` return type is replaced by portable
   `row_stream<T>` so an SQL/decode error after earlier rows has an explicit error
   channel. Fresh [configured toolchain probes](evidence/streaming-acceptance/toolchain.json)
   compile coroutines (exit 0) but cannot include `<generator>` (exit 1). An optional
   feature-probed generator adapter remains unimplemented and is not an acceptance requirement.
2. The original proposal's two “backends” are PostgreSQL transport strategies,
   rather than two database providers. Cursor mode requires a caller-managed
   transaction; neither strategy inserts implicit transaction boundaries.
3. Dropping a reader discards its physical connection rather than waiting for
   cancel/drain or issuing CLOSE in a destructor. Explicit cursor close is the
   path for retaining a healthy transaction; early single-row close discards.
4. Observation is one terminal event after dispatch, with quiet destruction and
   the parent-invalidation exception described above. It does not reuse the
   synchronous materializing execution helper.

The cleanup observation uses PostgreSQL's
[`pg_stat_activity`](https://www.postgresql.org/docs/16/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW)
and [transaction advisory lock functions](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)
from separate autocommit monitor queries. It first verifies that the subject owns
the lock, then checks both backend exit and lock availability after discard. It
does not infer server cleanup solely from local pool statistics or add a hard
cleanup deadline to the API.

## Memory observations

Each measurement runs in its own process on macOS arm64 with Apple Clang 21,
libc++ and the standard allocator, against PostgreSQL 16.14 / libpq 18.6.
The baseline is `getrusage(RUSAGE_SELF).ru_maxrss` after connection/view setup and
BEGIN; the peak is the same process high-water mark after consumption. Darwin
bytes and Linux KiB are normalized to bytes. The fixture maps integer ID, nullable
text and boolean fields; payload width below refers to the text field, not total
native/C++ row overhead. Only the buffered control retains a result collection.

| Repository path | Rows × payload | Native row bound | Baseline bytes | Peak bytes | Growth MiB |
| --- | --- | --- | --- | --- | --- |
| Single-row | 100,000 × 1 KiB | 1 | 21,151,744 | 21,512,192 | 0.3438 |
| Cursor | 100,000 × 1 KiB | 256 | 21,266,432 | 22,020,096 | 0.7188 |
| Buffered `find_all` control | 100,000 × 1 KiB | Whole result | 21,200,896 | 311,443,456 | 276.7969 |
| Single-row, larger values | 4,096 × 64 KiB | 1 | 21,266,432 | 22,511,616 | 1.1875 |
| Cursor, larger values | 4,096 × 64 KiB | 64 | 21,102,592 | 29,261,824 | 7.7812 |

The positive control shows that this fixture detects repository materialization.
The larger-value observations show why a native row-count bound is not a fixed
byte bound. They are local client-memory results, not throughput comparisons.
Server plans, libpq buffers, allocator/platform changes, individual huge rows and
caller-retained entities can change memory usage.

## Fresh verification

| Command/check | Observed result |
| --- | --- |
| `just test` | `Ok: 19`, `Fail: 0`; core 160 cases / 2026 assertions, PostgreSQL adapter 7 / 138, ordinary live PostgreSQL 69 / 102490, plus SQLite/MySQL regressions, both compiled guides and five fresh-process memory scenarios |
| `buildDir/tests/pg_integration '[streaming-acceptance]'` | All six strategy/ending combinations pass; independent backend/lock cleanup observation and pool recovery |
| `meson compile -C buildDir`, `just format-check`, `just lint` | Exit 0; lint covers 56 translation units. New fixture diagnostics corrected; final targeted lint and the five-process memory matrix pass, with existing diagnostics retained |
| `python3 docs/evidence/streaming-acceptance/check_consumers.py` | Fresh temporary-prefix build/install; CMake and shared pkg-config programs run both guide paths, check two rows and one pinned SELECT event per path, and verify no private cursor portal remains |
| `python3 docs/evidence/streaming-acceptance/check_toolchain.py --build-root buildDir --json-out <receipt>` | Probe runner exit 0; coroutine compilation 0, optional generator compilation 1 (missing header) |
| Strict standards validator (`--mode strict-governance --workspace --today 2026-11-07`) | Exit 1: unchanged 53 errors, 12 warnings, 0 infos; no findings in the edited canonical contexts |

[Provenance and reproduction](evidence/streaming-acceptance/README.md) bind reviewed
source/test/build files, selected environment facts, documents and fresh receipts.
The historical native inventory remains unchanged. Connection settings and
credentials are omitted from published evidence.

## Limits and next work

This is local acceptance of the revised v1 streaming contract. Remote CI, release
readiness, all platforms/server versions, sanitizer runs, static linking and live
ClickHouse behavior are not established. The pre-existing standards findings are
not waived. No performance or memory guarantee applies beyond the stated fixtures.

Next follow the plan with the **transactional test harness inventory and contract**:
check existing transaction/savepoint behavior, Catch2 opt-in integration, reference
data lifetime and fixture isolation before implementing a harness or migrating tests.
