# Production connection pool: inventory and v1 contract

Reviewed on 2026-11-08. **Inventory and implementation contract complete;
production-pool expansion remains planned.** Current code is
[connection_pool.hpp](../include/asterorm/pool/connection_pool.hpp), with adapter
state/cache behavior in `src/database/{postgres,sqlite,mysql}/connection.cpp`.
[Evidence](evidence/production-pool-inventory/README.md) binds fresh probes,
regression checks and current source hashes. Proposed names below are not available
APIs. The [advanced-query review](advanced-query-acceptance.md) remains historical
acceptance of that separate feature.

## Current behavior and gaps

| Area | Observed current behavior | Required consequence |
| --- | --- | --- |
| Size validation | `min_size=3,max_size=1` warms three connections; a zero maximum on a lazy pool is not a retained configuration error | Validate capacity/durations before dialing, including lazy pools |
| Borrowing | Exclusive move-only lease; reserved capacity while connecting/setup runs outside the mutex; idle vector is LIFO | Retain exclusive ownership and reserved-slot accounting; no fairness guarantee |
| Acquire timeout | Bounds capacity waiting, not synchronous driver connect/setup. A gated connect finishes successfully after the capacity deadline | Preserve this meaning; do not advertise an end-to-end network deadline |
| Health | PG `is_open()` reads libpq status; SQLite/MySQL check a native handle. No on-borrow round trip | A terminated disposable PG backend is still reported open and leased; its first query fails. Optional bounded health validation must precede handoff |
| Transactions/readers | PG reuse hook rejects active readers, but does not require native transaction-idle state. Without an explicit timeout baseline, raw BEGIN survives return/reborrow on all three native adapters | Keep managed transaction/stream pins; drop dirty native returns without committing or silently rolling them back for reuse |
| Timeout baseline | Optional PG session setting applied on warmup/new/idle checkout; active/failed returned transaction rejected on the next configured checkout | Preserve absent/zero/max semantics, setup error precedence and no observer events; account for proxy restrictions separately |
| Statement caching | Size zero already disables PG named preparations and routes `execute_prepared` to `execute_params`/`PQexecParams`. All three adapters retain zero cached entries at size zero, one at size two after repeat execution | Add explicit policy without losing the existing zero-size escape hatch; distinguish native statement preparation from caching on SQLite/MySQL |
| Close | Sets closed and notifies waiters before waiting, so a pending acquisition rejects while another lease is still held. All pool-close errors use `connection_failed`; close itself waits up to ten seconds | Retain prompt waiter rejection; separate it from draining active borrowers and from native connect cancellation |
| Lifetime | Leases retain raw pool pointer plus atomic alive flag; in-flight slot guards reference the pool; close clears idle connections under the mutex | Shared state for leases/jobs; destruction/cleanup outside the pool lock; no check-then-use lifetime dependency |
| Returns | Idle vector insertion may allocate/throw; a bare lease destructor calls it | Destruction/release must be nonthrowing; allocation failure discards safely and frees capacity |
| Aging/diagnostics | No birth/return/checkout timestamps, reaper, waiter gauge, acquisition histogram or lease-duration diagnostic | Add explicit entry states, bounded metrics and structured warning delivery |

These are inventory findings, not failing new production acceptance tests. Native
raw-transaction probes use direct connection APIs, then explicitly roll back;
normal managed-session transaction ownership remains covered by its existing tests.
The alive flag supports sequential late lease destruction today; it does not pin
the raw pool object across a concurrent return/destruction interleaving.

## Public boundary and configuration

Keep `connection_pool<Driver>`, `lease_type`, `acquire()`, `stats()`,
`warmup_error()` and idempotent `close()`. The pool remains noncopyable/nonmovable.
Introduce options with their implementation; do not advertise enabled features
as successful no-ops in earlier slices. Append configuration rather than reinterpret
existing fields:

| Proposed option | Default and rule |
| --- | --- |
| `pre_ping` | `false`; true requires a supported bounded adapter operation |
| `pre_ping_timeout` | `1000ms`; strictly positive when enabled, nonnegative otherwise |
| `idle_timeout` | `0ms` disables; otherwise expire idle entries since their latest return |
| `max_lifetime` | `0ms` disables; otherwise age since successful native connection creation |
| `leak_threshold` | `0ms` disables; otherwise warn once per checkout generation after this held duration |
| `maintenance_interval` | `1000ms`, strictly positive; worker starts only when aging/leak detection is enabled |
| `statement_caching` | `per_connection` or `off`; default per-connection, bounded by existing cache size; size zero still disables caching |
| `profile` | `direct` by default; optional PG-only `transaction_pool` with explicit restrictions below |

Reject `max_size==0`, `min_size>max_size`, negative durations, unsupported enabled
capabilities and conflicting proxy settings before **any** connection attempt.
Use overflow-safe monotonic deadline arithmetic. `acquire_timeout==0` permits an
immediately available idle connection/new slot but never queues. `min_size` remains
a constructor warmup target; retirement may reduce the pool below it, and the next
borrow lazily replaces capacity. There is no background reconnect/refill loop.

Invalid configuration and constructor warmup failure remain retained in
`warmup_error()`; ordinary acquisition/setup failure permits an explicit later
retry. On entry to acquisition, a closed pool returns the closed error even if
warmup had failed. During native work, preserve an operation failure/exception;
if native work succeeds but close wins before handoff, return the closed error.
Append `db_error_kind::pool_closed` without renumbering existing values; use the
existing `timeout` kind for capacity wait expiry. Invalid configuration keeps
`connection_failed` with an option-specific diagnostic; native failures preserve
their kind/SQLSTATE/details. Pool closure is terminal and must not enter session
retry loops. These are intentional error-kind compatibility changes.
Acquisition may still propagate C++ allocation/driver exceptions with its reserved
slot released; constructor warmup keeps its existing retained-diagnostic policy.

`release_to_pool()` and lease destruction become `noexcept`, exactly once across
moves and explicit release. A moved/released lease confers no further connection
access. Keep access through `operator*`/`operator->`. The current direct lease
constructor taking a raw pool/alive token becomes an internal construction path;
users constructing leases manually must migrate to `acquire()`.

## State, shutdown and lock ownership

Store Driver/configuration, entries, counters and synchronization in shared state.
A lease owns its connection/entry identity and a shared return token rather than
a raw pool pointer. It may outlive the pool facade and closes on return after
shutdown. Facades still must outlive calls made on them; concurrent destruction
with a new/in-progress member call is not a supported caller lifetime pattern.
Sessions retain their existing requirement that the pool facade outlive them.

Entries transition through `connecting`, `checking`, `idle`, `leased` and
`retiring`. Native close completes before a retiring entry frees its capacity
slot. Preserve `total = idle + in_use`; `in_use` continues to include reserved
non-idle slots, not only user-held leases. Add separate `leased`, `connecting`,
`checking`, `retiring` and `waiters` gauges so that ambiguity is observable.
Keep `leaked_at_close` for compatibility, but define it as the non-idle slot count
at the first close transition; it no longer implies expiry of the old ten-second
wait or proves a leak. Document this changed snapshot meaning.

Use one mutex for state transitions. Driver connect, health SQL, timeout setup,
native destruction/close and user-facing diagnostic consumption happen outside
it. Detach entries under lock, clean up outside, then retire their slots and wake
waiters. Reserve idle storage during construction where useful, but retain a
nonthrowing discard path for any return/allocation failure. A release cannot lose
its slot or notify twice. Lease return after close does not enqueue idle work.

`close()` marks closed, wakes all capacity waiters, stops new maintenance scheduling
and detaches idle entries. It **does not wait for active leases or an in-flight
connect/ping/setup**. Cleanup of detached idle native handles can take native-close
time outside the mutex; this is not a hard wall-clock close guarantee. A connect
finishing after close discards instead of returning a lease. Generic synchronous
driver connects are not forcibly interrupted; their native connection settings
bound that work. The existing ten-second active-lease wait is removed. Do not
interrupt a transaction/stream already owned by a borrower or close its handle
from a maintenance thread.

The facade owns one optional maintenance `jthread`; it uses weak state access,
stop-aware waiting and no user callbacks. Shutdown requests stop and wakes it;
destruction joins the worker outside the state mutex. Destruction can still await
an already-running native idle-close operation. Avoid a state/worker shared-pointer
cycle, detached threads and self-join. All native background jobs hold explicit
state until their accounting completes. Never call a method on a destroyed pool.
Connection moves/destruction must be nonthrowing; diagnose unsupported throwing
move/destruction types at compile time. Driver connect calls may overlap outside
the state mutex; built-in drivers support this, and custom stateful drivers must
serialize their own shared state. No new guarantee of driver-call serialization
is introduced.

## Health, dirty returns and age

Checkout order: reserve/pop a candidate, reject locally unusable/expired state,
run optional health validation, apply the existing explicit timeout baseline,
recheck closure/age, then hand out exactly one lease. Health/setup consumes the
same reserved slot. A failed health operation closes the candidate and returns
that diagnostic; it does not transparently replay work or start a reconnect loop.
Locally closed or age-expired idle entries may be swept before choosing a candidate.

V1 bounded pre-ping is **PostgreSQL-only**. SQLite/MySQL/ClickHouse reject an enabled
option before dialing until their bounded implementations are separately added;
disabled health checking preserves their normal operation. Keep the optional
adapter capability vendor-neutral (for example `pool_ping(deadline) -> result<void>`).
PG runs one uncached SELECT 1 on an idle, reader-free connection. Its deadline
covers flush/read/result-drain, using nonblocking libpq and readiness waits; timeout
closes the disposable connection rather than waiting for cancellation cleanup.
Success drains the final null result and restores native mode before handoff.
No query observer event or statement-cache entry is generated. Preserve server
diagnostics on a completed failure; a client-side ping deadline uses `timeout`
with no invented SQLSTATE. Check health before every handoff, including new lazy
connections. Constructor warmup retains its existing timeout setup, with health
checked when that warmed connection is first borrowed. Local `PQstatus` alone is
not this health check.
[libpq asynchronous processing](https://www.postgresql.org/docs/16/libpq-async.html)
documents send/read/result-drain ownership; this bounded operation must respect it.

Built-in PG/SQLite/MySQL adapters provide nonthrowing local reuse/close capabilities
and reject known non-idle transactions or unread native work on **return**, even
with no timeout baseline. They close/discard; they never commit, reset, or roll
back and silently reuse dirty work. Custom legacy adapters without a readiness
capability retain the documented `is_open` fallback; strict built-in guarantees
must not be claimed for them. Do not add native I/O to a local readiness predicate.

The worker expires only idle connections. Over-age leased connections are marked
for retirement and close on return; streams/transactions are never revoked.
Perform the same age check on checkout/return so worker scheduling delay cannot
hand an already-expired idle entry to a borrower. Expiry while checking retires
that candidate before handoff. Use steady-clock timestamps and an internal fake
clock/maintenance seam for deterministic boundary tests. No wall-clock scheduling,
background health SQL, min-size refill or native automatic reconnect is added.
If a candidate ages out during its check/setup, discard and return a descriptive
`connection_failed` checkout-expiry error rather than repeatedly creating
connections that cannot survive their own setup time.

SQLite private `:memory:` data belongs to the physical connection. Opting into
retirement/replacement can destroy that database even with `min_size=max_size=1`;
the warmup target does not pin it. Keep age retirement disabled for private
in-memory fixtures that require persistence, and use file-backed or explicitly
disposable fixtures for aging tests. No schema/data copy is performed on replacement.

## Metrics and leak warnings

Extend the snapshot with the state gauges above, monotonic saturating counters
for acquisition success/wait timeout/connection failure/health failure/retirement,
and a fixed histogram of acquisition-call duration. Include successful and failed
calls, with outcome counters separate. Proposed bucket upper bounds are 1, 5, 10,
50, 100, 500 and 1000 milliseconds plus an overflow bucket. This measures the
library call, including connect/setup when present; it is not query latency or a
promised deadline. `waiters` counts threads actually in capacity waits and returns
to zero on timeout, exception or shutdown. No FIFO scheduling promise is made.

Leak detection means **long-held**, not proven lost. Maintain a lease generation,
checkout timestamp and warned bit without touching the borrower's connection.
Expose an `overdue_leases` gauge and one warning event per checkout generation.
Use a bounded queue (at most `max_size` outstanding warning records), with a
`diagnostics_dropped` counter on overflow and a `take_diagnostics()` polling API.
Events contain only slot/generation/duration/severity, never connection strings,
SQL, parameter values, native pointers or entity data. Polling may allocate and
report allocation failure to its caller; pool/lease destruction cannot throw.
Applications explicitly consume warnings and route them to logs. This revises the
proposal's unspecified “warn” into structured pull delivery: no global logger,
worker callback, stderr output or borrower revocation. Counters remain available
if warning records are not consumed. Worker shutdown never invokes application code.

## Caching and PgBouncer boundary

Implement `off` and `per_connection` for v1. Off bypasses retained cached statements
and PG named preparation, including ordinary observed/repository/typed paths;
streams continue their existing uncached protocols. SQLite/MySQL may still need
native ephemeral preparation to bind values. Cache size remains the physical
connection limit; no double source of truth or silent mode alias is introduced.

**Per-session caching is deferred**, rather than aliased to per-connection mode.
A normal session releases between operations and may acquire a different physical
connection. A server statement name cannot migrate with that C++ session. Pinning
every session would change pool capacity/transaction behavior, while partitioning
every physical cache by logical owner needs a separate eviction/lifetime contract.
The original three-mode proposal is therefore not a v1 completion claim.

The explicit PG transaction-pool profile requires effective cache `off` (off mode
or zero cache capacity) and an absent pool
session `statement_timeout`; reject conflicting options before dialing. It rejects
library-managed session advisory scopes and migrations relying on session-level
advisory serialization before their SQL. Scoped SET LOCAL timeout inside a managed
transaction and WITHOUT HOLD cursor use stay within that transaction. Profile
metadata must reach session/connection admission checks through optional capability
dispatch; generic headers must contain no libpq calls or proxy SQL parser.

Off alone does **not** make arbitrary session features proxy-safe. PgBouncer returns
server connections at transaction boundaries; its feature matrix restricts SET/
RESET, session advisory locks and persistent TEMP state in transaction pooling.
Modern protocol prepared-plan support depends on nonzero proxy
`max_prepared_statements`, while SQL-level DEALLOCATE handling differs. The
conservative off profile avoids relying on this machinery; it does not assert all
PgBouncer installations reject named protocol preparations. See the authoritative
[feature matrix](https://www.pgbouncer.org/features.html) and
[prepared-statement configuration](https://www.pgbouncer.org/config.html#max_prepared_statements).
Raw SQL remains the caller's responsibility; no SQL sandbox is promised.

Actual proxy acceptance requires an isolated version-pinned PgBouncer service in
transaction mode, with named-plan support disabled, and repeated explicit
transactions on multiple backing connections. Check bound CRUD, no retained
named preparations, supported SET LOCAL/cursor use and prohibited high-level
session features. Current direct-server probes establish none of that integration.
No proxy is present in the current compose configuration; adding the profile alone
must not be labeled PgBouncer acceptance.

## Delivery and acceptance order

| Delivery | Required checks before completion |
| --- | --- |
| **Next: ownership/configuration/shutdown foundation** | Shared state/entries and move-safe noexcept lease return; eager capacity/duration validation; closed/timeout error semantics; close wakes waiters without waiting for borrowers; completion after close discards safely. Deterministic blocked-connect/return/close races, allocation failure, late leases, repeated close and unchanged session/stream/testing ownership; all native adapters and installed consumers |
| Bounded health and dirty-return handling | Native PG stale-backend replacement/rejection; successful and timed-out ping fully drained/discarded; no mutex-held I/O, retry or observer/cache pollution; PG/SQLite/MySQL dirty returns/readers discarded; unsupported pre-ping rejected before connect |
| Aging and diagnostics | Fake-clock checkout/return/reaper boundaries, over-age held lease retention, stop/join/shutdown races; queue gauges and histogram/counter invariants, warning once per generation, bounded warning overflow and no callbacks on destruction |
| Cache/profile policy | Off/per-connection native results/cache counts on all three; PG parameterized off path, proxy admission and migration/advisory rejection; actual isolated PgBouncer transaction-mode acceptance before compatibility claims |
| Combined local acceptance | Fresh full regression, sanitizer/race coverage where supported, build/format/lint/standards comparison, source-bound evidence, installed CMake/pkg-config consumers, public guide/migration notes and explicit remaining limitations |

Rebuild library and consumers when implementation changes pool/config/stats/lease/
adapter layouts or error enums. Preserve the umbrella CMake target and pkg-config
dependency contract; export required thread linkage for public header workers.
No implementation or ABI changes occur in this inventory step. The next action
is the **ownership/configuration/shutdown foundation**. Health validation follows
that lifetime work.
