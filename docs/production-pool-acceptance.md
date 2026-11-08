# Production connection pool acceptance

Reviewed locally on 2026-11-09. The production pool is **implemented and locally
accepted within the revised v1 contract** in the
[design](production-pool-design.md). This review closes the combined acceptance
work after ownership, bounded health, aging/diagnostics and cache/profile delivery.
No production code change was needed for this review. New composed checks and
fresh verification bind the current implementation; the earlier inventory and
individual delivery captures remain historical evidence.

## Accepted boundary

| Requirement | Accepted v1 behavior and evidence |
| --- | --- |
| Ownership and capacity | Shared state and owned intrusive entries preserve exclusive move-only leases, including return after facade destruction. Return bookkeeping is allocation-free and nonthrowing. Connecting/checking/retiring slots stay reserved until work/cleanup finishes. Sessions and facade member calls keep caller lifetime requirements. Foundation races, allocation denial and native late-lease checks run again. |
| Borrow health | Optional PG-only nonblocking, uncached SELECT 1 checks each handoff, including new lazy connections. A steady deadline covers flush/read/terminal drain. Success restores native mode; timeout/failure closes and returns once. Completed server diagnostics survive. SQLite/MySQL/ClickHouse reject enabled unsupported health before connect. Fourteen real-libpq wire scenarios cover failure/drain/mode/diagnostic boundaries. |
| Dirty returns | PG/SQLite/MySQL close known non-idle transaction/protocol work on return without reset-for-reuse SQL. Native abort cleanup is checked through separate connections. Reader/pipeline/busy-statement checks prevent unsafe reuse. New composed checks combine dirty discard, off caching, health where supported and later idle retirement. |
| Idle and lifetime retirement | The optional stop-aware worker expires only idle entries. Checkout/return also enforce steady age; expiry during setup returns once. Held transactions/streams remain usable and retire on return. No background reconnect/refill, borrower revocation or state copying is added. Fake-clock boundaries, real-worker cleanup controls and new native overdue/close checks pass. |
| Metrics | Explicit state/waiter/overdue gauges, saturating outcome/drop/retirement counters and eight fixed noncumulative acquisition-duration buckets cover completed calls, including errors/exceptions. Capacity timeout and health timeout have separate counters. New composed scenarios assert slot and histogram/outcome conservation. No percentile, fairness or query-latency guarantee is implied. |
| Long-held diagnostics | A generation/timestamp/warned bit produces at most one warning per checkout. Preallocated queue capacity is max_size; overflow increments diagnostics_dropped without retrying that generation. Polling allocation failure preserves records. Only slot/generation/duration/severity are exposed. No application logging callback runs on return, maintenance or destruction. This detects overdue ownership, not proven lost leases. |
| Cache policy | Explicit off/per-connection policy works on PG/SQLite/MySQL; zero capacity remains an off escape hatch. PG bound calls use unnamed parameter execution when off. SQLite/MySQL may use ephemeral native preparation. Observed/repository/typed results and cache counts, plus real-libpq packet checks with a named-Parse negative control, pass. Per-session caching remains deferred. |
| Shutdown and failures | Non-draining close rejects/wakes acquisitions with pool_closed and stops scheduling. Native detached-idle cleanup can still take time. Destruction joins the worker outside the mutex and may await running native cleanup. Held work remains owned; successful late setup is discarded while native failure/exception retains precedence. New combined health/age/profile/close controls verify this ordering. |
| Validation and configuration | Size/duration/enum/capability/profile conflicts reject before eager or lazy connect. Invalid policy/conflict errors are connection_failed; unsupported capabilities are query_failed. Acquire timeout bounds capacity waiting, not synchronous connect or total acquisition. Health has its separate operation deadline. |
| Deployment profile | PG transaction_pool requires effective cache off and absent pool session timeout. Session advisory scopes and serialized migrator entry points reject before SQL; native PG session setters also reject. Local timeout scopes and WITHOUT HOLD cursor work remain transaction-bound. The pinned, configuration-checked proxy fixture below supports only this scoped claim. |
| Public consumption | Fresh tests-disabled installs compile all four literal pool guides and additional composed runtime checks through CMake and shared pkg-config on all three adapters, without Catch2. Thread linkage is exported. The existing guide compilation and full regression targets pass. |

The original three-way cache proposal is explicitly revised to two supported
policies. Per-session ownership/eviction requires a separate design; it is not
aliased to per-connection caching. The original unspecified warning callback is
revised to structured pull delivery. The old ten-second shutdown drain is removed.
Off describes retained caching and the ordinary bound execution path; raw/control
SQL and streaming keep their dedicated protocols.

## Composed checks

The new [core case](../tests/unit/test_pool_acceptance.cpp) runs a blocked health
operation with aging, warning detection, off caching and transaction-profile
metadata enabled. Maintenance retains its reserved slot and produces no held-lease
warning before handoff. Age expiry, close, native health failure and a C++ exception
keep the specified outcome and metric precedence without reconnecting.

The [shared native checks](../tests/support/pool_acceptance.hpp) run on PG, SQLite
and MySQL. An overdue managed transaction stays usable after close, commits and
releases its slot. Capacity expiry and subsequent closure have distinct errors;
health/setup produce no query events or cached entries. A second pool combines
dirty raw-return discard, explicit later replacement and idle retirement with
counter/histogram conservation. SQLite memory is deliberately disposable here.

The [PG composed cases](../tests/integration/test_pool_acceptance.cpp) also keep
both repository single-row and cursor streams open beyond lifetime/warning
thresholds. Pool close preserves their native handles and stream admission gates;
EOF emits one terminal event and leaves the caller's transaction pin intact until
commit. Retirement then releases capacity.

The opt-in proxy fixture pins its image by digest in the
[overlay](../compose.pgbouncer.yaml). Its runner verifies PgBouncer **1.24.1**,
transaction mode, two server slots, round-robin reuse and max_prepared_statements=0.
The existing six-round bound CRUD/local-timeout/cursor matrix runs again. A new
case holds one backend with another client, forcing the original client's next
transaction onto the other backend. A counting driver confirms that original
client connected only once. Bound SQL, local timeout/cursor work, warning delivery,
zero named plans and close with a live transaction continue to work. This is local
acceptance of that exact fixture, not all PgBouncer versions/configurations.

## Verification and migration

[Source-bound evidence](evidence/pool-acceptance/README.md) records fresh full
regression, focused native/no-server controls, allocation denial, both packet
checkers, proxy integration, SQLite and focused PG address/undefined sanitizers,
adapter-disabled ThreadSanitizer, all four installed guide consumers, formatting,
full project lint and the same-date strict standards comparison. The receipt
hashes production/build/test/docs and local logs, plus ignored canonical contexts.
Hashes verify bytes rather than independently attest execution; temporary logs
may expire.

Use the [ownership](how-to/pool-ownership-and-shutdown.md),
[health](how-to/pool-health-and-reuse.md),
[aging/diagnostic](how-to/pool-aging-and-diagnostics.md) and
[cache/profile](how-to/pool-cache-and-profiles.md) guides for migration. Rebuild the
library and consumers together for the accumulated pool/config/stats/entry and
native PG layout/API changes; update exhaustive error switches for pool_closed.
Direct raw lease construction/pool return are removed. Moved/released leases grant
no native access. Do not mutate pooled cache/profile hooks manually or use raw
references concurrently/after return.

SQLite private memory contents disappear with the physical connection; keep
retirement disabled when such a fixture must survive. Arbitrary session SQL,
persistent TEMP state, caller session settings and proxy startup/session behavior
remain caller responsibilities. The profile is not a SQL sandbox or session reset.

The recorded servers report PostgreSQL 16.14 and MySQL 8.4.13. Installed SQLite
reports runtime 3.54.1 while local build/pkg-config metadata reports 3.54.0; the
evidence retains that discrepancy.

Acceptance is scoped to this macOS arm64 library build, configured native servers
and pinned Linux proxy container. Other host platforms, older servers, other proxy
configurations, ClickHouse execution, static package linking, remote CI/release
provenance, performance and hard scheduling/native cleanup deadlines remain
outside this evidence. Existing strict standards findings are retained without a
waiver; local functional acceptance is not release readiness.

Next follow the shared feature order with the **PostgreSQL type coverage inventory
and v1 contract**: reconcile current codecs/query support with planned ranges,
interval/network/hstore types, JSONB operators, composites and array boundaries
before implementing that expansion.
