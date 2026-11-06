# Typed versus string projection row decoding

The [2026-11-06 results](results/row-decoding-2026-11-06/ANALYSIS.md) retain the
raw samples and interpretation for PostgreSQL, SQLite and MySQL.

This benchmark compares the actual tuple decoding helper used by typed queries
(`sql::typed::detail::hydrate_dto`) with the helper used by repository string
projections (`detail::decode_projection`). Both return exactly the same tuple
types and use the shared codecs. It measures the decoding layer, rather than
the complete public query call.

## Build and reproduce

```sh
meson setup buildRowDecode --buildtype=release --default-library=static \
  -Db_lto=false -Dbenchmarks=true -Dtests=false -Dexamples=false \
  -Dpostgres=enabled -Dsqlite=enabled -Dmysql=enabled -Dclickhouse=disabled
meson compile -C buildRowDecode
meson test -C buildRowDecode --print-errorlogs
python3 benchmarks/run_row_decode_benchmark.py --build-root buildRowDecode \
  --output benchmarks/results/my-row-decoding-run
```

The normal PostgreSQL, SQLite and MySQL client build dependencies must be
available. No database servers, connections or credentials are needed by these
executables. The output directory must be new. Existing construction/compiler
benchmarks remain available; the row-decoding targets are built when all three
adapters and `benchmarks` are enabled.

## Boundary and fixtures

- Both APIs decode the same immutable native result object within each paired
  sample. PostgreSQL uses an owned `PGresult` inside `pg::result`; SQLite and
  MySQL use their real materialized result classes. Fixture creation and
  validation occur before timing/counting and the fixture stays alive throughout.
- PostgreSQL fixtures use `PQmakeEmptyPGresult`, `PQsetResultAttrs` and
  `PQsetvalue`. The latter APIs copy input descriptors/values; RAII transfers
  cleanup to `pg::result`. See the official
  [libpq result construction documentation](https://www.postgresql.org/docs/18/libpq-misc.html).
- The data is synthetic text at the adapter result boundary, not output captured
  from executed SQL. PostgreSQL boolean fields use `t`/`f`, SQLite/MySQL use `1`/`0`.
  Numeric/average text variants decode to identical expected values. The decimal
  wrapper preserves a long exact text value, including trailing scale; this is
  not evidence of SQLite/MySQL/PostgreSQL arithmetic or aggregate precision.
- Shapes: integer/string/boolean/double; nullable integer/string/double plus a
  required boolean; aggregate projected types (`COUNT`, integral `SUM`, floating
  `AVG`, exact decimal `numeric`). NULL and empty strings remain distinct.
- Result sizes are 1, 256 and 16,384 rows. Empty results and error cases are
  correctness checks outside the timed workloads. Invalid required fields must
  return matching `parse_failed` errors and messages; successful rows must match
  both the other API and a fixture value oracle, with the expected row count.

## Measurement protocol

The hypothesis is no material typed decoding overhead for equivalent tuple
projections. Before measurement, use the same investigation screen as the SQL
benchmark: paired median typed/string ratio above 1.10 in every process, or
extra C++ allocation calls/requested bytes. This is an investigation rule, not a
timing-based CI assertion or proof of equivalence.

`row_decode` decodes, observes and destroys one tuple at a time. `result_vector`
reserves a vector, decodes/appends every row and observes/destroys the resulting
vector, matching the collection pattern of both public projection paths. Both
paths use the same collection loop; only the production per-row decoder differs.
SQL construction, execution, shape validation, observers around query execution,
session/pool overhead and result acquisition are outside the measured boundary.

Timing uses the standard allocator. A separate executable counts successful C++
`new` calls and requested bytes through the existing calibrated allocation probe.
Fixture allocations, direct C `malloc`, allocator bookkeeping and peak/RSS memory
are excluded. The statically linked adapters' C++ allocations are counted. An
out-of-line observation barrier and disabled LTO keep decoded values observable.

Default timing: three sequential processes, 21 paired samples per case and 8,192
target decoded rows per batch. Batch repetitions are
`max(1, floor(target_rows / rows_per_result))`; therefore large cases process a
whole 16,384-row result. Warmup similarly uses at least 1,024 decoded rows or one
whole result. API order alternates by sample and process seed. The separate
allocation pass targets 256 rows, again processing at least one whole result.

Raw CSV records actual decoded-row counts. Reporting includes median ns/row,
paired ratios and IQR, allocations/row and bytes/row, split by backend, shape,
result size and phase. IQR is sample dispersion, not a confidence interval.
Metadata records compiler/build settings, machine, source hashes and verification
output. Do not generalize one-machine helper measurements to full query latency,
server/network behavior, all DTO mappings, error-path performance or portable
runtime equivalence.
