# SQL DSL benchmark

Hypothesis: typed and string query construction/compilation produce identical SQL
and parameters without a material increase in runtime cost or allocations. This
benchmark measures the client-side DSL boundary only. Decoding, database queries,
network latency, pool behavior, and compile-time cost remain outside its scope.
Do not use these measurements to claim equivalent end-to-end query performance.

## Build and run

```sh
meson setup buildBenchmark --buildtype=release --default-library=static \
  -Db_lto=false -Dbenchmarks=true -Dtests=false -Dexamples=false \
  -Dpostgres=disabled -Dsqlite=disabled -Dmysql=disabled -Dclickhouse=disabled
meson compile -C buildBenchmark
meson test -C buildBenchmark --print-errorlogs
python3 benchmarks/run_dsl_benchmark.py --build-root buildBenchmark \
  --output benchmarks/results/my-run
```

The output directory must not already exist. The runner records three processes,
21 paired samples per case, and 10,000 operations per timing sample by default.
Use `--runs`, `--samples`, and `--iterations` to increase these counts. Raw samples,
allocation counts, compiler/build settings, machine details, source hashes, and
a summary table are preserved in the output directory. Run on an otherwise idle
machine; repeat on other platforms before making portable performance claims.

The [2026-11-06 typed/string baseline](results/typed-vs-string-dsl-2026-11-06/ANALYSIS.md) records observed
construction overhead and its allocation causes. It does not establish M8's
unchanged-runtime-cost criterion.

The [vector reservation results](results/typed-dsl-vector-reserve-2026-11-06/ANALYSIS.md)
retain the same benchmark and remove the measured growth allocations. Simple and
aggregate allocation counts/bytes match the string baseline; nullable projections
still request more bytes because of their AST representation.

The [plain-column storage results](results/typed-dsl-column-storage-2026-11-06/ANALYSIS.md)
remove that remaining excess for the measured nullable projection. Built-in
plain projections use the compact column list; aggregates and custom lowering
retain select items. All tested cases fall within the original timing/allocation
investigation screen, without establishing broader runtime equivalence.

The opt-in Meson option does not add timing thresholds to the normal test suite.
The optimized build registers two fast correctness checks with Meson, including
SQL/parameter parity for 32 inputs across three workloads and both dialects, and
allocation-probe calibration. Neither needs a database or Catch2.

## Measurement contract

- Workloads: whole-entity SELECT with an integer filter; inferred nullable tuple
  projection with boolean, text and NULL predicates; grouped aggregates.
- Phases: build/destroy the AST; compile/destroy SQL and parameters from a prebuilt
  AST; build plus compile plus destruction. The compile phase reuses input 0;
  other phases vary inputs. `postgres_sqlite` denotes the default SQL compiler
  dialect shared by PostgreSQL and SQLite; `mysql` selects the other dialect.
  Construction itself is dialect-independent: the two build-phase labels repeat
  the same builder workload rather than exercise backend-specific construction.
- Baseline: the public string builder creates the same SQL as the public typed
  builder. It uses legacy column lists for plain columns and select items for
  aggregates. Representation differences are part of the comparison.
- Warmup: 1,000 operations per API/phase/workload/dialect before samples. Each
  sample measures both APIs, reversing their order on alternating samples and
  process seeds. An opaque observation call in a separate translation unit keeps
  complete results visible to the compiler. LTO is disabled; its common call
  overhead is included in both paths.
- Timing and allocation counting are separate executables. The timing program
  uses the standard allocator. Only `dsl_allocations` replaces global C++ `new`
  (scalar/array, aligned/unaligned, throwing/nothrow). Counts cover successful
  calls and requested bytes, including the statically linked core. They exclude
  direct `malloc`, allocator metadata, peak/live memory, and OS allocations.
- Reporting: median nanoseconds per operation and median/IQR of paired
  typed/string ratios across all processes, plus allocation calls/bytes per
  operation. IQR is dispersion, not a confidence interval.
- Before measurement, define material differences as a paired median ratio above
  1.10 in every process, or extra allocations/requested bytes. These are signals
  for investigation, not timing-based test failures or proof of equivalence.

Investigate differences in construction separately from compilation. For example,
different AST vectors or capacity growth can change allocation counts even when
SQL output is identical. Preserve the measured baseline and raw data when
considering a later optimization; do not silently redefine the comparison.

`dsl_benchmark --describe-asts` records vector sizes/capacities and element sizes
to help interpret allocation differences. The runner saves this as `ast_layouts.csv`.
