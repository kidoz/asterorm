# Typed DSL plain-column storage results

Plain projections made entirely of built-in `typed_col` items now use
`select_ast::columns`. Aggregate, mixed, aliased, and custom selectable projections
keep `select_ast::items`. The measured nullable projection no longer requests
extra allocation bytes relative to the public string builder.

The optimization recognizes exact built-in column types at compile time. It does
not infer plainness from the presence of a `column_name()` method: a derived or
custom selectable can expose that method while its `to_select_item()` supplies a
function or alias. Such custom lowering remains intact. Result tuple/DTO types
and hydration continue to use the original item typelist.

## Same measurement protocol

The [original baseline](../typed-vs-string-dsl-2026-11-06/ANALYSIS.md) and
[vector reservation results](../typed-dsl-vector-reserve-2026-11-06/ANALYSIS.md)
are retained. The benchmark source, inputs, build settings, and sampling protocol
are unchanged; only `include/asterorm/sql/typed.hpp` differs among the measured
source hashes compared with the vector-reservation run.

Apple M4 Max, Apple Clang 21.0.0, C++23, `-O3`, static core, LTO disabled;
three processes, 21 paired samples per case/process, 10,000 operations per sample.
Allocation counts use a separate executable, with 256 operations per case. Raw
artifacts: [timings](timings.csv), [allocation counts](allocations.csv),
[AST layouts](ast_layouts.csv), [metadata/source hashes](metadata.json), and the
[generated report](report.md). Reproduce with the [benchmark instructions](../../README.md)
and a new output directory.

## Allocation result

All values are per construction operation. Requested bytes count successful C++
`new` calls, not live/peak memory or RSS. Both dialect-labelled build runs have
the same allocation counts.

| Workload | Typed after reservation: calls / bytes | Typed with column storage: calls / bytes | String: calls / bytes |
|---|---:|---:|---:|
| Simple filter | 1 / 96 | 1 / 96 | 1 / 96 |
| Nullable projection | 8 / 2,664 | 8 / 2,496 | 9 / 2,536 |
| Aggregate | 8 / 1,368 | 8 / 1,368 | 8 / 1,368 |

For the three-column nullable projection, vector storage changes from three
80-byte `select_item` elements to three 24-byte strings on this toolchain:
**168 fewer requested bytes/query**. The previous 128-byte excess becomes a
40-byte saving relative to the string workload, which makes an extra copy of its
long text parameter through the expression operators. No parameter-copy
optimization was added here. Allocation calls are unchanged by this storage
change; the earlier reserve optimization already removed growth calls.

Prebuilt compilation counts/bytes still match the string path in every case.
Construction plus compilation for the nullable projection now uses 18 calls /
3,928 bytes, versus 19 / 3,968 for the string path. Simple and aggregate workloads
retain matching allocation counts/bytes throughout.

## Timing result and limits

The current median paired typed/string ratios are:

| Workload | Build | Build + compile | Compile prebuilt AST |
|---|---:|---:|---:|
| Simple filter | 0.833–0.839 | 0.949–0.968 | 1.000–1.001 |
| Nullable projection | 0.829–0.833 | 0.910–0.912 | 1.002–1.004 |
| Aggregate | 0.862–0.871 | 0.930–0.931 | 1.035–1.038 |

Ranges span the two compiler dialect labels. Construction is dialect-independent;
its labels repeat the same builder workload. Each timing sample pairs typed and
string APIs within a process. The older result sets were measured at different
times, so cross-version absolute timings are not a simultaneous experiment.

All measured cases fall within the predeclared investigation screen: no extra
typed allocation calls/bytes and no repeatable slowdown above 10%. That is evidence
for these client-side workloads on this machine, not a statistical equivalence
proof or a portable runtime guarantee. The report retains ratio dispersion.

Database execution, networking, row decoding, pool behavior, and compiler
compile-time cost are not measured. Broader decoding/equivalence evidence and the
incremental migration guide remain separate feature-contract work.

## Verification

- `meson test -C buildBenchmark --print-errorlogs`: `Ok: 2`, `Fail: 0`, including
  SQL/parameter parity for 192 input/dialect/workload pairs and probe calibration.
- `python3 benchmarks/run_dsl_benchmark.py --build-root buildBenchmark --output benchmarks/results/typed-dsl-column-storage-2026-11-06`:
  all three processes completed and all 2,268 timing samples were positive.
- `just test`: `Ok: 6`, `Fail: 0`, including PostgreSQL, SQLite, MySQL and compile checks.
- New unit cases preserve custom derived-column aliases, function aliases, and
  projection order for mixed columns/aggregates. Existing tests cover inferred
  types, explicit DTOs, SQL/parameter equivalence and backend hydration.
- `just format-check`: exit 0.
- `just lint` and `meson compile -C buildBenchmark clang-tidy`: both exited 0,
  with warnings.
- Final measured-source hashes match the retained metadata; compiler, settings,
  machine, inputs and sample counts match the preceding run. Both earlier result
  directories remain unchanged in Git.
